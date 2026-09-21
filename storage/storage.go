// Package storage 提供分帧校验、批次原子提交、崩溃恢复与归档压缩的本地存储。
//
// 文件布局（工作目录/batches/ 下，每个批次两个文件）：
//
//	<batchID>.meta  JSON 批次头（temp+rename 原子落盘）
//	<batchID>.data  分帧记录 + 提交尾标
//
// 数据帧：magic "BIOS"(4) | type(1) | payloadLen(4 BE) | payload | crc32(payload)(4)
// 提交尾标：magic "EOT!"(4) | recordCount(8 BE) | frameBytes(8 BE) | crc32(全部数据帧字节)(4)
//
// 只有带合法尾标的批次才算完整；崩溃残留的不完整批次在打开时被隔离为
// .incomplete 文件，任何读取路径都不会把它与新批次静默拼接。
package storage

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	// ErrSealed 批次已封板，拒绝再写。
	ErrSealed = errors.New("storage: batch already sealed")
	// ErrIncompleteBatch 批次未完整提交，不能作为数据读出。
	ErrIncompleteBatch = errors.New("storage: batch is incomplete (crash before commit) and is quarantined")
	// ErrCorrupt 帧校验或尾标校验失败。
	ErrCorrupt = errors.New("storage: corrupt frame or commit trailer")
	// ErrRecordTooLarge 单条记录超过上限。
	ErrRecordTooLarge = errors.New("storage: record exceeds 16MiB limit")
	// ErrArchived 批次已压缩归档，拒绝写。
	ErrArchived = errors.New("storage: batch already archived")
)

// RecordType 记录类型。
type RecordType byte

// 批次内记录类型。
const (
	RecordSample     RecordType = 1
	RecordGap        RecordType = 2
	RecordConclusion RecordType = 3
)

const maxRecordPayload = 16 << 20

var (
	frameMagic  = [4]byte{'B', 'I', 'O', 'S'}
	commitMagic [4]byte
)

// Record 是一条带类型的不透明负载记录（负载为 JSON）。
type Record struct {
	Type    RecordType
	Payload []byte
}

// BatchHeader 是批次元信息。
type BatchHeader struct {
	BatchID   string    `json:"batch_id"`
	SessionID string    `json:"session_id"`
	StartedAt time.Time `json:"started_at"`
}

// BatchInfo 描述一个批次的可观测状态。
type BatchInfo struct {
	Header   BatchHeader
	Sealed   bool
	Archived bool
	Complete bool
}

// Batch 是一个正在写入或已封板的数据批次。
type Batch struct {
	store  *Store
	header BatchHeader

	mu       sync.Mutex
	f        *os.File
	sealed   bool
	archived bool
	count    uint64
	dataHash uint32 // 已写全部帧的滚动 crc32（IEEE）
}

// Header 返回批次头。
func (b *Batch) Header() BatchHeader { return b.header }

// Mu 返回批次互斥锁，供编排层协调封板条件。
func (b *Batch) Mu() *sync.Mutex { return &b.mu }

// Count 返回已追加记录数（调用方须持有或自行处理并发）。
func (b *Batch) Count() int { return int(b.count) }

func init() {
	// 提交尾标 magic：E,O,T,!
	commitMagic = [4]byte{'E', 'O', 'T', '!'}
}

// Append 向开放批次追加一条分帧记录；并发安全，单帧一次写入，不会出现半条记录。
func (b *Batch) Append(ctx context.Context, rec Record) error {
	if len(rec.Payload) > maxRecordPayload {
		return ErrRecordTooLarge
	}
	frame := make([]byte, 0, 4+1+4+len(rec.Payload)+4)
	frame = append(frame, frameMagic[:]...)
	frame = append(frame, byte(rec.Type))
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(rec.Payload)))
	frame = append(frame, lenBuf[:]...)
	frame = append(frame, rec.Payload...)
	crc := crc32.ChecksumIEEE(rec.Payload)
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc)
	frame = append(frame, crcBuf[:]...)

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.archived {
		return ErrArchived
	}
	if b.sealed {
		return ErrSealed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Write 对普通文件通常一次性接受全部字节；用 full write 保证整帧落盘。
	if _, err := io.Copy(b.f, bytes.NewReader(frame)); err != nil {
		return fmt.Errorf("storage: append frame: %w", err)
	}
	b.count++
	b.dataHash = crc32.Update(b.dataHash, crc32.IEEETable, frame)
	return nil
}

// Seal 原子提交批次：刷盘后写提交尾标并 fsync；此后才可被读取。
func (b *Batch) Seal(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.f.Sync(); err != nil {
		return fmt.Errorf("storage: sync before commit: %w", err)
	}

	var trailer [24]byte
	copy(trailer[0:4], commitMagic[:])
	binary.BigEndian.PutUint64(trailer[4:12], b.count)
	// dataLen 需要由文件当前大小得到。
	pos, err := b.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint64(trailer[12:20], uint64(pos)) // pos 即全部帧总字节
	binary.BigEndian.PutUint32(trailer[20:24], b.dataHash)
	if _, err := b.f.Write(trailer[:]); err != nil {
		return fmt.Errorf("storage: write commit trailer: %w", err)
	}
	if err := b.f.Sync(); err != nil {
		return err
	}
	if err := b.f.Close(); err != nil {
		return err
	}
	b.f = nil
	b.sealed = true

	b.store.mu.Lock()
	if info, ok := b.store.infos[b.header.BatchID]; ok {
		info.Complete = true
		info.Sealed = true
		b.store.infos[b.header.BatchID] = info
	}
	b.store.mu.Unlock()
	return nil
}

// Store 管理目录下的全部批次。
type Store struct {
	dir string

	mu      sync.Mutex
	batches map[string]*Batch // 打开的批次
	infos   map[string]BatchInfo
}

// Open 打开（或创建）存储目录，并恢复区分完整/不完整批次。
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(dir, "batches"), 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, batches: make(map[string]*Batch), infos: make(map[string]BatchInfo)}
	if err := s.recover(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) batchDir() string { return filepath.Join(s.dir, "batches") }

// recover 扫描批次目录：完整批次登记；无尾标/坏尾标的批次隔离为 .incomplete。
func (s *Store) recover() error {
	entries, err := os.ReadDir(s.batchDir())
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".meta") {
			continue
		}
		id := strings.TrimSuffix(name, ".meta")
		hdr, err := readHeader(filepath.Join(s.batchDir(), name))
		if err != nil {
			return fmt.Errorf("storage: read meta %s: %w", id, err)
		}
		dataPath := filepath.Join(s.batchDir(), id+".data")
		gzPath := filepath.Join(s.batchDir(), id+".data.gz")
		info := BatchInfo{Header: hdr}

		switch {
		case fileExists(dataPath):
			complete, _, _, inspectErr := inspectDataFile(dataPath)
			if inspectErr != nil {
				// 帧损坏（CRC/魔数/尾标不一致）：不可信，隔离而非中止整个存储打开。
				complete = false
				if err := os.Rename(dataPath, dataPath+".corrupt"); err != nil {
					return err
				}
			}
			info.Complete = complete
			info.Sealed = complete
			if !complete && inspectErr == nil {
				// 尾标缺失（崩溃截断）：隔离不完整批次，绝不与后续写入拼接。
				if err := os.Rename(dataPath, dataPath+".incomplete"); err != nil {
					return err
				}
			}
		case fileExists(gzPath):
			info.Complete = true
			info.Sealed = true
			info.Archived = true
		default:
			// meta 存在但数据缺失：视为不完整并隔离 meta。
			info.Complete = false
			if err := os.Rename(filepath.Join(s.batchDir(), name), filepath.Join(s.batchDir(), name+".incomplete")); err != nil {
				return err
			}
		}
		s.infos[id] = info
	}
	return nil
}

// inspectDataFile 校验尾标：返回 (complete, recordCount, 已解析完整帧数, err)。
// 帧级 CRC 全部校验；尾部残缺帧不算完整记录，但只要尾标合法即 complete=true。
func inspectDataFile(path string) (bool, uint64, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, 0, 0, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var count uint64
	parsed := 0
	var rollingHash uint32
	var totalFrameBytes uint64
	for {
		// 先探测是否到达尾标。
		peek, err := r.Peek(4)
		if err == io.EOF && parsed == 0 {
			return false, 0, 0, nil
		}
		if err == io.EOF {
			// 数据在帧中途结束：崩溃截断 => 不完整。
			return false, count, parsed, nil
		}
		if err != nil {
			return false, count, parsed, err
		}
		if string(peek) == string(commitMagic[:]) {
			// 读取并校验整个尾标。
			var trailer [24]byte
			if _, err := io.ReadFull(r, trailer[:]); err != nil {
				return false, count, parsed, nil // 尾标截断 => 不完整
			}
			recCount := binary.BigEndian.Uint64(trailer[4:12])
			frameBytes := binary.BigEndian.Uint64(trailer[12:20])
			wantHash := binary.BigEndian.Uint32(trailer[20:24])
			if recCount != count {
				return false, count, parsed, fmt.Errorf("%w: trailer count %d != frames %d", ErrCorrupt, recCount, count)
			}
			if frameBytes != totalFrameBytes {
				return false, count, parsed, fmt.Errorf("%w: trailer frame-bytes %d != actual %d", ErrCorrupt, frameBytes, totalFrameBytes)
			}
			if wantHash != rollingHash {
				return false, count, parsed, fmt.Errorf("%w: trailer hash mismatch", ErrCorrupt)
			}
			// 尾标之后不应有任何字节。
			if _, err := r.Peek(1); err != io.EOF {
				return false, count, parsed, fmt.Errorf("%w: trailing bytes after commit", ErrCorrupt)
			}
			return true, count, parsed, nil
		}

		// 普通帧。
		var head [9]byte
		if _, err := io.ReadFull(r, head[:]); err != nil {
			return false, count, parsed, nil // 帧头截断 => 不完整
		}
		if string(head[0:4]) != string(frameMagic[:]) {
			return false, count, parsed, fmt.Errorf("%w: bad frame magic %q", ErrCorrupt, head[0:4])
		}
		payloadLen := binary.BigEndian.Uint32(head[5:9])
		if payloadLen > maxRecordPayload {
			return false, count, parsed, fmt.Errorf("%w: frame payload length %d", ErrCorrupt, payloadLen)
		}
		payload := make([]byte, payloadLen+4)
		if _, err := io.ReadFull(r, payload); err != nil {
			return false, count, parsed, nil // 帧体截断 => 不完整
		}
		want := binary.BigEndian.Uint32(payload[payloadLen:])
		if got := crc32.ChecksumIEEE(payload[:payloadLen]); got != want {
			return false, count, parsed, fmt.Errorf("%w: frame crc mismatch", ErrCorrupt)
		}
		rollingHash = crc32.Update(rollingHash, crc32.IEEETable, head[:])
		rollingHash = crc32.Update(rollingHash, crc32.IEEETable, payload)
		totalFrameBytes += uint64(len(head)) + uint64(len(payload))
		count++
		parsed++
	}
}

// StartBatch 开启一个新的写入批次；meta 原子落盘，失败不残留。
func (s *Store) StartBatch(sessionID string) (*Batch, error) {
	id := fmt.Sprintf("batch-%d", time.Now().UnixNano())
	hdr := BatchHeader{BatchID: id, SessionID: sessionID, StartedAt: time.Now().UTC()}

	metaBytes, err := json.MarshalIndent(hdr, "", "  ")
	if err != nil {
		return nil, err
	}
	metaPath := filepath.Join(s.batchDir(), id+".meta")
	tmp := metaPath + ".tmp"
	if err := os.WriteFile(tmp, metaBytes, 0o644); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, metaPath); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}

	dataPath := filepath.Join(s.batchDir(), id+".data")
	f, err := os.OpenFile(dataPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	b := &Batch{store: s, header: hdr, f: f}

	s.mu.Lock()
	s.batches[id] = b
	s.infos[id] = BatchInfo{Header: hdr}
	s.mu.Unlock()
	return b, nil
}

// Batches 返回当前已知批次状态快照。
func (s *Store) Batches() []BatchInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]BatchInfo, 0, len(s.infos))
	for _, info := range s.infos {
		out = append(out, info)
	}
	return out
}

// ReadBatch 读取一个已完整提交的批次；未提交批次返回 ErrIncompleteBatch，绝不拼接读出。
func (s *Store) ReadBatch(ctx context.Context, batchID string) (BatchHeader, []Record, error) {
	s.mu.Lock()
	info, ok := s.infos[batchID]
	s.mu.Unlock()
	if !ok {
		return BatchHeader{}, nil, fmt.Errorf("storage: batch %s not found", batchID)
	}
	if !info.Complete {
		return BatchHeader{}, nil, fmt.Errorf("%w: %s", ErrIncompleteBatch, batchID)
	}
	if err := ctx.Err(); err != nil {
		return BatchHeader{}, nil, err
	}

	var rc io.ReadCloser
	dataPath := filepath.Join(s.batchDir(), batchID+".data")
	switch {
	case fileExists(dataPath):
		f, err := os.Open(dataPath)
		if err != nil {
			return BatchHeader{}, nil, err
		}
		rc = f
	default:
		gz, err := os.Open(filepath.Join(s.batchDir(), batchID+".data.gz"))
		if err != nil {
			return BatchHeader{}, nil, err
		}
		zr, err := gzip.NewReader(gz)
		if err != nil {
			gz.Close()
			return BatchHeader{}, nil, err
		}
		rc = &gzipReadCloser{zr: zr, f: gz}
	}
	defer rc.Close()

	recs, err := parseFrames(rc)
	if err != nil {
		return BatchHeader{}, nil, err
	}
	return info.Header, recs, nil
}

// parseFrames 从 reader 解析帧直到合法尾标。
func parseFrames(r io.Reader) ([]Record, error) {
	br := bufio.NewReader(r)
	var recs []Record
	for {
		var magic [4]byte
		if _, err := io.ReadFull(br, magic[:]); err != nil {
			return nil, fmt.Errorf("%w: missing commit trailer", ErrCorrupt)
		}
		if magic == commitMagic {
			var trailer [20]byte
			if _, err := io.ReadFull(br, trailer[:]); err != nil {
				return nil, fmt.Errorf("%w: truncated trailer", ErrCorrupt)
			}
			return recs, nil
		}
		if magic != frameMagic {
			return nil, fmt.Errorf("%w: bad magic", ErrCorrupt)
		}
		var rest [5]byte
		if _, err := io.ReadFull(br, rest[:]); err != nil {
			return nil, fmt.Errorf("%w: truncated frame header", ErrCorrupt)
		}
		typ := RecordType(rest[0])
		payloadLen := binary.BigEndian.Uint32(rest[1:5])
		if payloadLen > maxRecordPayload {
			return nil, fmt.Errorf("%w: oversized frame", ErrCorrupt)
		}
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(br, payload); err != nil {
			return nil, fmt.Errorf("%w: truncated payload", ErrCorrupt)
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(br, crcBuf[:]); err != nil {
			return nil, fmt.Errorf("%w: truncated crc", ErrCorrupt)
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(crcBuf[:]) {
			return nil, fmt.Errorf("%w: crc mismatch", ErrCorrupt)
		}
		recs = append(recs, Record{Type: typ, Payload: payload})
	}
}

// Archive 将已封板批次原子压缩为 gzip：先完整写出并回读校验 sha256，
// 再 rename 为正式归档，最后删除原数据文件（rename 先于 unlink，
// 任一时点数据文件或归档至少其一存在，并发读者不会看到空窗）。
func (s *Store) Archive(ctx context.Context, batchID string) error {
	s.mu.Lock()
	b, open := s.batches[batchID]
	info := s.infos[batchID]
	s.mu.Unlock()
	if !info.Complete {
		return fmt.Errorf("%w: %s", ErrIncompleteBatch, batchID)
	}
	if info.Archived {
		return nil
	}
	if open {
		b.mu.Lock()
		if !b.sealed {
			b.mu.Unlock()
			return ErrSealed
		}
		b.mu.Unlock()
	}

	dataPath := filepath.Join(s.batchDir(), batchID+".data")
	if !fileExists(dataPath) {
		return nil // 已被归档（例如并发调用）
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	raw, err := os.ReadFile(dataPath)
	if err != nil {
		return err
	}
	wantSum := sha256.Sum256(raw)

	gzPath := filepath.Join(s.batchDir(), batchID+".data.gz")
	tmp := gzPath + ".tmp"
	_ = os.Remove(tmp)
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	zw := gzip.NewWriter(out)
	if _, err := zw.Write(raw); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := zw.Close(); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	// 回读校验：解压后 sha256 必须一致，失败绝不留半成品。
	if err := verifyGzip(tmp, wantSum[:]); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("storage: archive verification failed: %w", err)
	}
	if err := os.Rename(tmp, gzPath); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Remove(dataPath); err != nil {
		// 归档已生效；原文件残留不影响正确性，下次打开会以前者优先。
		return nil
	}

	s.mu.Lock()
	info.Archived = true
	s.infos[batchID] = info
	if b != nil {
		b.mu.Lock()
		b.archived = true
		b.mu.Unlock()
	}
	s.mu.Unlock()
	return nil
}

func verifyGzip(path string, wantSHA []byte) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	h := sha256.New()
	if _, err := io.Copy(h, zr); err != nil {
		return err
	}
	if err := zr.Close(); err != nil {
		return err
	}
	if string(h.Sum(nil)) != string(wantSHA) {
		return fmt.Errorf("decompressed content mismatch")
	}
	return nil
}

// Close 关闭存储：未封板批次显式视为失败写入，关闭文件句柄但不写尾标，
// 下次 Open 将隔离该不完整批次，避免部分解析状态被拼接。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for id, b := range s.batches {
		b.mu.Lock()
		if b.f != nil {
			if err := b.f.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			b.f = nil
			info := s.infos[id]
			info.Complete = false
			s.infos[id] = info
		}
		b.mu.Unlock()
	}
	return firstErr
}

func readHeader(path string) (BatchHeader, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return BatchHeader{}, err
	}
	var hdr BatchHeader
	if err := json.Unmarshal(data, &hdr); err != nil {
		return BatchHeader{}, err
	}
	return hdr, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

type gzipReadCloser struct {
	zr *gzip.Reader
	f  *os.File
}

func (g *gzipReadCloser) Read(p []byte) (int, error) { return g.zr.Read(p) }
func (g *gzipReadCloser) Close() error {
	_ = g.zr.Close()
	return g.f.Close()
}
