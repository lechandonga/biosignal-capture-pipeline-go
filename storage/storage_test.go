package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestWriteSealRead(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b, err := s.StartBatch("sess-1")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := b.Append(ctx, Record{Type: RecordSample, Payload: []byte(`{"v":` + string(rune('0'+i)) + `}`)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Append(ctx, Record{Type: RecordSample, Payload: []byte("x")}); err != ErrSealed {
		t.Fatalf("append after seal must fail ErrSealed, got %v", err)
	}

	hdr, recs, err := s.ReadBatch(ctx, b.header.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 10 || hdr.SessionID != "sess-1" {
		t.Fatalf("want 10 records, got %d", len(recs))
	}
	t.Logf("BATCH %s sealed and read back %d records", hdr.BatchID, len(recs))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	defer s.Close()
	b, _ := s.StartBatch("sess-c")
	ctx := context.Background()

	var wg sync.WaitGroup
	n := 20
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				if err := b.Append(ctx, Record{Type: RecordSample, Payload: []byte("payload-payload")}); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if err := b.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	_, recs, err := s.ReadBatch(ctx, b.header.BatchID)
	if err != nil {
		t.Fatalf("concurrent batch must be readable intact: %v", err)
	}
	if len(recs) != 8*n {
		t.Fatalf("want %d records, got %d (lost or torn frames)", 8*n, len(recs))
	}
	t.Logf("CONCURRENT 8 writers x %d appends -> %d intact records", n, len(recs))
}

func TestCrashRecoveryDistinguishesIncomplete(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	bGood, _ := s.StartBatch("sess-r")
	bBad, _ := s.StartBatch("sess-r")
	ctx := context.Background()
	_ = bGood.Append(ctx, Record{Type: RecordSample, Payload: []byte("g1")})
	_ = bGood.Append(ctx, Record{Type: RecordSample, Payload: []byte("g2")})
	if err := bGood.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	// 坏批次：只追加不封板，模拟进程崩溃（Close 不写尾标）。
	_ = bBad.Append(ctx, Record{Type: RecordSample, Payload: []byte("half")})
	_ = s.Close()

	// 进程重启。
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	var goodID, badID string
	for _, info := range s2.Batches() {
		switch {
		case info.Complete:
			goodID = info.Header.BatchID
		default:
			badID = info.Header.BatchID
		}
	}
	if goodID == "" || badID == "" {
		t.Fatalf("reopen must distinguish complete vs incomplete, got good=%q bad=%q", goodID, badID)
	}
	if _, _, err := s2.ReadBatch(ctx, badID); !errors.Is(err, ErrIncompleteBatch) {
		t.Fatalf("incomplete batch must not be read/stitched, got %v", err)
	}
	_, recs, err := s2.ReadBatch(ctx, goodID)
	if err != nil || len(recs) != 2 {
		t.Fatalf("complete batch must remain readable, got %d err=%v", len(recs), err)
	}
	// 不完整数据文件必须被物理隔离为 .incomplete，防止静默拼接。
	matches, _ := filepath.Glob(filepath.Join(dir, "batches", "*.incomplete"))
	if len(matches) != 1 {
		t.Fatalf("expected 1 quarantined file, got %v", matches)
	}
	t.Logf("RECOVERY complete=%s (2 records) incomplete=%s quarantined=%v", goodID, badID, matches)
}

func TestArchiveRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	ctx := context.Background()
	b, _ := s.StartBatch("sess-a")
	for i := 0; i < 50; i++ {
		if err := b.Append(ctx, Record{Type: RecordConclusion, Payload: []byte("conclusion-data")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Archive(ctx, b.header.BatchID); err != nil {
		t.Fatal(err)
	}
	// 归档后读取仍得到相同记录（透明解压缩）。
	_, recs, err := s.ReadBatch(ctx, b.header.BatchID)
	if err != nil || len(recs) != 50 {
		t.Fatalf("archived batch must read back 50 records, got %d err=%v", len(recs), err)
	}
	if _, err := os.Stat(filepath.Join(dir, "batches", b.header.BatchID+".data")); !os.IsNotExist(err) {
		t.Fatalf("raw data file should be removed after verified archive")
	}
	t.Logf("ARCHIVE %s -> gzip, read back %d records", b.header.BatchID, len(recs))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentReadWriteArchive(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	defer s.Close()
	ctx := context.Background()
	b, _ := s.StartBatch("sess-x")

	var writers sync.WaitGroup
	var side sync.WaitGroup
	side.Add(3)
	// 持续写
	writers.Add(1)
	go func() {
		defer writers.Done()
		defer side.Done()
		for i := 0; i < 100; i++ {
			if err := b.Append(ctx, Record{Type: RecordSample, Payload: []byte("streaming")}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	// 封板与归档必须在全部写入结束后进行。
	writers.Wait()
	go func() {
		defer side.Done()
		if err := b.Seal(ctx); err != nil {
			t.Error(err)
			return
		}
		if err := s.Archive(ctx, b.header.BatchID); err != nil {
			t.Error(err)
		}
	}()
	// 并发读取批次列表（不应 panic/数据竞争，配合 -race 验证）。
	go func() {
		defer side.Done()
		for i := 0; i < 100; i++ {
			_ = s.Batches()
		}
	}()
	side.Wait()

	_, recs, err := s.ReadBatch(ctx, b.header.BatchID)
	if err != nil || len(recs) != 100 {
		t.Fatalf("final read must see exactly 100 records, got %d err=%v", len(recs), err)
	}
	t.Logf("CONCURRENT rw+archive -> %d intact records", len(recs))
}

// TestConcurrentReadWhileWritingAndArchiving 验证写入新批次、读取与归档旧批次并发进行：
// 读始终基于已提交批次；归档 rename 先于 unlink，读者任一时刻都能拿到完整数据。
func TestConcurrentReadWhileWritingAndArchiving(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	defer s.Close()
	ctx := context.Background()

	sealedID := make(chan string, 4)
	var wg sync.WaitGroup

	// 生产者：周期性封板，产出多个完整批次。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 4; round++ {
			b, err := s.StartBatch("sess-conc")
			if err != nil {
				t.Error(err)
				return
			}
			for i := 0; i < 40; i++ {
				if err := b.Append(ctx, Record{Type: RecordSample, Payload: []byte("row")}); err != nil {
					t.Error(err)
					return
				}
			}
			if err := b.Seal(ctx); err != nil {
				t.Error(err)
				return
			}
			sealedID <- b.Header().BatchID
		}
		close(sealedID)
	}()

	// 读者：每个封板批次反复读，归档前后都必须读到完整 40 条。
	var readers sync.WaitGroup
	for id := range sealedID {
		for r := 0; r < 3; r++ {
			readers.Add(1)
			go func(batchID string) {
				defer readers.Done()
				_, recs, err := s.ReadBatch(ctx, batchID)
				if err != nil {
					t.Errorf("read %s: %v", batchID, err)
					return
				}
				if len(recs) != 40 {
					t.Errorf("torn read: %s got %d records", batchID, len(recs))
				}
			}(id)
		}
		// 归档与读者并发。
		id := id
		readers.Add(1)
		go func() {
			defer readers.Done()
			if err := s.Archive(ctx, id); err != nil {
				t.Errorf("archive %s: %v", id, err)
			}
		}()
	}
	wg.Wait()
	readers.Wait()
	t.Log("CONCURRENT write/read/archive: every sealed batch read as 40 intact records")
}

// TestCorruptFrameRejected 验证损坏帧不会被静默当作好数据读出。
func TestCorruptFrameRejected(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	ctx := context.Background()
	b, _ := s.StartBatch("sess-bad")
	_ = b.Append(ctx, Record{Type: RecordSample, Payload: []byte("good")})
	_ = b.Seal(ctx)

	path := filepath.Join(dir, "batches", b.Header().BatchID+".data")
	raw, _ := os.ReadFile(path)
	// 翻转第一条帧负载中的一个字节（帧头在最前 9 字节之后）。
	raw[12] ^= 0xFF
	_ = os.WriteFile(path, raw, 0o644)
	_ = s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	info := s2.Batches()
	for _, in := range info {
		if in.Header.BatchID == b.Header().BatchID && in.Complete {
			t.Fatal("corrupt frame must fail trailer/frame validation and be treated incomplete")
		}
	}
	t.Log("CORRUPT frame detected on reopen; batch not treated as complete, not stitched")
}
