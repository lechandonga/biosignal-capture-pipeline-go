// Package storage provides crash-consistent batch persistence.
//
// On-disk layout under the store root:
//
//	batches/<id>.partial  - an open batch (state INCOMPLETE)
//	batches/<id>.batch    - a sealed, CRC-checked batch (state COMPLETE)
//	archive/<id>.batch.gz - a gzip-compressed archive (state ARCHIVED)
//
// A batch is sealed atomically with fsync + rename(.partial -> .batch), so a
// crash can only leave a .partial file; on reopen such batches are recovered
// frame by frame and explicitly marked INCOMPLETE. They are never silently
// concatenated with newer data. Archive compaction renames the .batch to the
// .gz area; concurrent readers always see exactly one consistent copy.
package storage

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

const (
	dirBatches = "batches"
	dirArchive = "archive"

	extPartial = ".partial"
	extBatch   = ".batch"
	extGz      = ".batch.gz"

	recHeader byte = 1
	recSample byte = 2
	recEvent  byte = 3
	recReject byte = 4
)

var (
	// ErrNotFound is returned for unknown batch ids.
	ErrNotFound = errors.New("storage: batch not found")
	// ErrBatchOpen is returned when an operation requires an open batch.
	ErrBatchOpen = errors.New("storage: batch already sealed/archived")
	// ErrBatchNotComplete is returned when archiving a non-complete batch.
	ErrBatchNotComplete = errors.New("storage: batch not complete")
	// ErrCorrupt marks an unreadable frame inside a batch.
	ErrCorrupt = errors.New("storage: corrupt batch frame")
)

type jsonHeader struct {
	ID        string
	SessionID string
	StartedAt time.Time
}

type jsonSample struct {
	Device      string
	Sensor      string
	Kind        string
	Seq         int64
	UniTime     time.Time
	RecvTime    time.Time
	Values      []float64
	Quality     float64
	Gap         bool
	CalVersion  string
	CalValues   []float64
	Baseline    []float64
	Uncertain   bool
	Uncertainty string
}

type jsonEvent struct {
	Device     string
	Sensor     string
	Kind       string
	Level      string
	Start      time.Time
	End        time.Time
	Value      float64
	Threshold  float64
	RuleID     string
	CalVersion string
	Evidence   string
	At         time.Time
}

type jsonReject struct {
	Action string
	Device string
	Sensor string
	Seq    int64
	Reason string
	Detail string
	At     time.Time
}

type envelope struct {
	Type byte
	Raw  []byte
}

// handle is an open batch writer.
type handle struct {
	mu        sync.Mutex
	f         *os.File
	id        string
	bytes     int64
	sessionID string
	startedAt time.Time
	firstTime time.Time
	lastTime  time.Time
	samples   int
	events    int
	rejected  int
}

// Store is the file-backed batch store.
type Store struct {
	root    string
	mu      sync.Mutex
	open    map[string]*handle
	meta    map[string]*SegmentInfo
	leaseMu sync.Mutex
	leases  map[string]int
	ioMu    sync.RWMutex // readers RLock; archive/reclaim Lock
	quotaMu sync.Mutex
	quota   quotaState
}

// Open opens (or recovers) a store rooted at dir.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(dir, dirBatches), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, dirArchive), 0o755); err != nil {
		return nil, err
	}
	s := &Store{root: dir, open: map[string]*handle{}, leases: map[string]int{}}
	if err := s.indexLocked(); err != nil {
		return nil, err
	}
	if err := s.reconcileReclaimsLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) partialPath(id string) string {
	return filepath.Join(s.root, dirBatches, id+extPartial)
}
func (s *Store) batchPath(id string) string {
	return filepath.Join(s.root, dirBatches, id+extBatch)
}
func (s *Store) archivePath(id string) string {
	return filepath.Join(s.root, dirArchive, id+extGz)
}

// CreateBatch starts a new batch; its on-disk state is INCOMPLETE until
// CompleteBatch succeeds. The header record is fsynced before return so the
// batch identity survives a crash immediately after creation.
func (s *Store) CreateBatch(id, sessionID string, startedAt time.Time) error {
	s.mu.Lock()
	if _, ok := s.open[id]; ok {
		s.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrBatchOpen, id)
	}
	path := s.partialPath(id)
	if _, err := os.Stat(s.batchPath(id)); err == nil {
		s.mu.Unlock()
		return fmt.Errorf("%w: %s already sealed", ErrBatchOpen, id)
	}
	if _, err := os.Stat(s.archivePath(id)); err == nil {
		s.mu.Unlock()
		return fmt.Errorf("%w: %s already archived", ErrBatchOpen, id)
	}
	if _, ok := s.meta[id]; ok {
		s.mu.Unlock()
		return fmt.Errorf("%w: %s already exists (possibly incomplete)", ErrBatchOpen, id)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	h := &handle{f: f, id: id}
	s.open[id] = h
	s.mu.Unlock()

	hdr := jsonHeader{ID: id, SessionID: sessionID, StartedAt: startedAt}
	if _, err := h.append(recHeader, hdr); err != nil {
		_ = f.Close()
		s.mu.Lock()
		delete(s.open, id)
		s.mu.Unlock()
		return err
	}
	h.mu.Lock()
	h.sessionID = sessionID
	h.startedAt = startedAt
	size := h.bytes
	h.mu.Unlock()

	s.mu.Lock()
	s.meta[id] = &SegmentInfo{
		ID: id, SessionID: sessionID, State: model.BatchIncomplete,
		File: relPath(s.root, s.partialPath(id)), Size: size,
		StartedAt: startedAt,
	}
	if err := s.persistIndexLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	return nil
}

func (s *Store) getOpen(batchID string) (*handle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.open[batchID]
	if h == nil {
		return nil, fmt.Errorf("%w: %s", ErrBatchOpen, batchID)
	}
	return h, nil
}

// AppendBatchSample appends one calibrated sample atomically (frame + fsync).
func (s *Store) AppendBatchSample(batchID string, smp model.CalibratedSample) error {
	h, err := s.getOpen(batchID)
	if err != nil {
		return err
	}
	js := jsonSample{
		Device: string(smp.Aligned.Device), Sensor: string(smp.Aligned.Sensor),
		Kind: string(smp.Aligned.Kind), Seq: smp.Aligned.Seq,
		UniTime: smp.Aligned.UniTime, RecvTime: smp.Aligned.RecvTime,
		Values: smp.Aligned.Values, Quality: smp.Aligned.Quality, Gap: smp.Aligned.Gap,
		CalVersion: smp.CalVersion, CalValues: smp.Values, Baseline: smp.Baseline,
		Uncertain: smp.Uncertain, Uncertainty: smp.Uncertainty,
	}
	n, err := h.append(recSample, js)
	if err == nil {
		h.noteSample(n, smp.Aligned.UniTime)
		s.refreshOpenMeta(batchID)
	}
	return err
}

// AppendBatchEvent appends one event atomically.
func (s *Store) AppendBatchEvent(batchID string, ev model.Event) error {
	h, err := s.getOpen(batchID)
	if err != nil {
		return err
	}
	je := jsonEvent{
		Device: string(ev.Device), Sensor: string(ev.Sensor), Kind: string(ev.Kind),
		Level: string(ev.Level), Start: ev.Start, End: ev.End, Value: ev.Value,
		Threshold: ev.Threshold, RuleID: ev.RuleID, CalVersion: ev.CalVersion,
		Evidence: ev.Evidence, At: ev.At,
	}
	n, err := h.append(recEvent, je)
	if err == nil {
		h.noteEvent(n, ev.At)
		s.refreshOpenMeta(batchID)
	}
	return err
}

// AppendBatchRejected records a rejected/handled sample with its reason.
func (s *Store) AppendBatchRejected(batchID string, r model.RejectedRecord) error {
	h, err := s.getOpen(batchID)
	if err != nil {
		return err
	}
	jr := jsonReject{
		Action: string(r.Action), Device: string(r.Sample.Device),
		Sensor: string(r.Sample.Sensor), Seq: r.Sample.Seq,
		Reason: string(r.Reason), Detail: r.Detail, At: r.At,
	}
	n, err := h.append(recReject, jr)
	if err == nil {
		h.noteReject(n, r.At)
		s.refreshOpenMeta(batchID)
	}
	return err
}

// refreshOpenMeta copies live handle stats into the in-memory index so range
// selection can observe an open segment's current extent without a rescan.
// The durable index file is only rewritten on seal/archive/reclaim.
func (s *Store) refreshOpenMeta(id string) {
	h, err := s.getOpen(id)
	if err != nil {
		return
	}
	h.mu.Lock()
	s.mu.Lock()
	if m, ok := s.meta[id]; ok {
		m.FirstTime = h.firstTime
		m.LastTime = h.lastTime
		m.Samples = h.samples
		m.Events = h.events
		m.Rejected = h.rejected
		m.Size = h.bytes
	}
	s.mu.Unlock()
	h.mu.Unlock()
}

func (h *handle) append(typ byte, v any) (int, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	payload := append([]byte{typ}, raw...)
	if err := writeFrame(h.f, payload); err != nil {
		return 0, err
	}
	if err := h.f.Sync(); err != nil {
		return 0, err
	}
	n := lenSize + len(payload) + crcSize
	h.bytes += int64(n)
	return n, nil
}

func (h *handle) noteTimeLocked(t time.Time) {
	if t.IsZero() {
		return
	}
	if h.firstTime.IsZero() || t.Before(h.firstTime) {
		h.firstTime = t
	}
	if t.After(h.lastTime) {
		h.lastTime = t
	}
}

func (h *handle) noteSample(n int, t time.Time) {
	h.mu.Lock()
	h.samples++
	h.noteTimeLocked(t)
	h.mu.Unlock()
}

func (h *handle) noteEvent(n int, t time.Time) {
	h.mu.Lock()
	h.events++
	h.noteTimeLocked(t)
	h.mu.Unlock()
}

func (h *handle) noteReject(n int, t time.Time) {
	h.mu.Lock()
	h.rejected++
	h.noteTimeLocked(t)
	h.mu.Unlock()
}

// CompleteBatch seals a batch: fsync and atomic rename to .batch.
func (s *Store) CompleteBatch(batchID string, closedAt time.Time) error {
	s.mu.Lock()
	h := s.open[batchID]
	if h == nil {
		s.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrBatchOpen, batchID)
	}
	delete(s.open, batchID)
	s.mu.Unlock()

	// Durable footer: readers can distinguish a cleanly sealed segment from
	// one merely containing intact frames up to the current file position.
	if _, err := h.append(recFooter, jsonFooter{ClosedAt: closedAt}); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.f.Sync(); err != nil {
		return err
	}
	if err := h.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(s.partialPath(batchID), s.batchPath(batchID)); err != nil {
		return err
	}
	if dir, err := os.Open(filepath.Join(s.root, dirBatches)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	st, statErr := os.Stat(s.batchPath(batchID))
	s.mu.Lock()
	if m, ok := s.meta[batchID]; ok {
		m.State = model.BatchComplete
		m.File = relPath(s.root, s.batchPath(batchID))
		m.ClosedAt = closedAt
		m.FirstTime = h.firstTime
		m.LastTime = h.lastTime
		m.Samples = h.samples
		m.Events = h.events
		m.Rejected = h.rejected
		m.TornTail = false
		if statErr == nil {
			m.Size = st.Size()
		}
	}
	_ = s.persistIndexLocked()
	s.mu.Unlock()
	return nil
}

// ArchiveBatch gzip-compresses a completed batch into archive/ and removes
// the uncompressed original, streaming frame-by-frame (constant memory). A
// segment that is currently being read/exported is left untouched
// (ErrSegmentBusy) and can be retried later.
func (s *Store) ArchiveBatch(batchID string) error {
	s.mu.Lock()
	m, ok := s.meta[batchID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrNotFound, batchID)
	}
	if m.State != model.BatchComplete {
		state := m.State
		s.mu.Unlock()
		return fmt.Errorf("%w: %s state=%s", ErrBatchNotComplete, batchID, state)
	}
	s.mu.Unlock()
	if s.leased(batchID) {
		return fmt.Errorf("%w: %s", ErrSegmentBusy, batchID)
	}
	// Block until no reader is opening/holding this segment, and stay ahead
	// of new openers for the duration of the file swap.
	s.ioMu.Lock()
	defer s.ioMu.Unlock()
	if s.leased(batchID) {
		return fmt.Errorf("%w: %s", ErrSegmentBusy, batchID)
	}
	in, err := os.Open(s.batchPath(batchID))
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := s.archivePath(batchID) + ".tmp"
	gf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(gf)
	if _, err := io.Copy(gz, bufioNewReader(in)); err != nil {
		_ = gz.Close()
		_ = gf.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := gz.Close(); err != nil {
		_ = gf.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := gf.Sync(); err != nil {
		_ = gf.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := gf.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, s.archivePath(batchID)); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Remove(s.batchPath(batchID)); err != nil {
		return err
	}
	if dir, err := os.Open(filepath.Join(s.root, dirBatches)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	if dir, err := os.Open(filepath.Join(s.root, dirArchive)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	st, statErr := os.Stat(s.archivePath(batchID))
	s.mu.Lock()
	if mm, ok := s.meta[batchID]; ok {
		mm.State = model.BatchArchived
		mm.File = relPath(s.root, s.archivePath(batchID))
		if statErr == nil {
			mm.Size = st.Size()
		}
	}
	err = s.persistIndexLocked()
	s.mu.Unlock()
	return err
}

// LoadBatch reads a batch back in any state. A .partial batch is reported
// INCOMPLETE; a torn trailing frame drops only that one record and the state
// stays INCOMPLETE (never silently merged).
func (s *Store) LoadBatch(batchID string) (*model.Batch, error) {
	var (
		path  string
		state model.BatchState
		gz    bool
	)
	switch {
	case s.exists(s.archivePath(batchID)):
		path, state, gz = s.archivePath(batchID), model.BatchArchived, true
	case s.exists(s.batchPath(batchID)):
		path, state = s.batchPath(batchID), model.BatchComplete
	case s.exists(s.partialPath(batchID)):
		path, state = s.partialPath(batchID), model.BatchIncomplete
	default:
		return nil, fmt.Errorf("%w: %s", ErrNotFound, batchID)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var rdr io.Reader = f
	if gz {
		zr, zerr := gzip.NewReader(f)
		if zerr != nil {
			return nil, zerr
		}
		defer zr.Close()
		rdr = zr
	}
	b := &model.Batch{ID: batchID, State: state}
	fs := newFrameScanner(bufioNewReader(rdr))
	for fs.next() {
		fr := fs.bytes()
		if len(fr) == 0 {
			continue
		}
		env := envelope{Type: fr[0], Raw: fr[1:]}
		if env.Type == recFooter {
			var jf jsonFooter
			if perr := json.Unmarshal(env.Raw, &jf); perr != nil {
				return nil, fmt.Errorf("%w: %v", ErrCorrupt, perr)
			}
			b.ClosedAt = jf.ClosedAt
			continue
		}
		if perr := decodeInto(&env, b); perr != nil {
			return nil, fmt.Errorf("%w: %v", ErrCorrupt, perr)
		}
	}
	if err := fs.err(); err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	if errors.Is(fs.err(), io.ErrUnexpectedEOF) {
		// A torn frame can only happen on an open/partial batch; downgrade
		// state so callers can refuse to treat it as finished.
		state = model.BatchIncomplete
		b.State = state
	}
	return b, nil
}

func (s *Store) exists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// ListBatches lists batch ids sorted, with state detectable via LoadBatch.
func (s *Store) ListBatches() ([]string, error) {
	seen := map[string]struct{}{}
	add := func(dir, ext string) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			n := e.Name()
			if len(n) > len(ext) && n[len(n)-len(ext):] == ext {
				seen[n[:len(n)-len(ext)]] = struct{}{}
			}
		}
		return nil
	}
	if err := add(filepath.Join(s.root, dirBatches), extPartial); err != nil {
		return nil, err
	}
	if err := add(filepath.Join(s.root, dirBatches), extBatch); err != nil {
		return nil, err
	}
	if err := add(filepath.Join(s.root, dirArchive), extGz); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// Close seals no batch but releases open file handles of still-open batches.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for id, h := range s.open {
		h.mu.Lock()
		if err := h.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		h.mu.Unlock()
		delete(s.open, id)
	}
	return firstErr
}

// bufioNewReader wraps an io.Reader with buffering.
func bufioNewReader(r io.Reader) *bufio.Reader { return bufio.NewReader(r) }

// decodeInto decodes one framed record into the batch accumulator.
func decodeInto(env *envelope, b *model.Batch) error {
	switch env.Type {
	case recHeader:
		var h jsonHeader
		if err := json.Unmarshal(env.Raw, &h); err != nil {
			return err
		}
		b.SessionID = h.SessionID
		b.StartedAt = h.StartedAt
	case recSample:
		var js jsonSample
		if err := json.Unmarshal(env.Raw, &js); err != nil {
			return err
		}
		b.Samples = append(b.Samples, model.CalibratedSample{
			Aligned: model.AlignedSample{
				Device:   model.DeviceID(js.Device),
				Sensor:   model.SensorID(js.Sensor),
				Kind:     model.SignalKind(js.Kind),
				Seq:      js.Seq,
				UniTime:  js.UniTime,
				RecvTime: js.RecvTime,
				Values:   js.Values,
				Quality:  js.Quality,
				Gap:      js.Gap,
			},
			CalVersion:  js.CalVersion,
			Values:      js.CalValues,
			Baseline:    js.Baseline,
			Uncertain:   js.Uncertain,
			Uncertainty: js.Uncertainty,
		})
	case recEvent:
		var je jsonEvent
		if err := json.Unmarshal(env.Raw, &je); err != nil {
			return err
		}
		b.Events = append(b.Events, model.Event{
			Device: model.DeviceID(je.Device), Sensor: model.SensorID(je.Sensor),
			Kind: model.SignalKind(je.Kind), Level: model.EventLevel(je.Level),
			Start: je.Start, End: je.End, Value: je.Value, Threshold: je.Threshold,
			RuleID: je.RuleID, CalVersion: je.CalVersion, Evidence: je.Evidence, At: je.At,
		})
	case recReject:
		var jr jsonReject
		if err := json.Unmarshal(env.Raw, &jr); err != nil {
			return err
		}
		b.Rejected = append(b.Rejected, model.RejectedRecord{
			Action: model.Action(jr.Action),
			Sample: model.RawSample{
				Device: model.DeviceID(jr.Device), Sensor: model.SensorID(jr.Sensor), Seq: jr.Seq,
			},
			Reason: model.RejectReason(jr.Reason), Detail: jr.Detail, At: jr.At,
		})
	default:
		return fmt.Errorf("unknown record type %d", env.Type)
	}
	return nil
}
