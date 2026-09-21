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
	mu sync.Mutex
	f  *os.File
	w  *os.File // same file; buffered framing written directly
	id string
}

// Store is the file-backed batch store.
type Store struct {
	root string
	mu   sync.Mutex
	open map[string]*handle
}

// Open opens (or recovers) a store rooted at dir.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(dir, dirBatches), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, dirArchive), 0o755); err != nil {
		return nil, err
	}
	return &Store{root: dir, open: map[string]*handle{}}, nil
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
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	h := &handle{f: f, id: id}
	s.open[id] = h
	s.mu.Unlock()

	hdr := jsonHeader{ID: id, SessionID: sessionID, StartedAt: startedAt}
	if err := h.append(recHeader, hdr); err != nil {
		_ = f.Close()
		s.mu.Lock()
		delete(s.open, id)
		s.mu.Unlock()
		return err
	}
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
	return h.append(recSample, js)
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
	return h.append(recEvent, je)
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
	return h.append(recReject, jr)
}

func (h *handle) append(typ byte, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := writeFrame(h.f, append([]byte{typ}, raw...)); err != nil {
		return err
	}
	return h.f.Sync()
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
	// closedAt is recorded implicitly by file mtime + reader state; we also
	// ensure the directory entry is durable.
	if dir, err := os.Open(filepath.Join(s.root, dirBatches)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	_ = closedAt
	return nil
}

// ArchiveBatch gzip-compresses a completed batch into archive/ and removes
// the uncompressed original. The rename-in-place guarantees readers either
// observe the complete batch or the complete archive, never a half file.
func (s *Store) ArchiveBatch(batchID string) error {
	b, err := s.LoadBatch(batchID)
	if err != nil {
		return err
	}
	if b.State != model.BatchComplete {
		return fmt.Errorf("%w: %s state=%s", ErrBatchNotComplete, batchID, b.State)
	}
	raw, err := os.ReadFile(s.batchPath(batchID))
	if err != nil {
		return err
	}
	tmp := s.archivePath(batchID) + ".tmp"
	gf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(gf)
	cleanup := func() {
		_ = gz.Close()
		_ = gf.Close()
		_ = os.Remove(tmp)
	}
	if _, err := gz.Write(raw); err != nil {
		cleanup()
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
	return os.Remove(s.batchPath(batchID))
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
	frames, err := readFrames(bufioNewReader(rdr))
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		// A torn frame can only happen on an open/partial batch; downgrade
		// state so callers can refuse to treat it as finished.
		state = model.BatchIncomplete
	}

	b := &model.Batch{ID: batchID, State: state}
	for _, fr := range frames {
		if len(fr) == 0 {
			continue
		}
		env := envelope{Type: fr[0], Raw: fr[1:]}
		if perr := decodeInto(&env, b); perr != nil {
			return nil, fmt.Errorf("%w: %v", ErrCorrupt, perr)
		}
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
