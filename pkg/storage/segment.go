package storage

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

const (
	dirIndex   = "index"
	dirReclaim = "reclaim"

	indexFile = "segments.json"

	recFooter byte = 5
)

// ErrSegmentBusy indicates a segment is currently leased by a reader/export
// and cannot be archived or reclaimed right now.
var ErrSegmentBusy = errors.New("storage: segment is in use by a reader/export")

type jsonFooter struct {
	ClosedAt time.Time
}

// SegmentInfo is the indexed metadata of one durable segment. Times and
// counters are maintained incrementally while writing and reconciled against
// the files on disk after a restart, so range queries can pick segments by
// index lookup instead of scanning the whole store.
type SegmentInfo struct {
	ID        string
	SessionID string
	State     model.BatchState
	File      string // path relative to the store root
	Size      int64  // bytes currently occupied on disk (gzip size when archived)
	StartedAt time.Time
	ClosedAt  time.Time
	FirstTime time.Time // earliest record timestamp observed in the segment
	LastTime  time.Time // latest record timestamp observed in the segment
	Samples   int
	Events    int
	Rejected  int
	TornTail  bool // a torn trailing frame was observed (segment never sealed)
}

// SegmentRecord is one streamed record of a segment. Exactly one of Sample,
// Event or Rejected is populated. FramePos is the record's zero-based
// position inside the segment, used as a stable merge tie-breaker.
type SegmentRecord struct {
	Time         time.Time
	Device       model.DeviceID
	Sensor       model.SensorID
	FramePos     int
	SegmentID    string
	SegmentState model.BatchState
	Sample       *model.CalibratedSample
	Event        *model.Event
	Rejected     *model.RejectedRecord
}

type diskIndex struct {
	Segments []*SegmentInfo
}

// indexLocked loads/reconciles the on-disk index. It is called once from
// Open. Cached metadata is reused when the data file is unchanged (immutable
// once sealed); new files and the writable .partial are streamed once.
func (s *Store) indexLocked() error {
	if err := os.MkdirAll(filepath.Join(s.root, dirIndex), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(s.root, dirReclaim), 0o755); err != nil {
		return err
	}
	s.meta = map[string]*SegmentInfo{}
	if raw, err := os.ReadFile(s.indexPath()); err == nil {
		var saved diskIndex
		if jerr := json.Unmarshal(raw, &saved); jerr == nil {
			for _, m := range saved.Segments {
				if m != nil && m.ID != "" {
					s.meta[m.ID] = m
				}
			}
		}
	}

	ids, err := s.listIDsLocked()
	if err != nil {
		return err
	}
	live := map[string]bool{}
	for _, id := range ids {
		live[id] = true
		path, state := s.locateLocked(id)
		st, statErr := os.Stat(path)
		if statErr != nil {
			continue
		}
		m := s.meta[id]
		fresh := m != nil && filepath.Join(s.root, m.File) == path && m.Size == st.Size() &&
			m.State == state && state != model.BatchIncomplete
		if !fresh {
			scanned, serr := scanSegment(path, state == model.BatchArchived)
			if serr != nil {
				scanned = &SegmentInfo{}
			}
			scanned.ID = id
			scanned.State = state
			scanned.File = relPath(s.root, path)
			scanned.Size = st.Size()
			s.meta[id] = scanned
		}
	}
	// Drop index entries whose data file is gone (a crash between data-file
	// removal and index rewrite during reclaim).
	for id := range s.meta {
		if !live[id] {
			delete(s.meta, id)
		}
	}
	return s.persistIndexLocked()
}

func (s *Store) indexPath() string { return filepath.Join(s.root, dirIndex, indexFile) }

func relPath(root, p string) string {
	if rel, err := filepath.Rel(root, p); err == nil {
		return rel
	}
	return p
}

// listIDsLocked enumerates segment ids from the three data directories.
func (s *Store) listIDsLocked() ([]string, error) {
	seen := map[string]struct{}{}
	add := func(dir, ext string) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
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

// locateLocked returns the on-disk path/current state of a segment id.
func (s *Store) locateLocked(id string) (string, model.BatchState) {
	switch {
	case s.exists(s.archivePath(id)):
		return s.archivePath(id), model.BatchArchived
	case s.exists(s.batchPath(id)):
		return s.batchPath(id), model.BatchComplete
	case s.exists(s.partialPath(id)):
		return s.partialPath(id), model.BatchIncomplete
	default:
		return "", ""
	}
}

// scanSegment streams a segment file once, collecting only lightweight
// metadata (constant memory regardless of segment size).
func scanSegment(path string, gz bool) (*SegmentInfo, error) {
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
	fs := newFrameScanner(bufioNewReader(rdr))
	m := &SegmentInfo{}
	for fs.next() {
		raw := fs.bytes()
		env := envelope{Type: raw[0], Raw: raw[1:]}
		var t time.Time
		switch env.Type {
		case recHeader:
			var h jsonHeader
			if err := json.Unmarshal(env.Raw, &h); err != nil {
				return nil, err
			}
			m.SessionID = h.SessionID
			m.StartedAt = h.StartedAt
			t = h.StartedAt
		case recSample:
			var js jsonSample
			if err := json.Unmarshal(env.Raw, &js); err != nil {
				return nil, err
			}
			m.Samples++
			t = js.UniTime
		case recEvent:
			var je jsonEvent
			if err := json.Unmarshal(env.Raw, &je); err != nil {
				return nil, err
			}
			m.Events++
			t = je.At
		case recReject:
			var jr jsonReject
			if err := json.Unmarshal(env.Raw, &jr); err != nil {
				return nil, err
			}
			m.Rejected++
			t = jr.At
		case recFooter:
			var jf jsonFooter
			if err := json.Unmarshal(env.Raw, &jf); err != nil {
				return nil, err
			}
			m.ClosedAt = jf.ClosedAt
		}
		if !t.IsZero() {
			if m.FirstTime.IsZero() || t.Before(m.FirstTime) {
				m.FirstTime = t
			}
			if t.After(m.LastTime) {
				m.LastTime = t
			}
		}
	}
	if err := fs.err(); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			m.TornTail = true
			return m, nil
		}
		return nil, err
	}
	return m, nil
}

// persistIndexLocked atomically rewrites the segment index.
func (s *Store) persistIndexLocked() error {
	ids := make([]string, 0, len(s.meta))
	for id := range s.meta {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	disk := diskIndex{Segments: make([]*SegmentInfo, 0, len(ids))}
	for _, id := range ids {
		disk.Segments = append(disk.Segments, s.meta[id])
	}
	raw, err := json.MarshalIndent(disk, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.indexPath() + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.indexPath()); err != nil {
		return err
	}
	if dir, err := os.Open(filepath.Join(s.root, dirIndex)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

// Segments returns metadata for every segment, ordered on the timeline
// (FirstTime, then id). It reads from the in-memory index: no data files are
// opened.
func (s *Store) Segments() []SegmentInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SegmentInfo, 0, len(s.meta))
	for _, m := range s.meta {
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].FirstTime.Equal(out[j].FirstTime) {
			return out[i].FirstTime.Before(out[j].FirstTime)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// SessionSegments returns the timeline-ordered segments of one session.
func (s *Store) SessionSegments(sessionID string) []SegmentInfo {
	all := s.Segments()
	out := make([]SegmentInfo, 0, len(all))
	for _, m := range all {
		if m.SessionID == sessionID {
			out = append(out, m)
		}
	}
	return out
}

// NextSessionSeq returns one past the highest -batch-NNNNNN suffix used for
// the given session, so a restarted session appends new segments without ever
// reusing an existing file name.
func (s *Store) NextSessionSeq(sessionID string) int {
	prefix := sessionID + "-batch-"
	maxSeq := 0
	for _, m := range s.Segments() {
		if len(m.ID) > len(prefix) && m.ID[:len(prefix)] == prefix {
			n := 0
			for _, c := range m.ID[len(prefix):] {
				if c < '0' || c > '9' {
					n = -1
					break
				}
				n = n*10 + int(c-'0')
			}
			if n > maxSeq {
				maxSeq = n
			}
		}
	}
	return maxSeq + 1
}

// BatchSizeBytes reports the current on-disk size of an open batch handle.
func (s *Store) BatchSizeBytes(batchID string) int64 {
	h, err := s.getOpen(batchID)
	if err != nil {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.bytes
}

// Lease pins a segment against archive/reclaim while held. A false result
// means the segment is unknown to the store.
func (s *Store) Lease(id string) bool {
	s.mu.Lock()
	_, known := s.meta[id]
	s.mu.Unlock()
	if !known {
		return false
	}
	s.leaseMu.Lock()
	s.leases[id]++
	s.leaseMu.Unlock()
	return true
}

// Release drops one lease previously taken with Lease.
func (s *Store) Release(id string) {
	s.leaseMu.Lock()
	if s.leases[id] > 0 {
		s.leases[id]--
		if s.leases[id] == 0 {
			delete(s.leases, id)
		}
	}
	s.leaseMu.Unlock()
}

func (s *Store) leased(id string) bool {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	return s.leases[id] > 0
}

// SegmentReader streams one segment record by record. The segment is leased
// for the reader's whole lifetime, so reclaim/archive cannot remove it
// mid-read. Callers must Close the reader.
type SegmentReader struct {
	store *Store
	id    string
	f     *os.File
	zr    *gzip.Reader
	fs    *frameScanner
	info  SegmentInfo
	pos   int
	last  *SegmentRecord
}

// OpenSegmentReader leases the segment and opens its data file for streaming.
func (s *Store) OpenSegmentReader(id string) (*SegmentReader, error) {
	s.ioMu.RLock()
	defer s.ioMu.RUnlock()
	for {
		s.mu.Lock()
		if _, ok := s.meta[id]; !ok {
			s.mu.Unlock()
			return nil, ErrNotFound
		}
		path, state := s.locateLocked(id)
		if path == "" {
			s.mu.Unlock()
			return nil, ErrNotFound
		}
		s.leaseMu.Lock()
		s.leases[id]++
		s.leaseMu.Unlock()
		// Re-validate under the lock after acquiring the lease: archive and
		// reclaim both take s.mu and refuse/remove leased segments, so the
		// path and state cannot change while the lease is held.
		m2, ok2 := s.meta[id]
		path2, state2 := s.locateLocked(id)
		var infoCopy SegmentInfo
		if m2 != nil {
			infoCopy = *m2
		}
		s.mu.Unlock()
		if !ok2 || path2 != path || state2 != state {
			s.Release(id)
			if !ok2 {
				return nil, ErrNotFound
			}
			continue // raced an archive state flip; retry
		}
		f, err := os.Open(path)
		if err != nil {
			s.Release(id)
			if os.IsNotExist(err) {
				return nil, ErrNotFound
			}
			return nil, err
		}
		var zr *gzip.Reader
		var rdr io.Reader = f
		if state == model.BatchArchived {
			zr, err = gzip.NewReader(f)
			if err != nil {
				_ = f.Close()
				s.Release(id)
				return nil, err
			}
			rdr = zr
		}
		return &SegmentReader{
			store: s, id: id, f: f, zr: zr,
			fs:   newFrameScanner(bufioNewReader(rdr)),
			info: infoCopy,
		}, nil
	}
}

// Info returns the indexed metadata snapshot of the segment.
func (r *SegmentReader) Info() SegmentInfo { return r.info }

// Next advances to the next intact record. After Next returns false, Err is
// nil for a clean end or io.ErrUnexpectedEOF for a torn tail (open segments
// only; all intact records have already been yielded).
func (r *SegmentReader) Next() bool {
	for {
		if !r.fs.next() {
			return false
		}
		raw := r.fs.bytes()
		rec, err := decodeRecord(envelope{Type: raw[0], Raw: raw[1:]}, r.id, r.info.State, r.pos)
		if err != nil {
			r.store.setCorrupt(r.id)
			continue
		}
		r.pos++
		if rec == nil {
			continue // header/footer
		}
		r.last = rec
		return true
	}
}

// Record returns the current record after a successful Next.
func (r *SegmentReader) Record() SegmentRecord { return *r.last }

// Err reports the terminal scan error, if any.
func (r *SegmentReader) Err() error { return r.fs.err() }

// Close releases the file handle and the segment lease.
func (r *SegmentReader) Close() error {
	var err error
	if r.zr != nil {
		err = r.zr.Close()
	}
	if cerr := r.f.Close(); err == nil {
		err = cerr
	}
	r.store.Release(r.id)
	return err
}
