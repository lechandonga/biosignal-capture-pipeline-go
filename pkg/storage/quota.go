package storage

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

const reclaimLogFile = "reclaim" + string(os.PathSeparator) + "journal.jsonl"

// ErrNoQuota is returned when no quota has been configured.
var ErrNoQuota = errors.New("storage: no storage quota configured")

// ReclaimEntry is the durable audit trail of one reclaimed segment.
type ReclaimEntry struct {
	OpID       string    `json:"op_id"`
	SegmentID  string    `json:"segment_id"`
	SessionID  string    `json:"session_id"`
	State      string    `json:"state"`
	BytesFreed int64     `json:"bytes_freed"`
	At         time.Time `json:"at"`
	Reason     string    `json:"reason"`
	Result     string    `json:"result"` // COMMITTED | PENDING (crash mid-reclaim)
}

// reclaimOp is the crash-consistency journal for a single reclaim action.
// Flow: write PENDING -> remove data file -> remove journal record & write
// audit COMMITTED -> update index. A PENDING record left by a crash is
// resolved on next Open: if the data file is already gone, COMMIT it (audit
// + index cleanup); otherwise abort (segment stays fully readable).
type reclaimOp struct {
	OpID       string    `json:"op_id"`
	SegmentID  string    `json:"segment_id"`
	SessionID  string    `json:"session_id"`
	State      string    `json:"state"`
	BytesFreed int64     `json:"bytes_freed"`
	StartedAt  time.Time `json:"started_at"`
}

// quotaState holds the configured capacity limit.
type quotaState struct {
	limit int64
}

// SetQuota configures the maximum total bytes occupied by data files. Zero
// (the default) disables quota enforcement.
func (s *Store) SetQuota(limitBytes int64) {
	s.quotaMu.Lock()
	s.quota.limit = limitBytes
	s.quotaMu.Unlock()
}

// Quota returns the configured limit and the current usage.
func (s *Store) Quota() (limit, used int64) {
	s.quotaMu.Lock()
	limit = s.quota.limit
	s.quotaMu.Unlock()
	s.mu.Lock()
	for _, m := range s.meta {
		used += m.Size
	}
	s.mu.Unlock()
	return limit, used
}

func newOpID() string {
	var b [8]byte
	_, _ = io.ReadFull(rand.Reader, b[:])
	return hex.EncodeToString(b[:])
}

func (s *Store) journalPath() string { return filepath.Join(s.root, reclaimLogFile) }
func (s *Store) pendingDir() string  { return filepath.Join(s.root, dirReclaim, "pending") }

func (s *Store) writePendingLocked(op *reclaimOp) error {
	if err := os.MkdirAll(s.pendingDir(), 0o755); err != nil {
		return err
	}
	path := filepath.Join(s.pendingDir(), op.OpID+".json")
	raw, err := json.Marshal(op)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return s.fsyncDir(filepath.Join(s.root, dirReclaim))
}

func (s *Store) fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *Store) removePendingLocked(opID string) error {
	return os.Remove(filepath.Join(s.pendingDir(), opID+".json"))
}

// appendAudit appends one committed reclaim entry to the JSONL audit log.
func (s *Store) appendAuditLocked(e ReclaimEntry) error {
	f, err := os.OpenFile(s.journalPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

// ReclaimLog returns all committed audit entries, oldest first.
func (s *Store) ReclaimLog() ([]ReclaimEntry, error) {
	raw, err := os.ReadFile(s.journalPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []ReclaimEntry
	start := 0
	for i := 0; i <= len(raw); i++ {
		if i == len(raw) || raw[i] == '\n' {
			line := raw[start:i]
			start = i + 1
			if len(line) == 0 {
				continue
			}
			var e ReclaimEntry
			if err := json.Unmarshal(line, &e); err != nil {
				continue
			}
			out = append(out, e)
		}
	}
	return out, nil
}

// reclaimCandidatesLocked returns sealed, complete/archived segments older
// first. Incomplete and currently-open segments are never candidates.
func (s *Store) reclaimCandidatesLocked() []*SegmentInfo {
	var out []*SegmentInfo
	for _, m := range s.meta {
		if m.State == model.BatchComplete || m.State == model.BatchArchived {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].FirstTime.Equal(out[j].FirstTime) {
			return out[i].FirstTime.Before(out[j].FirstTime)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// EnforceQuota removes oldest sealed segments until usage fits the configured
// limit (or nothing eligible remains). Leased (being read/exported), open and
// incomplete segments are never touched, so the returned residual overage may
// be nonzero when only protected segments exist.
func (s *Store) EnforceQuota() (freed int64, reclaimed []ReclaimEntry, residual int64, err error) {
	s.quotaMu.Lock()
	limit := s.quota.limit
	s.quotaMu.Unlock()
	if limit <= 0 {
		_, used := s.Quota()
		return 0, nil, used, ErrNoQuota
	}
	for {
		s.mu.Lock()
		var used int64
		for _, m := range s.meta {
			used += m.Size
		}
		if used <= limit {
			s.mu.Unlock()
			return freed, reclaimed, 0, nil
		}
		cands := s.reclaimCandidatesLocked()
		var victim *SegmentInfo
		for _, m := range cands {
			if !s.leased(m.ID) {
				victim = m
				break
			}
		}
		s.mu.Unlock()
		if victim == nil {
			return freed, reclaimed, used - limit, nil
		}
		entry, rerr := s.reclaimOne(victim, "quota")
		if rerr != nil {
			return freed, reclaimed, 0, rerr
		}
		freed += entry.BytesFreed
		reclaimed = append(reclaimed, entry)
	}
}

// ReclaimSegment removes one explicitly named sealed segment. Open,
// incomplete and leased segments are rejected.
func (s *Store) ReclaimSegment(id, reason string) (ReclaimEntry, error) {
	s.mu.Lock()
	m, ok := s.meta[id]
	if !ok {
		s.mu.Unlock()
		return ReclaimEntry{}, ErrNotFound
	}
	if m.State == model.BatchIncomplete {
		s.mu.Unlock()
		return ReclaimEntry{}, ErrBatchNotComplete
	}
	cp := *m
	s.mu.Unlock()
	return s.reclaimOne(&cp, reason)
}

func (s *Store) reclaimOne(m *SegmentInfo, reason string) (ReclaimEntry, error) {
	if s.leased(m.ID) {
		return ReclaimEntry{}, fmt.Errorf("%w: %s", ErrSegmentBusy, m.ID)
	}
	s.ioMu.Lock()
	defer s.ioMu.Unlock()
	if s.leased(m.ID) {
		return ReclaimEntry{}, fmt.Errorf("%w: %s", ErrSegmentBusy, m.ID)
	}
	op := &reclaimOp{
		OpID: newOpID(), SegmentID: m.ID, SessionID: m.SessionID,
		State: string(m.State), BytesFreed: m.Size, StartedAt: time.Now().UTC(),
	}
	s.mu.Lock()
	if err := s.writePendingLocked(op); err != nil {
		s.mu.Unlock()
		return ReclaimEntry{}, err
	}
	path, _ := s.locateLocked(m.ID)
	s.mu.Unlock()

	if path != "" {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return ReclaimEntry{}, err
		}
		if dir := filepath.Dir(path); true {
			_ = s.fsyncDir(dir)
		}
	}

	s.mu.Lock()
	delete(s.meta, m.ID)
	entry := ReclaimEntry{
		OpID: op.OpID, SegmentID: m.ID, SessionID: m.SessionID,
		State: string(m.State), BytesFreed: m.Size, At: time.Now().UTC(),
		Reason: reason, Result: "COMMITTED",
	}
	if err := s.appendAuditLocked(entry); err != nil {
		s.mu.Unlock()
		return entry, err
	}
	if err := s.removePendingLocked(op.OpID); err != nil {
		s.mu.Unlock()
		return entry, err
	}
	if err := s.persistIndexLocked(); err != nil {
		s.mu.Unlock()
		return entry, err
	}
	s.mu.Unlock()
	return entry, nil
}

// reconcileReclaimsLocked resolves PENDING journals left by a crashed
// reclaim. Data file already gone -> commit (audit + index drop); data file
// present -> abort (drop the pending record, nothing was changed).
func (s *Store) reconcileReclaimsLocked() error {
	entries, err := os.ReadDir(s.pendingDir())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		raw, rerr := os.ReadFile(filepath.Join(s.pendingDir(), e.Name()))
		if rerr != nil {
			continue
		}
		var op reclaimOp
		if jerr := json.Unmarshal(raw, &op); jerr != nil {
			continue
		}
		path, _ := s.locateLocked(op.SegmentID)
		if path == "" {
			delete(s.meta, op.SegmentID)
			_ = s.appendAuditLocked(ReclaimEntry{
				OpID: op.OpID, SegmentID: op.SegmentID, SessionID: op.SessionID,
				State: op.State, BytesFreed: op.BytesFreed, At: time.Now().UTC(),
				Reason: "crash-recovery", Result: "COMMITTED",
			})
		}
		_ = os.Remove(filepath.Join(s.pendingDir(), e.Name()))
	}
	return s.persistIndexLocked()
}
