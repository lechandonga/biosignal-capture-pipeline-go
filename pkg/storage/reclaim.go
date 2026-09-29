package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

const (
	dirReclaim = "reclaim"
	dirAudit   = "audit"
	auditFile  = "reclaim.log"
)

// ReclaimEntry is one durable reclaim-journal / audit record.
type ReclaimEntry struct {
	ID        string    `json:"id"`
	SessionID string    `json:"session_id"`
	State     string    `json:"state"` // COMPLETE or ARCHIVED at reclaim time
	Bytes     int64     `json:"bytes"`
	Reason    string    `json:"reason"`
	At        time.Time `json:"at"`
}

func (s *Store) journalPath(id string) string {
	return filepath.Join(s.root, dirReclaim, id+".json")
}

func (s *Store) auditPath() string { return filepath.Join(s.root, dirAudit, auditFile) }

func (s *Store) dataFilePaths(id string) []string {
	return []string{s.batchPath(id), s.archivePath(id)}
}

// segmentBytes reports the current on-disk size of a segment.
func (s *Store) segmentBytes(m *model.SegmentMeta) int64 {
	for _, p := range s.dataFilePaths(m.ID) {
		if fi, err := os.Stat(p); err == nil {
			return fi.Size()
		}
	}
	return 0
}

// UsedBytes sums bytes currently occupied by live segments (data files only).
func (s *Store) UsedBytes() (int64, error) {
	metas, err := s.Segments()
	if err != nil {
		return 0, err
	}
	var total int64
	for _, m := range metas {
		if m.State == model.BatchReclaimed {
			continue
		}
		total += s.segmentBytes(m)
	}
	return total, nil
}

// EnforceQuota reclaims oldest sealed COMPLETE/ARCHIVED segments until used
// bytes are at or below limitBytes. Never touches: the open segment,
// INCOMPLETE segments, or pinned (being exported) segments. All steps are
// journaled before deletion, so a crash mid-reclaim is self-consistent.
func (s *Store) EnforceQuota(limitBytes int64) ([]ReclaimEntry, error) {
	if limitBytes <= 0 {
		return nil, nil
	}
	metas, err := s.Segments()
	if err != nil {
		return nil, err
	}
	candidates := make([]*model.SegmentMeta, 0, len(metas))
	var used int64
	for _, m := range metas {
		if m.State == model.BatchReclaimed {
			continue
		}
		b := s.segmentBytes(m)
		used += b
		eligible := (m.State == model.BatchComplete || m.State == model.BatchArchived)
		if eligible && !s.IsPinned(m.ID) {
			candidates = append(candidates, m)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].FirstTime.Equal(candidates[j].FirstTime) {
			return candidates[i].FirstTime.Before(candidates[j].FirstTime)
		}
		if candidates[i].Index != candidates[j].Index {
			return candidates[i].Index < candidates[j].Index
		}
		return candidates[i].ID < candidates[j].ID
	})
	var done []ReclaimEntry
	for used > limitBytes && len(candidates) > 0 {
		m := candidates[0]
		candidates = candidates[1:]
		// Never reclaim a segment being read/exported.
		if s.IsPinned(m.ID) {
			continue
		}
		b := s.segmentBytes(m)
		s.lifeMu.Lock()
		if s.IsPinned(m.ID) {
			s.lifeMu.Unlock()
			continue
		}
		entry := ReclaimEntry{
			ID: m.ID, SessionID: m.SessionID, State: string(m.State),
			Bytes: b, Reason: "quota", At: time.Now().UTC(),
		}
		if err := s.writeJournal(entry); err != nil {
			s.lifeMu.Unlock()
			return done, err
		}
		removeErr := func() error {
			for _, p := range s.dataFilePaths(m.ID) {
				if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
					return err
				}
			}
			return nil
		}()
		s.lifeMu.Unlock()
		if removeErr != nil {
			_ = os.Remove(s.journalPath(m.ID))
			return done, removeErr
		}
		fsyncDir(filepath.Join(s.root, dirBatches))
		fsyncDir(filepath.Join(s.root, dirArchive))
		m.State = model.BatchReclaimed
		s.setMeta(m.ID, m)
		if err := s.writeSidecar(m); err != nil {
			return done, err
		}
		if err := s.appendAudit(entry); err != nil {
			return done, err
		}
		if err := os.Remove(s.journalPath(m.ID)); err != nil && !os.IsNotExist(err) {
			return done, err
		}
		used -= b
		done = append(done, entry)
	}
	return done, nil
}

func (s *Store) writeJournal(e ReclaimEntry) error {
	if err := os.MkdirAll(filepath.Join(s.root, dirReclaim), 0o755); err != nil {
		return err
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	p := s.journalPath(e.ID)
	if err := os.WriteFile(p+".tmp", raw, 0o644); err != nil {
		return err
	}
	if err := os.Rename(p+".tmp", p); err != nil {
		return err
	}
	fsyncDir(filepath.Join(s.root, dirReclaim))
	return nil
}

func (s *Store) appendAudit(e ReclaimEntry) error {
	if err := os.MkdirAll(filepath.Join(s.root, dirAudit), 0o755); err != nil {
		return err
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(s.auditPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

// ReclaimAudit returns all completed reclaim records, oldest first.
func (s *Store) ReclaimAudit() ([]ReclaimEntry, error) {
	raw, err := os.ReadFile(s.auditPath())
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
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
				return nil, fmt.Errorf("storage: corrupt audit log: %w", err)
			}
			out = append(out, e)
		}
	}
	return out, nil
}

// recoverReclaim finishes or rolls back reclaim journals left by a crash.
// Order guarantees consistency: if the data is gone the tombstone+audit are
// (re)written; if data remains the journal is discarded and nothing was lost.
func (s *Store) recoverReclaim() error {
	dir := filepath.Join(s.root, dirReclaim)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, en := range entries {
		name := en.Name()
		if filepath.Ext(name) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		var e ReclaimEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		dataGone := true
		for _, p := range s.dataFilePaths(e.ID) {
			if _, err := os.Stat(p); err == nil {
				dataGone = false
				break
			}
		}
		if dataGone {
			// Deletion committed before crash: finish metadata + audit.
			if m, err := s.SegmentMeta(e.ID); err == nil {
				m.State = model.BatchReclaimed
				s.setMeta(e.ID, m)
				if err := s.writeSidecar(m); err != nil {
					return err
				}
			}
			existing, _ := s.ReclaimAudit()
			seen := false
			for _, x := range existing {
				if x.ID == e.ID {
					seen = true
					break
				}
			}
			if !seen {
				if err := s.appendAudit(e); err != nil {
					return err
				}
			}
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
