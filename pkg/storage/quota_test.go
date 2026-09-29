package storage

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

func sealSegment(t *testing.T, st *Store, id, sess string, n int64, t0 time.Time) int64 {
	t.Helper()
	if err := st.CreateBatch(id, sess, t0); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < n; i++ {
		c := mkCal(i, float64(i))
		c.Aligned.UniTime = t0.Add(time.Duration(i) * time.Millisecond)
		if err := st.AppendBatchSample(id, c); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.CompleteBatch(id, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	size := st.meta[id].Size
	st.mu.Unlock()
	return size
}

// TestQuota_OldestFirstReclaim verifies oldest-sealed-first eviction, that
// open/incomplete segments are protected, and the audit log records every
// reclaimed segment and freed bytes.
func TestQuota_OldestFirstReclaim(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	t0 := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)

	s1 := sealSegment(t, st, "q-batch-000001", "q", 5, t0)
	s2 := sealSegment(t, st, "q-batch-000002", "q", 5, t0.Add(time.Hour))
	s3 := sealSegment(t, st, "q-batch-000003", "q", 5, t0.Add(2*time.Hour))

	// An open segment must never be reclaimed regardless of pressure.
	if err := st.CreateBatch("q-batch-000004", "q", t0.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	_ = st.AppendBatchSample("q-batch-000004", mkCal(1, 1))
	st.mu.Lock()
	openSize := st.meta["q-batch-000004"].Size
	st.mu.Unlock()

	// Open segment is protected and counts toward usage; the limit forces
	// exactly one sealed eviction.
	st.SetQuota(s2 + s3 + openSize)
	freed, entries, residual, err := st.EnforceQuota()
	if err != nil {
		t.Fatal(err)
	}
	if residual != 0 || freed != s1 || len(entries) != 1 || entries[0].SegmentID != "q-batch-000001" {
		t.Fatalf("reclaim wrong: freed=%d residual=%d entries=%+v", freed, residual, entries)
	}
	if _, err := st.LoadBatch("q-batch-000001"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("oldest should be gone, got %v", err)
	}
	if _, err := st.LoadBatch("q-batch-000004"); err != nil {
		t.Fatalf("open segment must survive: %v", err)
	}
	log, err := st.ReclaimLog()
	if err != nil {
		t.Fatal(err)
	}
	if len(log) != 1 || log[0].BytesFreed != s1 || log[0].Result != "COMMITTED" {
		t.Fatalf("audit log wrong: %+v", log)
	}
	// Audit survives restart.
	_ = st.Close()
	st2, _ := Open(dir)
	log2, _ := st2.ReclaimLog()
	if len(log2) != 1 {
		t.Fatalf("audit log lost across restart: %+v", log2)
	}
	if _, err := st2.LoadBatch("q-batch-000001"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("index must not resurrect reclaimed data: %v", err)
	}
}

// TestQuota_IncompleteNotEvicted verifies that when only protected segments
// remain, EnforceQuota reports residual overage rather than deleting them.
func TestQuota_IncompleteNotEvicted(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	_ = st.CreateBatch("open1", "s", time.Now())
	_ = st.AppendBatchSample("open1", mkCal(1, 1))
	st.SetQuota(1)
	freed, _, residual, err := st.EnforceQuota()
	if err != nil {
		t.Fatal(err)
	}
	if freed != 0 || residual <= 0 {
		t.Fatalf("open segment must not be evicted: freed=%d residual=%d", freed, residual)
	}
	if _, err := os.Stat(st.partialPath("open1")); err != nil {
		t.Fatalf("partial removed: %v", err)
	}
}

// TestQuota_ReclaimCrashRecovery simulates each crash point of a reclaim:
// PENDING journal present with data file removed (commit) and with data file
// intact (abort, nothing lost).
func TestQuota_ReclaimCrashRecovery(t *testing.T) {
	t.Run("data already removed", func(t *testing.T) {
		dir := t.TempDir()
		st, _ := Open(dir)
		size := sealSegment(t, st, "c-batch-000001", "c", 3, time.Now())
		entry, err := st.ReclaimSegment("c-batch-000001", "quota")
		if err != nil {
			t.Fatal(err)
		}
		// Fabricate a PENDING record that references an already-gone file,
		// as if the process died after os.Remove but before cleanup.
		st.mu.Lock()
		op := &reclaimOp{
			OpID: "deadbeef", SegmentID: "c-batch-000002", SessionID: "c",
			State: string(model.BatchComplete), BytesFreed: size,
			StartedAt: time.Now(),
		}
		if err := st.writePendingLocked(op); err != nil {
			t.Fatal(err)
		}
		st.mu.Unlock()
		_ = st.Close()

		st2, _ := Open(dir)
		if _, err := st2.LoadBatch("c-batch-000002"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("pending-for-missing file must resolve committed: %v", err)
		}
		log, _ := st2.ReclaimLog()
		found := false
		for _, e := range log {
			if e.OpID == "deadbeef" && e.Reason == "crash-recovery" {
				found = true
			}
		}
		if !found {
			t.Fatalf("crash-recovery audit missing: %+v", log)
		}
		_ = entry
	})

	t.Run("data still present aborts", func(t *testing.T) {
		dir := t.TempDir()
		st, _ := Open(dir)
		_ = sealSegment(t, st, "c-batch-000001", "c", 3, time.Now())
		st.mu.Lock()
		op := &reclaimOp{
			OpID: "abort1234", SegmentID: "c-batch-000001", SessionID: "c",
			State: string(model.BatchComplete), StartedAt: time.Now(),
		}
		if err := st.writePendingLocked(op); err != nil {
			t.Fatal(err)
		}
		st.mu.Unlock()
		_ = st.Close()

		st2, _ := Open(dir)
		b, err := st2.LoadBatch("c-batch-000001")
		if err != nil {
			t.Fatalf("aborted reclaim must leave data readable: %v", err)
		}
		if b.State != model.BatchComplete || len(b.Samples) != 3 {
			t.Fatalf("aborted reclaim data wrong: state=%s n=%d", b.State, len(b.Samples))
		}
		log, _ := st2.ReclaimLog()
		for _, e := range log {
			if e.OpID == "abort1234" {
				t.Fatalf("aborted reclaim must not be audited as committed: %+v", e)
			}
		}
		// Pending journal cleared, future reclaim works normally.
		if _, err := st2.ReclaimSegment("c-batch-000001", "quota"); err != nil {
			t.Fatal(err)
		}
	})
}
