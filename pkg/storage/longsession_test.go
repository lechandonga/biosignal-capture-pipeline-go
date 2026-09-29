package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

func mkCalAt(dev string, seq int64, at time.Time, v float64) model.CalibratedSample {
	return model.CalibratedSample{
		Aligned: model.AlignedSample{
			Device: model.DeviceID(dev), Sensor: "ecg", Kind: model.SignalECG, Seq: seq,
			UniTime: at, Values: []float64{v}, Quality: 0.9,
		},
		CalVersion: "v1", Values: []float64{v * 2}, Baseline: []float64{0},
	}
}

func mkGapAt(seq int64, at time.Time) model.CalibratedSample {
	c := mkCalAt("d1", seq, at, 0)
	c.Aligned.Gap = true
	c.Values = nil
	c.Uncertain = true
	c.Uncertainty = "packet loss gap marker"
	return c
}

// TestSegmentRolling_BoundaryNoLoss verifies that two consecutively sealed
// segments, written in interleaved wall order across sensors, read back as
// one strict global timeline identical to an unsegmented write.
func TestSegmentRolling_BoundaryNoLoss(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	t0 := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)

	// Interleave two sensors so per-sample times are NOT append-ordered.
	makeSeg := func(id string, n []model.CalibratedSample) {
		if err := st.CreateBatch(id, "sess", t0); err != nil {
			t.Fatal(err)
		}
		// write in a shuffled order; seal sorts canonically
		order := []int{0, 2, 1, 3}
		for len(order) < len(n) {
			order = append(order, len(order))
		}
		for _, i := range order {
			if i >= len(n) {
				continue
			}
			if err := st.AppendBatchSample(id, n[i]); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.AppendBatchEvent(id, model.Event{
			Device: "d1", Sensor: "ecg", Kind: model.SignalECG, Level: model.LevelEvent,
			At: n[len(n)-1].Aligned.UniTime.Add(time.Microsecond), RuleID: "r",
		}); err != nil {
			t.Fatal(err)
		}
		if err := st.CompleteBatch(id, t0.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	n1 := []model.CalibratedSample{
		mkCalAt("d1", 1, t0.Add(1*time.Millisecond), 1),
		mkCalAt("d2", 1, t0.Add(2*time.Millisecond), 1),
		mkCalAt("d1", 2, t0.Add(3*time.Millisecond), 2),
		mkCalAt("d2", 2, t0.Add(4*time.Millisecond), 2),
	}
	n2 := []model.CalibratedSample{
		mkCalAt("d1", 3, t0.Add(5*time.Millisecond), 3),
		mkCalAt("d2", 3, t0.Add(6*time.Millisecond), 3),
		mkCalAt("d1", 4, t0.Add(7*time.Millisecond), 4),
		mkCalAt("d2", 4, t0.Add(8*time.Millisecond), 4),
	}
	makeSeg("sess-batch-000001", n1)
	makeSeg("sess-batch-000002", n2)

	f := RangeFilter{From: t0, To: t0.Add(10 * time.Millisecond)}
	var times []time.Time
	var kinds []byte
	var devices []string
	var seqOK int64
	sum, err := st.ExportRange(f, func(r RangeRecord) error {
		times = append(times, r.Time)
		kinds = append(kinds, r.Kind)
		switch r.Kind {
		case recSample:
			devices = append(devices, string(r.Sample.Aligned.Device))
			if string(r.Sample.Aligned.Device) == "d1" {
				seqOK++
				if r.Sample.Aligned.Seq != seqOK {
					t.Fatalf("d1 seq not continuous: want %d got %d", seqOK, r.Sample.Aligned.Seq)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	wantTimes := []time.Time{
		t0.Add(1 * time.Millisecond), t0.Add(2 * time.Millisecond),
		t0.Add(3 * time.Millisecond), t0.Add(4 * time.Millisecond),
		t0.Add(4001 * time.Microsecond),
		t0.Add(5 * time.Millisecond), t0.Add(6 * time.Millisecond),
		t0.Add(7 * time.Millisecond), t0.Add(8 * time.Millisecond),
		t0.Add(8001 * time.Microsecond),
	}
	if len(times) != len(wantTimes) {
		t.Fatalf("got %d records, want %d", len(times), len(wantTimes))
	}
	for i := range wantTimes {
		if !times[i].Equal(wantTimes[i]) {
			t.Fatalf("position %d: got %s want %s (boundary loss/reorder)", i, times[i], wantTimes[i])
		}
	}
	if sum.Samples != 8 || sum.Events != 2 {
		t.Fatalf("counts wrong: %+v", sum)
	}
	if len(sum.CompleteIntervals) == 0 {
		t.Fatalf("expected complete intervals, got %+v", sum)
	}
	t.Logf("SEGMENTS merged=%d samples=%d events=%d complete=%d no-data=%d",
		len(sum.ScannedSegments), sum.Samples, sum.Events,
		len(sum.CompleteIntervals), len(sum.NoData))
}

func buildThreeSegments(t *testing.T, st *Store, t0 time.Time) {
	// seg1: d1 ecg seq1..10, gap marker at seq5
	id1 := "sess-batch-000001"
	if err := st.CreateBatch(id1, "sess", t0); err != nil {
		t.Fatal(err)
	}
	for seq := int64(1); seq <= 10; seq++ {
		at := t0.Add(time.Duration(seq) * 10 * time.Millisecond)
		s := mkCalAt("d1", seq, at, 1)
		if seq == 5 {
			s = mkGapAt(seq, at)
		}
		if err := st.AppendBatchSample(id1, s); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.AppendBatchRejected(id1, model.RejectedRecord{
		Action: model.ActionRejected, Reason: model.ReasonDuplicate,
		Sample: model.RawSample{Device: "d1", Sensor: "ecg", Seq: 7},
		At:     t0.Add(75 * time.Millisecond), Detail: "dup",
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.CompleteBatch(id1, t0.Add(200*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	// seg2: d2 ecg seq1..10 at 1s.. (a no-data gap between 100ms and 1s)
	id2 := "sess-batch-000002"
	if err := st.CreateBatch(id2, "sess", t0); err != nil {
		t.Fatal(err)
	}
	for seq := int64(1); seq <= 10; seq++ {
		if err := st.AppendBatchSample(id2, mkCalAt("d2", seq,
			t0.Add(time.Second).Add(time.Duration(seq)*10*time.Millisecond), 2)); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.CompleteBatch(id2, t0.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	// seg3: open partial segment d1 seq11 at 3s
	id3 := "sess-batch-000003"
	if err := st.CreateBatch(id3, "sess", t0); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendBatchSample(id3, mkCalAt("d1", 11, t0.Add(3*time.Second), 1)); err != nil {
		t.Fatal(err)
	}
}

// TestRangeQuery_CrossSegmentsArchiveAndCompleteness covers cross-segment
// merge over a COMPLETE and an ARCHIVED segment, device filtering, and the
// distinction between complete, incomplete and no-data intervals.
func TestRangeQuery_CrossSegmentsArchiveAndCompleteness(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	t0 := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	buildThreeSegments(t, st, t0)
	if err := st.ArchiveBatch("sess-batch-000002"); err != nil {
		t.Fatal(err)
	}

	f := RangeFilter{From: t0, To: t0.Add(4 * time.Second), Devices: []model.DeviceID{"d1"}}
	var streamed int
	var maxBuf int
	sum, err := st.ExportRange(f, func(r RangeRecord) error {
		streamed++
		// simulate a streaming consumer that keeps no history
		maxBuf++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Samples != 11 {
		t.Fatalf("samples=%d want 11", sum.Samples)
	}
	if sum.Rejected != 1 {
		t.Fatalf("rejected=%d want 1", sum.Rejected)
	}
	if len(sum.ScannedSegments) != 2 {
		t.Fatalf("scanned=%v want d1-bearing seg1+seg3 only", sum.ScannedSegments)
	}
	// No-data interval must exist between 100ms-ish and 1s (d1 timeline).
	var hasNoData bool
	for _, nd := range sum.NoData {
		if nd.From.Equal(t0.Add(100*time.Millisecond+time.Nanosecond)) && nd.To.Equal(t0.Add(3*time.Second)) {
			hasNoData = true
		}
	}
	if !hasNoData {
		t.Fatalf("no-data gap not detected: %+v", sum.NoData)
	}
	var reasons = map[model.RejectReason]bool{}
	for _, inc := range sum.Incomplete {
		reasons[inc.Reason] = true
	}
	if !reasons[model.ReasonPacketLoss] || !reasons[model.ReasonDuplicate] ||
		!reasons[model.ReasonSegmentIncomplete] {
		t.Fatalf("incomplete reasons missing: %+v", reasons)
	}
	t.Logf("RANGE streamed=%d scanned=%v noData=%d incomplete=%v counts=%v",
		streamed, sum.ScannedSegments, len(sum.NoData), reasons, sum.CountsByReason)
	_ = maxBuf

	// Deterministic re-run yields the same order.
	var first []time.Time
	_, _ = st.ExportRange(f, func(r RangeRecord) error { first = append(first, r.Time); return nil })
	var second []time.Time
	_, _ = st.ExportRange(f, func(r RangeRecord) error { second = append(second, r.Time); return nil })
	if len(first) != len(second) {
		t.Fatal("rerun length differs")
	}
	for i := range first {
		if !first[i].Equal(second[i]) {
			t.Fatalf("non-deterministic order at %d", i)
		}
	}

	// Only covering segments are scanned for a narrow range.
	narrow := RangeFilter{From: t0.Add(1050 * time.Millisecond), To: t0.Add(1060 * time.Millisecond)}
	sum2, err := st.ExportRange(narrow, func(r RangeRecord) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(sum2.ScannedSegments) != 1 || sum2.ScannedSegments[0] != "sess-batch-000002" {
		t.Fatalf("narrow query scanned %v, want only seg2", sum2.ScannedSegments)
	}
	t.Logf("NARROW scanned only=%v (no full-store scan)", sum2.ScannedSegments)
}

func sealSeg(t *testing.T, st *Store, id string, dev model.DeviceID, fromSeq int64, n int, t0 time.Time) {
	t.Helper()
	if err := st.CreateBatch(id, "sess", t0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		seq := fromSeq + int64(i)
		at := t0.Add(time.Duration(seq) * time.Millisecond)
		if err := st.AppendBatchSample(id, mkCalAt(string(dev), seq, at, float64(seq))); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.CompleteBatch(id, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
}

// TestQuota_ReclaimsOldestOnly verifies oldest-first reclamation, pin
// protection, audit trail, and that queries mark reclaimed spans incomplete.
func TestQuota_ReclaimsOldestOnly(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	t0 := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)
	sealSeg(t, st, "sess-batch-000001", "d1", 1, 20, t0)
	sealSeg(t, st, "sess-batch-000002", "d1", 101, 20, t0.Add(time.Hour))
	sealSeg(t, st, "sess-batch-000003", "d1", 201, 20, t0.Add(2*time.Hour))
	// open segment must be immune
	if err := st.CreateBatch("sess-batch-000004", "sess", t0); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendBatchSample("sess-batch-000004",
		mkCalAt("d1", 301, t0.Add(3*time.Hour), 1)); err != nil {
		t.Fatal(err)
	}

	before, _ := st.UsedBytes()
	// Pin seg2 during enforcement: it must survive.
	st.Pin("sess-batch-000002")
	oneSeg, _ := st.SegmentMeta("sess-batch-000001")
	// Allow three sealed segments minus a bit: only oldest seg1 must go.
	done, err := st.EnforceQuota(before - oneSeg.Bytes + 100)
	if err != nil {
		t.Fatal(err)
	}
	st.Unpin("sess-batch-000002")
	if len(done) == 0 {
		t.Fatal("expected reclaim events")
	}
	if done[0].ID != "sess-batch-000001" {
		t.Fatalf("oldest must go first, got %s", done[0].ID)
	}
	for _, e := range done {
		if e.ID == "sess-batch-000002" || e.ID == "sess-batch-000004" {
			t.Fatalf("protected segment reclaimed: %s", e.ID)
		}
	}
	if _, err := osStat(st.batchPath("sess-batch-000001")); !os.IsNotExist(err) {
		t.Fatalf("reclaimed data file must be gone, err=%v", err)
	}
	if _, err := osStat(st.partialPath("sess-batch-000004")); err != nil {
		t.Fatal("open segment must be untouched")
	}
	audit, err := st.ReclaimAudit()
	if err != nil {
		t.Fatal(err)
	}
	if len(audit) != len(done) {
		t.Fatalf("audit=%d events=%d", len(audit), len(done))
	}
	var freed int64
	for _, a := range audit {
		freed += a.Bytes
	}
	if freed <= 0 {
		t.Fatal("audit must report freed bytes")
	}
	t.Logf("RECLAIM events=%d freed=%d bytes audit=%+v", len(done), freed, audit)

	// Query across the reclaimed span: flagged incomplete, no phantom data.
	f := RangeFilter{From: t0, To: t0.Add(4 * time.Hour)}
	sum, err := st.ExportRange(f, func(r RangeRecord) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	var sawReclaim bool
	for _, inc := range sum.Incomplete {
		if inc.Reason == model.ReasonSegmentReclaimed {
			sawReclaim = true
		}
	}
	if !sawReclaim {
		t.Fatalf("reclaimed span must be reported incomplete: %+v", sum.Incomplete)
	}
	if sum.Samples != 41 { // seg2 20 + seg3 20 + open seg4 1
		t.Fatalf("samples=%d want 41 (seg1 gone)", sum.Samples)
	}
}

func osStat(p string) (os.FileInfo, error) { return os.Stat(p) }

// TestReclaim_CrashMidway simulates a crash after the journal exists but
// before (a) data removal and (b) after data removal, and checks reopen is
// self-consistent in both cases.
func TestReclaim_CrashMidway(t *testing.T) {
	t0 := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)

	// Case A: journal present but data still on disk -> rollback, no loss.
	dir := t.TempDir()
	st, _ := Open(dir)
	sealSeg(t, st, "sess-batch-000001", "d1", 1, 5, t0)
	_ = st.Close()
	m, _ := st.SegmentMeta("sess-batch-000001")
	journal := ReclaimEntry{ID: "sess-batch-000001", SessionID: "sess",
		State: "COMPLETE", Bytes: m.Bytes, Reason: "quota", At: time.Now()}
	if err := st.writeJournal(journal); err != nil {
		t.Fatal(err)
	}
	st2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := st2.LoadBatch("sess-batch-000001")
	if err != nil {
		t.Fatalf("rollback must preserve data: %v", err)
	}
	if b.State != model.BatchComplete || len(b.Samples) != 5 {
		t.Fatalf("unexpected rolled-back batch: %s n=%d", b.State, len(b.Samples))
	}
	if _, err := os.Stat(st2.journalPath("sess-batch-000001")); !os.IsNotExist(err) {
		t.Fatal("journal must be cleared after rollback")
	}
	audit, _ := st2.ReclaimAudit()
	if len(audit) != 0 {
		t.Fatalf("rolled-back reclaim must not hit audit, got %d", len(audit))
	}

	// Case B: data removed but tombstone/audit unfinished -> complete it.
	if err := os.Remove(st2.batchPath("sess-batch-000001")); err != nil {
		t.Fatal(err)
	}
	if err := st2.writeJournal(journal); err != nil {
		t.Fatal(err)
	}
	st3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	m3, err := st3.SegmentMeta("sess-batch-000001")
	if err != nil {
		t.Fatal(err)
	}
	if m3.State != model.BatchReclaimed {
		t.Fatalf("state=%s want RECLAIMED", m3.State)
	}
	audit3, _ := st3.ReclaimAudit()
	if len(audit3) != 1 || audit3[0].Bytes != m.Bytes {
		t.Fatalf("audit mismatch: %+v", audit3)
	}
	if _, err := os.Stat(st3.journalPath("sess-batch-000001")); !os.IsNotExist(err) {
		t.Fatal("journal must be cleared after completion")
	}
	t.Logf("CRASH RECOVERY rollback=5 samples intact; commit-after-removal audit=%d freed=%d",
		len(audit3), audit3[0].Bytes)
}

// TestConcurrent_WriteQueryArchiveReclaim runs all lifecycle operations at
// once and requires every read to see frame-intact, ordered segments.
func TestConcurrent_WriteQueryArchiveReclaim(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	t0 := time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, 16)

	// Writer: seal a fresh segment every ~30 samples.
	wg.Add(1)
	go func() {
		defer wg.Done()
		seg := 1
		var seq int64
		for {
			select {
			case <-stop:
				return
			default:
			}
			id := fmt.Sprintf("sess-batch-%06d", seg)
			if err := st.CreateBatch(id, "sess", t0); err != nil {
				errCh <- err
				return
			}
			for k := 0; k < 30; k++ {
				seq++
				if err := st.AppendBatchSample(id, mkCalAt("d1", seq,
					t0.Add(time.Duration(seq)*time.Millisecond), float64(seq))); err != nil {
					errCh <- err
					return
				}
			}
			if err := st.CompleteBatch(id, time.Now()); err != nil {
				errCh <- err
				return
			}
			seg++
		}
	}()

	// Querier: repeatedly export a sliding range; order must be non-decreasing.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			var last time.Time
			_, err := st.ExportRange(RangeFilter{From: t0, To: t0.Add(time.Hour)},
				func(r RangeRecord) error {
					if r.Time.Before(last) {
						return fmt.Errorf("out-of-order export: %s after %s", r.Time, last)
					}
					last = r.Time
					return nil
				})
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Maintainer: archive aged ones (all) and enforce quota repeatedly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			metas, _ := st.Segments()
			for _, mm := range metas {
				if mm.State == model.BatchComplete {
					_ = st.ArchiveBatch(mm.ID)
				}
			}
			used, _ := st.UsedBytes()
			_, _ = st.EnforceQuota(used * 8 / 10)
			time.Sleep(2 * time.Millisecond)
			if i > 500 {
				return
			}
		}
	}()

	time.Sleep(120 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	t.Logf("CONCURRENT lifecycle stress finished without torn/shifted reads")
}

// TestLegacySegments_QueryWithoutMigration builds segments with the OLD
// on-disk format (plain frames, no footer/index) and proves range queries
// still read them with identical semantics.
func TestLegacySegments_QueryWithoutMigration(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	t0 := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	id := "sess-old-batch-000007"
	path := st.batchPath(id)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	hdr, _ := json.Marshal(jsonHeader{ID: id, SessionID: "sess-old", StartedAt: t0})
	if err := writeFrame(f, append([]byte{recHeader}, hdr...)); err != nil {
		t.Fatal(err)
	}
	for seq := int64(1); seq <= 10; seq++ {
		js := sampleToJSON(&[]model.CalibratedSample{mkCalAt("d1", seq,
			t0.Add(time.Duration(seq)*time.Millisecond), 1)}[0])
		raw, _ := json.Marshal(js)
		if err := writeFrame(f, append([]byte{recSample}, raw...)); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	sum, err := st.ExportRange(RangeFilter{From: t0, To: t0.Add(time.Second)},
		func(r RangeRecord) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if sum.Samples != 10 {
		t.Fatalf("legacy samples=%d want 10 (no migration required)", sum.Samples)
	}
	if len(sum.ScannedSegments) != 1 || sum.ScannedSegments[0] != id {
		t.Fatalf("legacy segment not discovered: %v", sum.ScannedSegments)
	}
	m, err := st.SegmentMeta(id)
	if err != nil {
		t.Fatal(err)
	}
	if m.SessionID != "sess-old" || m.State != model.BatchComplete {
		t.Fatalf("legacy meta wrong: %+v", m)
	}
	t.Logf("LEGACY read %d samples from footer-less segment %s (zero migration)",
		sum.Samples, id)
}
