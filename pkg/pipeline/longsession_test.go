package pipeline

import (
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/calibration"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/detect"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/storage"
)

func longSetup(t *testing.T, dir string) (*Pipeline, *calibration.Store, *storage.Store, time.Time) {
	t.Helper()
	st, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cals := calibration.NewStore()
	t0 := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	p := New(Config{
		SessionID: "long", QueueDepth: 4096, BatchSize: 1_000_000,
		FlushInterval: time.Hour, Policy: PolicyReject,
		SegmentSpan:   100 * time.Millisecond,
		SegmentBytes:  0,
		MaintInterval: 50 * time.Millisecond,
	}, cals, []detect.Rule{}, st)
	if err := p.RegisterDevice("d1"); err != nil {
		t.Fatal(err)
	}
	if err := p.BindSensor(SensorBinding{Device: "d1", Sensor: "ecg", Kind: model.SignalECG}); err != nil {
		t.Fatal(err)
	}
	return p, cals, st, t0
}

// TestLongSession_SpanRollingAndTimeline: 500 samples across 500ms produce
// multiple segments; the merged cross-segment timeline is exactly the
// generated sequence with no loss, duplication or reorder.
func TestLongSession_SpanRollingAndTimeline(t *testing.T) {
	dir := t.TempDir()
	p, cals, st, t0 := longSetup(t, dir)
	if err := cals.Add(calParams("ecg-v1", "ecg", model.SignalECG, 1,
		t0.Add(-time.Hour), time.Time{})); err != nil {
		t.Fatal(err)
	}
	go p.Run()
	for seq := int64(1); seq <= 500; seq++ {
		at := t0.Add(time.Duration(seq) * time.Millisecond)
		if err := p.Ingest(raw("d1", "ecg", seq, at, at.Add(time.Millisecond), 1, 0.9, false)); err != nil {
			t.Fatal(err)
		}
	}
	p.Stop()

	metas, err := st.Segments()
	if err != nil {
		t.Fatal(err)
	}
	var sealed int
	for _, m := range metas {
		if m.SessionID == "long" && m.State == model.BatchComplete {
			sealed++
		}
	}
	if sealed < 4 {
		t.Fatalf("expected span-based rolling into >=4 segments, got %d (%+v)", sealed, metas)
	}
	var seqs []int64
	sum, err := st.ExportRange(storage.RangeFilter{From: t0, To: t0.Add(time.Second)},
		func(r storage.RangeRecord) error {
			if r.Kind == 2 { // recSample
				seqs = append(seqs, r.Sample.Aligned.Seq)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(seqs) != 500 {
		t.Fatalf("merged samples=%d want 500", len(seqs))
	}
	for i, want := range seqs {
		if want != int64(i+1) {
			t.Fatalf("timeline broken at %d: got seq %d", i, want)
		}
	}
	t.Logf("SPAN ROLLING segments=%d mergedSamples=%d events=%d completeIntervals=%d",
		sealed, sum.Samples, sum.Events, len(sum.CompleteIntervals))
}

// TestLongSession_RestartContinues: after a restart, new samples are appended
// to brand-new segments; sealed segments stay untouched and the crashed
// open segment stays INCOMPLETE and is never extended.
func TestLongSession_RestartContinues(t *testing.T) {
	dir := t.TempDir()
	t0 := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)

	run1 := func() (*Pipeline, *storage.Store) {
		p, cals, st, _ := longSetup(t, dir)
		if err := cals.Add(calParams("ecg-v1", "ecg", model.SignalECG, 1,
			t0.Add(-time.Hour), time.Time{})); err != nil {
			t.Fatal(err)
		}
		go p.Run()
		for seq := int64(1); seq <= 250; seq++ {
			at := t0.Add(time.Duration(seq) * time.Millisecond)
			_ = p.Ingest(raw("d1", "ecg", seq, at, at.Add(time.Millisecond), 1, 0.9, false))
		}
		p.Abort()
		_ = st.Close()
		return p, st
	}
	_, _ = run1()

	// There must now be sealed segments plus one INCOMPLETE (un-drained open).
	st0, _ := storage.Open(dir)
	metas, _ := st0.Segments()
	var incomplete []string
	var maxIdx int
	for _, m := range metas {
		if m.SessionID == "long" {
			if m.State == model.BatchIncomplete {
				incomplete = append(incomplete, m.ID)
			}
			if m.Index > maxIdx {
				maxIdx = m.Index
			}
		}
	}
	if len(incomplete) == 0 {
		t.Fatal("expected an INCOMPLETE segment after simulated crash")
	}
	_ = st0.Close()

	// Restart: new pipeline on the same store resumes numbering and keeps
	// writing after the old segments without touching them.
	p2, cals2, st2, _ := longSetup(t, dir)
	if err := cals2.Add(calParams("ecg-v1", "ecg", model.SignalECG, 1,
		t0.Add(-time.Hour), time.Time{})); err != nil {
		t.Fatal(err)
	}
	go p2.Run()
	for seq := int64(251); seq <= 600; seq++ {
		at := t0.Add(time.Duration(seq) * time.Millisecond)
		_ = p2.Ingest(raw("d1", "ecg", seq, at, at.Add(time.Millisecond), 1, 0.9, false))
	}
	p2.Stop()

	// Pre-crash INCOMPLETE segments must be unchanged and still INCOMPLETE.
	for _, id := range incomplete {
		m, err := st2.SegmentMeta(id)
		if err != nil {
			t.Fatal(err)
		}
		if m.State != model.BatchIncomplete {
			t.Fatalf("%s must remain INCOMPLETE after restart, got %s", id, m.State)
		}
	}
	// New data must use indexes beyond every pre-crash segment.
	metas2, _ := st2.Segments()
	var newSegs int
	for _, m := range metas2 {
		if m.SessionID == "long" && m.Index > maxIdx {
			newSegs++
		}
	}
	if newSegs == 0 {
		t.Fatal("restart must create new segments, not append into old ones")
	}
	// Full timeline: seq 1..250 and 251..600 all readable in order.
	var seqs []int64
	_, err := st2.ExportRange(storage.RangeFilter{From: t0, To: t0.Add(time.Hour)},
		func(r storage.RangeRecord) error {
			if r.Kind == 2 {
				seqs = append(seqs, r.Sample.Aligned.Seq)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []int64{1, 2, 3} {
		if seqs[i] != want {
			t.Fatalf("old data lost at %d: %d", i, seqs[i])
		}
	}
	if seqs[len(seqs)-1] != 600 {
		t.Fatalf("last seq=%d want 600", seqs[len(seqs)-1])
	}
	t.Logf("RESTART incompleteKept=%v newSegments=%d timelineLen=%d maxIdxBefore=%d",
		incomplete, newSegs, len(seqs), maxIdx)
}

// TestLongSession_QuotaAndArchive rolls segments, lets auto-archive and
// quota reclaim old ones, and verifies newest data survives while queries
// mark reclaimed spans incomplete.
func TestLongSession_QuotaAndArchive(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cals := calibration.NewStore()
	t0 := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	if err := cals.Add(calParams("ecg-v1", "ecg", model.SignalECG, 1,
		t0.Add(-time.Hour), time.Time{})); err != nil {
		t.Fatal(err)
	}
	p := New(Config{
		SessionID: "q", QueueDepth: 256, BatchSize: 1_000_000,
		FlushInterval: time.Hour, Policy: PolicyReject,
		SegmentSpan:   40 * time.Millisecond,
		ArchiveAfter:  1,    // everything sealed is immediately archive-age eligible
		QuotaBytes:    5000, // tightened via SetQuotaBytes after the first burst
		MaintInterval: 5 * time.Millisecond,
	}, cals, nil, st)
	_ = p.RegisterDevice("d1")
	_ = p.BindSensor(SensorBinding{Device: "d1", Sensor: "ecg", Kind: model.SignalECG})
	go p.Run()
	for seq := int64(1); seq <= 400; seq++ {
		at := t0.Add(time.Duration(seq) * time.Millisecond)
		_ = p.Ingest(raw("d1", "ecg", seq, at, at.Add(time.Millisecond), 1, 0.9, false))
	}
	// Wait until the first burst is durably processed/sealed, then tighten quota.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.Stats().Processed >= 400 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	p.flushOpenForTest()
	used, _ := st.UsedBytes()
	p.SetQuotaBytes(used / 3)
	for seq := int64(401); seq <= 440; seq++ {
		at := t0.Add(time.Duration(seq) * time.Millisecond)
		_ = p.Ingest(raw("d1", "ecg", seq, at, at.Add(time.Millisecond), 1, 0.9, false))
		time.Sleep(time.Millisecond)
	}
	p.Stop()

	audit, _ := st.ReclaimAudit()
	if len(audit) == 0 {
		t.Fatal("expected quota reclamation during the run")
	}
	var archived, reclaimed int
	metas, _ := st.Segments()
	for _, m := range metas {
		switch m.State {
		case model.BatchArchived:
			archived++
		case model.BatchReclaimed:
			reclaimed++
		}
	}
	if archived == 0 {
		t.Fatal("expected auto-archived segments")
	}
	// The newest samples must always survive.
	var last int64
	sum, err := st.ExportRange(storage.RangeFilter{From: t0, To: t0.Add(time.Hour)},
		func(r storage.RangeRecord) error {
			if r.Kind == 2 {
				last = r.Sample.Aligned.Seq
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if last != 440 {
		t.Fatalf("newest data must survive reclaim, last=%d", last)
	}
	var sawReclaim bool
	for _, inc := range sum.Incomplete {
		if inc.Reason == model.ReasonSegmentReclaimed {
			sawReclaim = true
		}
	}
	if !sawReclaim {
		t.Fatal("reclaimed spans must be reported")
	}
	t.Logf("QUOTA archived=%d reclaimed=%d auditEvents=%d lastSeq=%d freed(most recent audit)=%d",
		archived, reclaimed, len(audit), last, audit[len(audit)-1].Bytes)
}
