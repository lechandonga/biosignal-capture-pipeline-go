package pipeline

import (
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/calibration"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/detect"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/query"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/storage"
)

func longSetup(t *testing.T, dir string, batchSize int, maxAge time.Duration, maxBytes int64, limit int64) (*Pipeline, *calibration.Store, *storage.Store, time.Time) {
	t.Helper()
	st, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cals := calibration.NewStore()
	t0 := time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)
	_ = cals.Add(calibration.Params{
		Version: "v1", Sensor: "ecg", Effective: t0.Add(-time.Hour),
		Gain: []float64{2}, Offset: []float64{0}, Baseline: []float64{0},
	})
	p := New(Config{
		SessionID: "long", QueueDepth: 4096, BatchSize: batchSize,
		SegmentMaxAge: maxAge, SegmentMaxBytes: maxBytes, StorageLimit: limit,
		Maintenance: time.Hour, // keep test deterministic; manual quota via seal
		Policy:      PolicyReject,
	}, cals, []detect.Rule{}, st)
	if err := p.RegisterDevice("d1"); err != nil {
		t.Fatal(err)
	}
	if err := p.BindSensor(SensorBinding{Device: "d1", Sensor: "ecg", Kind: model.SignalECG, Width: 1}); err != nil {
		t.Fatal(err)
	}
	return p, cals, st, t0
}

func ingestN(t *testing.T, p *Pipeline, from time.Time, n int, step time.Duration) {
	t.Helper()
	for i := 0; i < n; i++ {
		s := raw("d1", "ecg", int64(i+1), from.Add(time.Duration(i)*step), from.Add(time.Duration(i)*step), 1, 0.9, false)
		if err := p.Ingest(s); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}
}

// TestSegmentation_ByCountAndTime verifies a long session rolls into ordered
// sealed segments; a cross-segment query reproduces the original timeline
// with no lost, duplicated or reordered samples.
func TestSegmentation_ByCountAndTime(t *testing.T) {
	dir := t.TempDir()
	p, _, st, t0 := longSetup(t, dir, 50, 0, 0, 0)
	go p.Run()
	ingestN(t, p, t0, 250, time.Millisecond)
	p.Stop()

	st2 := p.Stats()
	if len(st2.Batches) != 5 {
		t.Fatalf("want 5 sealed segments, got %d: %v", len(st2.Batches), st2.Batches)
	}
	segs := st.SessionSegments("long")
	if len(segs) != 5 {
		t.Fatalf("indexed segments: %d", len(segs))
	}
	for _, m := range segs {
		if m.State != model.BatchComplete {
			t.Fatalf("segment %s not complete: %s", m.ID, m.State)
		}
	}

	// Cross-segment timeline must equal the unsegmented ingest order.
	var times []time.Time
	stats, _, err := query.Export(st, query.Filter{
		From: t0, To: t0.Add(249 * time.Millisecond), SessionID: "long",
	}, func(r storage.SegmentRecord) error {
		if r.Sample != nil {
			times = append(times, r.Time)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Samples != 250 || len(times) != 250 {
		t.Fatalf("lost/dup across segments: stats=%+v got=%d", stats, len(times))
	}
	for i := range times {
		want := t0.Add(time.Duration(i) * time.Millisecond)
		if !times[i].Equal(want) {
			t.Fatalf("timeline reordered at %d: %v want %v", i, times[i], want)
		}
	}

	// Segment selection: a narrow range touches only one segment.
	_, rep, err := query.Export(st, query.Filter{
		From: t0.Add(60 * time.Millisecond), To: t0.Add(60 * time.Millisecond),
		SessionID: "long",
	}, func(r storage.SegmentRecord) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Segments) != 1 {
		t.Fatalf("narrow range must touch 1 segment, got %d", len(rep.Segments))
	}
}

// TestSegmentation_ByBytes verifies the cumulative-size roll condition.
func TestSegmentation_ByBytes(t *testing.T) {
	dir := t.TempDir()
	p, _, st, t0 := longSetup(t, dir, 100000, 0, 300, 0)
	go p.Run()
	ingestN(t, p, t0, 50, time.Millisecond)
	p.Stop()
	segs := st.SessionSegments("long")
	if len(segs) < 3 {
		t.Fatalf("byte-size roll expected multiple segments, got %d", len(segs))
	}
	var total int
	var times []time.Time
	_, _, err := query.Export(st, query.Filter{From: t0, To: t0.Add(time.Hour), SessionID: "long"},
		func(r storage.SegmentRecord) error {
			if r.Sample != nil {
				total++
				times = append(times, r.Time)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if total != 50 {
		t.Fatalf("byte roll lost samples: %d", total)
	}
	for i := 1; i < len(times); i++ {
		if !times[i].After(times[i-1]) {
			t.Fatal("order changed across byte-rolled segments")
		}
	}
}

// TestRestart_ContinuesAppends verifies crash/restart continuation: old
// sealed and incomplete segments are preserved, new data lands in a fresh
// segment with a new id and a contiguous timeline reads back.
func TestRestart_ContinuesAppends(t *testing.T) {
	dir := t.TempDir()
	p1, _, st, t0 := longSetup(t, dir, 100, 0, 0, 0)
	go p1.Run()
	ingestN(t, p1, t0, 100, time.Millisecond) // exactly one seal
	// Now create an unfinished segment by ingesting a few more, then crash
	// without Stop (close store abruptly via the process-restart emulation).
	ingestN2(t, p1, t0.Add(200*time.Millisecond), 3)
	_ = st.Close() // emulate process death mid-segment

	// Restart: new pipeline instance over the same store.
	st2, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	cals := calibration.NewStore()
	_ = cals.Add(calibration.Params{
		Version: "v1", Sensor: "ecg", Effective: t0.Add(-time.Hour),
		Gain: []float64{2}, Offset: []float64{0}, Baseline: []float64{0},
	})
	p2 := New(Config{
		SessionID: "long", QueueDepth: 4096, BatchSize: 100,
		Maintenance: time.Hour, Policy: PolicyReject,
	}, cals, nil, st2)
	p2.Resume()
	if err := p2.RegisterDevice("d1"); err != nil {
		t.Fatal(err)
	}
	if err := p2.BindSensor(SensorBinding{Device: "d1", Sensor: "ecg", Kind: model.SignalECG, Width: 1}); err != nil {
		t.Fatal(err)
	}
	go p2.Run()

	// Old incomplete segment still present with its 3 samples.
	var inc int
	for _, m := range st2.SessionSegments("long") {
		if m.State == model.BatchIncomplete {
			inc++
			if m.Samples != 3 {
				t.Fatalf("unfinished segment samples changed: %d", m.Samples)
			}
		}
	}
	if inc != 1 {
		t.Fatalf("want 1 preserved incomplete segment, got %d", inc)
	}

	// Continuation writes beyond previous ids; the first new segment must be
	// id 3 (batch-000002 was the unfinished one).
	ingestNFrom(t, p2, t0.Add(300*time.Millisecond), 5, 104)
	p2.Stop()

	for _, m := range st2.SessionSegments("long") {
		t.Logf("post-restart segment %s state=%s samples=%d", m.ID, m.State, m.Samples)
		if m.ID == "long-batch-000002" && m.State != model.BatchIncomplete {
			t.Fatalf("new data must not seal the previously unfinished segment: %s", m.State)
		}
	}
	if _, err := st2.LoadBatch("long-batch-000003"); err != nil {
		t.Fatalf("continuation segment 000003 missing: %v", err)
	}

	// Full timeline query: 100 sealed + 3 old partial + 5 new, order intact.
	var times []time.Time
	stats, _, err := query.Export(st2, query.Filter{From: t0, To: t0.Add(time.Hour), SessionID: "long"},
		func(r storage.SegmentRecord) error {
			if r.Sample != nil {
				times = append(times, r.Time)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Samples != 108 {
		t.Fatalf("post-restart timeline samples=%d want 108", stats.Samples)
	}
	// Timelines of the two runs are disjoint ranges, but each must be sorted.
	for i := 1; i < 100; i++ {
		if !times[i].After(times[i-1]) {
			t.Fatal("first-run order changed")
		}
	}
}

func ingestN2(t *testing.T, p *Pipeline, from time.Time, n int) {
	t.Helper()
	// Continue sequences after the first 100.
	for i := 0; i < n; i++ {
		seq := int64(101 + i)
		s := raw("d1", "ecg", seq, from.Add(time.Duration(i)*time.Millisecond),
			from.Add(time.Duration(i)*time.Millisecond), 1, 0.9, false)
		if err := p.Ingest(s); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(150 * time.Millisecond) // let worker persist
}

func ingestNFrom(t *testing.T, p *Pipeline, from time.Time, n, startSeq int) {
	t.Helper()
	for i := 0; i < n; i++ {
		s := raw("d1", "ecg", int64(startSeq+i), from.Add(time.Duration(i)*time.Millisecond),
			from.Add(time.Duration(i)*time.Millisecond), 1, 0.9, false)
		if err := p.Ingest(s); err != nil {
			if errors.Is(err, ErrBackpressure) {
				t.Fatal("unexpected backpressure")
			}
		}
	}
}

// TestSegmentation_ByTime verifies the max wall-clock-age roll condition.
func TestSegmentation_ByTime(t *testing.T) {
	dir := t.TempDir()
	// SegmentMaxAge is measured in wall time since segment open. Use a tiny
	// age and pace ingests so the flush ticker fires between batches.
	p, _, st, t0 := longSetup(t, dir, 100000, 20*time.Millisecond, 0, 0)
	go p.Run()
	for i := 0; i < 6; i++ {
		ts := t0.Add(time.Duration(i) * 15 * time.Millisecond)
		s := raw("d1", "ecg", int64(i+1), ts, ts, 1, 0.9, false)
		if err := p.Ingest(s); err != nil {
			t.Fatal(err)
		}
		time.Sleep(12 * time.Millisecond)
	}
	p.Stop()
	segs := st.SessionSegments("long")
	if len(segs) < 2 {
		t.Fatalf("time-based roll expected >=2 segments, got %d", len(segs))
	}
	for _, m := range segs {
		if m.State != model.BatchComplete {
			t.Fatalf("time-rolled segment %s not sealed: %s", m.ID, m.State)
		}
	}
	var n int
	_, _, err := query.Export(st, query.Filter{From: t0, To: t0.Add(time.Hour), SessionID: "long"},
		func(r storage.SegmentRecord) error {
			if r.Sample != nil {
				n++
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if n != 6 {
		t.Fatalf("time roll lost samples: %d", n)
	}
}
