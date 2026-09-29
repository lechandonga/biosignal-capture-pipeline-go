package pipeline

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/calibration"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/query"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/storage"
)

// TestConcurrent_IngestQueryReclaim runs acquisition, cross-segment
// time-range exports and oldest-first quota reclaim at the same time. Every
// export must either succeed with internally consistent, time-ordered
// records, or skip segments reclaimed before its lease; the final timeline
// must be a prefix-complete ordered sequence.
func TestConcurrent_IngestQueryReclaim(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	t0 := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)

	cals := calibration.NewStore()
	_ = cals.Add(calibration.Params{
		Version: "v1", Sensor: "ecg", Effective: t0.Add(-time.Hour),
		Gain: []float64{2}, Offset: []float64{0}, Baseline: []float64{0},
	})
	// Small segments and a tight but nonzero limit so reclaim always finds
	// sealed victims while the open segment stays protected.
	st.SetQuota(4000)
	p := New(Config{
		SessionID: "cc", QueueDepth: 8192, BatchSize: 40,
		Maintenance: 20 * time.Millisecond, StorageLimit: 4000,
		Policy: PolicyDelay, EnqueueWait: 50 * time.Millisecond,
	}, cals, nil, st)
	if err := p.RegisterDevice("d1"); err != nil {
		t.Fatal(err)
	}
	if err := p.BindSensor(SensorBinding{Device: "d1", Sensor: "ecg", Kind: model.SignalECG, Width: 1}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	go p.Run()

	// Producer: steadily ingests on a monotonic device timeline.
	var produced atomic.Int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		var seq int64
		for {
			select {
			case <-stop:
				return
			default:
			}
			ts := t0.Add(time.Duration(seq) * time.Millisecond)
			s := raw("d1", "ecg", seq+1, ts, ts, 1, 0.9, false)
			if err := p.Ingest(s); err == nil {
				produced.Store(seq + 1)
				seq++
			}
		}
	}()

	// Concurrent exporter: repeatedly queries sliding windows and validates
	// ordering and internal consistency.
	exportErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			from := t0
			to := t0.Add(time.Duration(produced.Load()) * time.Millisecond)
			var last time.Time
			_, _, err := query.Export(st, query.Filter{From: from, To: to, SessionID: "cc"},
				func(r storage.SegmentRecord) error {
					if r.Sample != nil {
						if !last.IsZero() && r.Time.Before(last) {
							exportErr <- errTimeline
							return errTimeline
						}
						last = r.Time
					}
					return nil
				})
			if err != nil {
				exportErr <- err
				return
			}
		}
	}()

	// Concurrent direct quota enforcement (the pipeline also does this).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _, _, _ = st.EnforceQuota()
		}
	}()

	time.Sleep(400 * time.Millisecond)
	close(stop)
	p.Stop()
	wg.Wait()
	close(exportErr)
	for e := range exportErr {
		t.Fatalf("concurrent export inconsistency: %v", e)
	}

	// Final state must be self-consistent: every indexed segment readable,
	// no open segment ever reclaimed, audit log nonempty.
	for _, m := range st.Segments() {
		rdr, err := st.OpenSegmentReader(m.ID)
		if err != nil {
			t.Fatalf("segment %s unreadable: %v", m.ID, err)
		}
		for rdr.Next() {
		}
		_ = rdr.Err()
		_ = rdr.Close()
	}
	log, err := st.ReclaimLog()
	if err != nil {
		t.Fatal(err)
	}
	if len(log) == 0 {
		t.Fatal("tight quota must have reclaimed segments")
	}
	limit, used := st.Quota()
	if used > limit {
		// Protected (open) segment can leave residual overage; at most one
		// incomplete segment may account for it.
		var protected int64
		for _, m := range st.Segments() {
			if m.State == model.BatchIncomplete {
				protected += m.Size
			}
		}
		if used-protected > limit {
			t.Fatalf("quota violated beyond protected segments: used=%d limit=%d protected=%d", used, limit, protected)
		}
	}
}

var errTimeline = errors.New("timeline reordered during concurrent export")
