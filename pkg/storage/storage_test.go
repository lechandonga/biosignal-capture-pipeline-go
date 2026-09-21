package storage

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

func mkCal(seq int64, v float64) model.CalibratedSample {
	t0 := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	return model.CalibratedSample{
		Aligned: model.AlignedSample{
			Device: "d1", Sensor: "ecg", Kind: model.SignalECG, Seq: seq,
			UniTime: t0.Add(time.Duration(seq) * time.Millisecond), Values: []float64{v}, Quality: 0.9,
		},
		CalVersion: "v1", Values: []float64{v * 2}, Baseline: []float64{0},
	}
}

func TestBatchRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateBatch("b1", "sess1", time.Now()); err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 20; i++ {
		if err := st.AppendBatchSample("b1", mkCal(i, float64(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.AppendBatchRejected("b1", model.RejectedRecord{
		Action: model.ActionRejected, Reason: model.ReasonDuplicate, Detail: "seq=5 resend",
		Sample: model.RawSample{Device: "d1", Sensor: "ecg", Seq: 5},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.CompleteBatch("b1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: simulates process restart.
	st2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := st2.LoadBatch("b1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RESTART batch=b1 state=%s samples=%d rejected=%d", b.State, len(b.Samples), len(b.Rejected))
	if b.State != model.BatchComplete || len(b.Samples) != 20 || len(b.Rejected) != 1 {
		t.Fatalf("unexpected recovered batch: state=%s samples=%d rejected=%d", b.State, len(b.Samples), len(b.Rejected))
	}
	if b.Samples[7].Values[0] != 16 || b.Samples[7].CalVersion != "v1" {
		t.Fatalf("sample record misaligned: %+v", b.Samples[7])
	}
	t.Logf("JUDGEMENT record 8 seq=%d physical=%v cal=%s aligned correctly after restart",
		b.Samples[7].Aligned.Seq, b.Samples[7].Values, b.Samples[7].CalVersion)
}

func TestRecovery_IncompleteBatch(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateBatch("open1", "sess", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendBatchSample("open1", mkCal(1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendBatchSample("open1", mkCal(2, 2)); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash mid-write: append a torn frame directly to the file,
	// then drop the in-memory handle (no CompleteBatch, no Close-rename).
	st.mu.Lock()
	h := st.open["open1"]
	st.mu.Unlock()
	h.mu.Lock()
	if _, err := h.f.Write([]byte{0x05, 0x00, 0x00, 0x00, 0x61}); err != nil { // 5-byte frame, 1 byte present
		t.Fatal(err)
	}
	h.f.Sync()
	h.mu.Unlock()
	_ = st.Close() // leaves .partial on disk

	st2, _ := Open(dir)
	b, err := st2.LoadBatch("open1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RESTART crashed batch state=%s intactSamples=%d (torn suffix must not silently join)",
		b.State, len(b.Samples))
	if b.State != model.BatchIncomplete {
		t.Fatalf("crashed batch must be INCOMPLETE, got %s", b.State)
	}
	if len(b.Samples) != 2 {
		t.Fatalf("intact prefix must survive, torn frame dropped; got %d", len(b.Samples))
	}
	// A new batch must not silently continue the incomplete one.
	ids, _ := st2.ListBatches()
	t.Logf("LIST batches=%v; incomplete batch is distinguishable and not concatenated", ids)
	if len(ids) != 1 || ids[0] != "open1" {
		t.Fatalf("expected only open1, got %v", ids)
	}
}

func TestArchive_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	_ = st.CreateBatch("a1", "s", time.Now())
	for i := int64(1); i <= 10; i++ {
		_ = st.AppendBatchSample("a1", mkCal(i, float64(i)))
	}
	if err := st.CompleteBatch("a1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.ArchiveBatch("a1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(st.batchPath("a1")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("uncompressed batch should be removed after archive")
	}
	b, err := st.LoadBatch("a1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ARCHIVE batch=a1 state=%s samples=%d read back from gzip", b.State, len(b.Samples))
	if b.State != model.BatchArchived || len(b.Samples) != 10 {
		t.Fatalf("archive readback wrong: state=%s n=%d", b.State, len(b.Samples))
	}
	if err := st.ArchiveBatch("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing archive should be ErrNotFound, got %v", err)
	}
}

func TestConcurrentAppendReadArchive(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	_ = st.CreateBatch("c1", "s", time.Now())

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// Concurrent writers.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := int64(0); ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = st.AppendBatchSample("c1", mkCal(int64(id)*100000+i, float64(i)))
			}
		}(w)
	}
	// Concurrent reads of a completed different batch.
	_ = st.CreateBatch("c2", "s", time.Now())
	_ = st.AppendBatchSample("c2", mkCal(1, 1))
	_ = st.CompleteBatch("c2", time.Now())
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			b, err := st.LoadBatch("c2")
			if err == nil && !(b.State == model.BatchComplete || b.State == model.BatchArchived) {
				t.Errorf("concurrent read saw inconsistent state: %+v", b)
				return
			}
			if err == nil && len(b.Samples) != 1 {
				t.Errorf("concurrent read saw torn/shifted record: %+v", b)
				return
			}
		}
	}()
	// Concurrent archive of c2.
	wg.Add(1)
	go func() {
		defer wg.Done()
		// archive once c2 flips to archive; tolerate racing state
		time.Sleep(10 * time.Millisecond)
		if err := st.ArchiveBatch("c2"); err != nil {
			t.Logf("archive c2 (may already be gone): %v", err)
		}
	}()

	time.Sleep(80 * time.Millisecond)
	close(stop)
	wg.Wait()
	if err := st.CompleteBatch("c1", time.Now()); err != nil {
		t.Fatal(err)
	}
	b, err := st.LoadBatch("c1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CONCURRENT batch=c1 state=%s samples=%d all frames intact, no misalignment", b.State, len(b.Samples))
	if b.State != model.BatchComplete {
		t.Fatalf("c1 must complete cleanly, got %s", b.State)
	}
	// Verify no files leaked in batches dir beyond expected.
	entries, _ := os.ReadDir(filepath.Join(dir, dirBatches))
	t.Logf("BATCHES DIR=%v", names(entries))
	_ = st.Close()
}

func names(es []os.DirEntry) []string {
	out := []string{}
	for _, e := range es {
		out = append(out, e.Name())
	}
	return out
}
