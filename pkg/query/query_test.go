package query

import (
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/storage"
)

func mkSample(dev model.DeviceID, sn model.SensorID, seq int64, t time.Time, v float64) model.CalibratedSample {
	return model.CalibratedSample{
		Aligned: model.AlignedSample{
			Device: dev, Sensor: sn, Kind: model.SignalECG, Seq: seq,
			UniTime: t, Values: []float64{v}, Quality: 0.9,
		},
		CalVersion: "v1", Values: []float64{v * 2}, Baseline: []float64{0},
	}
}

func writeSeg(t *testing.T, st *storage.Store, id, sess string,
	times []time.Time, dev model.DeviceID, sn model.SensorID, rejects []int, seal bool) {
	t.Helper()
	t0 := times[0]
	if err := st.CreateBatch(id, sess, t0); err != nil {
		t.Fatal(err)
	}
	for i, ts := range times {
		if err := st.AppendBatchSample(id, mkSample(dev, sn, int64(i+1), ts, float64(i))); err != nil {
			t.Fatal(err)
		}
	}
	for _, ri := range rejects {
		if err := st.AppendBatchRejected(id, model.RejectedRecord{
			Action: model.ActionHandled,
			Sample: model.RawSample{Device: dev, Sensor: sn, Seq: int64(ri)},
			Reason: model.ReasonPacketLoss, Detail: "gap",
			At: times[0].Add(time.Duration(ri) * time.Millisecond),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if seal {
		if err := st.CompleteBatch(id, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
}

// TestExport_CrossSegmentOrderAndEquivalence verifies that a range spanning
// multiple segments (one of them archived) yields exactly the same records,
// in the same total order, as the unsegmented stream would.
func TestExport_CrossSegmentOrderAndEquivalence(t *testing.T) {
	dir := t.TempDir()
	st, _ := storage.Open(dir)
	t0 := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)

	// Three timeline segments, interleaved devices; archive the middle one.
	var base []time.Time
	for i := 0; i < 30; i++ {
		base = append(base, t0.Add(time.Duration(i)*time.Second))
	}
	writeSeg(t, st, "e-batch-000001", "e", base[0:10], "d1", "ecg", nil, true)
	writeSeg(t, st, "e-batch-000002", "e", base[10:20], "d2", "ecg", nil, true)
	writeSeg(t, st, "e-batch-000003", "e", base[20:30], "d1", "ecg", nil, true)
	if err := st.ArchiveBatch("e-batch-000002"); err != nil {
		t.Fatal(err)
	}

	f := Filter{From: base[0], To: base[29], SessionID: "e"}
	var got []time.Time
	stats, rep, err := Export(st, f, func(r storage.SegmentRecord) error {
		got = append(got, r.Time)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Samples != 30 {
		t.Fatalf("want 30 samples across segments+archive, got %d", stats.Samples)
	}
	if len(rep.Segments) != 3 {
		t.Fatalf("range query must touch only 3 overlapping segments, got %d", len(rep.Segments))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Before(got[i-1]) {
			t.Fatalf("export order not monotonic at %d", i)
		}
	}
	if len(got) != len(base) {
		t.Fatalf("lost records: got %d want %d", len(got), len(base))
	}

	// Equivalence with the unsegmented whole: re-read all 3 segments and
	// merge in memory; compare the ordered time series.
	var all []time.Time
	for _, id := range []string{"e-batch-000001", "e-batch-000002", "e-batch-000003"} {
		rdr, err := st.OpenSegmentReader(id)
		if err != nil {
			t.Fatal(err)
		}
		for rdr.Next() {
			all = append(all, rdr.Record().Time)
		}
		_ = rdr.Err()
		_ = rdr.Close()
	}
	sortTimes(all)
	if len(all) != len(got) {
		t.Fatalf("equivalence length mismatch %d vs %d", len(all), len(got))
	}
	for i := range all {
		if !all[i].Equal(got[i]) {
			t.Fatalf("equivalence mismatch at %d: %v vs %v", i, all[i], got[i])
		}
	}

	// Sub-range only hits the middle (archived) segment.
	var sub int
	_, subRep, err := Export(st, Filter{From: base[10], To: base[19], SessionID: "e"},
		func(r storage.SegmentRecord) error { sub++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if sub != 10 || len(subRep.Segments) != 1 {
		t.Fatalf("sub-range must touch exactly 1 archived segment, got n=%d segs=%d", sub, len(subRep.Segments))
	}
	if !subRep.IsCompleteAt(base[15]) {
		t.Fatal("archived-only range must be COMPLETE")
	}
}

// TestExport_DeviceSensorFilter verifies filtering and deterministic
// interleaving of samples and rejection records.
func TestExport_DeviceSensorFilter(t *testing.T) {
	dir := t.TempDir()
	st, _ := storage.Open(dir)
	t0 := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	times := []time.Time{t0, t0.Add(time.Second), t0.Add(2 * time.Second), t0.Add(3 * time.Second)}
	writeSeg(t, st, "f-batch-000001", "f", times, "dX", "ecg", []int{2}, true)
	writeSeg(t, st, "f-batch-000002", "f", times, "dY", "ppg", nil, true)

	var devs []model.DeviceID
	stats, _, err := Export(st, Filter{From: t0, To: t0.Add(3 * time.Second), Device: "dX", SessionID: "f"},
		func(r storage.SegmentRecord) error { devs = append(devs, r.Device); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if stats.Samples != 4 || stats.Rejected != 1 {
		t.Fatalf("filter counts wrong: %+v", stats)
	}
	for _, d := range devs {
		if d != "dX" {
			t.Fatalf("foreign device leaked: %s", d)
		}
	}
}

// TestCompleteness_Verdicts verifies COMPLETE / INCOMPLETE / NO_DATA
// sub-ranges and reason-separated summary.
func TestCompleteness_Verdicts(t *testing.T) {
	dir := t.TempDir()
	st, _ := storage.Open(dir)
	t0 := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)

	// Complete segment at [0s,9s] with a packet-loss defect at 4s.
	good := []time.Time{}
	for i := 0; i < 10; i++ {
		good = append(good, t0.Add(time.Duration(i)*time.Second))
	}
	writeSeg(t, st, "g-batch-000001", "g", good, "d1", "ecg", []int{4}, true)

	// Unfinished segment at [20s,23s].
	bad := []time.Time{t0.Add(20 * time.Second), t0.Add(21 * time.Second), t0.Add(23 * time.Second)}
	writeSeg(t, st, "g-batch-000002", "g", bad, "d1", "ecg", nil, false)

	// Clean complete segment at [30s,32s].
	clean := []time.Time{t0.Add(30 * time.Second), t0.Add(31 * time.Second), t0.Add(32 * time.Second)}
	writeSeg(t, st, "g-batch-000003", "g", clean, "d1", "ecg", nil, true)

	from := t0
	to := t0.Add(40 * time.Second)
	rep, err := Completeness(st, Filter{From: from, To: to, SessionID: "g"})
	if err != nil {
		t.Fatal(err)
	}

	var labels []Verdict
	for _, rg := range rep.Ranges {
		labels = append(labels, rg.Verdict)
	}
	want := []Verdict{
		VerdictComplete,   // [0,4)
		VerdictIncomplete, // [4,4] packet loss
		VerdictComplete,   // (4,9]
		VerdictNoData,     // (9,20)
		VerdictIncomplete, // [20,23] unfinished
		VerdictNoData,     // (23,30)
		VerdictComplete,   // [30,32] clean
		VerdictNoData,     // (32,40]
	}
	if len(labels) != len(want) {
		t.Fatalf("range partition wrong: %+v", rep.Ranges)
	}
	for i := range want {
		if labels[i] != want[i] {
			t.Fatalf("range %d verdict=%s want %s; ranges=%+v", i, labels[i], want[i], rep.Ranges)
		}
	}
	if rep.ReasonCounts[model.ReasonPacketLoss] != 1 {
		t.Fatalf("packet-loss reason count wrong: %+v", rep.ReasonCounts)
	}
	// Verify the NO_DATA gap (9s,20s) exists explicitly, distinct from the
	// INCOMPLETE unfinished segment at [20s,23s].
	var noDataGap *Range
	for i := range rep.Ranges {
		rg := rep.Ranges[i]
		if rg.Verdict == VerdictNoData && rg.From.Equal(t0.Add(9*time.Second)) {
			noDataGap = &rep.Ranges[i]
		}
	}
	if noDataGap == nil || !noDataGap.To.Equal(t0.Add(20*time.Second)) {
		t.Fatalf("NO_DATA gap must distinguish 'no data' from incomplete: %+v", rep.Ranges)
	}
	if rep.IsCompleteAt(t0.Add(15 * time.Second)) {
		t.Fatal("NO_DATA instant must not report complete")
	}
	if rep.IsCompleteAt(t0.Add(4 * time.Millisecond)) {
		t.Fatal("loss defect instant must not report complete")
	}
	if !rep.IsCompleteAt(t0.Add(8 * time.Second)) {
		t.Fatal("instant outside the defect must remain COMPLETE")
	}
	if !rep.IsCompleteAt(t0.Add(31 * time.Second)) {
		t.Fatal("clean complete-segment instant must report complete")
	}
}

// TestExport_StreamingBounded verifies export does not accumulate all
// records: yield callback observes every record while the store holds many
// large segments (behavioral check via counts/order, memory bound asserted
// structurally by the implementation - one sorted slice per segment).
func TestExport_StreamingBounded(t *testing.T) {
	dir := t.TempDir()
	st, _ := storage.Open(dir)
	t0 := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	const segCount = 8
	const perSeg = 200
	for s := 0; s < segCount; s++ {
		times := make([]time.Time, perSeg)
		for i := 0; i < perSeg; i++ {
			times[i] = t0.Add(time.Duration(s*perSeg+i) * time.Millisecond)
		}
		id := "h-batch-" + pad(s+1)
		writeSeg(t, st, id, "h", times, "d1", "ecg", nil, true)
	}
	total := 0
	var last time.Time
	stats, _, err := Export(st, Filter{From: t0, To: t0.Add(segCount * perSeg * time.Millisecond), SessionID: "h"},
		func(r storage.SegmentRecord) error {
			if !last.IsZero() && r.Time.Before(last) {
				t.Fatal("non-monotonic export")
			}
			last = r.Time
			total++
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if total != segCount*perSeg || stats.Samples != segCount*perSeg {
		t.Fatalf("streaming count wrong: %d stats=%+v", total, stats)
	}
}

func sortTimes(ts []time.Time) {
	for i := 1; i < len(ts); i++ {
		for j := i; j > 0 && ts[j].Before(ts[j-1]); j-- {
			ts[j], ts[j-1] = ts[j-1], ts[j]
		}
	}
}

func pad(n int) string {
	if n < 10 {
		return "00000" + string(rune('0'+n))
	}
	return "0000" + string(rune('0'+n/10)) + string(rune('0'+n%10))
}
