package detect

import (
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/timeseries"
)

func mkAligned(sensor string, value, quality float64, seq uint64) timeseries.AlignedSample {
	base := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	return timeseries.AlignedSample{
		Sample: timeseries.Sample{
			DeviceID: "dev1", SensorID: sensor, Signal: timeseries.SignalEDA,
			Seq: seq, Value: value, Quality: quality,
			DeviceTime: base, ReceivedAt: base,
		},
		UnifiedTime: base.Add(time.Duration(seq) * time.Second),
	}
}

func newTestDetector(t *testing.T) *Detector {
	t.Helper()
	d, err := NewDetector([]Rule{{
		Signal: timeseries.SignalEDA, Event: "eda_spike",
		Low: 0, High: 20, MinQuality: 0.5, MinConsecutive: 3,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func logConclusion(t *testing.T, label string, c Conclusion) {
	t.Logf("%s INPUT sensor=%s raw=%.2f physical=%.2f quality=%.2f cal=%s => STATUS=%s REASON=%s BASIS{consec=%d/%d low=%.1f high=%.1f notes=%v}",
		label, c.SensorID, c.Basis.RawValue, c.Basis.CalibratedValue, c.Basis.Quality,
		c.Basis.CalVersion, c.Status, c.Reason, c.Basis.Consecutive, c.Basis.Required,
		c.Basis.Low, c.Basis.High, c.Basis.Notes)
}

func TestConsecutiveEvent(t *testing.T) {
	d := newTestDetector(t)
	// 两个越限：仍 normal；第三个越限：event。
	seq := uint64(0)
	feed := func(v, q float64) Conclusion {
		seq++
		cs := d.Evaluate(mkAligned("s1", v, q, seq), v, 10, "v1")
		logConclusion(t, "INPUT", cs[0])
		return cs[0]
	}
	if c := feed(25, 0.9); c.Status != StatusNormal {
		t.Fatalf("1st violation must be normal streak=1, got %s", c.Status)
	}
	if c := feed(26, 0.9); c.Status != StatusNormal {
		t.Fatalf("2nd violation must be normal streak=2, got %s", c.Status)
	}
	if c := feed(27, 0.9); c.Status != StatusEvent {
		t.Fatalf("3rd violation must be event, got %s", c.Status)
	}
	// 回到正常 => 计数复位。
	if c := feed(10, 0.9); c.Status != StatusNormal || c.Basis.Consecutive != 0 {
		t.Fatalf("normal value must reset streak, got status=%s consec=%d", c.Status, c.Basis.Consecutive)
	}
}

func TestLowQualityIsUncertainAndFreezesCounter(t *testing.T) {
	d := newTestDetector(t)
	var c Conclusion
	c = d.Evaluate(mkAligned("s2", 25, 0.9, 1), 25, 10, "v1")[0] // 越限 streak=1
	logConclusion(t, "INPUT", c)
	// 低质量越限样本：不确定，且不得推进连续计数。
	for i := uint64(2); i <= 5; i++ {
		c = d.Evaluate(mkAligned("s2", 25, 0.2, i), 25, 10, "v1")[0]
		if c.Status != StatusUncertain || c.Reason != UncertainQuality {
			t.Fatalf("low quality must be uncertain, got %s/%s", c.Status, c.Reason)
		}
		logConclusion(t, "INPUT-LOWQ", c)
		if c.Basis.Consecutive != 1 {
			t.Fatalf("consecutive counter must stay frozen at 1, got %d", c.Basis.Consecutive)
		}
	}
	// 质量恢复后只需再两个越限即事件（冻结前计数保留）。
	c = d.Evaluate(mkAligned("s2", 25, 0.9, 6), 25, 10, "v1")[0]
	logConclusion(t, "INPUT-RECOVER", c)
	if c.Status != StatusNormal || c.Basis.Consecutive != 2 {
		t.Fatalf("after recovery streak should be 2, got %s consec=%d", c.Status, c.Basis.Consecutive)
	}
	c = d.Evaluate(mkAligned("s2", 25, 0.9, 7), 25, 10, "v1")[0]
	logConclusion(t, "INPUT-RECOVER", c)
	if c.Status != StatusEvent {
		t.Fatalf("event must fire after 1 + 2 good violations, got %s", c.Status)
	}
}

func TestUncalibratedIsUncertain(t *testing.T) {
	d := newTestDetector(t)
	s := mkAligned("s3", 99, 0.9, 1)
	in := EvaluateInput{Sample: s, CalibratedOK: false}
	cs := d.EvaluateUncertain(in, "no_calibration_effective")
	if cs[0].Status != StatusUncertain {
		t.Fatalf("uncalibrated sample must be uncertain, got %s", cs[0].Status)
	}
	logConclusion(t, "INPUT-UNCAL", cs[0])

	// 再喂两个高质量越限样本：不确定期间计数为 0，不能凑成事件。
	c := d.Evaluate(mkAligned("s3", 25, 0.9, 2), 25, 10, "v1")[0]
	if c.Basis.Consecutive != 1 || c.Status != StatusNormal {
		t.Fatalf("after uncertain input streak must start fresh at 1, got %d/%s", c.Basis.Consecutive, c.Status)
	}
}

func TestInvalidRuleRejected(t *testing.T) {
	if _, err := NewDetector([]Rule{{Event: "x", Low: 10, High: 5, MinQuality: 0.5, MinConsecutive: 1}}); err == nil {
		t.Fatal("invalid rule (low>=high) must be rejected")
	}
}
