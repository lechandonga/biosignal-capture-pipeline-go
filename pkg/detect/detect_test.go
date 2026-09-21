package detect

import (
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

func mkSample(q float64, v float64, when time.Time, gap bool) model.CalibratedSample {
	return model.CalibratedSample{
		Aligned: model.AlignedSample{
			Device: "dev1", Sensor: "ecg", Kind: model.SignalECG,
			UniTime: when, Quality: q, Gap: gap,
		},
		CalVersion: "v1",
		Values:     []float64{v},
		Baseline:   []float64{0},
	}
}

func TestPush_EventAnomalyAndReproducibility(t *testing.T) {
	r := Rule{ID: "ecg-high", Sensor: "ecg", Kind: model.SignalECG,
		Window: 1, MinQuality: 0.5, High: 10, Low: 20, Op: OpGT, Cooldown: 0}
	t0 := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)

	run := func() []model.Event {
		e := NewEngine([]Rule{r})
		var out []model.Event
		inputs := []float64{1, 11, 1, 25}
		for i, v := range inputs {
			evs := e.Push(mkSample(0.9, v, t0.Add(time.Duration(i)*time.Second), false))
			for _, ev := range evs {
				t.Logf("INPUT v=%.1f q=0.9 => LEVEL=%s value=%.2f thr=%.2f evidence=%s",
					v, ev.Level, ev.Value, ev.Threshold, ev.Evidence)
			}
			out = append(out, evs...)
		}
		return out
	}
	out1 := run()
	out2 := run()
	if len(out1) != 2 || out1[0].Level != model.LevelEvent || out1[1].Level != model.LevelAnomaly {
		t.Fatalf("expected EVENT then ANOMALY, got %+v", out1)
	}
	if out1[0].Evidence == "" || out1[0].CalVersion != "v1" {
		t.Fatalf("event must carry evidence and calibration version")
	}
	// Reproducible: same inputs => same conclusions.
	if len(out1) != len(out2) {
		t.Fatal("non reproducible run length")
	}
	for i := range out1 {
		if out1[i].Level != out2[i].Level || out1[i].Evidence != out2[i].Evidence {
			t.Fatal("non reproducible conclusions")
		}
	}
	t.Logf("JUDGEMENT two identical runs produce identical conclusions (reproducible)")
}

func TestPush_LowQualityIsOnlyUncertain(t *testing.T) {
	r := Rule{ID: "ecg-high", Sensor: "ecg", Kind: model.SignalECG,
		Window: 1, MinQuality: 0.8, High: 10, Op: OpGT}
	e := NewEngine([]Rule{r})
	t0 := time.Now()
	evs := e.Push(mkSample(0.3, 99, t0, false))
	t.Logf("INPUT v=99 q=0.3 (below minQ=0.8) => %+v evidence=%s", evs, evs[0].Evidence)
	if len(evs) != 1 || evs[0].Level != model.LevelUncertain {
		t.Fatalf("low quality high value must be UNCERTAIN only, got %+v", evs)
	}

	// Gap markers are also uncertain, never a definite event.
	evs = e.Push(mkSample(0.99, 99, t0.Add(time.Second), true))
	if len(evs) != 1 || evs[0].Level != model.LevelUncertain {
		t.Fatalf("gap sample must be UNCERTAIN, got %+v", evs)
	}
	t.Logf("INPUT gap marker v=99 q=0.99 => UNCERTAIN (no definite conclusion on missing data)")
}

func TestPush_Cooldown(t *testing.T) {
	r := Rule{ID: "r", Sensor: "ecg", Kind: model.SignalECG,
		Window: 1, MinQuality: 0, High: 10, Op: OpGT, Cooldown: 2}
	e := NewEngine([]Rule{r})
	t0 := time.Now()
	var levels []model.EventLevel
	for i := 0; i < 5; i++ {
		for _, ev := range e.Push(mkSample(1, 11, t0.Add(time.Duration(i)*time.Second), false)) {
			levels = append(levels, ev.Level)
		}
	}
	t.Logf("INPUT five over-threshold samples cooldown=2 => definite events=%d", len(levels))
	if len(levels) != 2 { // sample 0 and sample 3 (cd window 1..2, 4..)
		t.Fatalf("cooldown should suppress to 2 events, got %d", len(levels))
	}
}
