package calibration

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/timeseries"
)

func mkCal(version string, from, to time.Time, scale float64) Calibration {
	return Calibration{
		SensorID:      "s1",
		Signal:        timeseries.SignalEDA,
		Version:       version,
		EffectiveFrom: from,
		EffectiveTo:   to,
		Offset:        0,
		Scale:         scale,
		Baseline:      10,
		MinRaw:        0,
		MaxRaw:        100,
		CreatedAt:     from.Add(-time.Minute),
	}
}

func TestAddAndVersionedLookupNoRetroactiveChange(t *testing.T) {
	t0 := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	r := NewRegistry()
	c1 := mkCal("v1", t0, t0.Add(24*time.Hour), 1.0)
	c2 := mkCal("v2", t0.Add(24*time.Hour), time.Time{}, 2.0)
	if err := r.Add(c1); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(c2); err != nil {
		t.Fatal(err)
	}

	// 历史时刻必须命中 v1，标定更新不得回溯改变历史结论。
	got1, err := r.At("s1", timeseries.SignalEDA, t0.Add(time.Hour))
	if err != nil || got1.Version != "v1" {
		t.Fatalf("historical lookup must return v1, got %+v err=%v", got1, err)
	}
	got2, err := r.At("s1", timeseries.SignalEDA, t0.Add(25*time.Hour))
	if err != nil || got2.Version != "v2" {
		t.Fatalf("new lookup must return v2, got %+v err=%v", got2, err)
	}

	v1, _ := r.At("s1", timeseries.SignalEDA, t0.Add(time.Hour))
	phys1, err := v1.Apply(5)
	if err != nil || phys1 != 5 {
		t.Fatalf("v1 physical want 5, got %v err=%v", phys1, err)
	}
	v2, _ := r.At("s1", timeseries.SignalEDA, t0.Add(25*time.Hour))
	phys2, err := v2.Apply(5)
	if err != nil || phys2 != 10 {
		t.Fatalf("v2 physical want 10, got %v err=%v", phys2, err)
	}
	t.Logf("INPUT raw=5 at t0+1h -> CAL_VERSION=%s physical=%v; at t0+25h -> CAL_VERSION=%s physical=%v (no retroactive change)",
		v1.Version, phys1, v2.Version, phys2)
}

func TestRejectInvalidOverlapDuplicateAndExpired(t *testing.T) {
	t0 := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	r := NewRegistry()

	bad := mkCal("bad", t0, t0.Add(time.Hour), 0) // scale=0 非法
	if err := r.Add(bad); !errors.Is(err, ErrInvalidCalibration) {
		t.Fatalf("scale=0 must be rejected, got %v", err)
	} else {
		t.Logf("INPUT scale=0 REJECTED=%v", err)
	}

	ok := mkCal("v1", t0, t0.Add(2*time.Hour), 1)
	if err := r.Add(ok); err != nil {
		t.Fatal(err)
	}
	dup := mkCal("v1", t0.Add(3*time.Hour), time.Time{}, 1)
	if err := r.Add(dup); !errors.Is(err, ErrInvalidCalibration) {
		t.Fatalf("duplicate version must be rejected, got %v", err)
	}
	overlap := mkCal("v2", t0.Add(time.Hour), time.Time{}, 1)
	if err := r.Add(overlap); !errors.Is(err, ErrOverlappingCalibration) {
		t.Fatalf("overlapping interval must be rejected, got %v", err)
	} else {
		t.Logf("INPUT v2 from=t0+1h (open-ended, overlaps v1) REJECTED=%v", err)
	}

	// 关闭区间结束后 => 过期，绝不退化为默认值。
	if _, err := r.At("s1", timeseries.SignalEDA, t0.Add(5*time.Hour)); !errors.Is(err, ErrExpiredCalibration) {
		t.Fatalf("expired lookup must fail with ErrExpiredCalibration, got %v", err)
	} else {
		t.Logf("LOOKUP at t0+5h REJECTED=%v", err)
	}

	// 从未配置的传感器。
	if _, err := r.At("other", timeseries.SignalEDA, t0); !errors.Is(err, ErrNoCalibration) {
		t.Fatalf("missing sensor must fail with ErrNoCalibration, got %v", err)
	}
}

func TestRawOutOfRange(t *testing.T) {
	t0 := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	r := NewRegistry()
	if err := r.Add(mkCal("v1", t0, time.Time{}, 1)); err != nil {
		t.Fatal(err)
	}
	c, _ := r.At("s1", timeseries.SignalEDA, t0)
	if _, err := c.Apply(150); !errors.Is(err, ErrRawOutOfRange) {
		t.Fatalf("raw out of range must fail, got %v", err)
	} else {
		t.Logf("INPUT raw=150 range=[0,100] REJECTED=%v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	t0 := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	r := NewRegistry()
	if err := r.Add(mkCal("v1", t0, t0.Add(2*time.Hour), 1)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				got, err := r.At("s1", timeseries.SignalEDA, t0.Add(time.Hour))
				if err != nil || got.Version != "v1" {
					t.Errorf("concurrent read inconsistent: %v %v", got, err)
				}
			}
		}()
	}
	wg.Wait()
}
