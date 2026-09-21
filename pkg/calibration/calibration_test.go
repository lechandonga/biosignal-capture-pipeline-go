package calibration

import (
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

func baseParams(ver string, eff, exp time.Time) Params {
	return Params{
		Version: ver, Sensor: "ecg", Effective: eff, Expires: exp,
		Gain: []float64{2}, Offset: []float64{1}, Baseline: []float64{0.5},
	}
}

func TestAdd_RejectsInvalid(t *testing.T) {
	s := NewStore()
	t0 := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)

	bad := baseParams("", t0, time.Time{})
	err := s.Add(bad)
	t.Logf("INPUT empty version => err=%v", err)
	if !errors.Is(err, ErrInvalidParams) {
		t.Fatalf("expected ErrInvalidParams, got %v", err)
	}

	badGain := baseParams("v1", t0, time.Time{})
	badGain.Gain = []float64{0}
	if err := s.Add(badGain); !errors.Is(err, ErrInvalidParams) {
		t.Fatalf("zero gain must be rejected, got %v", err)
	}
	t.Logf("INPUT zero gain => REJECT reason=bad_calibration (no default identity fallback)")

	badWin := baseParams("v2", t0.Add(2*time.Hour), t0.Add(time.Hour))
	if err := s.Add(badWin); !errors.Is(err, ErrInvalidParams) {
		t.Fatalf("expires<=effective must be rejected, got %v", err)
	}
	t.Logf("INPUT inverted window => REJECT reason=bad_calibration")

	expired := baseParams("v3", t0.Add(-2*time.Hour), t0.Add(-time.Hour))
	if err := s.Add(expired); err != nil {
		t.Fatalf("registering a historical window should be allowed, got %v", err)
	}
	// A sample after that window ended (with no successor) is rejected with a
	// distinct expired reason; no default calibration applies.
	if _, err := s.At("ecg", t0.Add(-30*time.Minute)); !errors.Is(err, ErrCalibrationExpired) {
		t.Fatalf("expired window must yield ErrCalibrationExpired, got %v", err)
	}
	t.Logf("INPUT sample at -30m after v3 ended => REJECT reason=expired_calibration")
	// Before any window ever became effective: distinct reason.
	if _, err := s.At("ecg", t0.Add(-3*time.Hour)); !errors.Is(err, ErrNoValidCalibration) {
		t.Fatalf("pre-effective time must yield ErrNoValidCalibration, got %v", err)
	}
	t.Logf("INPUT sample at -3h before first window => REJECT reason=no_valid_calibration")
}

func TestAt_VersionBySampleTime_NoRetroactiveChange(t *testing.T) {
	s := NewStore()
	t0 := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	v1 := baseParams("v1", t0, t0.Add(time.Hour))
	v1.Gain = []float64{2}
	v2 := baseParams("v2", t0.Add(time.Hour), time.Time{})
	v2.Gain = []float64{3}
	if err := s.Add(v1); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(v2); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(v1); !errors.Is(err, ErrDuplicateVersion) {
		t.Fatalf("duplicate version must be rejected, got %v", err)
	}
	t.Logf("INPUT re-add v1 => REJECT duplicate version")

	// A historical sample still resolves to v1 after v2 is registered.
	at1, err := s.At("ecg", t0.Add(30*time.Minute))
	if err != nil || at1.Version != "v1" {
		t.Fatalf("historical sample must pin v1, got %+v err=%v", at1, err)
	}
	at2, err := s.At("ecg", t0.Add(90*time.Minute))
	if err != nil || at2.Version != "v2" {
		t.Fatalf("new sample must use v2, got %+v err=%v", at2, err)
	}
	t.Logf("JUDGEMENT sample@+30m -> %s, sample@+90m -> %s; adding v2 did not change v1 conclusion",
		at1.Version, at2.Version)

	phys, base, err := at1.Apply([]float64{10})
	if err != nil || phys[0] != 21 || base[0] != 0.5 {
		t.Fatalf("apply v1 = %v/%v err=%v", phys, base, err)
	}
	t.Logf("INPUT raw=10 cal=v1 gain=2 off=1 => physical=%v baseline=%v", phys, base)

	// Gap between windows (here: before first effective) must be rejected.
	if _, err := s.At("ecg", t0.Add(-time.Minute)); !errors.Is(err, ErrNoValidCalibration) {
		t.Fatalf("uncovered time must yield ErrNoValidCalibration, got %v", err)
	}
	t.Logf("INPUT sample before any window => REJECT reason=expired_calibration (no default)")
}

func TestApply_RejectsWidthMismatch(t *testing.T) {
	p := baseParams("v1", time.Now(), time.Time{})
	if _, _, err := p.Apply([]float64{1, 2}); !errors.Is(err, ErrInvalidParams) {
		t.Fatalf("width mismatch must be rejected, got %v", err)
	}
	t.Logf("INPUT 2-channel sample vs 1-channel calibration => REJECT bad_calibration")

	// keep model import meaningful if constants referenced elsewhere later
	_ = model.SensorID("ecg")
}
