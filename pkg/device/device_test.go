package device

import (
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

func testCfg(loss float64, drift time.Duration) Config {
	return Config{
		ID: "d1", DriftPerSec: drift, LossRate: loss, Seed: 42,
		Sensors: []SensorSpec{
			{ID: "ecg", Kind: model.SignalECG, RateHz: 100, Width: 1},
			{ID: "temp", Kind: model.SignalTemp, RateHz: 10, Width: 1},
		},
	}
}

func TestSample_RatesDriftAndDeterminism(t *testing.T) {
	t0 := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	d := New(testCfg(0, 10*time.Millisecond), t0) // gains 10ms per real second

	s1, _ := d.Sample("ecg", t0)
	s2, _ := d.Sample("ecg", t0.Add(10*time.Millisecond))
	t.Logf("INPUT ecg seq=1 dev=%s recv=%s ; seq=2 dev=%s",
		s1.DevTime.Format(time.RFC3339Nano), s1.RecvTime.Format(time.RFC3339Nano),
		s2.DevTime.Format(time.RFC3339Nano))
	period := s2.DevTime.Sub(s1.DevTime)
	if s2.Seq != 2 || period < 10*time.Millisecond || period > 11*time.Millisecond {
		t.Fatalf("100Hz period expected ~10ms incl drift, got %v seq=%d", period, s2.Seq)
	}
	// After 1s of real samples the device clock has drifted +~10ms.
	d2 := New(testCfg(0, 10*time.Millisecond), t0)
	var last model.RawSample
	for i := 0; i < 100; i++ {
		last, _ = d2.Sample("ecg", t0.Add(time.Duration(i+1)*10*time.Millisecond))
	}
	drift := last.DevTime.Sub(t0).Seconds() - last.RecvTime.Sub(t0).Seconds()
	t.Logf("JUDGEMENT after 1.0s real time, device clock offset=%.4fs (drift applied)", drift)
	if drift < 0.005 || drift > 0.015 {
		t.Fatalf("drift should be ~10ms, got %v", drift)
	}

	// Determinism: same seed => same waveform.
	a, _ := New(testCfg(0, 0), t0).Sample("ecg", t0)
	b, _ := New(testCfg(0, 0), t0).Sample("ecg", t0)
	if a.Values[0] != b.Values[0] {
		t.Fatal("simulation must be deterministic for a fixed seed")
	}
}

func TestSample_LossMarkers(t *testing.T) {
	t0 := time.Now()
	d := New(testCfg(0.5, 0), t0)
	lost, got := 0, 0
	for i := 0; i < 200; i++ {
		s, err := d.Sample("ecg", t0.Add(time.Duration(i)*10*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		if s.Lost {
			lost++
		}
		got++
		if s.Seq != int64(i+1) {
			t.Fatalf("sequence must be gapless even when data is lost: got %d want %d", s.Seq, i+1)
		}
	}
	t.Logf("INPUT 200 positions lossRate=0.5 => %d samples, %d loss markers; sequences gapless", got, lost)
	if lost < 40 || lost > 160 {
		t.Fatalf("loss fraction out of stochastic band: %d", lost)
	}
}

func TestDisconnectReconnect_NoReusedSeq(t *testing.T) {
	t0 := time.Now()
	d := New(testCfg(0, 0), t0)
	s, _ := d.Sample("ecg", t0)
	d.Disconnect()
	if _, err := d.Sample("ecg", t0.Add(10*time.Millisecond)); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("offline sample must fail ErrDisconnected, got %v", err)
	}
	t.Log("INPUT sample during disconnect => rejected (device disconnected), no fabrication")

	back := t0.Add(time.Second)
	d.Reconnect(back)
	gap, err := d.EmitGap("ecg", 10, back)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := d.Sample("ecg", back.Add(10*time.Millisecond))
	t.Logf("RECONNECT lastSeq before=%d gap ends at seq=%d next real seq=%d", s.Seq, gap.Seq, after.Seq)
	if gap.Seq != 11 || after.Seq != 12 {
		t.Fatalf("gap must reserve seqs 2..11 and next real be 12, got gap=%d next=%d", gap.Seq, after.Seq)
	}
	if !gap.Lost {
		t.Fatal("EmitGap must produce a gap marker")
	}
}

func TestRollback(t *testing.T) {
	t0 := time.Now()
	d := New(testCfg(0, 0), t0)
	a, _ := d.Sample("ecg", t0)
	d.Rollback(50 * time.Millisecond)
	b, _ := d.Sample("ecg", t0.Add(10*time.Millisecond))
	t.Logf("INPUT before dev=%s ; after rollback(-50ms) seq=%d dev=%s (pipeline must reject)",
		a.DevTime.Format(time.RFC3339Nano), b.Seq, b.DevTime.Format(time.RFC3339Nano))
	if !b.DevTime.Before(a.DevTime) {
		t.Fatal("rollback should move device clock backwards")
	}
}
