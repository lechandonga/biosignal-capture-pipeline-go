package clock

import (
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

func TestCorrect_DriftAndMonotonic(t *testing.T) {
	d := NewDiscipline("dev1")
	sensor := model.SensorID("ecg")
	t0 := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	dev := t0
	host := t0.Add(100 * time.Millisecond)
	for seq := int64(1); seq <= 10; seq++ {
		u, err := d.Correct(sensor, seq, dev, host)
		t.Logf("INPUT dev=%s seq=%d recv=%s => unified=%s offset=%v err=%v",
			dev.Format(time.RFC3339Nano), seq, host.Format(time.RFC3339Nano),
			u.Format(time.RFC3339Nano), d.Offset(sensor), err)
		if err != nil {
			t.Fatalf("seq %d unexpected error: %v", seq, err)
		}
		dev = dev.Add(10 * time.Millisecond)
		host = host.Add(12 * time.Millisecond) // device runs slow
	}
}

func TestCorrect_Duplicate(t *testing.T) {
	d := NewDiscipline("dev1")
	sensor := model.SensorID("ecg")
	t0 := time.Now()
	if _, err := d.Correct(sensor, 1, t0, t0); err != nil {
		t.Fatal(err)
	}
	_, err := d.Correct(sensor, 1, t0.Add(time.Millisecond), t0.Add(time.Millisecond))
	t.Logf("INPUT duplicate seq=1 => err=%v (reason: duplicate_sample)", err)
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate, got %v", err)
	}
}

func TestCorrect_SeqRegression(t *testing.T) {
	d := NewDiscipline("dev1")
	sensor := model.SensorID("ecg")
	t0 := time.Now()
	_, _ = d.Correct(sensor, 5, t0, t0)
	_, err := d.Correct(sensor, 4, t0.Add(time.Millisecond), t0.Add(time.Millisecond))
	t.Logf("INPUT seq regression 5->4 => err=%v (reason: sequence_regression)", err)
	if !errors.Is(err, ErrSeqRegression) {
		t.Fatalf("expected ErrSeqRegression, got %v", err)
	}
}

func TestCorrect_ClockRollback(t *testing.T) {
	d := NewDiscipline("dev1")
	sensor := model.SensorID("ecg")
	t0 := time.Now()
	_, _ = d.Correct(sensor, 1, t0, t0)
	_, err := d.Correct(sensor, 2, t0.Add(-50*time.Millisecond), t0.Add(10*time.Millisecond))
	t.Logf("INPUT dev clock rollback -50ms seq=2 => err=%v (reason: clock_rollback)", err)
	if !errors.Is(err, ErrClockRollback) {
		t.Fatalf("expected ErrClockRollback, got %v", err)
	}
}

func TestReconnect_NoDoubleCount(t *testing.T) {
	d := NewDiscipline("dev1")
	sensor := model.SensorID("ecg")
	t0 := time.Now()
	for seq := int64(1); seq <= 3; seq++ {
		_, _ = d.Correct(sensor, seq, t0.Add(time.Duration(seq)*10*time.Millisecond),
			t0.Add(time.Duration(seq)*10*time.Millisecond))
	}
	if err := d.Reconnect(sensor, t0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	t.Logf("RECONNECT epoch now=%d; resend seq=3 must be rejected, seq=4 accepted", d.Epoch(sensor))
	if _, err := d.Correct(sensor, 3, t0.Add(30*time.Millisecond), t0.Add(time.Second)); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("post-reconnect replay seq=3 expected ErrDuplicate, got %v", err)
	}
	if _, err := d.Correct(sensor, 4, t0.Add(40*time.Millisecond), t0.Add(time.Second+10*time.Millisecond)); err != nil {
		t.Fatalf("post-reconnect seq=4 expected accepted, got %v", err)
	}
}
