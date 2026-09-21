package session

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/device"
	"github.com/lechandonga/biosignal-capture-pipeline-go/timeseries"
)

func TestSessionDeviceEndToEnd(t *testing.T) {
	cfg := Config{ReorderWindow: 20 * time.Millisecond, MaxRegression: 50 * time.Millisecond, Buffer: 1024}
	sess := New("sess1", cfg)

	devCfg := device.Config{
		DeviceID: "devA",
		Sensors: []device.SensorConfig{
			{SensorID: "ecgA", Signal: timeseries.SignalECG, RateHz: 100, DropRate: 0.2, DriftPPM: 800, Baseline: 0, Amplitude: 1},
			{SensorID: "edaA", Signal: timeseries.SignalEDA, RateHz: 20, DropRate: 0, Baseline: 10, Amplitude: 1},
		},
		DisconnectAfter: 200 * time.Millisecond,
		ReconnectAfter:  150 * time.Millisecond,
	}
	dev := device.New(devCfg, 123)
	sess.Attach(dev, map[string]float64{"ecgA": 800, "edaA": 0})

	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()

	go func() { _ = sess.Run(ctx) }()

	var accepted, rejected, gaps, reconnected int
	reasonCounts := map[timeseries.RejectReason]int{}
	for res := range sess.Events() {
		if res.Accepted {
			accepted++
		} else {
			rejected++
		}
		if res.Reason != "" {
			reasonCounts[res.Reason]++
		}
		gaps += len(res.Gaps)
		t.Logf("INPUT device=%s sensor=%s seq=%d raw=%.3f q=%.2f => accepted=%v reason=%s gaps=%d detail=%s",
			res.Sample.DeviceID, res.Sample.SensorID, res.Sample.Seq, res.Sample.Value,
			res.Sample.Quality, res.Accepted, res.Reason, len(res.Gaps), res.Detail)
		if len(res.Gaps) > 0 {
			reconnected++
		}
	}
	cancel()

	if accepted == 0 {
		t.Fatal("expected accepted samples")
	}
	if gaps == 0 {
		t.Fatal("expected at least one detected gap from drops/disconnect")
	}
	t.Logf("SUMMARY accepted=%d rejected=%d gapEvents=%d reasons=%v", accepted, rejected, gaps, reasonCounts)
}

func TestSessionManualReplayRejectedAfterReconnect(t *testing.T) {
	sess := New("sess2", Config{ReorderWindow: 10 * time.Millisecond, MaxRegression: 50 * time.Millisecond})
	base := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	ctx := context.Background()

	mk := func(seq uint64) timeseries.Sample {
		return timeseries.Sample{DeviceID: "d", SensorID: "s", Signal: timeseries.SignalTemp,
			Seq: seq, DeviceTime: base.Add(time.Duration(seq) * time.Second),
			ReceivedAt: base.Add(time.Duration(seq) * time.Second), Value: 36, Quality: 0.9}
	}
	sess.Ingest(ctx, mk(1))
	sess.Ingest(ctx, mk(2))
	sess.NotifyReconnect([]string{"s"}, base.Add(10*time.Second))
	// 重放旧 seq=2（设备时间也旧）：必须按重复拒绝。
	r := sess.Ingest(ctx, mk(2))
	if r.Accepted || r.Reason != timeseries.ReasonDuplicate {
		t.Fatalf("replayed seq after reconnect must be duplicate, got accepted=%v reason=%s", r.Accepted, r.Reason)
	}
	t.Logf("REPLAY seq=2 after reconnect => rejected reason=%s basis=%s", r.Reason, r.Detail)
}
