package device

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/timeseries"
)

func TestDeviceEmitsMultiRateSamplesAndDrops(t *testing.T) {
	cfg := Config{
		DeviceID: "dev1",
		Sensors: []SensorConfig{
			{SensorID: "ecg1", Signal: timeseries.SignalECG, RateHz: 50, DropRate: 0.3, DriftPPM: 500, Jitter: time.Millisecond, Baseline: 0, Amplitude: 1},
			{SensorID: "eda1", Signal: timeseries.SignalEDA, RateHz: 10, DropRate: 0, Baseline: 10, Amplitude: 1},
		},
	}
	d := New(cfg, 42)
	out := make(chan Event, 256)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	if err := d.Run(ctx, out); err != nil {
		t.Fatal(err)
	}
	close(out)

	lastSeq := map[string]uint64{}
	counts := map[string]int{}
	for ev := range out {
		if ev.Kind != EventSample {
			continue
		}
		s := ev.Sample
		counts[s.SensorID]++
		if prev, ok := lastSeq[s.SensorID]; ok && s.Seq <= prev {
			t.Fatalf("seq must strictly increase for %s: %d after %d", s.SensorID, s.Seq, prev)
		}
		lastSeq[s.SensorID] = s.Seq
	}
	// ecg1: 0.6 接收率 * 50Hz * 0.6s ≈ 18 条；放宽容差。
	if counts["ecg1"] < 8 || counts["ecg1"] > 40 {
		t.Fatalf("ecg1 received count out of plausible range: %d", counts["ecg1"])
	}
	if counts["eda1"] < 4 {
		t.Fatalf("eda1 should receive most samples, got %d", counts["eda1"])
	}
	// 丢包 => 最后序列号明显大于实际收到条数（缺口被序列号如实表达）。
	if lastSeq["ecg1"] <= uint64(counts["ecg1"]) {
		t.Fatalf("drops must consume seq numbers: lastSeq=%d received=%d", lastSeq["ecg1"], counts["ecg1"])
	}
	t.Logf("RUN 0.6s: ecg1 received=%d lastSeq=%d (gaps encoded by seq); eda1 received=%d",
		counts["ecg1"], lastSeq["ecg1"], counts["eda1"])
}

func TestDeviceDisconnectReconnectEvents(t *testing.T) {
	cfg := Config{
		DeviceID:        "dev2",
		Sensors:         []SensorConfig{{SensorID: "s", Signal: timeseries.SignalTemp, RateHz: 20, Baseline: 36}},
		DisconnectAfter: 100 * time.Millisecond,
		ReconnectAfter:  100 * time.Millisecond,
	}
	d := New(cfg, 7)
	out := make(chan Event, 256)
	ctx, cancel := context.WithTimeout(context.Background(), 450*time.Millisecond)
	defer cancel()
	if err := d.Run(ctx, out); err != nil {
		t.Fatal(err)
	}
	close(out)

	var disconnects, reconnects int
	for ev := range out {
		switch ev.Kind {
		case EventDisconnected:
			disconnects++
		case EventReconnected:
			reconnects++
		}
	}
	if disconnects < 1 || reconnects < 1 {
		t.Fatalf("expected disconnect/reconnect events, got d=%d r=%d", disconnects, reconnects)
	}
	t.Logf("RUN 450ms disconnects=%d reconnects=%d (offline seq advances encode gaps)", disconnects, reconnects)
}

func TestInvalidConfigRejected(t *testing.T) {
	d := New(Config{DeviceID: "x", Sensors: []SensorConfig{{SensorID: "s", RateHz: 0}}}, 1)
	if err := d.Run(context.Background(), make(chan Event)); err == nil {
		t.Fatal("zero-rate sensor must be rejected")
	}
}
