// Command demo runs a local end-to-end acquisition session with simulated
// wearable devices, packet loss, clock drift, disconnect/reconnect, clock
// rollback and a mid-session calibration change. It prints every input
// sample together with the decision basis, the rejection/handling log and
// the final session accounting, then reads durable batches back to show the
// complete/incomplete distinction.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/calibration"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/detect"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/device"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/pipeline"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/storage"
)

func main() {
	dir := flag.String("dir", filepath.Join(os.TempDir(), "biosignal-demo"), "storage root")
	duration := flag.Duration("duration", 3*time.Second, "simulated wall-clock session length")
	flag.Parse()

	// Fresh area for a repeatable run; remove the root to start clean.
	_ = os.RemoveAll(*dir)
	st, err := storage.Open(*dir)
	must(err)
	defer func() { _ = st.Close() }()

	t0 := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	cals := calibration.NewStore()

	// Calibration: ecg v1 for the first second, v2 afterwards.
	must(cals.Add(calibration.Params{
		Version: "ecg-v1", Sensor: "ecg", Effective: t0.Add(-time.Hour), Expires: t0.Add(time.Second),
		Gain: []float64{2}, Offset: []float64{0}, Baseline: []float64{0},
	}))
	must(cals.Add(calibration.Params{
		Version: "ecg-v2", Sensor: "ecg", Effective: t0.Add(time.Second),
		Gain: []float64{3}, Offset: []float64{0}, Baseline: []float64{0},
	}))
	must(cals.Add(calibration.Params{
		Version: "temp-v1", Sensor: "temp", Effective: t0.Add(-time.Hour),
		Gain: []float64{1}, Offset: []float64{0}, Baseline: []float64{36.6},
	}))

	rules := []detect.Rule{{
		ID: "ecg-burst", Sensor: "ecg", Kind: model.SignalECG, Channel: 0,
		Window: 1, MinQuality: 0.5, High: 15, Low: 25, Op: detect.OpGT,
	}}
	p := pipeline.New(pipeline.Config{
		SessionID: "demo", QueueDepth: 4096, BatchSize: 128,
		FlushInterval: time.Second, Policy: pipeline.PolicyDelay, EnqueueWait: 20 * time.Millisecond,
	}, cals, rules, st)

	must(p.RegisterDevice("wrist-1"))
	must(p.BindSensor(pipeline.SensorBinding{Device: "wrist-1", Sensor: "ecg", Kind: model.SignalECG, Width: 1}))
	must(p.BindSensor(pipeline.SensorBinding{Device: "wrist-1", Sensor: "temp", Kind: model.SignalTemp, Width: 1}))
	go p.Run()

	dev := device.New(device.Config{
		ID: "wrist-1", DriftPerSec: 20 * time.Millisecond, LossRate: 0.05, Seed: 7,
		Sensors: []device.SensorSpec{
			{ID: "ecg", Kind: model.SignalECG, RateHz: 100, Width: 1},
			{ID: "temp", Kind: model.SignalTemp, RateHz: 10, Width: 1},
		},
	}, t0)

	fmt.Println("rules:")
	fmt.Print(detect.RulesSummary(rules))
	fmt.Println("streaming simulated samples ...")

	deadline := t0.Add(*duration)
	now := t0
	ecgN, tempN := 0, 0
	for now.Before(deadline) {
		now = now.Add(10 * time.Millisecond)
		if ecgN < int(duration.Seconds()*100) {
			s, err := dev.Sample("ecg", now)
			if err == nil {
				ecgN++
				logSample(s)
				if err := p.Ingest(s); err != nil {
					fmt.Printf("ingest backpressure: %v\n", err)
				}
			}
		}
		if tempN < int(duration.Seconds()*10) && now.UnixMilli()%100 == 0 {
			s, err := dev.Sample("temp", now)
			if err == nil {
				tempN++
				logSample(s)
				_ = p.Ingest(s)
			}
		}

		// Mid-session fault injection: disconnect at 1.2s, rollback at 1.5s,
		// reconnect at 1.8s and account the outage with a gap marker.
		switch {
		case now.Equal(t0.Add(1200 * time.Millisecond)):
			dev.Disconnect()
			fmt.Println(">>> device disconnected")
		case now.Equal(t0.Add(1500 * time.Millisecond)):
			dev.Rollback(40 * time.Millisecond)
			fmt.Println(">>> injected clock rollback -40ms (still offline)")
		case now.Equal(t0.Add(1800 * time.Millisecond)):
			dev.Reconnect(now)
			_ = p.Reconnect("wrist-1", "ecg", now)
			gap, _ := dev.EmitGap("ecg", 60, now) // 60 missed positions @100Hz
			fmt.Printf(">>> reconnected; accounting outage gap seq=%d\n", gap.Seq)
			_ = p.Ingest(gap)
		}
	}

	p.Stop()

	st2 := stats(p)
	fmt.Println("\n=== session accounting ===")
	fmt.Printf("submitted=%d accepted=%d processed=%d gaps=%d rejected=%d events=%d batches=%d\n",
		st2.Submitted, st2.Accepted, st2.Processed, st2.Gaps, st2.Rejected, st2.Events, len(st2.Batches))

	fmt.Println("\n=== rejection / handling log (distinct reasons) ===")
	seen := map[model.RejectReason]int{}
	for _, r := range p.Rejected() {
		seen[r.Reason]++
		fmt.Printf("[%s] dev=%s sensor=%s seq=%d reason=%s detail=%s\n",
			r.Action, r.Sample.Device, r.Sample.Sensor, r.Sample.Seq, r.Reason, r.Detail)
	}
	fmt.Printf("reason totals: %v\n", seen)

	// Read batches back: demonstrate state distinction and traceability.
	fmt.Println("\n=== durable batch readback ===")
	ids, _ := st.ListBatches()
	for _, id := range ids {
		b, err := st.LoadBatch(id)
		must(err)
		fmt.Printf("batch %s state=%s samples=%d events=%d rejected=%d\n",
			id, b.State, len(b.Samples), len(b.Events), len(b.Rejected))
		for _, ev := range b.Events {
			fmt.Printf("  EVENT %s sensor=%s value=%.3f thr=%.3f cal=%s\n    evidence: %s\n",
				ev.Level, ev.Sensor, ev.Value, ev.Threshold, ev.CalVersion, ev.Evidence)
		}
	}
	fmt.Println("\nstorage root:", *dir)
}

func stats(p *pipeline.Pipeline) pipeline.Stats { return p.Stats() }

func logSample(s model.RawSample) {
	if s.Lost {
		fmt.Printf("INPUT dev=%s sensor=%s seq=%d LOST gap devTime=%s\n",
			s.Device, s.Sensor, s.Seq, s.DevTime.Format("15:04:05.000"))
		return
	}
	fmt.Printf("INPUT dev=%s sensor=%s seq=%d raw=%v q=%.2f devTime=%s recv=%s\n",
		s.Device, s.Sensor, s.Seq, s.Values, s.Quality,
		s.DevTime.Format("15:04:05.000"), s.RecvTime.Format("15:04:05.000"))
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
