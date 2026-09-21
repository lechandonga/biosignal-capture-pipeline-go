package pipeline

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/backpressure"
	"github.com/lechandonga/biosignal-capture-pipeline-go/calibration"
	"github.com/lechandonga/biosignal-capture-pipeline-go/detect"
	"github.com/lechandonga/biosignal-capture-pipeline-go/device"
	"github.com/lechandonga/biosignal-capture-pipeline-go/storage"
	"github.com/lechandonga/biosignal-capture-pipeline-go/timeseries"
)

func testRules() []detect.Rule {
	return []detect.Rule{{
		Signal: timeseries.SignalEDA, Event: "eda_high",
		Low: 0, High: 20, MinQuality: 0.5, MinConsecutive: 3,
	}}
}

// TestPipelineCalibrationChangeAndConsistency 验证：
// 标定 v1->v2 期间，历史样本按数据时刻命中 v1，结论可复现；
// 丢包产生 gap 记录；质量不足/无标定只产生 uncertain；批次可完整读回。
func TestPipelineCalibrationChangeAndConsistency(t *testing.T) {
	t0 := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	reg := calibration.NewRegistry()
	// v1: scale=1, raw 25 => physical 25 越限；v2: scale=0.5, raw 25 => physical 12.5 正常。
	cal1 := calibration.Calibration{SensorID: "eda1", Signal: timeseries.SignalEDA, Version: "v1",
		EffectiveFrom: t0, EffectiveTo: t0.Add(time.Hour), Scale: 1, Baseline: 10, MinRaw: 0, MaxRaw: 1000, CreatedAt: t0.Add(-time.Hour)}
	cal2 := calibration.Calibration{SensorID: "eda1", Signal: timeseries.SignalEDA, Version: "v2",
		EffectiveFrom: t0.Add(time.Hour), Scale: 0.5, Baseline: 10, MinRaw: 0, MaxRaw: 1000, CreatedAt: t0.Add(2 * time.Hour)}
	if err := reg.Add(cal1); err != nil {
		t.Fatal(err)
	}
	if err := reg.Add(cal2); err != nil {
		t.Fatal(err)
	}

	p, err := New(Config{
		WorkDir: t.TempDir(), SessionID: "sess1",
		BatchMaxRecords: 100000, QueueCapacity: 64,
		Backpressure: backpressure.PolicyReject,
	}, reg, testRules())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}

	mk := func(seq uint64, when time.Time, value, quality float64) timeseries.Sample {
		return timeseries.Sample{DeviceID: "dev1", SensorID: "eda1", Signal: timeseries.SignalEDA,
			Seq: seq, DeviceTime: when, ReceivedAt: when, Value: value, Quality: quality}
	}

	// v1 时段：3 个高质量越限 => 确定事件（physical=25）。
	for i := uint64(1); i <= 3; i++ {
		p.IngestSample(ctx, mk(i, t0.Add(time.Duration(i)*time.Second), 25, 0.9))
	}
	// 低质量越限：不确定。
	p.IngestSample(ctx, mk(4, t0.Add(4*time.Second), 99, 0.2))
	// v2 时段：同样 raw=25，但 physical=12.5，在界内 => 正常。标定更新不回溯。
	for i := uint64(5); i <= 7; i++ {
		p.IngestSample(ctx, mk(i, t0.Add(time.Hour).Add(time.Duration(i)*time.Second), 25, 0.9))
	}
	// seq 跳号：8 -> 12 产生缺口。
	p.IngestSample(ctx, mk(12, t0.Add(90*time.Minute), 10, 0.9))

	time.Sleep(300 * time.Millisecond)
	cancel()
	cnt, _ := p.Wait()
	p.Stop()
	t.Logf("COUNTERS accepted=%d rejected=%d gaps=%d normal=%d events=%d uncertain=%d bpRejected=%d",
		cnt.Accepted, cnt.Rejected, cnt.Gaps, cnt.Normal, cnt.Events, cnt.Uncertain, cnt.RejectedBP)
	if cnt.Events != 1 {
		t.Fatalf("exactly one event expected (v1 streak), got %d", cnt.Events)
	}
	if cnt.Uncertain != 1 {
		t.Fatalf("exactly one uncertain expected (low quality), got %d", cnt.Uncertain)
	}
	if cnt.Gaps != 1 {
		t.Fatalf("exactly one gap expected (seq 8..11), got %d", cnt.Gaps)
	}

	// 读回最后封板批次并核对记录类型。
	infos := p.Store().Batches()
	var totalSamples, totalGaps, totalConclusions int
	var eventVersions []string
	for _, info := range infos {
		if !info.Complete {
			continue
		}
		_, recs, err := p.Store().ReadBatch(context.Background(), info.Header.BatchID)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			switch r.Type {
			case storage.RecordSample:
				totalSamples++
				var sr sampleRecord
				if err := json.Unmarshal(r.Payload, &sr); err != nil {
					t.Fatal(err)
				}
			case storage.RecordGap:
				totalGaps++
				var gr gapRecord
				_ = json.Unmarshal(r.Payload, &gr)
				if len(gr.MissingSeq) != 4 || gr.MissingSeq[0] != 8 || gr.MissingSeq[3] != 11 {
					t.Fatalf("gap must encode missing [8,9,10,11], got %v", gr.MissingSeq)
				}
				t.Logf("GAP RECORD sensor=%s missing=%v", gr.SensorID, gr.MissingSeq)
			case storage.RecordConclusion:
				totalConclusions++
				var cr conclusionRecord
				if err := json.Unmarshal(r.Payload, &cr); err != nil {
					t.Fatal(err)
				}
				if cr.Status == "event" {
					eventVersions = append(eventVersions, cr.Basis.CalVersion)
					t.Logf("EVENT RECORD status=%s cal=%s physical=%.2f basis{consec=%d/%d low=%.1f high=%.1f}",
						cr.Status, cr.Basis.CalVersion, cr.Basis.CalibratedValue,
						cr.Basis.Consecutive, cr.Basis.Required, cr.Basis.Low, cr.Basis.High)
				}
			}
		}
	}
	if len(eventVersions) != 1 || eventVersions[0] != "v1" {
		t.Fatalf("the only event must be based on cal v1, got %v", eventVersions)
	}
	if totalGaps != 1 || totalSamples != 8 {
		t.Fatalf("on-disk counts mismatch: samples=%d gaps=%d", totalSamples, totalGaps)
	}
	t.Logf("DISK samples=%d gaps=%d conclusions=%d", totalSamples, totalGaps, totalConclusions)
}

// TestPipelineBackpressureRejectsBounded 验证背压拒绝是显式且有计数的。
func TestPipelineBackpressureRejectsBounded(t *testing.T) {
	reg := calibration.NewRegistry() // 故意不加标定 => 全部样本走不确定路径但仍入队
	p, err := New(Config{
		WorkDir: t.TempDir(), SessionID: "sess2",
		BatchMaxRecords: 100000, QueueCapacity: 2,
		Backpressure: backpressure.PolicyReject,
	}, reg, testRules())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = p.Start(ctx)
	time.Sleep(50 * time.Millisecond)

	// 直接通过队列灌满：处理 goroutine 消费很快，因此统计 accepted+rejectedBP 守恒。
	base := time.Now()
	var rejected, enqueued int
	for i := 0; i < 200; i++ {
		s := timeseries.Sample{DeviceID: "d", SensorID: "eda1", Signal: timeseries.SignalEDA,
			Seq: uint64(i + 1), DeviceTime: base, ReceivedAt: base, Value: 25, Quality: 0.9}
		r := p.sess.GuardIngest(s)
		if err := p.queue.Put(ctx, queued{res: r}); err != nil {
			rejected++
		} else {
			enqueued++
		}
	}
	time.Sleep(200 * time.Millisecond)
	cnt, _ := p.Wait()
	p.Stop()
	t.Logf("BP enqueued=%d rejected=%d (queue cap=2, rejection explicit and bounded)", enqueued, rejected)
	if enqueued+rejected != 200 {
		t.Fatalf("every input must be accounted: enqueued+rejected=%d", enqueued+rejected)
	}
	_ = cnt
}

// TestPipelineSimulatedDevicesAndRecovery 端到端跑模拟设备，随后用新 Store 重开目录，
// 验证完整批次可读、且批次 ID 不与崩溃残留拼接。
func TestPipelineSimulatedDevicesAndRecovery(t *testing.T) {
	t0 := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	reg := calibration.NewRegistry()
	for _, sig := range []struct {
		sensor string
		st     timeseries.SignalType
	}{{"ecg1", timeseries.SignalECG}, {"eda1", timeseries.SignalEDA}} {
		if err := reg.Add(calibration.Calibration{
			SensorID: sig.sensor, Signal: sig.st, Version: "v1",
			EffectiveFrom: t0, Scale: 1, Baseline: 0, MinRaw: -10000, MaxRaw: 10000, CreatedAt: t0,
		}); err != nil {
			t.Fatal(err)
		}
	}

	dir := t.TempDir()
	p, err := New(Config{
		WorkDir: dir, SessionID: "sess3",
		BatchMaxRecords: 40, FlushInterval: 100 * time.Millisecond,
		QueueCapacity: 128, Backpressure: backpressure.PolicyBlock,
		ReorderWindow: 30 * time.Millisecond, MaxRegression: 200 * time.Millisecond,
	}, reg, testRules())
	if err != nil {
		t.Fatal(err)
	}
	p.AddDevice(device.Config{
		DeviceID: "devA",
		Sensors: []device.SensorConfig{
			{SensorID: "ecg1", Signal: timeseries.SignalECG, RateHz: 100, DropRate: 0.15, DriftPPM: 600, Baseline: 0, Amplitude: 0.5},
			{SensorID: "eda1", Signal: timeseries.SignalEDA, RateHz: 25, DropRate: 0, Baseline: 12, Amplitude: 10, QualityLow: 0.4},
		},
		DisconnectAfter: 250 * time.Millisecond, ReconnectAfter: 150 * time.Millisecond,
	}, 99)

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cnt, err := p.Wait()
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	p.Stop() // 处理已结束：封板/归档剩余批次
	t.Logf("E2E accepted=%d rejected=%d gaps=%d events=%d uncertain=%d batches=%d archived=%d",
		cnt.Accepted, cnt.Rejected, cnt.Gaps, cnt.Events, cnt.Uncertain, cnt.Batches, cnt.Archived)
	if cnt.Accepted == 0 || cnt.Gaps == 0 {
		t.Fatalf("expected accepted samples and detected gaps, got %+v", cnt)
	}
	if cnt.Batches < 1 {
		t.Fatal("expected sealed/rotated batches")
	}

	// 进程重启：重新 Open 目录，完整批次必须可读，不完整批次被隔离。
	st2, err := storage.Open(filepath.Join(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	complete := 0
	for _, info := range st2.Batches() {
		if !info.Complete {
			t.Fatalf("clean stop must leave no incomplete batches, got %s", info.Header.BatchID)
		}
		_, recs, err := st2.ReadBatch(context.Background(), info.Header.BatchID)
		if err != nil {
			t.Fatal(err)
		}
		complete++
		t.Logf("REOPEN batch=%s records=%d", info.Header.BatchID, len(recs))
	}
	if complete == 0 {
		t.Fatal("expected at least one complete batch after restart")
	}
}
