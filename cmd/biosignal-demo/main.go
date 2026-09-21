// Command biosignal-demo 在本地运行一条模拟可穿戴生物信号采集处理管线。
//
// 它演示：多设备多传感器不同采样率归一到统一时间轴；丢包与断连产生
// 可区分的 missing_gap；漂移校正；版本化标定（中途更新不回溯历史结论）；
// 质量不足时只输出不确定；批次原子落盘、归档压缩与重启恢复。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/backpressure"
	"github.com/lechandonga/biosignal-capture-pipeline-go/calibration"
	"github.com/lechandonga/biosignal-capture-pipeline-go/detect"
	"github.com/lechandonga/biosignal-capture-pipeline-go/device"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pipeline"
	"github.com/lechandonga/biosignal-capture-pipeline-go/storage"
	"github.com/lechandonga/biosignal-capture-pipeline-go/timeseries"
)

func main() {
	dir := flag.String("dir", "./.demo-data", "working directory for batch files")
	dur := flag.Duration("duration", 3*time.Second, "simulation duration")
	verbose := flag.Bool("v", false, "log every input sample and detection basis")
	flag.Parse()

	if err := os.RemoveAll(*dir); err != nil {
		log.Fatal(err)
	}

	// 标定时间锚点：现在。v1 生效前 2/3 时长，v2 生效后 1/3 时长，演示非回溯更新。
	now := time.Now().UTC()
	reg := calibration.NewRegistry()
	mustAddCal := func(c calibration.Calibration) {
		if err := reg.Add(c); err != nil {
			log.Fatalf("add calibration: %v", err)
		}
	}
	mustAddCal(calibration.Calibration{SensorID: "ecg1", Signal: timeseries.SignalECG, Version: "ecg-v1",
		EffectiveFrom: now.Add(-time.Hour), EffectiveTo: now.Add(*dur / 3 * 2),
		Scale: 1, Baseline: 0, MinRaw: -100, MaxRaw: 100, CreatedAt: now.Add(-2 * time.Hour)})
	mustAddCal(calibration.Calibration{SensorID: "ecg1", Signal: timeseries.SignalECG, Version: "ecg-v2",
		EffectiveFrom: now.Add(*dur / 3 * 2),
		Scale:         2, Baseline: 0, MinRaw: -100, MaxRaw: 100, CreatedAt: now.Add(-time.Hour)})
	mustAddCal(calibration.Calibration{SensorID: "eda1", Signal: timeseries.SignalEDA, Version: "eda-v1",
		EffectiveFrom: now.Add(-time.Hour), Scale: 1, Baseline: 12, MinRaw: -1000, MaxRaw: 1000, CreatedAt: now.Add(-2 * time.Hour)})
	mustAddCal(calibration.Calibration{SensorID: "eda2", Signal: timeseries.SignalEDA, Version: "eda-v1",
		EffectiveFrom: now.Add(-time.Hour), Scale: 1, Baseline: 12, MinRaw: -1000, MaxRaw: 1000, CreatedAt: now.Add(-2 * time.Hour)})

	// 可配置、可复现的检测规则。
	rules := []detect.Rule{
		{Signal: timeseries.SignalEDA, Event: "eda_high", Low: 0, High: 20, MinQuality: 0.5, MinConsecutive: 3},
		{Signal: timeseries.SignalECG, Event: "ecg_clip", Low: -2.2, High: 2.2, MinQuality: 0.6, MinConsecutive: 4},
	}

	p, err := pipeline.New(pipeline.Config{
		WorkDir:         *dir,
		SessionID:       "demo-session",
		BatchMaxRecords: 120,
		FlushInterval:   500 * time.Millisecond,
		QueueCapacity:   256,
		Backpressure:    backpressure.PolicyBlock,
		ReorderWindow:   30 * time.Millisecond,
		MaxRegression:   200 * time.Millisecond,
		VerboseLog:      *verbose,
	}, reg, rules)
	if err != nil {
		log.Fatal(err)
	}

	// 设备 A：ECG(100Hz, 10% 丢包, 漂移) + EDA(25Hz)，周期性断连重连。
	p.AddDevice(device.Config{
		DeviceID: "wearable-A",
		Sensors: []device.SensorConfig{
			{SensorID: "ecg1", Signal: timeseries.SignalECG, RateHz: 100, DropRate: 0.10, DriftPPM: 600,
				Jitter: time.Millisecond, Baseline: 0, Amplitude: 1.2},
			{SensorID: "eda1", Signal: timeseries.SignalEDA, RateHz: 25, DropRate: 0,
				Baseline: 15, Amplitude: 8, QualityLow: 0.55},
		},
		DisconnectAfter: 800 * time.Millisecond,
		ReconnectAfter:  400 * time.Millisecond,
	}, 20260922)

	// 设备 B：单 EDA，低频无丢包，验证多设备并行。
	p.AddDevice(device.Config{
		DeviceID: "wearable-B",
		Sensors: []device.SensorConfig{
			{SensorID: "eda2", Signal: timeseries.SignalEDA, RateHz: 10, DropRate: 0,
				Baseline: 12, Amplitude: 3},
		},
	}, 77)

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()
	if err := p.Start(ctx); err != nil {
		log.Fatal(err)
	}

	cnt, err := p.Wait()
	if err != nil {
		log.Fatal(err)
	}
	cancel()
	p.Stop()

	fmt.Println("================ PIPELINE COUNTERS ================")
	fmt.Printf("accepted samples : %d\n", cnt.Accepted)
	fmt.Printf("rejected samples : %d (duplicate/out-of-order/clock-regression)\n", cnt.Rejected)
	fmt.Printf("detected gaps    : %d (packet loss or disconnect, not silent)\n", cnt.Gaps)
	fmt.Printf("normal           : %d\n", cnt.Normal)
	fmt.Printf("events           : %d\n", cnt.Events)
	fmt.Printf("uncertain        : %d (low quality / no calibration; never asserted)\n", cnt.Uncertain)
	fmt.Printf("sealed batches   : %d\n", cnt.Batches)
	fmt.Printf("archived batches : %d\n", cnt.Archived)

	// 模拟进程重启：重新打开目录并读回完整批次。
	st, err := storage.Open(filepath.Join(*dir))
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()
	fmt.Println("================ RESTART READ-BACK ================")
	for _, info := range st.Batches() {
		state := "INCOMPLETE(quarantined)"
		if info.Complete {
			_, recs, err := st.ReadBatch(context.Background(), info.Header.BatchID)
			if err != nil {
				log.Fatalf("read %s: %v", info.Header.BatchID, err)
			}
			archived := ""
			if info.Archived {
				archived = " [gzip archived]"
			}
			fmt.Printf("batch %s: %d records%s\n", info.Header.BatchID, len(recs), archived)
			continue
		}
		fmt.Printf("batch %s: %s\n", info.Header.BatchID, state)
	}
}
