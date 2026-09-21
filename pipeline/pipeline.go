// Package pipeline 把会话、时间对齐、标定、检测与落盘组装为完整管线。
package pipeline

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/backpressure"
	"github.com/lechandonga/biosignal-capture-pipeline-go/calibration"
	"github.com/lechandonga/biosignal-capture-pipeline-go/detect"
	"github.com/lechandonga/biosignal-capture-pipeline-go/device"
	"github.com/lechandonga/biosignal-capture-pipeline-go/session"
	"github.com/lechandonga/biosignal-capture-pipeline-go/storage"
	"github.com/lechandonga/biosignal-capture-pipeline-go/timeseries"
)

// Config 是管线配置：吞吐上限、背压、批次与栅格周期均可配置。
type Config struct {
	WorkDir         string
	SessionID       string
	BatchMaxRecords int           // 单批最大记录数，达到即封板滚动
	FlushInterval   time.Duration // 定时封板间隔
	QueueCapacity   int
	Backpressure    backpressure.Policy
	ReorderWindow   time.Duration
	MaxRegression   time.Duration
	ArchiveAfter    time.Duration // 批次封板后多久归档压缩（零值=立即）
	VerboseLog      bool          // 打印每条输入与判定依据
}

// Counters 是管线运行计数，便于验证与演示。
type Counters struct {
	Accepted   int64
	Rejected   int64
	Gaps       int64
	Normal     int64
	Events     int64
	Uncertain  int64
	Batches    int64
	Archived   int64
	RejectedBP int64
}

// sampleRecord / gapRecord / conclusionRecord 是落盘 JSON 结构。
type sampleRecord struct {
	Kind         string  `json:"kind"`
	DeviceID     string  `json:"device_id"`
	SensorID     string  `json:"sensor_id"`
	Signal       string  `json:"signal"`
	Seq          uint64  `json:"seq"`
	UnifiedTime  string  `json:"unified_time"`
	DeviceTime   string  `json:"device_time"`
	RawValue     float64 `json:"raw_value"`
	Physical     float64 `json:"physical,omitempty"`
	Quality      float64 `json:"quality"`
	CalVersion   string  `json:"cal_version,omitempty"`
	AcceptReason string  `json:"accept_reason,omitempty"`
	RejectReason string  `json:"reject_reason,omitempty"`
	Detail       string  `json:"detail,omitempty"`
}

type gapRecord struct {
	Kind       string   `json:"kind"`
	DeviceID   string   `json:"device_id"`
	SensorID   string   `json:"sensor_id"`
	Signal     string   `json:"signal"`
	MissingSeq []uint64 `json:"missing_seq"`
	FromTime   string   `json:"from_time"`
	ToTime     string   `json:"to_time"`
	DetectedAt string   `json:"detected_at"`
}

type conclusionRecord struct {
	Kind     string       `json:"kind"`
	Time     string       `json:"time"`
	DeviceID string       `json:"device_id"`
	SensorID string       `json:"sensor_id"`
	Signal   string       `json:"signal"`
	Event    string       `json:"event"`
	Status   string       `json:"status"`
	Reason   string       `json:"reason"`
	Basis    detect.Basis `json:"basis"`
}

type queued struct {
	res   timeseries.IngestResult
	flush bool
}

// Pipeline 是完整采集处理管线。
type Pipeline struct {
	cfg   Config
	reg   *calibration.Registry
	det   *detect.Detector
	store *storage.Store
	sess  *session.Session

	queue   *backpressure.Queue[queued]
	control chan queued // flush 等控制消息与数据处理同 goroutine 串行

	devCfgs []device.Config
	seeds   []int64

	counters Counters

	procDone chan struct{}
	pumpDone chan struct{}

	mu       sync.Mutex
	curBatch *storage.Batch
}

// New 创建管线。
func New(cfg Config, reg *calibration.Registry, rules []detect.Rule) (*Pipeline, error) {
	if cfg.BatchMaxRecords <= 0 {
		cfg.BatchMaxRecords = 500
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 256
	}
	if cfg.ReorderWindow == 0 {
		cfg.ReorderWindow = 20 * time.Millisecond
	}
	if cfg.MaxRegression == 0 {
		cfg.MaxRegression = 100 * time.Millisecond
	}
	det, err := detect.NewDetector(rules)
	if err != nil {
		return nil, err
	}
	st, err := storage.Open(cfg.WorkDir)
	if err != nil {
		return nil, err
	}
	sess := session.New(cfg.SessionID, session.Config{
		ReorderWindow: cfg.ReorderWindow,
		MaxRegression: cfg.MaxRegression,
		Buffer:        cfg.QueueCapacity,
	})
	return &Pipeline{
		cfg:      cfg,
		reg:      reg,
		det:      det,
		store:    st,
		sess:     sess,
		queue:    backpressure.New[queued](cfg.QueueCapacity, cfg.Backpressure),
		control:  make(chan queued, 4),
		procDone: make(chan struct{}),
		pumpDone: make(chan struct{}),
	}, nil
}

// AddDevice 向管线注册一台模拟设备（启动前调用）。driftPPM 为每传感器漂移率。
func (p *Pipeline) AddDevice(cfg device.Config, seed int64) {
	p.devCfgs = append(p.devCfgs, cfg)
	p.seeds = append(p.seeds, seed)
	drifts := make(map[string]float64, len(cfg.Sensors))
	for _, sc := range cfg.Sensors {
		drifts[sc.SensorID] = sc.DriftPPM
	}
	dev := device.New(cfg, seed)
	p.sess.Attach(dev, drifts)
}

// IngestSample 同步注入一个样本并按背压策略入队（确定性输入，主要用于测试）。
func (p *Pipeline) IngestSample(ctx context.Context, sample timeseries.Sample) (timeseries.IngestResult, error) {
	res := p.guardIngest(sample)
	err := p.queue.Put(ctx, queued{res: res})
	return res, err
}

// guardIngest 直接走时钟保护器，避免与会话事件泵并发使用同一通道。
func (p *Pipeline) guardIngest(sample timeseries.Sample) timeseries.IngestResult {
	return p.sess.GuardIngest(sample)
}

// Start 启动管线：会话设备泵、背压入队、处理 worker 与批次滚动。
func (p *Pipeline) Start(ctx context.Context) error {
	// 处理 worker：从有界数据队列与控制队列取消息，完成标定、检测与落盘。
	go p.processLoop(ctx)

	// 设备泵：session.Run 结束后关闭事件流，入队循环随之结束并关闭队列。
	go func() {
		defer close(p.pumpDone)
		_ = p.sess.Run(ctx)
	}()

	go func() {
		for res := range p.sess.Events() {
			if err := p.queue.Put(ctx, queued{res: res}); err != nil {
				// ctx 取消时不算背压拒绝；其余策略性拒绝显式计数。
				if ctx.Err() == nil {
					atomic.AddInt64(&p.counters.RejectedBP, 1)
				}
			}
		}
		p.queue.Close()
	}()
	return nil
}

// Wait 等待设备泵停止且处理 worker 排空后返回。必须在 Stop 之前调用。
func (p *Pipeline) Wait() (Counters, error) {
	<-p.pumpDone
	<-p.procDone
	return p.snapshotCounters(), nil
}

func (p *Pipeline) snapshotCounters() Counters {
	return Counters{
		Accepted:   atomic.LoadInt64(&p.counters.Accepted),
		Rejected:   atomic.LoadInt64(&p.counters.Rejected),
		Gaps:       atomic.LoadInt64(&p.counters.Gaps),
		Normal:     atomic.LoadInt64(&p.counters.Normal),
		Events:     atomic.LoadInt64(&p.counters.Events),
		Uncertain:  atomic.LoadInt64(&p.counters.Uncertain),
		Batches:    atomic.LoadInt64(&p.counters.Batches),
		Archived:   atomic.LoadInt64(&p.counters.Archived),
		RejectedBP: atomic.LoadInt64(&p.counters.RejectedBP),
	}
}

// Stop 优雅停止（须在 Wait 之后调用）：非空批次封板并归档，关闭存储；
// 空批次直接丢弃，不残留不完整文件，不产生 0 记录批次。
func (p *Pipeline) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p.mu.Lock()
	b := p.curBatch
	p.curBatch = nil
	p.mu.Unlock()
	if b != nil {
		b.Mu().Lock()
		empty := b.Count() == 0
		b.Mu().Unlock()
		if empty {
			// 空批次：不写尾标，直接关闭文件并清理，恢复阶段本就会隔离；这里主动删除避免噪音。
			_ = os.Remove(filepath.Join(p.cfg.WorkDir, "batches", b.Header().BatchID+".data"))
			_ = os.Remove(filepath.Join(p.cfg.WorkDir, "batches", b.Header().BatchID+".meta"))
		} else {
			if err := b.Seal(ctx); err != nil {
				log.Printf("pipeline: seal final batch: %v", err)
			} else {
				atomic.AddInt64(&p.counters.Batches, 1)
				if err := p.store.Archive(ctx, b.Header().BatchID); err != nil {
					log.Printf("pipeline: archive final batch: %v", err)
				} else {
					atomic.AddInt64(&p.counters.Archived, 1)
				}
			}
		}
	}
	_ = p.store.Close()
}

// processLoop 是处理/落盘核心循环；数据与 flush 控制在此串行，批次状态不被并发撕裂。
func (p *Pipeline) processLoop(ctx context.Context) {
	defer close(p.procDone)

	// 单一 bridge：把有界队列的 Get 永久转发到 dataCh，
	// 避免 select 控制消息时产生的孤儿读取造成静默丢数据。
	dataCh := make(chan queued)
	go func() {
		defer close(dataCh)
		for {
			v, ok := p.queue.Get(ctx)
			if !ok {
				return
			}
			select {
			case dataCh <- v:
			case <-ctx.Done():
				return
			}
		}
	}()

	tickCtx, stopTicks := context.WithCancel(context.Background())
	defer stopTicks()
	if p.cfg.FlushInterval > 0 {
		t := time.NewTicker(p.cfg.FlushInterval)
		defer t.Stop()
		go func() {
			for {
				select {
				case <-tickCtx.Done():
					return
				case <-t.C:
					select {
					case p.control <- queued{flush: true}:
					case <-tickCtx.Done():
						return
					}
				}
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			stopTicks() // 排空阶段不再接受定时封板，统一由 Stop 收尾提交
			// 设备泵随后停止并关闭队列；继续排空已接收数据，不能静默丢失。
			for v := range dataCh {
				p.handle(context.Background(), v.res)
			}
			return
		case msg := <-p.control:
			if msg.flush {
				p.rotateBatch(ctx, true)
			}
		case v, ok := <-dataCh:
			if !ok {
				return
			}
			p.handle(ctx, v.res)
		}
	}
}

// rotateBatch 封板当前批次（若有记录），归档旧批次，并开启新批次。
// 提交属于收尾动作：ctx 取消后仍给予有限宽限完成封板/归档，避免残留半成品。
func (p *Pipeline) rotateBatch(ctx context.Context, archive bool) {
	// 封板/归档是收尾提交：独立于调用 ctx（设备 ctx 到期不影响已在途批次的完整性）。
	commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = ctx // 调用方 ctx 不再决定提交生死，仅用于调用点语义

	p.mu.Lock()
	b := p.curBatch
	p.mu.Unlock()
	if b == nil {
		return
	}
	b.Mu().Lock()
	empty := b.Count() == 0
	b.Mu().Unlock()
	if empty {
		// 空批次不封板：避免产生 0 记录批次与无意义归档。
		return
	}
	if err := b.Seal(commitCtx); err != nil {
		log.Printf("pipeline: seal batch: %v", err)
		return
	}
	atomic.AddInt64(&p.counters.Batches, 1)
	id := b.Header().BatchID
	if archive {
		if err := p.store.Archive(commitCtx, id); err != nil {
			log.Printf("pipeline: archive %s: %v", id, err)
		} else {
			atomic.AddInt64(&p.counters.Archived, 1)
		}
	}
	nb, err := p.store.StartBatch(p.cfg.SessionID) // StartBatch 不依赖 ctx
	if err != nil {
		log.Printf("pipeline: start new batch: %v", err)
		p.mu.Lock()
		p.curBatch = nil
		p.mu.Unlock()
		return
	}
	p.mu.Lock()
	p.curBatch = nb
	p.mu.Unlock()
}

// handle 处理一个 ingest 结论：记录缺口、标定、检测，并按批次落盘。
func (p *Pipeline) handle(ctx context.Context, res timeseries.IngestResult) {
	if res.Accepted {
		atomic.AddInt64(&p.counters.Accepted, 1)
	} else {
		atomic.AddInt64(&p.counters.Rejected, 1)
	}

	// 缺口先落盘（即使后续处理失败，缺口记录本身也是独立帧）。
	for _, gap := range res.Gaps {
		atomic.AddInt64(&p.counters.Gaps, 1)
		p.writeJSON(ctx, storage.RecordGap, gapRecord{
			Kind: "gap", DeviceID: gap.DeviceID, SensorID: gap.SensorID,
			Signal: string(gap.Signal), MissingSeq: gap.MissingSeq,
			FromTime:   gap.FromTime.Format(time.RFC3339Nano),
			ToTime:     gap.ToTime.Format(time.RFC3339Nano),
			DetectedAt: gap.DetectedAt.Format(time.RFC3339Nano),
		})
	}

	if !res.Accepted {
		// 被时钟/重复/乱序拒绝的样本只记录拒绝原因，不进入检测。
		p.writeSampleRecord(ctx, res, 0, "", false)
		return
	}

	a := res.Aligned
	// 标定查询：按统一时间（数据时间）取当时生效版本；失败显式不确定。
	cal, err := p.reg.At(a.SensorID, a.Signal, a.UnifiedTime)
	if err != nil {
		p.writeSampleRecord(ctx, res, 0, "", false)
		in := detect.EvaluateInput{Sample: *a, CalibratedOK: false}
		p.evaluateAndStore(ctx, in, "no_effective_calibration: "+err.Error())
		return
	}
	physical, err := cal.Apply(a.Value)
	if err != nil {
		p.writeSampleRecord(ctx, res, 0, cal.Version, false)
		in := detect.EvaluateInput{Sample: *a, CalibratedOK: false, CalVersion: cal.Version}
		p.evaluateAndStore(ctx, in, "calibration_apply_failed: "+err.Error())
		return
	}
	p.writeSampleRecord(ctx, res, physical, cal.Version, true)

	conclusions := p.det.Evaluate(*a, physical, cal.Baseline, cal.Version)
	p.storeConclusions(ctx, conclusions)
	p.rotateIfFull(ctx)
}

// evaluateAndStore 为不可标定样本输出全部相关事件的不确定结论。
func (p *Pipeline) evaluateAndStore(ctx context.Context, in detect.EvaluateInput, reason string) {
	conclusions := p.det.EvaluateUncertain(in, reason)
	p.storeConclusions(ctx, conclusions)
}

func (p *Pipeline) storeConclusions(ctx context.Context, cs []detect.Conclusion) {
	for _, c := range cs {
		switch c.Status {
		case detect.StatusEvent:
			atomic.AddInt64(&p.counters.Events, 1)
		case detect.StatusUncertain:
			atomic.AddInt64(&p.counters.Uncertain, 1)
		default:
			atomic.AddInt64(&p.counters.Normal, 1)
		}
		if p.cfg.VerboseLog {
			log.Printf("DETECT sensor=%s event=%s status=%s reason=%s physical=%.4f quality=%.3f cal=%s",
				c.SensorID, c.Event, c.Status, c.Reason, c.Basis.CalibratedValue, c.Basis.Quality, c.Basis.CalVersion)
		}
		p.writeJSON(ctx, storage.RecordConclusion, conclusionRecord{
			Kind: "conclusion", Time: c.Time.Format(time.RFC3339Nano),
			DeviceID: c.DeviceID, SensorID: c.SensorID, Signal: string(c.Signal),
			Event: c.Event, Status: string(c.Status), Reason: c.Reason, Basis: c.Basis,
		})
	}
}

func (p *Pipeline) writeSampleRecord(ctx context.Context, res timeseries.IngestResult, physical float64, calVersion string, calibrated bool) {
	if res.Aligned == nil {
		return
	}
	a := res.Aligned
	rec := sampleRecord{
		Kind:        "sample",
		DeviceID:    a.DeviceID,
		SensorID:    a.SensorID,
		Signal:      string(a.Signal),
		Seq:         a.Seq,
		UnifiedTime: a.UnifiedTime.Format(time.RFC3339Nano),
		DeviceTime:  a.DeviceTime.Format(time.RFC3339Nano),
		RawValue:    a.Value,
		Quality:     a.Quality,
		Detail:      res.Detail,
	}
	if calibrated {
		rec.Physical = physical
		rec.CalVersion = calVersion
	}
	if res.Accepted {
		rec.AcceptReason = string(res.Reason)
	} else {
		rec.RejectReason = string(res.Reason)
	}
	if p.cfg.VerboseLog {
		log.Printf("INPUT sensor=%s seq=%d raw=%.4f physical=%.4f q=%.3f accepted=%v reason=%s",
			rec.SensorID, rec.Seq, rec.RawValue, rec.Physical, rec.Quality, res.Accepted, res.Reason)
	}
	p.writeJSON(ctx, storage.RecordSample, rec)
}

// rotateIfFull 在当前批次达到上限时投递滚动消息（由处理 goroutine 串行执行）。
func (p *Pipeline) rotateIfFull(ctx context.Context) {
	p.mu.Lock()
	b := p.curBatch
	p.mu.Unlock()
	if b == nil {
		return
	}
	b.Mu().Lock()
	full := b.Count() >= p.cfg.BatchMaxRecords
	b.Mu().Unlock()
	if full {
		select {
		case p.control <- queued{flush: true}:
		case <-ctx.Done():
		}
	}
}

// ensureBatch 返回当前开放批次，必要时在处理 goroutine 内串行创建。
func (p *Pipeline) ensureBatch() (*storage.Batch, error) {
	p.mu.Lock()
	b := p.curBatch
	p.mu.Unlock()
	if b != nil {
		return b, nil
	}
	nb, err := p.store.StartBatch(p.cfg.SessionID)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.curBatch == nil {
		p.curBatch = nb
	} else {
		nb = p.curBatch
	}
	p.mu.Unlock()
	return nb, nil
}

func (p *Pipeline) writeJSON(ctx context.Context, typ storage.RecordType, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		// 序列化失败属于程序缺陷，记录但不写半条记录。
		log.Printf("pipeline: marshal %T: %v", v, err)
		return
	}
	b, err := p.ensureBatch()
	if err != nil {
		log.Printf("pipeline: start batch: %v", err)
		return
	}
	// 已进入处理阶段的数据必须落盘：写帧独立于设备 ctx，仅受有限宽限约束。
	appendCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.Append(appendCtx, storage.Record{Type: typ, Payload: data}); err != nil {
		log.Printf("pipeline: append record: %v", err)
	}
}

// Store 暴露存储以便验证读取。
func (p *Pipeline) Store() *storage.Store { return p.store }
