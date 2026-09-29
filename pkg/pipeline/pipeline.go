// Package pipeline wires simulated devices, clock discipline, calibration,
// detection and storage into a bounded, back-pressured acquisition session.
//
// Invariants:
//   - Ingest never silently drops a sample. Accepted input is either processed
//     or recorded in the rejection log with a distinct reason; when the bounded
//     queue is full the caller receives ErrBackpressure and no partial parse
//     state is retained.
//   - Samples are placed on one unified time axis per sensor via clock
//     discipline; duplicates, sequence regression and clock rollback are all
//     rejected with distinct reasons.
//   - Lost positions are gap markers, recorded (ReasonPacketLoss) and passed
//     downstream so detectors can only say UNCERTAIN about them.
//   - Calibration is chosen by the unified sample time and pinned onto every
//     produced record; expired/missing calibration rejects the sample.
//   - Processing and durable batching run in one worker goroutine; Stop drains
//     the queue and seals the batch, releasing every resource.
package pipeline

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/calibration"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/clock"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/detect"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/storage"
)

// Backpressure policy when the bounded queue is saturated.
type Policy string

const (
	// PolicyReject rejects new samples immediately (ErrBackpressure).
	PolicyReject Policy = "REJECT"
	// PolicyDelay blocks the producer for up to a bounded wait for room.
	PolicyDelay Policy = "DELAY"
)

var (
	// ErrBackpressure is returned when the queue is full under PolicyReject.
	ErrBackpressure = errors.New("pipeline: queue full, backpressure reject")
	// ErrNotRegistered is returned for samples from unknown devices/sensors.
	ErrNotRegistered = errors.New("pipeline: device/sensor not registered")
	// ErrClosed is returned after the session has been stopped.
	ErrClosed = errors.New("pipeline: session closed")
)

// SensorBinding maps an incoming sensor to its signal kind and channel width.
type SensorBinding struct {
	Device model.DeviceID
	Sensor model.SensorID
	Kind   model.SignalKind
	Width  int
}

// Config configures the pipeline bounds and batching.
type Config struct {
	SessionID       string
	QueueDepth      int           // max queued raw samples (backpressure bound)
	BatchSize       int           // max calibrated samples per segment
	SegmentMaxAge   time.Duration // max wall-clock span of one open segment
	SegmentMaxBytes int64         // max on-disk bytes of one open segment
	FlushInterval   time.Duration // deprecated: alias for SegmentMaxAge
	ArchiveAfter    time.Duration // auto-archive sealed segments older than this
	StorageLimit    int64         // max total bytes on disk (oldest sealed reclaimed)
	Maintenance     time.Duration // archive/quota maintenance interval
	Policy          Policy        // backpressure policy
	EnqueueWait     time.Duration // max block under PolicyDelay
}

// Stats summarizes a session for verification.
type Stats struct {
	Submitted int // every Ingest call, including ones rejected before enqueue
	Accepted  int // samples accepted into the bounded queue
	Processed int // fully processed to calibrated records
	Gaps      int // packet-loss gap markers accounted (HANDLED)
	Rejected  int // hard rejections with a distinct reason
	Events    int
	Batches   []string
}

// Pipeline runs one acquisition session.
type Pipeline struct {
	cfg      Config
	cals     *calibration.Store
	eng      *detect.Engine
	st       *storage.Store
	bindings map[model.DeviceID]map[model.SensorID]SensorBinding
	clocks   map[model.DeviceID]*clock.Discipline

	queue chan model.RawSample
	wg    sync.WaitGroup // worker goroutine
	// ingestGate is held in read mode for the whole Ingest critical section;
	// Stop takes the write lock, guaranteeing no Ingest is racing a shutdown.
	ingestGate sync.RWMutex
	mu         sync.Mutex
	closed     bool
	started    chan struct{}

	logMu    sync.Mutex
	rejected []model.RejectedRecord

	statsMu sync.Mutex
	stats   Stats

	batchID     int
	openBatch   string
	batchCount  int
	batchSince  time.Time
	rejectedDur int // rejection-log entries already persisted to storage
	maintTimer  *time.Ticker
}

// New constructs a pipeline.
func New(cfg Config, cals *calibration.Store, rules []detect.Rule, st *storage.Store) *Pipeline {
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = 1024
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	if cfg.SegmentMaxAge <= 0 {
		cfg.SegmentMaxAge = cfg.FlushInterval
	}
	if cfg.Maintenance <= 0 {
		cfg.Maintenance = time.Second
	}
	if cfg.Policy == "" {
		cfg.Policy = PolicyReject
	}
	if cfg.EnqueueWait <= 0 {
		cfg.EnqueueWait = 50 * time.Millisecond
	}
	return &Pipeline{
		cfg:      cfg,
		cals:     cals,
		eng:      detect.NewEngine(rules),
		st:       st,
		bindings: map[model.DeviceID]map[model.SensorID]SensorBinding{},
		clocks:   map[model.DeviceID]*clock.Discipline{},
		queue:    make(chan model.RawSample, cfg.QueueDepth),
		started:  make(chan struct{}),
	}
}

// RegisterDevice adds a device to the session.
func (p *Pipeline) RegisterDevice(id model.DeviceID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosed
	}
	if _, ok := p.bindings[id]; ok {
		return nil
	}
	p.bindings[id] = map[model.SensorID]SensorBinding{}
	p.clocks[id] = clock.NewDiscipline(id)
	return nil
}

// BindSensor declares a sensor's kind/width so only known streams are ingested.
func (p *Pipeline) BindSensor(b SensorBinding) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosed
	}
	sensors, ok := p.bindings[b.Device]
	if !ok {
		return fmt.Errorf("%w: device %s", ErrNotRegistered, b.Device)
	}
	sensors[b.Sensor] = b
	return nil
}

// Ingest accepts one raw sample. It applies the configured backpressure and
// never silently drops: rejected/handled outcomes are recorded and the
// returned error names the cause. Gap markers (Lost=true) are accepted so the
// gap is accounted for.
func (p *Pipeline) Ingest(s model.RawSample) error {
	// The read gate overlaps the whole accept+enqueue critical section. Stop
	// takes the write gate before closing the queue, so a successful Ingest
	// can never race the queue close (no send-on-closed-channel) and every
	// accepted item is processed during the graceful drain.
	p.ingestGate.RLock()
	defer p.ingestGate.RUnlock()
	p.statsMu.Lock()
	p.stats.Submitted++
	p.statsMu.Unlock()
	if p.isClosed() {
		p.record(model.RejectedRecord{Action: model.ActionRejected, Sample: s,
			Reason: model.ReasonSessionClosed, Detail: "ingest after Stop", At: recvOrNow(s)})
		return ErrClosed
	}
	sensors, ok := p.bindings[s.Device]
	if !ok {
		p.record(model.RejectedRecord{Action: model.ActionRejected, Sample: s,
			Reason: model.ReasonUnknownSensor, Detail: "device not registered", At: recvOrNow(s)})
		return fmt.Errorf("%w: device %s", ErrNotRegistered, s.Device)
	}
	if _, ok := sensors[s.Sensor]; !ok {
		p.record(model.RejectedRecord{Action: model.ActionRejected, Sample: s,
			Reason: model.ReasonUnknownSensor, Detail: "sensor not bound", At: recvOrNow(s)})
		return fmt.Errorf("%w: sensor %s", ErrNotRegistered, s.Sensor)
	}

	switch p.cfg.Policy {
	case PolicyDelay:
		timer := time.NewTimer(p.cfg.EnqueueWait)
		defer timer.Stop()
		select {
		case p.queue <- s:
			p.countAccepted()
			return nil
		case <-timer.C:
			// Nothing is parsed before enqueue, so no partial state exists.
			p.record(model.RejectedRecord{Action: model.ActionRejected, Sample: s,
				Reason: model.ReasonBufferFull, Detail: "enqueue wait elapsed", At: recvOrNow(s)})
			return ErrBackpressure
		}
	default: // PolicyReject
		select {
		case p.queue <- s:
			p.countAccepted()
			return nil
		default:
			p.record(model.RejectedRecord{Action: model.ActionRejected, Sample: s,
				Reason: model.ReasonBufferFull, Detail: "queue full", At: recvOrNow(s)})
			return ErrBackpressure
		}
	}
}

func (p *Pipeline) countAccepted() {
	p.statsMu.Lock()
	p.stats.Accepted++
	p.statsMu.Unlock()
}

func (p *Pipeline) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// Resume prepares the pipeline to continue an existing session after a
// process restart: it advances the segment counter past every segment id
// already present on disk. Incomplete segments from the previous run are
// never reopened or appended to — new samples always land in a fresh segment.
func (p *Pipeline) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := p.st.NextSessionSeq(p.cfg.SessionID); n > p.batchID {
		p.batchID = n - 1
	}
}

// applyStoragePolicy pushes the configured quota onto the store. It is safe
// to call before Run and again after a restart.
func (p *Pipeline) applyStoragePolicy() {
	if p.cfg.StorageLimit > 0 {
		p.st.SetQuota(p.cfg.StorageLimit)
	}
}

// Run is the single processing worker. Stop closes the queue once all in-flight
// Ingest calls have finished; ranging over it drains every accepted sample
// (none is silently dropped) before sealing the final segment.
func (p *Pipeline) Run() {
	p.wg.Add(1)
	defer p.wg.Done()
	close(p.started)
	p.applyStoragePolicy()

	var ticker *time.Ticker
	if p.cfg.SegmentMaxAge > 0 {
		ticker = time.NewTicker(p.cfg.SegmentMaxAge)
		defer ticker.Stop()
	}
	// With ticker == nil the flush arm reads from a nil channel and never
	// fires, disabling periodic sealing safely.
	p.maintTimer = time.NewTicker(p.cfg.Maintenance)
	defer p.maintTimer.Stop()

	var flushC <-chan time.Time
	if ticker != nil {
		flushC = ticker.C
	}
	for {
		select {
		case s, ok := <-p.queue:
			if !ok {
				p.sealBatch()
				return
			}
			p.process(s)
		case <-flushC:
			p.flushIfOld()
		case <-p.maintTimer.C:
			p.maintain()
		}
	}
}

// process advances one sample through clock -> calibration -> detect -> store.
// Each failure path records a distinct reason and leaves no partial state.
func (p *Pipeline) process(s model.RawSample) {
	p.mu.Lock()
	disc := p.clocks[s.Device]
	binding := p.bindings[s.Device][s.Sensor]
	p.mu.Unlock()

	// Lost positions are explicit gaps: account them, never fabricate. A gap
	// only reserves its sequence position; its synthetic timing must NOT be
	// fitted into the clock model (that would corrupt drift estimation for
	// later real samples). Its unified time uses the device reading directly.
	if s.Lost {
		if err := disc.CheckSequence(s.Sensor, s.Seq); err != nil {
			p.recordClockError(s, err)
			return
		}
		gap := model.AlignedSample{
			Device: s.Device, Sensor: s.Sensor, Kind: binding.Kind, Seq: s.Seq,
			UniTime: s.DevTime, RecvTime: s.RecvTime, Quality: 0, Gap: true,
		}
		cal := model.CalibratedSample{
			Aligned: gap, CalVersion: "", Values: nil, Baseline: nil,
			Uncertain: true, Uncertainty: "packet loss gap marker",
		}
		p.record(model.RejectedRecord{Action: model.ActionHandled, Sample: s,
			Reason: model.ReasonPacketLoss, Detail: fmt.Sprintf("gap seq=%d at=%s", s.Seq, s.DevTime),
			At: recvOrNow(s)})
		p.persist(cal, nil)
		p.statsMu.Lock()
		p.stats.Gaps++
		p.statsMu.Unlock()
		return
	}

	// Clock correction / unified axis.
	uni, err := disc.Correct(s.Sensor, s.Seq, s.DevTime, s.RecvTime)
	if err != nil {
		p.recordClockError(s, err)
		return
	}

	aligned := model.AlignedSample{
		Device: s.Device, Sensor: s.Sensor, Kind: binding.Kind, Seq: s.Seq,
		UniTime: uni, RecvTime: s.RecvTime, Values: s.Values, Quality: s.Quality,
	}

	// Calibration selected by unified sample time (never retroactive).
	params, err := p.cals.At(s.Sensor, uni)
	if err != nil {
		reason := model.ReasonNoCalibration
		detail := "no calibration covers sample time"
		if errors.Is(err, calibration.ErrCalibrationExpired) {
			reason = model.ReasonExpiredCalibration
			detail = "calibration expired at sample time"
		} else if errors.Is(err, calibration.ErrInvalidParams) {
			reason = model.ReasonBadCalibration
			detail = err.Error()
		}
		p.record(model.RejectedRecord{Action: model.ActionRejected, Sample: s,
			Reason: reason, Detail: detail, At: recvOrNow(s)})
		return
	}
	physical, baseline, err := params.Apply(s.Values)
	if err != nil {
		p.record(model.RejectedRecord{Action: model.ActionRejected, Sample: s,
			Reason: model.ReasonBadCalibration, Detail: err.Error(), At: recvOrNow(s)})
		return
	}
	cal := model.CalibratedSample{
		Aligned: aligned, CalVersion: params.Version, Values: physical, Baseline: baseline,
	}

	// Detection: definite or uncertain, each with evidence.
	events := p.eng.Push(cal)
	for i := range events {
		if events[i].At.IsZero() {
			events[i].At = time.Now()
		}
	}
	p.persist(cal, events)

	p.statsMu.Lock()
	p.stats.Processed++
	p.stats.Events += len(events)
	p.statsMu.Unlock()
}

func (p *Pipeline) recordClockError(s model.RawSample, err error) {
	reason := model.ReasonClockRollback
	switch {
	case errors.Is(err, clock.ErrDuplicate):
		reason = model.ReasonDuplicate
	case errors.Is(err, clock.ErrSeqRegression):
		reason = model.ReasonSeqRegression
	case errors.Is(err, clock.ErrUnifiedRollback):
		reason = model.ReasonUnifiedRollback
	case errors.Is(err, clock.ErrClockRollback):
		reason = model.ReasonClockRollback
	}
	p.record(model.RejectedRecord{Action: model.ActionRejected, Sample: s,
		Reason: reason, Detail: err.Error(), At: recvOrNow(s)})
}

// persist writes into the current durable batch, rolling to a new batch at
// BatchSize. Storage errors are recorded but never poison the in-memory
// stream: the sample's decision remains visible in the rejection log.
func (p *Pipeline) persist(cal model.CalibratedSample, events []model.Event) {
	if err := p.ensureBatch(cal.Aligned.UniTime); err != nil {
		p.record(model.RejectedRecord{Action: model.ActionRejected,
			Sample: cal.Aligned.AsRaw(), Reason: model.ReasonSessionClosed,
			Detail: "batch create failed: " + err.Error(), At: time.Now()})
		return
	}
	if err := p.st.AppendBatchSample(p.openBatch, cal); err != nil {
		p.record(model.RejectedRecord{Action: model.ActionRejected,
			Sample: cal.Aligned.AsRaw(), Reason: model.ReasonSessionClosed,
			Detail: "sample append failed: " + err.Error(), At: time.Now()})
		return
	}
	for _, ev := range events {
		if err := p.st.AppendBatchEvent(p.openBatch, ev); err != nil {
			p.record(model.RejectedRecord{Action: model.ActionRejected,
				Sample: cal.Aligned.AsRaw(), Reason: model.ReasonSessionClosed,
				Detail: "event append failed: " + err.Error(), At: time.Now()})
		}
	}
	p.flushRejectedIntoBatch()
	p.batchCount++
	roll := p.batchCount >= p.cfg.BatchSize
	if p.cfg.SegmentMaxBytes > 0 && p.st.BatchSizeBytes(p.openBatch) >= p.cfg.SegmentMaxBytes {
		roll = true
	}
	if roll {
		p.sealBatch()
	}
}

func (p *Pipeline) ensureBatch(now time.Time) error {
	if p.openBatch != "" {
		return nil
	}
	p.batchID++
	id := fmt.Sprintf("%s-batch-%06d", p.cfg.SessionID, p.batchID)
	if err := p.st.CreateBatch(id, p.cfg.SessionID, now); err != nil {
		return err
	}
	p.openBatch = id
	p.batchSince = now
	p.batchCount = 0
	p.flushRejectedIntoBatch()
	return nil
}

// maintain archives sealed segments older than ArchiveAfter (best effort;
// segments currently being read stay in place) and enforces the storage
// quota. It runs on the single worker goroutine, so it never races writing.
func (p *Pipeline) maintain() {
	if p.cfg.ArchiveAfter > 0 {
		cutoff := time.Now().Add(-p.cfg.ArchiveAfter)
		for _, m := range p.st.Segments() {
			if m.State == model.BatchComplete && !m.ClosedAt.IsZero() && m.ClosedAt.Before(cutoff) {
				_ = p.st.ArchiveBatch(m.ID)
			}
		}
	}
	if p.cfg.StorageLimit > 0 {
		_, _, _, _ = p.st.EnforceQuota()
	}
}

func (p *Pipeline) sealBatch() {
	if p.openBatch == "" {
		return
	}
	id := p.openBatch
	if err := p.st.CompleteBatch(id, time.Now()); err != nil {
		return
	}
	p.statsMu.Lock()
	p.stats.Batches = append(p.stats.Batches, id)
	p.statsMu.Unlock()
	p.openBatch = ""
	p.batchCount = 0
	if p.cfg.StorageLimit > 0 {
		_, _, _, _ = p.st.EnforceQuota()
	}
}

func (p *Pipeline) flushIfOld() {
	if p.openBatch == "" {
		return
	}
	if p.cfg.SegmentMaxAge > 0 && time.Since(p.batchSince) >= p.cfg.SegmentMaxAge {
		p.sealBatch()
	}
}

func (p *Pipeline) record(r model.RejectedRecord) {
	p.logMu.Lock()
	p.rejected = append(p.rejected, r)
	p.logMu.Unlock()
	p.statsMu.Lock()
	if r.Action == model.ActionRejected {
		p.stats.Rejected++
	}
	p.statsMu.Unlock()
}

// Reconnect informs clock discipline that a device is back so a new drift
// epoch begins. It does not replay or discard anything: sequences guarantee
// no double count and gaps account the outage.
func (p *Pipeline) Reconnect(id model.DeviceID, sensor model.SensorID, at time.Time) error {
	p.mu.Lock()
	disc, ok := p.clocks[id]
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: device %s", ErrNotRegistered, id)
	}
	return disc.Reconnect(sensor, at)
}

// flushRejectedIntoBatch durably appends not-yet-persisted log entries to the
// currently open batch, so rejections survive restart with their batch.
func (p *Pipeline) flushRejectedIntoBatch() {
	p.logMu.Lock()
	pending := append([]model.RejectedRecord(nil), p.rejected[p.rejectedDur:]...)
	p.logMu.Unlock()
	if len(pending) == 0 || p.openBatch == "" {
		return
	}
	for _, r := range pending {
		if err := p.st.AppendBatchRejected(p.openBatch, r); err != nil {
			return // retry on next batch/flush
		}
		p.logMu.Lock()
		p.rejectedDur++
		p.logMu.Unlock()
	}
}

// Rejected returns a copy of the rejection/handling log.
func (p *Pipeline) Rejected() []model.RejectedRecord {
	p.logMu.Lock()
	defer p.logMu.Unlock()
	out := make([]model.RejectedRecord, len(p.rejected))
	copy(out, p.rejected)
	return out
}

// Stats returns a copy of session statistics.
func (p *Pipeline) Stats() Stats {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	st := p.stats
	st.Batches = append([]string(nil), p.stats.Batches...)
	return st
}

// Stop performs a graceful shutdown: it waits for the worker to be running,
// blocks until every in-flight Ingest has completed (so no send can race a
// close), marks the session closed, closes the queue, and waits for the worker
// to drain all accepted samples and seal the final batch.
func (p *Pipeline) Stop() {
	<-p.started
	p.ingestGate.Lock() // acquires only after all in-flight Ingest RUnlock
	defer p.ingestGate.Unlock()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.wg.Wait()
		return
	}
	p.closed = true
	p.mu.Unlock()
	close(p.queue)
	p.wg.Wait()
}

func recvOrNow(s model.RawSample) time.Time {
	if !s.RecvTime.IsZero() {
		return s.RecvTime
	}
	return time.Now()
}
