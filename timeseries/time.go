// Package timeseries 提供生物信号样本模型、统一时间轴对齐与逻辑时钟保护。
package timeseries

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// SignalType 生理信号类型。
type SignalType string

// 支持的生理信号种类。
const (
	SignalECG   SignalType = "ECG"
	SignalPPG   SignalType = "PPG"
	SignalEDA   SignalType = "EDA"
	SignalTemp  SignalType = "TEMP"
	SignalAccel SignalType = "ACCEL"
)

// RejectReason 样本被拒绝或特殊处理的可区分原因。
type RejectReason string

// 样本处理/拒绝原因码。空值表示完全正常接收。
const (
	ReasonNone            RejectReason = ""
	ReasonDuplicate       RejectReason = "duplicate_sequence" // 序列号已接收过（含重连后重放），拒绝
	ReasonOutOfOrder      RejectReason = "out_of_order"       // 超出重排窗口的迟到乱序，拒绝
	ReasonLateAccepted    RejectReason = "late_within_window" // 窗口内迟到乱序，接收但统一时间不回退
	ReasonClockRegression RejectReason = "clock_regression"   // 超阈值时钟回退，拒绝
	ReasonClockCorrected  RejectReason = "clock_corrected"    // 阈值内时钟回退，已显式修正
	ReasonMissingGap      RejectReason = "missing_gap"        // 序列号缺口（当前样本仍接收，缺口单列）
)

// Sample 是设备上报的单个原始样本。
type Sample struct {
	DeviceID   string
	SensorID   string
	Signal     SignalType
	Seq        uint64    // 设备内单调递增序列号
	DeviceTime time.Time // 设备时钟时间戳
	ReceivedAt time.Time // 主机接收时刻
	Value      float64
	Quality    float64 // 信号质量 0..1
}

// Gap 描述序列号缺口（检测到的丢包），不会被静默填补。
type Gap struct {
	DeviceID   string
	SensorID   string
	Signal     SignalType
	MissingSeq []uint64
	FromTime   time.Time // 缺口估算起始统一时间
	ToTime     time.Time // 缺口估算结束统一时间
	DetectedAt time.Time
}

// AlignedSample 是归一到统一时间轴后的样本。
type AlignedSample struct {
	Sample
	UnifiedTime time.Time     // 校正后的统一时间轴时间
	ClockOffset time.Duration // 实际应用的时钟偏移
	SkewPPM     float64       // 实际应用的漂移率（ppm）
}

// IngestResult 是单个样本进入会话后的处理结论。
type IngestResult struct {
	Sample   Sample
	Aligned  *AlignedSample
	Gaps     []Gap
	Accepted bool
	Reason   RejectReason
	Detail   string
}

// sensorClock 是单个传感器的逻辑时钟状态。
type sensorClock struct {
	seen bool

	maxSeq     uint64              // 已接收的最大序列号
	contiguous uint64              // 已连续覆盖到的最大序列号（此前序列号全部已接收）
	filled     map[uint64]struct{} // 超过 contiguous 的迟到补缺序列号

	anchorHost   time.Time // 锚点主机时间
	anchorDevice time.Time // 锚点设备时间
	skewPPM      float64   // 应用的漂移修正率（ppm）

	lastUnified time.Time // 最近接收样本的统一时间（单调，不回退）

	reanchorPending bool // 重连后下一样本重新锚定
}

// isDuplicate 判断序列号是否此前已接收（含重连后的旧包重放）。
func (c *sensorClock) isDuplicate(seq uint64) bool {
	if seq <= c.contiguous {
		return true
	}
	_, ok := c.filled[seq]
	return ok
}

// recordAccepted 记录一个已接收序列号并维护连续覆盖区间。
func (c *sensorClock) recordAccepted(seq uint64) {
	if seq > c.maxSeq {
		c.maxSeq = seq
	}
	if seq == c.contiguous+1 {
		c.contiguous = seq
		for {
			next := c.contiguous + 1
			if _, ok := c.filled[next]; !ok {
				break
			}
			delete(c.filled, next)
			c.contiguous = next
		}
		return
	}
	if c.filled == nil {
		c.filled = make(map[uint64]struct{})
	}
	c.filled[seq] = struct{}{}
}

// unify 按当前锚点与漂移率把设备时间换算到统一时间轴。
func (c *sensorClock) unify(deviceTime time.Time) time.Time {
	elapsed := deviceTime.Sub(c.anchorDevice)
	corrected := time.Duration(float64(elapsed) * (1 + c.skewPPM/1_000_000))
	return c.anchorHost.Add(corrected)
}

// ClockGuard 按传感器维护逻辑时钟：去重、乱序重排、漂移校正与回退拒绝。
type ClockGuard struct {
	mu            sync.Mutex
	reorderWindow time.Duration
	maxRegression time.Duration
	clocks        map[string]*sensorClock
}

// NewClockGuard 创建逻辑时钟保护器。
// reorderWindow 为迟到乱序样本仍可接收的窗口；maxRegression 为可显式修正的时钟回退上界。
func NewClockGuard(reorderWindow, maxRegression time.Duration) *ClockGuard {
	return &ClockGuard{
		reorderWindow: reorderWindow,
		maxRegression: maxRegression,
		clocks:        make(map[string]*sensorClock),
	}
}

// SetSkewPPM 设置传感器已知的时钟漂移修正率（ppm）。
// 约定：设备时钟走慢（相对真实时间）时 ppm 为正，换算时拉长设备时间间隔。
func (g *ClockGuard) SetSkewPPM(sensorID string, ppm float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clocks[sensorID]
	if c == nil {
		c = &sensorClock{}
		g.clocks[sensorID] = c
	}
	c.skewPPM = ppm
}

// Reanchor 标记传感器在重连后需要重新锚定时钟；序列号历史保留，
// 因此重连不会导致已有数据被重复计入或漏计。
func (g *ClockGuard) Reanchor(sensorID string, at time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.clocks[sensorID]
	if c == nil {
		c = &sensorClock{}
		g.clocks[sensorID] = c
	}
	c.reanchorPending = true
}

// Ingest 处理一个原始样本，返回接收/拒绝结论及检测到的缺口。
func (g *ClockGuard) Ingest(s Sample) IngestResult {
	g.mu.Lock()
	defer g.mu.Unlock()

	res := IngestResult{Sample: s}
	c := g.clocks[s.SensorID]
	if c == nil {
		c = &sensorClock{}
		g.clocks[s.SensorID] = c
	}

	// 重复（含重连后旧包重放）优先判定，任何时刻都明确拒绝。
	if c.seen && c.isDuplicate(s.Seq) {
		res.Accepted = false
		res.Reason = ReasonDuplicate
		res.Detail = fmt.Sprintf("seq %d was already accepted by sensor %s (contiguous=%d)",
			s.Seq, s.SensorID, c.contiguous)
		return res
	}

	// 新传感器或重连后的首个样本：用主机接收时刻重新锚定，序列号历史保留。
	if !c.seen {
		c.anchorHost = s.ReceivedAt
		c.anchorDevice = s.DeviceTime
		c.lastUnified = s.ReceivedAt
		c.recordAccepted(s.Seq)
		c.seen = true
		res.Accepted = true
		res.Aligned = g.align(s, c, s.ReceivedAt)
		return res
	}
	if c.reanchorPending {
		c.anchorHost = s.ReceivedAt
		c.anchorDevice = s.DeviceTime
		c.reanchorPending = false
	}

	var gaps []Gap
	if s.Seq > c.maxSeq+1 {
		// 序列号缺口：当前样本仍然接收，缺口单独记录，绝不静默。
		missing := make([]uint64, 0, s.Seq-c.maxSeq-1)
		for q := c.maxSeq + 1; q < s.Seq; q++ {
			missing = append(missing, q)
		}
		gaps = append(gaps, Gap{
			DeviceID:   s.DeviceID,
			SensorID:   s.SensorID,
			Signal:     s.Signal,
			MissingSeq: missing,
			FromTime:   c.lastUnified,
			ToTime:     c.unify(s.DeviceTime),
			DetectedAt: s.ReceivedAt,
		})
	}

	unified := c.unify(s.DeviceTime)
	reason := RejectReason("")
	if len(gaps) > 0 {
		reason = ReasonMissingGap
	}

	// 时间轴单调性检查：时钟回退必须显式拒绝或修正。
	if unified.Before(c.lastUnified) {
		regression := c.lastUnified.Sub(unified)
		if regression <= g.reorderWindow {
			// 窗口内迟到：接收，统一时间不回退。
			reason = ReasonLateAccepted
			res.Detail = fmt.Sprintf("late sample %dns within reorder window; unified time kept at %s",
				regression.Nanoseconds(), c.lastUnified.Format(time.RFC3339Nano))
		} else if regression <= g.maxRegression {
			// 阈值内回退：显式修正为单调时间，并保留修正痕迹。
			reason = ReasonClockCorrected
			res.Detail = fmt.Sprintf("clock regression %dns corrected to monotonic time %s",
				regression.Nanoseconds(), c.lastUnified.Format(time.RFC3339Nano))
			unified = c.lastUnified
		} else {
			// 超阈值回退：明确拒绝，不污染时间轴。
			res.Accepted = false
			res.Reason = ReasonClockRegression
			res.Detail = fmt.Sprintf("clock regression %dns exceeds max allowed %dns",
				regression.Nanoseconds(), g.maxRegression.Nanoseconds())
			return res
		}
	}

	c.recordAccepted(s.Seq)
	if unified.After(c.lastUnified) {
		c.lastUnified = unified
	}
	res.Accepted = true
	res.Reason = reason
	res.Gaps = gaps
	res.Aligned = g.align(s, c, unified)
	return res
}

// align 构造对齐样本，附带实际使用的时钟参数以便追溯。
func (g *ClockGuard) align(s Sample, c *sensorClock, unified time.Time) *AlignedSample {
	return &AlignedSample{
		Sample:      s,
		UnifiedTime: unified,
		ClockOffset: c.anchorHost.Sub(c.anchorDevice),
		SkewPPM:     c.skewPPM,
	}
}

// Grid 把不同采样率的样本量化到固定周期的统一时间轴栅格。
type Grid struct {
	mu      sync.Mutex
	period  time.Duration
	slots   map[int64]*gridSlotData
	sensors map[string]struct{} // 已注册的预期传感器
}

// gridSlotData 是时间槽的内部可变状态。
type gridSlotData struct {
	values map[string]AlignedSample
}

// GridSlot 是统一时间轴上的一个时间槽。
type GridSlot struct {
	Start   time.Time
	Values  map[string]AlignedSample
	Missing []string // 已注册但本槽无样本的传感器
}

// NewGrid 创建周期为 period 的时间轴栅格。
func NewGrid(period time.Duration) *Grid {
	if period <= 0 {
		panic("timeseries: grid period must be positive")
	}
	return &Grid{
		period:  period,
		slots:   make(map[int64]*gridSlotData),
		sensors: make(map[string]struct{}),
	}
}

// Register 声明栅格需要承载的预期传感器；快照中这些传感器在无样本槽会被标记为 Missing。
func (g *Grid) Register(sensorID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sensors[sensorID] = struct{}{}
}

// slotIndex 把统一时间量化为栅格序号（向下取整）。
func (g *Grid) slotIndex(t time.Time) int64 {
	return t.UnixNano() / int64(g.period)
}

// Add 向栅格加入一个已对齐样本；同一传感器同一槽内的后到样本替换先到样本
// （重复样本已在 ClockGuard 层拒绝，正常不会发生）。
func (g *Grid) Add(s AlignedSample) {
	g.mu.Lock()
	defer g.mu.Unlock()

	idx := g.slotIndex(s.UnifiedTime)
	slot := g.slots[idx]
	if slot == nil {
		slot = &gridSlotData{values: make(map[string]AlignedSample)}
		g.slots[idx] = slot
	}
	slot.values[s.SensorID] = s
}

// Snapshot 返回按时间排序的栅格快照；已注册但缺位的传感器列在 Missing 中。
func (g *Grid) Snapshot() []GridSlot {
	g.mu.Lock()
	defer g.mu.Unlock()

	indexes := make([]int64, 0, len(g.slots))
	for idx := range g.slots {
		indexes = append(indexes, idx)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })

	out := make([]GridSlot, 0, len(indexes))
	for idx := indexes[0]; idx <= indexes[len(indexes)-1]; idx++ {
		// 显式合成跨度内没有任何样本的空槽，缺失不静默。
		data := g.slots[idx]
		start := time.Unix(0, idx*int64(g.period))
		values := make(map[string]AlignedSample)
		var missing []string
		if data != nil {
			for k, v := range data.values {
				values[k] = v
			}
			for sensor := range g.sensors {
				if _, ok := data.values[sensor]; !ok {
					missing = append(missing, sensor)
				}
			}
		} else {
			for sensor := range g.sensors {
				missing = append(missing, sensor)
			}
		}
		sort.Strings(missing)
		out = append(out, GridSlot{Start: start, Values: values, Missing: missing})
	}
	return out
}
