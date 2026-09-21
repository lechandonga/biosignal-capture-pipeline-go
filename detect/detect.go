// Package detect 提供阈值可配置、依据可复现的生理事件与异常判定。
package detect

import (
	"fmt"
	"sync"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/timeseries"
)

// EventStatus 判定结论状态。
type EventStatus string

// 判定状态：正常、确定事件、不确定（信号质量不足等）。
const (
	StatusNormal    EventStatus = "normal"
	StatusEvent     EventStatus = "event"
	StatusUncertain EventStatus = "uncertain"
)

// 不确定原因。
const (
	UncertainQuality      = "signal_quality_below_threshold"
	UncertainMissingInput = "missing_or_gapped_input"
)

// Rule 是单个事件的可配置判定规则。
// 物理量 <= Low 或 >= High 视为越限；需连续 MinConsecutive 个越限样本才输出确定事件。
type Rule struct {
	Signal         timeseries.SignalType
	Event          string
	Low            float64 // 物理量下界，低于等于则越限
	High           float64 // 物理量上界，高于等于则越限
	MinQuality     float64 // 所需最低信号质量，取值 [0,1]
	MinConsecutive int     // 连续越限样本数，必须 >=1
}

// Validate 校验规则合法性，非法配置显式失败。
func (r Rule) Validate() error {
	switch {
	case r.Event == "":
		return fmt.Errorf("detect: empty event name")
	case r.Low >= r.High:
		return fmt.Errorf("detect: event %q low %v must be below high %v", r.Event, r.Low, r.High)
	case r.MinQuality < 0 || r.MinQuality > 1:
		return fmt.Errorf("detect: event %q min-quality %v out of [0,1]", r.Event, r.MinQuality)
	case r.MinConsecutive < 1:
		return fmt.Errorf("detect: event %q min-consecutive must be >=1", r.Event)
	}
	return nil
}

// violation 判断物理量是否越限及方向。
func (r Rule) violation(physical float64) (bool, string) {
	switch {
	case physical <= r.Low:
		return true, fmt.Sprintf("physical %.4f <= low %.4f", physical, r.Low)
	case physical >= r.High:
		return true, fmt.Sprintf("physical %.4f >= high %.4f", physical, r.High)
	default:
		return false, ""
	}
}

// Basis 保留判定依据，保证结论可追溯、可复现。
type Basis struct {
	RawValue        float64
	CalibratedValue float64
	Baseline        float64
	Quality         float64
	Low             float64
	High            float64
	Consecutive     int
	Required        int
	CalVersion      string
	Notes           []string
}

// Conclusion 是一次判定的完整结论。
type Conclusion struct {
	Time     time.Time
	DeviceID string
	SensorID string
	Signal   timeseries.SignalType
	Event    string
	Status   EventStatus
	Reason   string
	Basis    Basis
}

type ruleState struct {
	consecutive int
}

// Detector 按规则对流式样本做事件判定，状态按“传感器+事件”维护。
type Detector struct {
	mu     sync.Mutex
	rules  map[timeseries.SignalType][]Rule
	states map[string]map[string]*ruleState // sensorID -> event -> state
}

// NewDetector 用给定规则创建检测器；任一规则非法即显式报错。
func NewDetector(rules []Rule) (*Detector, error) {
	d := &Detector{
		rules:  make(map[timeseries.SignalType][]Rule),
		states: make(map[string]map[string]*ruleState),
	}
	for _, r := range rules {
		if err := r.Validate(); err != nil {
			return nil, err
		}
		d.rules[r.Signal] = append(d.rules[r.Signal], r)
	}
	return d, nil
}

func (d *Detector) stateFor(sensorID, event string) *ruleState {
	m := d.states[sensorID]
	if m == nil {
		m = make(map[string]*ruleState)
		d.states[sensorID] = m
	}
	st := m[event]
	if st == nil {
		st = &ruleState{}
		m[event] = st
	}
	return st
}

// EvaluateInput 汇总检测输入（当标定缺失/原值越界时由管线构造）。
type EvaluateInput struct {
	Sample          timeseries.AlignedSample
	Physical        float64
	Baseline        float64
	CalVersion      string
	CalibratedOK    bool // 标定与原值是否合法可用
	MissingOrGapped bool // 样本来自缺口/补齐路径
}

// Evaluate 对一个已成功标定的样本做判定；质量不足或标定不可用时只能给出不确定结论。
func (d *Detector) Evaluate(s timeseries.AlignedSample, physical float64, baseline float64, calVersion string) []Conclusion {
	return d.evaluate(EvaluateInput{
		Sample:       s,
		Physical:     physical,
		Baseline:     baseline,
		CalVersion:   calVersion,
		CalibratedOK: true,
	})
}

// EvaluateUncertain 在标定缺失/原值非法/缺口等情况下输出全部相关规则的不确定结论，
// 绝不退化为默认阈值或默认正常。
func (d *Detector) EvaluateUncertain(in EvaluateInput, reason string) []Conclusion {
	in.CalibratedOK = false
	return d.evaluateWithReason(in, reason)
}

func (d *Detector) evaluate(in EvaluateInput) []Conclusion {
	return d.evaluateWithReason(in, "")
}

func (d *Detector) evaluateWithReason(in EvaluateInput, forcedReason string) []Conclusion {
	d.mu.Lock()
	defer d.mu.Unlock()

	s := in.Sample
	rules := d.rules[s.Signal]
	out := make([]Conclusion, 0, len(rules))
	for _, r := range rules {
		st := d.stateFor(s.SensorID, r.Event)
		c := Conclusion{
			Time:     s.UnifiedTime,
			DeviceID: s.DeviceID,
			SensorID: s.SensorID,
			Signal:   s.Signal,
			Event:    r.Event,
		}
		b := Basis{
			RawValue:        s.Value,
			CalibratedValue: in.Physical,
			Baseline:        in.Baseline,
			Quality:         s.Quality,
			Low:             r.Low,
			High:            r.High,
			Consecutive:     st.consecutive,
			Required:        r.MinConsecutive,
			CalVersion:      in.CalVersion,
		}

		// 第一道门：标定/输入不可用 => 不确定，并冻结连续计数（污染输入不计入判定状态）。
		if !in.CalibratedOK {
			b.Notes = append(b.Notes, "calibration or raw input unavailable; consecutive counter frozen")
			c.Status, c.Reason = StatusUncertain, forcedReason
			c.Basis = b
			out = append(out, c)
			continue
		}
		// 第二道门：信号质量不足 => 不确定，连续计数同样冻结。
		if s.Quality < r.MinQuality {
			b.Notes = append(b.Notes, fmt.Sprintf("quality %.3f below required %.3f; consecutive counter frozen", s.Quality, r.MinQuality))
			c.Status, c.Reason = StatusUncertain, UncertainQuality
			c.Basis = b
			out = append(out, c)
			continue
		}
		if in.MissingOrGapped {
			b.Notes = append(b.Notes, "input comes from a detected gap; consecutive counter frozen")
			c.Status, c.Reason = StatusUncertain, UncertainMissingInput
			c.Basis = b
			out = append(out, c)
			continue
		}

		violated, detail := r.violation(in.Physical)
		if violated {
			st.consecutive++
			b.Notes = append(b.Notes, detail)
		} else {
			if st.consecutive > 0 {
				b.Notes = append(b.Notes, fmt.Sprintf("streak reset (was %d); value within bounds", st.consecutive))
			}
			st.consecutive = 0
		}
		b.Consecutive = st.consecutive

		if st.consecutive >= r.MinConsecutive {
			c.Status = StatusEvent
			c.Reason = fmt.Sprintf("%d consecutive violating samples (>= %d)", st.consecutive, r.MinConsecutive)
		} else {
			c.Status = StatusNormal
			if violated {
				c.Reason = fmt.Sprintf("violating but only %d/%d consecutive samples", st.consecutive, r.MinConsecutive)
			} else {
				c.Reason = "value within bounds"
			}
		}
		c.Basis = b
		out = append(out, c)
	}
	return out
}
