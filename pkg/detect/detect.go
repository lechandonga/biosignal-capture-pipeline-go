// Package detect evaluates configurable, reproducible physiological event
// and anomaly rules.
//
// Semantics:
//   - Rules are pure configuration (id, sensor, channel, window, operator,
//     threshold, minimum quality, hysteresis, cooldown). Given the same input
//     stream they always produce the same conclusions.
//   - A sample whose reported quality is below the rule minimum produces an
//     UNCERTAIN conclusion only; it can never yield a definite EVENT/ANOMALY.
//   - Every conclusion embeds the triggering value, the threshold, the rule
//     id and the pinned calibration version (Evidence), so decisions are
//     fully traceable and re-checkable.
package detect

import (
	"fmt"
	"strings"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

// Operator defines how the window aggregate is compared to the threshold.
type Operator string

const (
	OpGT Operator = "GT" // aggregate > threshold
	OpLT Operator = "LT" // aggregate < threshold
)

// Rule is a fully self-contained, reproducible detection rule.
type Rule struct {
	ID         string
	Sensor     model.SensorID
	Kind       model.SignalKind
	Channel    int // which value channel the rule reads
	Window     int // samples in the aggregate window (>=1)
	MinQuality float64
	High       float64 // event threshold (anomaly level)
	Low        float64 // anomaly threshold (0 = disabled); used with Op direction
	Op         Operator
	Cooldown   int // minimum samples between two definite events from this rule
	// UseBaseline evaluates against (value - baseline) instead of value.
	UseBaseline bool
	// AggregateWindow selects the window reduction (default MEAN).
	AggregateWindow Aggregate
}

// Aggregate names the window reduction used by a rule.
type Aggregate string

const (
	AggMean Aggregate = "MEAN"
	AggMax  Aggregate = "MAX"
	AggMin  Aggregate = "MIN"
)

// Agg selects the window reduction (default MEAN).
func (r Rule) Agg() Aggregate {
	if r.AggregateWindow == "" {
		return AggMean
	}
	return r.AggregateWindow
}

// ruleState holds per-rule sliding window and emission bookkeeping.
type ruleState struct {
	window  []float64
	qual    []float64
	emitted bool
	sinceCD int // samples since last definite event
}

// Engine runs detection rules over calibrated samples.
type Engine struct {
	rules  []Rule
	states map[string]*ruleState // keyed by Rule.ID
}

// NewEngine creates a detection engine.
func NewEngine(rules []Rule) *Engine {
	cp := make([]Rule, len(rules))
	copy(cp, rules)
	return &Engine{rules: cp, states: map[string]*ruleState{}}
}

func (e *Engine) state(id string) *ruleState {
	st := e.states[id]
	if st == nil {
		st = &ruleState{}
		e.states[id] = st
	}
	return st
}

// Push feeds one calibrated sample and returns any conclusions.
func (e *Engine) Push(s model.CalibratedSample) []model.Event {
	var out []model.Event
	for _, r := range e.rules {
		if r.Sensor != s.Aligned.Sensor || r.Kind != s.Aligned.Kind {
			continue
		}
		if r.Channel < 0 || r.Channel >= len(s.Values) {
			continue // misconfigured channel: no conclusion; pipeline logs separately
		}
		st := e.state(r.ID)
		val := s.Values[r.Channel]
		if r.UseBaseline && r.Channel < len(s.Baseline) {
			val -= s.Baseline[r.Channel]
		}
		st.window = append(st.window, val)
		st.qual = append(st.qual, s.Aligned.Quality)
		if r.Window > 0 && len(st.window) > r.Window {
			st.window = st.window[1:]
			st.qual = st.qual[1:]
		}
		st.sinceCD++

		agg := reduce(r.Agg(), st.window)
		ev := model.Event{
			Device:     s.Aligned.Device,
			Sensor:     s.Aligned.Sensor,
			Kind:       r.Kind,
			Start:      s.Aligned.UniTime,
			End:        s.Aligned.UniTime,
			Value:      agg,
			Threshold:  r.High,
			RuleID:     r.ID,
			CalVersion: s.CalVersion,
		}

		// Insufficient quality: UNCERTAIN only, evidence retained.
		if s.Aligned.Quality < r.MinQuality || s.Uncertain || s.Aligned.Gap {
			ev.Level = model.LevelUncertain
			ev.Evidence = fmt.Sprintf(
				"rule=%s op=%s agg=%s aggVal=%.6f high=%.6f low=%.6f q=%.3f minQ=%.3f cal=%s => quality insufficient",
				r.ID, r.Op, r.Agg(), agg, r.High, r.Low, s.Aligned.Quality, r.MinQuality, s.CalVersion)
			out = append(out, ev)
			continue
		}

		level := evaluate(r, agg)
		if level == model.LevelNormal {
			continue
		}
		// Definite conclusion: enforce cooldown to keep events reproducible.
		// The first event for a rule always passes; later ones must be spaced
		// by strictly more than Cooldown samples.
		if r.Cooldown > 0 && st.emitted && st.sinceCD <= r.Cooldown {
			continue
		}
		st.emitted = true
		st.sinceCD = 0
		ev.Level = level
		ev.Threshold = thresholdFor(r, level)
		ev.Evidence = fmt.Sprintf(
			"rule=%s op=%s agg=%s aggVal=%.6f threshold=%.6f q=%.3f minQ=%.3f cal=%s win=%d => %s",
			r.ID, r.Op, r.Agg(), agg, ev.Threshold, s.Aligned.Quality, r.MinQuality,
			s.CalVersion, len(st.window), level)
		out = append(out, ev)
	}
	return out
}

func evaluate(r Rule, agg float64) model.EventLevel {
	hitHigh := (r.Op == OpGT && agg > r.High) || (r.Op == OpLT && agg < r.High)
	if !hitHigh {
		return model.LevelNormal
	}
	hitLow := false
	if r.Low != 0 {
		hitLow = (r.Op == OpGT && agg > r.Low) || (r.Op == OpLT && agg < r.Low)
	}
	if hitLow {
		return model.LevelAnomaly
	}
	return model.LevelEvent
}

func thresholdFor(r Rule, level model.EventLevel) float64 {
	if level == model.LevelAnomaly && r.Low != 0 {
		return r.Low
	}
	return r.High
}

func reduce(agg Aggregate, w []float64) float64 {
	if len(w) == 0 {
		return 0
	}
	switch agg {
	case AggMax:
		m := w[0]
		for _, v := range w[1:] {
			if v > m {
				m = v
			}
		}
		return m
	case AggMin:
		m := w[0]
		for _, v := range w[1:] {
			if v < m {
				m = v
			}
		}
		return m
	default:
		var sum float64
		for _, v := range w {
			sum += v
		}
		return sum / float64(len(w))
	}
}

// RulesSummary renders rule configuration for logs and the README validation
// section, making the decision basis reproducible.
func RulesSummary(rules []Rule) string {
	var b strings.Builder
	for _, r := range rules {
		fmt.Fprintf(&b, "rule %s sensor=%s ch=%d win=%d op=%s high=%v low=%v minQ=%v cd=%d base=%v\n",
			r.ID, r.Sensor, r.Channel, r.Window, r.Op, r.High, r.Low, r.MinQuality, r.Cooldown, r.UseBaseline)
	}
	return b.String()
}
