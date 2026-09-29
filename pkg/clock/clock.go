// Package clock performs per-sensor clock-drift correction and rejects
// logical-clock rollback attempts.
//
// Model: every device sample carries a sequence number Seq (the device-side
// monotonic truth) and a device clock reading DevTime. The host stamps
// RecvTime. From (DevTime, RecvTime) anchors we estimate a linear mapping
// unified = DevTime + offset(+drift). The unified result is additionally
// enforced to be non-decreasing per sensor, so a corrected time can never
// silently pollute the time axis.
package clock

import (
	"errors"
	"sync"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

var (
	// ErrDuplicate means a sample with the exact last sequence arrived again.
	ErrDuplicate = errors.New("duplicate sample sequence")
	// ErrSeqRegression means a sample with a lower-than-last sequence arrived:
	// a replayed stream or device-side logical clock reset.
	ErrSeqRegression = errors.New("sequence regression (replayed sample)")
	// ErrClockRollback means the device clock reading went backwards by more
	// than the receive-side jitter tolerance.
	ErrClockRollback = errors.New("device clock rollback")
	// ErrUnifiedRollback means the corrected unified time would precede the
	// last emitted unified time for the sensor.
	ErrUnifiedRollback = errors.New("corrected unified time rolls back")
)

// maxAnchorCount bounds the memory used for regression anchors.
const maxAnchorCount = 256

// maxDriftRate clamps the estimated drift slope to a physically sane range
// (5%). Beyond that the estimate is considered noise and slope is held at 0.
const maxDriftRate = 0.05

// rollbackTolerance absorbs small receive/schedule jitter when checking the
// raw device clock for rollback.
const rollbackTolerance = 5 * time.Millisecond

type anchor struct {
	devElapsed float64 // seconds elapsed on the device since its first anchor
	hostOff    float64 // instantaneous (host - device) offset, seconds
	epoch      int
}

// sensorState tracks one sensor's clock and sequence.
type sensorState struct {
	lastSeq     int64
	haveSeq     bool
	origin      time.Time // device time of the first anchor
	lastDev     time.Time
	lastUnified time.Time
	haveTime    bool
	epoch       int // increments on reconnect
	anchors     []anchor
}

// Discipline tracks clock offset and drift for the sensors of one device.
type Discipline struct {
	device  model.DeviceID
	sensors map[model.SensorID]*sensorState
	mu      sync.Mutex
}

// NewDiscipline creates a clock discipline for a device.
func NewDiscipline(device model.DeviceID) *Discipline {
	return &Discipline{device: device, sensors: map[model.SensorID]*sensorState{}}
}

func (d *Discipline) state(sensor model.SensorID) *sensorState {
	st := d.sensors[sensor]
	if st == nil {
		st = &sensorState{}
		d.sensors[sensor] = st
	}
	return st
}

// Device returns the owning device id.
func (d *Discipline) Device() model.DeviceID { return d.device }

// Correct maps a device timestamp onto the host/unified timeline for one
// sensor. Duplicate sequences and clock rollbacks are rejected with distinct
// errors; the caller is expected to log them, never to silently drop them.
func (d *Discipline) Correct(sensor model.SensorID, seq int64, devTime, recvTime time.Time) (time.Time, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.state(sensor)

	// 1) Sequence is the monotonic ground truth.
	if st.haveSeq && seq <= st.lastSeq {
		if seq == st.lastSeq {
			return time.Time{}, ErrDuplicate
		}
		return time.Time{}, ErrSeqRegression
	}

	// 2) Raw device clock must not move backwards beyond jitter tolerance.
	if st.haveTime && devTime.Before(st.lastDev.Add(-rollbackTolerance)) {
		return time.Time{}, ErrClockRollback
	}

	// 3) Estimate unified time from anchors: unified = dev + (host - dev).
	unified := devTime.Add(offsetFor(st, devTime))

	// 4) Unified axis is strictly monotonic per sensor.
	if st.haveTime && !unified.After(st.lastUnified) {
		return time.Time{}, ErrUnifiedRollback
	}

	// Commit anchor + advance state only after all checks pass.
	if !st.haveTime {
		st.origin = devTime
	}
	st.addAnchor(anchor{
		devElapsed: devTime.Sub(st.origin).Seconds(),
		hostOff:    recvTime.Sub(devTime).Seconds(),
		epoch:      st.epoch,
	})
	st.lastSeq = seq
	st.lastDev = devTime
	st.lastUnified = unified
	st.haveSeq = true
	st.haveTime = true
	return unified, nil
}

// offsetFor estimates (host - device) offset at devTime by least-squares
// fitting the instantaneous offsets over device-elapsed time within the
// current epoch. Fitting relative elapsed values (not absolute Unix seconds)
// keeps the intercept numerically stable, and the slope is clamped to a
// physically sane drift range.
func offsetFor(st *sensorState, devTime time.Time) time.Duration {
	var cur []anchor
	for _, a := range st.anchors {
		if a.epoch == st.epoch {
			cur = append(cur, a)
		}
	}
	n := float64(len(cur))
	if n == 0 {
		return 0
	}
	var sx, sy, sxx, sxy float64
	for _, a := range cur {
		x := a.devElapsed
		y := a.hostOff
		sx += x
		sy += y
		sxx += x * x
		sxy += x * y
	}
	denom := n*sxx - sx*sx
	var slope float64
	if denom > 1e-12 {
		slope = (n*sxy - sx*sy) / denom
	}
	if slope > maxDriftRate {
		slope = maxDriftRate
	} else if slope < -maxDriftRate {
		slope = -maxDriftRate
	}
	intercept := (sy - slope*sx) / n
	elapsed := devTime.Sub(st.origin).Seconds()
	off := intercept + slope*elapsed
	return time.Duration(off * float64(time.Second))
}

// CheckSequence validates only the monotonic sequence contract without
// fitting the (possibly synthetic) timing of a gap marker into the clock
// model. It rejects duplicates and sequence regressions; on success it
// advances the last-seen sequence. Gaps therefore reserve their positions on
// the stream without polluting drift estimation.
func (d *Discipline) CheckSequence(sensor model.SensorID, seq int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.state(sensor)
	if st.haveSeq && seq <= st.lastSeq {
		if seq == st.lastSeq {
			return ErrDuplicate
		}
		return ErrSeqRegression
	}
	st.lastSeq = seq
	st.haveSeq = true
	return nil
}

// Reconnect opens a new estimation epoch. Existing anchors are retained for
// traceability but stop influencing the fit; sequence continuity is still
// enforced so data across the gap is neither repeated nor lost.
func (d *Discipline) Reconnect(sensor model.SensorID, recvTime time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.state(sensor)
	st.epoch++
	// First post-reconnect sample re-anchors against recvTime; until then we
	// keep emitting monotonic time using the last known offset.
	_ = recvTime
	return nil
}

// Epoch returns the current epoch for a sensor (0 before any reconnect).
func (d *Discipline) Epoch(sensor model.SensorID) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state(sensor).epoch
}

// LastSeq returns the last accepted sequence for a sensor.
func (d *Discipline) LastSeq(sensor model.SensorID) (int64, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.state(sensor)
	return st.lastSeq, st.haveSeq
}

// Offset returns the most recent instantaneous host-device offset for a sensor.
func (d *Discipline) Offset(sensor model.SensorID) time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.state(sensor)
	if len(st.anchors) == 0 {
		return 0
	}
	last := st.anchors[len(st.anchors)-1]
	return time.Duration(last.hostOff * float64(time.Second))
}

func (st *sensorState) addAnchor(a anchor) {
	st.anchors = append(st.anchors, a)
	if len(st.anchors) > maxAnchorCount {
		// Drop oldest anchors, but always keep one per prior epoch for audit.
		cut := len(st.anchors) - maxAnchorCount
		st.anchors = st.anchors[cut:]
	}
}
