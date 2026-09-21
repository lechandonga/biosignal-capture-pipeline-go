// Package device implements local simulated wearable devices.
//
// A Device owns a drifting virtual clock and one or more sensors emitting at
// fixed rates. Packet loss, disconnect/reconnect and clock rollback are
// explicit, controllable actions so the pipeline can be exercised
// deterministically. Emission never fabricates a replacement sample for a
// lost position: the caller receives a gap marker (Lost=true) that flows
// through the pipeline and is recorded with reason packet_loss.
package device

import (
	"errors"
	"math"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

var (
	// ErrDisconnected is returned while the device is offline.
	ErrDisconnected = errors.New("device disconnected")
	// ErrUnknownSensor is returned for an unregistered sensor.
	ErrUnknownSensor = errors.New("unknown sensor")
)

// SensorSpec declares one simulated sensor.
type SensorSpec struct {
	ID     model.SensorID
	Kind   model.SignalKind
	RateHz float64
	Width  int // value channels
}

// Config configures a simulated device.
type Config struct {
	ID          model.DeviceID
	Sensors     []SensorSpec
	DriftPerSec time.Duration // signed: device clock drift vs real time
	LossRate    float64       // 0..1 probability a sample position is lost
	Seed        int64
}

type sensorState struct {
	spec    SensorSpec
	seq     int64
	devTime time.Time // device-clock position
}

// Device is a controllable sample source.
type Device struct {
	cfg     Config
	clock0  time.Time
	virtual time.Time // host-time base advanced by the driver
	sensors map[model.SensorID]*sensorState
	offline bool
	rng     *lcg
}

// New creates a simulated device; start anchors its virtual clock.
func New(cfg Config, start time.Time) *Device {
	d := &Device{
		cfg:     cfg,
		clock0:  start,
		virtual: start,
		sensors: map[model.SensorID]*sensorState{},
		rng:     newLCG(cfg.Seed),
	}
	for _, sp := range cfg.Sensors {
		d.sensors[sp.ID] = &sensorState{spec: sp, seq: 0, devTime: start}
	}
	return d
}

// ID returns the device id.
func (d *Device) ID() model.DeviceID { return d.cfg.ID }

// Sensor returns a registered sensor spec.
func (d *Device) Sensor(id model.SensorID) (SensorSpec, bool) {
	st, ok := d.sensors[id]
	if !ok {
		return SensorSpec{}, false
	}
	return st.spec, true
}

// drifted maps a device-clock nominal time (clock0 + device-elapsed) onto the
// drifting reading: drift accrues with device-elapsed time, and Rollback
// shifts clock0 backwards so subsequent readings move backwards too.
func (d *Device) drifted(nominal time.Time) time.Time {
	elapsed := nominal.Sub(d.clock0).Seconds()
	return d.clock0.Add(nominal.Sub(d.clock0) +
		time.Duration(elapsed*float64(d.cfg.DriftPerSec)))
}

// Sample emits the next sample for sensor at host time now, advancing that
// sensor by exactly one rate period. A lost position is returned as a gap
// marker (Lost=true) carrying the sequence and device time it WOULD have had,
// so the receiver can account for the gap without inventing data.
func (d *Device) Sample(sensor model.SensorID, now time.Time) (model.RawSample, error) {
	st, ok := d.sensors[sensor]
	if !ok {
		return model.RawSample{}, ErrUnknownSensor
	}
	if d.offline {
		return model.RawSample{}, ErrDisconnected
	}

	period := time.Duration(float64(time.Second) / st.spec.RateHz)
	d.virtual = now
	st.seq++
	st.devTime = st.devTime.Add(period)
	devTime := d.drifted(st.devTime)

	width := st.spec.Width
	if width <= 0 {
		width = 1
	}
	vals := make([]float64, width)
	for i := range vals {
		vals[i] = d.waveform(sensor, st.seq, i)
	}
	lost := d.cfg.LossRate > 0 && d.rng.float64() < d.cfg.LossRate
	q := 0.95
	if lost {
		q = 0
	}
	return model.RawSample{
		Device: d.cfg.ID, Sensor: sensor, Seq: st.seq,
		DevTime: devTime, RecvTime: now, Values: vals, Quality: q, Lost: lost,
	}, nil
}

// waveform is a deterministic synthetic physiological-like signal with the
// occasional burst so detection rules have something to find.
func (d *Device) waveform(sensor model.SensorID, seq int64, ch int) float64 {
	var kind model.SignalKind
	if st, ok := d.sensors[sensor]; ok {
		kind = st.spec.Kind
	}
	h := fnvOffset
	for _, c := range sensor {
		h = h*fnvPrime ^ uint64(c)
	}
	base := float64(h%7 + uint64(ch))
	switch kind {
	case model.SignalECG:
		v := base + 0.8*sin(seq, 20)
		if seq%37 == 0 {
			v += 12
		}
		return v
	case model.SignalPPG:
		return base + 0.5*sin(seq, 25)
	case model.SignalEDA:
		v := base + 0.01*float64(seq%200)
		if seq%53 == 0 {
			v += 4
		}
		return v
	case model.SignalTemp:
		return 36.6 + 0.2*sin(seq, 100)
	default:
		return base + sin(seq, 20)
	}
}

func sin(seq int64, period int64) float64 {
	x := 2 * math.Pi * float64(seq%period) / float64(period)
	return math.Sin(x)
}

// Disconnect takes the device offline; subsequent samples fail with
// ErrDisconnected until Reconnect.
func (d *Device) Disconnect() { d.offline = true }

// Reconnect brings the device back. Sequence numbers keep running; the
// receiver must emit/account gap markers for the offline window instead of
// re-reading old sequences (which would be rejected as duplicates).
func (d *Device) Reconnect(now time.Time) {
	d.offline = false
	d.virtual = now
}

// EmitGap advances one sensor across exactly n missing positions during an
// outage and returns one gap marker covering that span. Sequences are
// reserved, so later real samples never reuse them.
func (d *Device) EmitGap(sensor model.SensorID, n int, now time.Time) (model.RawSample, error) {
	st, ok := d.sensors[sensor]
	if !ok {
		return model.RawSample{}, ErrUnknownSensor
	}
	if n <= 0 {
		n = 1
	}
	period := time.Duration(float64(time.Second) / st.spec.RateHz)
	st.seq += int64(n)
	st.devTime = st.devTime.Add(period * time.Duration(n))
	return model.RawSample{
		Device: d.cfg.ID, Sensor: sensor, Seq: st.seq,
		DevTime: d.drifted(st.devTime), RecvTime: now,
		Values: nil, Quality: 0, Lost: true,
	}, nil
}

// Rollback rewinds the device clock by delta (negative shift of every
// sensor's time position) while leaving sequence numbers intact, simulating a
// wall-clock set-back. The next reading precedes the previous one; the
// pipeline must reject it rather than corrupt the time axis.
// Rollback rewinds the device clock backwards by delta (a positive duration)
// while leaving sequence numbers intact, simulating a wall-clock set-back.
// The next reading precedes the previous one; the pipeline must reject it
// rather than corrupt the time axis.
func (d *Device) Rollback(delta time.Duration) {
	for _, st := range d.sensors {
		period := time.Duration(float64(time.Second) / st.spec.RateHz)
		// Move back by delta plus one period so the NEXT reading (which adds a
		// period on emission) lands at lastReading - delta.
		st.devTime = st.devTime.Add(-(delta + period))
	}
}

// LastSeq returns the current sequence for a sensor.
func (d *Device) LastSeq(sensor model.SensorID) (int64, bool) {
	st, ok := d.sensors[sensor]
	if !ok {
		return 0, false
	}
	return st.seq, true
}

const (
	fnvOffset = uint64(1469598103934665603)
	fnvPrime  = uint64(1099511628211)
)

// lcg is a tiny deterministic PRNG (Numerical Recipes constants).
type lcg struct{ state uint64 }

func newLCG(seed int64) *lcg {
	if seed == 0 {
		return &lcg{state: 0x9E3779B97F4A7C15}
	}
	return &lcg{state: uint64(seed)}
}

func (l *lcg) next() uint64 {
	l.state = l.state*6364136223846793005 + 1442695040888963407
	return l.state
}

func (l *lcg) float64() float64 {
	return float64(l.next()>>11) / float64(1<<53)
}
