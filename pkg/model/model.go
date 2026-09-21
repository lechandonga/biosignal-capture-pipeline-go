// Package model defines the core domain types of the biosignal capture pipeline.
package model

import "time"

// DeviceID uniquely identifies a wearable device.
type DeviceID string

// SensorID uniquely identifies a sensor within a device.
type SensorID string

// SignalKind enumerates the physiological signal types.
type SignalKind string

const (
	SignalECG  SignalKind = "ECG"
	SignalPPG  SignalKind = "PPG"
	SignalEDA  SignalKind = "EDA"
	SignalTemp SignalKind = "TEMP"
)

// RejectReason explains why a sample or record was rejected.
type RejectReason string

const (
	ReasonNone               RejectReason = ""
	ReasonDuplicate          RejectReason = "duplicate_sample"
	ReasonSeqRegression      RejectReason = "sequence_regression"
	ReasonClockRollback      RejectReason = "clock_rollback"
	ReasonBeforeSessionStart RejectReason = "before_session_start"
	ReasonOutOfOrder         RejectReason = "out_of_order_unrecoverable"
	ReasonBufferFull         RejectReason = "buffer_full"
	ReasonUnknownSensor      RejectReason = "unknown_sensor"
	ReasonBadCalibration     RejectReason = "bad_calibration"
	ReasonExpiredCalibration RejectReason = "expired_calibration"
	ReasonPacketLoss         RejectReason = "packet_loss"
	ReasonUnifiedRollback    RejectReason = "unified_time_rollback"
	ReasonSessionClosed      RejectReason = "session_closed"
	ReasonNoCalibration      RejectReason = "no_valid_calibration"
)

// Action distinguishes a rejected record from one that was accepted but
// required explicit handling (reordering, gap filling, ...).
type Action string

const (
	ActionRejected Action = "REJECTED"
	ActionHandled  Action = "HANDLED"
)

// RawSample is a single measurement as emitted by a simulated device.
type RawSample struct {
	Device   DeviceID
	Sensor   SensorID
	Seq      int64     // device-side monotonic sequence number
	DevTime  time.Time // device-side clock reading
	RecvTime time.Time // host receive time
	Values   []float64
	Quality  float64 // 0..1 signal quality reported by the device
	Lost     bool    // true marks an injected packet-loss gap marker
}

// RejectedRecord records a rejected or explicitly-handled sample together
// with the reason, so nothing is silently dropped.
type RejectedRecord struct {
	Action Action
	Sample RawSample
	Reason RejectReason
	Detail string
	At     time.Time
}

// AlignedSample is a raw sample mapped onto the unified session time axis.
type AlignedSample struct {
	Device   DeviceID
	Sensor   SensorID
	Kind     SignalKind
	Seq      int64
	UniTime  time.Time // corrected, unified time
	RecvTime time.Time
	Values   []float64
	Quality  float64
	Gap      bool // true if this is a gap/loss marker rather than a real sample
}

// AsRaw reconstructs a RawSample-shaped value for logging/rejection records.
func (a AlignedSample) AsRaw() RawSample {
	return RawSample{
		Device: a.Device, Sensor: a.Sensor, Seq: a.Seq,
		DevTime: a.UniTime, RecvTime: a.RecvTime, Values: a.Values,
		Quality: a.Quality, Lost: a.Gap,
	}
}

// CalibratedSample carries physical units after calibration.
type CalibratedSample struct {
	Aligned     AlignedSample
	CalVersion  string
	Values      []float64
	Baseline    []float64
	Uncertain   bool
	Uncertainty string
}

// EventLevel classifies a detection conclusion.
type EventLevel string

const (
	LevelNormal    EventLevel = "NORMAL"
	LevelEvent     EventLevel = "EVENT"
	LevelAnomaly   EventLevel = "ANOMALY"
	LevelUncertain EventLevel = "UNCERTAIN"
)

// Event is a detection conclusion with fully traceable evidence.
type Event struct {
	Device     DeviceID
	Sensor     SensorID
	Kind       SignalKind
	Level      EventLevel
	Start      time.Time
	End        time.Time
	Value      float64
	Threshold  float64
	RuleID     string
	CalVersion string
	Evidence   string
	At         time.Time
}

// BatchState describes the durability state of a data batch.
type BatchState string

const (
	BatchIncomplete BatchState = "INCOMPLETE"
	BatchComplete   BatchState = "COMPLETE"
	BatchArchived   BatchState = "ARCHIVED"
)

// Batch is the unit of durable storage and recovery.
type Batch struct {
	ID        string
	SessionID string
	State     BatchState
	StartedAt time.Time
	ClosedAt  time.Time
	Samples   []CalibratedSample
	Events    []Event
	Rejected  []RejectedRecord
	CRC       uint32
}
