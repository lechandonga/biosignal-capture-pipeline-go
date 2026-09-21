// Package calibration manages versioned calibration parameters and baselines.
//
// Rules:
//   - Every calibration version is validated (version, time window, vector
//     lengths, finite gains). Invalid or expired parameters are REJECTED;
//     processing never silently falls back to a default identity calibration.
//   - The store is append-only. A sample is calibrated with the version that
//     was valid AT THE SAMPLE'S TIME; adding a newer calibration never changes
//     already-produced records (which pin CalVersion).
//   - A sample whose time is not covered by any valid window is rejected with
//     ReasonExpiredCalibration rather than processed uncalibrated.
package calibration

import (
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

var (
	// ErrInvalidParams is returned for malformed calibration parameters.
	ErrInvalidParams = errors.New("invalid calibration parameters")
	// ErrDuplicateVersion is returned when a version id already exists.
	ErrDuplicateVersion = errors.New("duplicate calibration version")
	// ErrNoValidCalibration means no version's window covers the sample time
	// (no calibration ever became effective that early).
	ErrNoValidCalibration = errors.New("no valid calibration at sample time")
	// ErrCalibrationExpired means some version was effective earlier but all
	// covering windows have expired by the sample time.
	ErrCalibrationExpired = errors.New("calibration expired at sample time")
)

// Params is one versioned calibration parameter set for a sensor.
type Params struct {
	Version   string
	Sensor    model.SensorID
	Effective time.Time // inclusive
	Expires   time.Time // exclusive; zero means "never expires"
	Gain      []float64
	Offset    []float64
	Baseline  []float64
}

// Valid checks internal consistency. Callers reject (never default) on false.
func (p Params) Valid() error {
	if p.Version == "" {
		return errors.Join(ErrInvalidParams, errors.New("empty version"))
	}
	if p.Sensor == "" {
		return errors.Join(ErrInvalidParams, errors.New("empty sensor"))
	}
	if !p.Expires.IsZero() && !p.Expires.After(p.Effective) {
		return errors.Join(ErrInvalidParams, errors.New("expires must be after effective"))
	}
	if len(p.Gain) == 0 {
		return errors.Join(ErrInvalidParams, errors.New("empty gain vector"))
	}
	if len(p.Offset) != len(p.Gain) || len(p.Baseline) != len(p.Gain) {
		return errors.Join(ErrInvalidParams, errors.New("gain/offset/baseline length mismatch"))
	}
	for i, g := range p.Gain {
		if math.IsNaN(g) || math.IsInf(g, 0) || g == 0 {
			return errors.Join(ErrInvalidParams, errors.New("non-finite or zero gain"))
		}
		o, b := p.Offset[i], p.Baseline[i]
		if math.IsNaN(o) || math.IsInf(o, 0) || math.IsNaN(b) || math.IsInf(b, 0) {
			return errors.Join(ErrInvalidParams, errors.New("non-finite offset/baseline"))
		}
	}
	return nil
}

// coversAt reports whether the window contains t.
func (p Params) coversAt(t time.Time) bool {
	if t.Before(p.Effective) {
		return false
	}
	if !p.Expires.IsZero() && !t.Before(p.Expires) {
		return false
	}
	return true
}

// Apply converts a raw value vector to calibrated physical units:
// physical[i] = gain[i]*raw[i] + offset[i]. Baseline is returned separately
// so detectors can choose baseline-relative reasoning while both stay
// traceable in the record.
func (p Params) Apply(values []float64) (physical, baseline []float64, err error) {
	if len(values) != len(p.Gain) {
		return nil, nil, errors.Join(ErrInvalidParams, errors.New("sample width does not match calibration"))
	}
	physical = make([]float64, len(values))
	baseline = make([]float64, len(values))
	for i, v := range values {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, nil, errors.Join(ErrInvalidParams, errors.New("non-finite sample value"))
		}
		physical[i] = p.Gain[i]*v + p.Offset[i]
		baseline[i] = p.Baseline[i]
	}
	return physical, baseline, nil
}

// Store holds the append-only calibration history for all sensors.
type Store struct {
	mu       sync.RWMutex
	bySensor map[model.SensorID][]Params
}

// NewStore creates an empty calibration store.
func NewStore() *Store {
	return &Store{bySensor: map[model.SensorID][]Params{}}
}

// Add validates and registers a calibration version. Invalid parameters
// (bad version, time window, vector lengths, non-finite/zero gains) are
// rejected, never silently defaulted. Expiry is enforced at lookup time by
// At: a sample falling outside every window is rejected rather than
// calibrated with an expired (or identity/default) parameter set.
func (s *Store) Add(p Params) error {
	if err := p.Valid(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.bySensor[p.Sensor]
	for _, q := range list {
		if q.Version == p.Version {
			return errors.Join(ErrDuplicateVersion, errors.New(p.Version))
		}
	}
	list = append(list, p)
	sort.SliceStable(list, func(i, j int) bool { return list[i].Effective.Before(list[j].Effective) })
	s.bySensor[p.Sensor] = list
	return nil
}

// At returns the calibration valid at time t for the sensor: the latest
// version whose window covers t. If that version has expired and no newer
// covering version exists, ErrNoValidCalibration is returned.
func (s *Store) At(sensor model.SensorID, t time.Time) (Params, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := s.bySensor[sensor]
	var best Params
	found := false
	for _, p := range list {
		if p.coversAt(t) && (!found || p.Effective.After(best.Effective) ||
			(p.Effective.Equal(best.Effective) && p.Version > best.Version)) {
			best = p
			found = true
		}
	}
	if !found {
		// Distinguish "before the first ever window" from "all windows ended".
		if len(list) > 0 && !t.Before(list[0].Effective) {
			return Params{}, ErrCalibrationExpired
		}
		return Params{}, ErrNoValidCalibration
	}
	return best, nil
}

// Versions returns the registered versions for a sensor (audit/traceability).
func (s *Store) Versions(sensor model.SensorID) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.bySensor[sensor]))
	for _, p := range s.bySensor[sensor] {
		out = append(out, p.Version)
	}
	return out
}
