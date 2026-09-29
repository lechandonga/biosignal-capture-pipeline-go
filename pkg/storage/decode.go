package storage

import (
	"encoding/json"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

// decodeRecord decodes one framed payload into a SegmentRecord. Header and
// footer frames yield (nil, nil) and are skipped by the reader.
func decodeRecord(env envelope, segmentID string, state model.BatchState, pos int) (*SegmentRecord, error) {
	rec := &SegmentRecord{FramePos: pos, SegmentID: segmentID, SegmentState: state}
	switch env.Type {
	case recHeader, recFooter:
		return nil, nil
	case recSample:
		var js jsonSample
		if err := json.Unmarshal(env.Raw, &js); err != nil {
			return nil, err
		}
		smp := model.CalibratedSample{
			Aligned: model.AlignedSample{
				Device:   model.DeviceID(js.Device),
				Sensor:   model.SensorID(js.Sensor),
				Kind:     model.SignalKind(js.Kind),
				Seq:      js.Seq,
				UniTime:  js.UniTime,
				RecvTime: js.RecvTime,
				Values:   js.Values,
				Quality:  js.Quality,
				Gap:      js.Gap,
			},
			CalVersion:  js.CalVersion,
			Values:      js.CalValues,
			Baseline:    js.Baseline,
			Uncertain:   js.Uncertain,
			Uncertainty: js.Uncertainty,
		}
		rec.Time, rec.Device, rec.Sensor = js.UniTime, smp.Aligned.Device, smp.Aligned.Sensor
		rec.Sample = &smp
	case recEvent:
		var je jsonEvent
		if err := json.Unmarshal(env.Raw, &je); err != nil {
			return nil, err
		}
		ev := model.Event{
			Device: model.DeviceID(je.Device), Sensor: model.SensorID(je.Sensor),
			Kind: model.SignalKind(je.Kind), Level: model.EventLevel(je.Level),
			Start: je.Start, End: je.End, Value: je.Value, Threshold: je.Threshold,
			RuleID: je.RuleID, CalVersion: je.CalVersion, Evidence: je.Evidence, At: je.At,
		}
		rec.Time, rec.Device, rec.Sensor = je.At, ev.Device, ev.Sensor
		rec.Event = &ev
	case recReject:
		var jr jsonReject
		if err := json.Unmarshal(env.Raw, &jr); err != nil {
			return nil, err
		}
		r := model.RejectedRecord{
			Action: model.Action(jr.Action),
			Sample: model.RawSample{
				Device: model.DeviceID(jr.Device), Sensor: model.SensorID(jr.Sensor), Seq: jr.Seq,
			},
			Reason: model.RejectReason(jr.Reason), Detail: jr.Detail, At: jr.At,
		}
		rec.Time, rec.Device, rec.Sensor = jr.At, r.Sample.Device, r.Sample.Sensor
		rec.Rejected = &r
	default:
		return nil, ErrCorrupt
	}
	return rec, nil
}

// setCorrupt flags a segment whose stream produced an undecodable frame.
func (s *Store) setCorrupt(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.meta[id]; ok {
		m.TornTail = true
	}
}
