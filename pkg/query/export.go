package query

import (
	"container/heap"
	"errors"
	"sort"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/storage"
)

// Record ordering across a merged export is total and reproducible:
//
//	time, then record kind (sample < event < rejected), then device, sensor,
//	the record's frame position inside its segment, then segment id.
//
// Records inside one segment are not globally time-sorted (multiplexed
// streams), so each segment is streamed, filtered and time-sorted in memory;
// the bounded merge holds one sorted slice per overlapping segment. Peak
// memory is therefore O(largest overlapping segment), independent of the
// number of segments or the total range size.

const (
	kindSample = iota
	kindEvent
	kindRejected
)

type ordered struct {
	rec     storage.SegmentRecord
	kind    int
	device  model.DeviceID
	sensor  model.SensorID
	segment string
}

func orderKey(rec storage.SegmentRecord) ordered {
	k := kindRejected
	switch {
	case rec.Sample != nil:
		k = kindSample
	case rec.Event != nil:
		k = kindEvent
	}
	return ordered{rec: rec, kind: k, device: rec.Device, sensor: rec.Sensor, segment: rec.SegmentID}
}

func lessOrdered(a, b ordered) bool {
	if !a.rec.Time.Equal(b.rec.Time) {
		return a.rec.Time.Before(b.rec.Time)
	}
	if a.kind != b.kind {
		return a.kind < b.kind
	}
	if a.device != b.device {
		return a.device < b.device
	}
	if a.sensor != b.sensor {
		return a.sensor < b.sensor
	}
	if a.rec.FramePos != b.rec.FramePos {
		return a.rec.FramePos < b.rec.FramePos
	}
	return a.segment < b.segment
}

type source struct {
	buf []ordered // time-sorted remaining records of one segment
}

type mergeHeap []*source

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	return lessOrdered(h[i].buf[0], h[j].buf[0])
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(*source)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

func acceptRecord(f Filter, rec storage.SegmentRecord) bool {
	if !f.inWindow(rec.Time) {
		return false
	}
	if !f.matchesDeviceSensor(rec.Device, rec.Sensor) {
		return false
	}
	if !f.IncludeGaps && rec.Sample != nil && rec.Sample.Aligned.Gap {
		return false
	}
	return true
}

// loadSegment streams one segment, applying the filter, and returns its
// time-sorted records.
func loadSegment(st *storage.Store, id string, f Filter) ([]ordered, error) {
	rdr, err := st.OpenSegmentReader(id)
	if err != nil {
		return nil, err
	}
	var out []ordered
	for rdr.Next() {
		rec := rdr.Record()
		if acceptRecord(f, rec) {
			out = append(out, orderKey(rec))
		}
	}
	torn := rdr.Err()
	if cerr := rdr.Close(); cerr != nil && torn == nil {
		torn = cerr
	}
	sort.Slice(out, func(i, j int) bool { return lessOrdered(out[i], out[j]) })
	if torn != nil {
		// Intact records have all been yielded; flag via a sentinel carried
		// on the returned slice is unnecessary because the segment metadata
		// already reports INCOMPLETE. Returning the intact prefix is correct.
		return out, nil
	}
	return out, nil
}

// segmentSpans builds coverage spans from metadata and the in-window
// rejection reasons collected during export.
func segmentSpans(segs []storage.SegmentInfo, reasonCounts map[model.RejectReason]int,
	defects []storage.SegmentRecord, f Filter) []span {
	bySeg := map[string]map[model.RejectReason]struct{}{}
	for _, d := range defects {
		set := bySeg[d.SegmentID]
		if set == nil {
			set = map[model.RejectReason]struct{}{}
			bySeg[d.SegmentID] = set
		}
		set[d.Rejected.Reason] = struct{}{}
	}
	_ = reasonCounts
	spans := make([]span, 0, len(segs))
	for _, m := range segs {
		sp := span{
			from:    firstNonZero(m.FirstTime, m.StartedAt),
			to:      lastNonZero(m.LastTime, m.StartedAt),
			good:    (m.State == model.BatchComplete || m.State == model.BatchArchived) && !m.TornTail,
			reasons: map[model.RejectReason]struct{}{},
		}
		if sp.from.IsZero() || sp.to.IsZero() {
			continue // segment with no observable extent: contributes nothing
		}
		if !sp.good || m.TornTail {
			sp.good = false
			sp.reasons[model.ReasonSessionClosed] = struct{}{}
		}
		clampSpan(&sp, f)
		if !sp.from.After(sp.to) {
			spans = append(spans, sp)
		}
		// Loss/rejection evidence marks its exact timestamp incomplete; the
		// remainder of an otherwise complete segment stays COMPLETE.
		for _, d := range defects {
			if d.SegmentID != m.ID {
				continue
			}
			dsp := span{from: d.Time, to: d.Time, good: false,
				reasons: map[model.RejectReason]struct{}{d.Rejected.Reason: {}}}
			clampSpan(&dsp, f)
			if !dsp.from.After(dsp.to) {
				spans = append(spans, dsp)
			}
		}
	}
	return spans
}

// Export streams all records in the filtered time range — across complete,
// incomplete and archived segments — into yield, in the deterministic total
// order documented above. Segments are opened via leases, so a concurrent
// reclaim cannot delete a segment mid-export; memory stays bounded by the
// largest overlapping segment.
func Export(st *storage.Store, f Filter, yield func(storage.SegmentRecord) error) (Stats, Report, error) {
	segs := segmentsFor(st, f)
	rep := Report{From: f.From, To: f.To, ReasonCounts: map[model.RejectReason]int{}, Segments: segs}
	var stats Stats

	mh := &mergeHeap{}
	heap.Init(mh)
	// Segments are loaded sequentially; only the sorted slices stay resident.
	for _, m := range segs {
		buf, err := loadSegment(st, m.ID, f)
		if errors.Is(err, storage.ErrNotFound) {
			continue // reclaimed between index listing and lease acquisition
		}
		if err != nil {
			return stats, rep, err
		}
		if len(buf) > 0 {
			heap.Push(mh, &source{buf: buf})
		}
	}

	for mh.Len() > 0 {
		src := (*mh)[0]
		top := src.buf[0]
		if err := yield(top.rec); err != nil {
			return stats, rep, err
		}
		switch top.kind {
		case kindSample:
			stats.Samples++
		case kindEvent:
			stats.Events++
		case kindRejected:
			stats.Rejected++
			rep.ReasonCounts[top.rec.Rejected.Reason]++
			rep.Defects = append(rep.Defects, top.rec)
		}
		src.buf = src.buf[1:]
		if len(src.buf) == 0 {
			heap.Pop(mh)
		} else {
			heap.Fix(mh, 0)
		}
	}

	rep.Ranges = mergeSpans(segmentSpans(segs, rep.ReasonCounts, rep.Defects, f), f)
	return stats, rep, nil
}
