// Package query provides time-range, cross-segment read/export over a
// storage.Store.
//
// Selection uses only the segment index (FirstTime/LastTime), so a query
// opens precisely the segments overlapping the requested interval instead of
// scanning the whole store. Records are produced through a callback while
// segments are streamed one at a time; memory is bounded by the largest
// involved segment, not by the exported time range.
//
// Every result carries a completeness verdict for the interval: sub-ranges
// covered by complete segments are COMPLETE, sub-ranges covered by
// incomplete/torn segments or containing loss/rejection evidence are
// INCOMPLETE (with per-reason totals), and every other sub-range is NO_DATA.
// "No data" is therefore never confused with "incomplete data".
package query

import (
	"errors"
	"sort"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/storage"
)

// Filter narrows a time-range query. Zero fields mean "any".
type Filter struct {
	From        time.Time // inclusive
	To          time.Time // inclusive; zero means open-ended
	Device      model.DeviceID
	Sensor      model.SensorID
	SessionID   string // empty means every session
	IncludeGaps bool   // include packet-loss gap marker samples (default true)
}

// Verdict labels a sub-range of the queried interval.
type Verdict string

const (
	VerdictComplete   Verdict = "COMPLETE"
	VerdictIncomplete Verdict = "INCOMPLETE"
	VerdictNoData     Verdict = "NO_DATA"
)

// Range is one disjoint sub-range of the queried interval with its verdict.
type Range struct {
	From    time.Time
	To      time.Time
	Verdict Verdict
	Reasons []model.RejectReason // INCOMPLETE only, deduplicated
}

// Report describes completeness over the queried interval.
type Report struct {
	From time.Time
	To   time.Time
	// Ranges covers [From,To] end-to-end, disjoint and time-ordered.
	Ranges []Range
	// ReasonCounts counts INCOMPLETE evidence records by reason inside the
	// interval (packet-loss gaps and rejection/handling records).
	ReasonCounts map[model.RejectReason]int
	// Defects lists every raw evidence record (one per loss/rejection mark).
	Defects []storage.SegmentRecord
	// Segments lists metadata of every segment that intersects the interval.
	Segments []storage.SegmentInfo
}

// IsCompleteAt reports whether the exact instant t is covered by complete
// data. Instants in NO_DATA gaps and INCOMPLETE ranges both return false but
// are distinguishable via the partition in Ranges.
func (r Report) IsCompleteAt(t time.Time) bool {
	complete := false
	for _, rg := range r.Ranges {
		if (t.After(rg.From) || t.Equal(rg.From)) && (t.Before(rg.To) || t.Equal(rg.To)) {
			switch rg.Verdict {
			case VerdictIncomplete:
				return false
			case VerdictComplete:
				complete = true
			}
		}
	}
	return complete
}

// Stats summarizes the records yielded by an export.
type Stats struct {
	Samples  int
	Events   int
	Rejected int
}

type span struct {
	from, to time.Time
	good     bool
	reasons  map[model.RejectReason]struct{}
}

func (f Filter) matchesDeviceSensor(d model.DeviceID, sn model.SensorID) bool {
	if f.Device != "" && f.Device != d {
		return false
	}
	if f.Sensor != "" && f.Sensor != sn {
		return false
	}
	return true
}

// overlaps reports whether segment metadata intersects the filter window.
// Boundaries are inclusive. A zero/unknown segment extent is conservatively
// included (possible with pre-upgrade data missing recorded extents).
func (f Filter) overlaps(m storage.SegmentInfo) bool {
	if f.SessionID != "" && m.SessionID != f.SessionID {
		return false
	}
	if !f.From.IsZero() && !m.LastTime.IsZero() && m.LastTime.Before(f.From) {
		return false
	}
	if !f.To.IsZero() && !m.FirstTime.IsZero() && m.FirstTime.After(f.To) {
		return false
	}
	return true
}

func (f Filter) inWindow(t time.Time) bool {
	if !f.From.IsZero() && t.Before(f.From) {
		return false
	}
	if !f.To.IsZero() && t.After(f.To) {
		return false
	}
	return true
}

// segmentsFor returns index-selected, timeline-ordered segments overlapping
// the filter. No data files are opened here.
func segmentsFor(st *storage.Store, f Filter) []storage.SegmentInfo {
	var all []storage.SegmentInfo
	if f.SessionID != "" {
		all = st.SessionSegments(f.SessionID)
	} else {
		all = st.Segments()
	}
	out := make([]storage.SegmentInfo, 0, len(all))
	for _, m := range all {
		if f.overlaps(m) {
			out = append(out, m)
		}
	}
	return out
}

// Completeness builds the completeness report for the filtered interval
// without exporting sample payloads; it still streams the rejection/loss
// records of intersecting segments (constant memory). If From is zero the
// report window starts at the earliest overlapping segment boundary.
func Completeness(st *storage.Store, f Filter) (Report, error) {
	segs := segmentsFor(st, f)
	rep := Report{From: f.From, To: f.To, ReasonCounts: map[model.RejectReason]int{}, Segments: segs}

	for _, m := range segs {
		rdr, err := st.OpenSegmentReader(m.ID)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return rep, err
		}
		for rdr.Next() {
			rec := rdr.Record()
			if rec.Rejected == nil || !f.inWindow(rec.Time) {
				continue
			}
			if !f.matchesDeviceSensor(rec.Device, rec.Sensor) {
				continue
			}
			rep.ReasonCounts[rec.Rejected.Reason]++
			rep.Defects = append(rep.Defects, rec)
		}
		_ = rdr.Close()
	}
	rep.Ranges = mergeSpans(segmentSpans(segs, rep.ReasonCounts, rep.Defects, f), f)
	return rep, nil
}

func clampSpan(sp *span, f Filter) {
	if !f.From.IsZero() && sp.from.Before(f.From) {
		sp.from = f.From
	}
	if !f.To.IsZero() && sp.to.After(f.To) {
		sp.to = f.To
	}
}

func firstNonZero(a, b time.Time) time.Time {
	if !a.IsZero() {
		return a
	}
	return b
}

func lastNonZero(a, b time.Time) time.Time {
	if !a.IsZero() {
		return a
	}
	return b
}

// mergeSpans turns coverage spans into a disjoint, end-to-end partition of
// the filter window, inserting NO_DATA gaps. Spans overlap on a closed
// interval basis; a piece is COMPLETE only when every span covering it is
// good. Point defects (zero-length spans) mark just their instant.
func mergeSpans(spans []span, f Filter) []Range {
	if len(spans) == 0 {
		if !f.From.IsZero() && !f.To.IsZero() {
			return []Range{{From: f.From, To: f.To, Verdict: VerdictNoData}}
		}
		return nil
	}
	sort.Slice(spans, func(i, j int) bool {
		if !spans[i].from.Equal(spans[j].from) {
			return spans[i].from.Before(spans[j].from)
		}
		return spans[i].to.Before(spans[j].to)
	})

	// Build the sorted boundary list (starts and ends) and evaluate the
	// covering spans at: each open piece (p_i, p_{i+1}) midpoint, and each
	// boundary instant itself (so zero-length point defects are honored).
	boundSet := map[time.Time]struct{}{}
	for _, sp := range spans {
		boundSet[sp.from] = struct{}{}
		boundSet[sp.to] = struct{}{}
	}
	bounds := make([]time.Time, 0, len(boundSet))
	for t := range boundSet {
		bounds = append(bounds, t)
	}
	sort.Slice(bounds, func(i, j int) bool { return bounds[i].Before(bounds[j]) })

	type piece struct {
		from, to time.Time
		good     bool
		reasons  map[model.RejectReason]struct{}
	}
	coverAt := func(t time.Time) (bool, map[model.RejectReason]struct{}, bool) {
		good := true
		covered := false
		rs := map[model.RejectReason]struct{}{}
		for _, sp := range spans {
			if (t.After(sp.from) || t.Equal(sp.from)) && (t.Before(sp.to) || t.Equal(sp.to)) {
				covered = true
				if !sp.good {
					good = false
					for r := range sp.reasons {
						rs[r] = struct{}{}
					}
				}
			}
		}
		return good, rs, covered
	}

	var pieces []piece
	for _, t := range bounds {
		if good, rs, covered := coverAt(t); covered {
			pieces = append(pieces, piece{from: t, to: t, good: good, reasons: rs})
		}
	}
	for i := 0; i+1 < len(bounds); i++ {
		a, b := bounds[i], bounds[i+1]
		mid := a.Add(b.Sub(a) / 2)
		if good, rs, covered := coverAt(mid); covered {
			pieces = append(pieces, piece{from: a, to: b, good: good, reasons: rs})
		}
	}
	sort.Slice(pieces, func(i, j int) bool {
		if !pieces[i].from.Equal(pieces[j].from) {
			return pieces[i].from.Before(pieces[j].from)
		}
		return pieces[i].to.Before(pieces[j].to)
	})

	// Coalesce adjacent pieces with the same verdict and reason set.
	var co []piece
	for _, p := range pieces {
		if len(co) > 0 && co[len(co)-1].to.Equal(p.from) &&
			co[len(co)-1].good == p.good && reasonsEqual(co[len(co)-1].reasons, p.reasons) {
			if p.to.After(co[len(co)-1].to) {
				co[len(co)-1].to = p.to
			}
			continue
		}
		co = append(co, p)
	}

	ranges := []Range{}
	cursor := f.From
	for _, p := range co {
		if f.From.IsZero() {
			cursor = p.from
		}
		if p.from.After(cursor) {
			ranges = append(ranges, Range{From: cursor, To: p.from, Verdict: VerdictNoData})
		}
		v := VerdictComplete
		if !p.good {
			v = VerdictIncomplete
		}
		ranges = append(ranges, Range{From: p.from, To: p.to, Verdict: v, Reasons: reasonList(p.reasons)})
		cursor = p.to
	}
	if !f.To.IsZero() && cursor.Before(f.To) {
		ranges = append(ranges, Range{From: cursor, To: f.To, Verdict: VerdictNoData})
	}
	return ranges
}

func cloneReasons(in map[model.RejectReason]struct{}) map[model.RejectReason]struct{} {
	out := make(map[model.RejectReason]struct{}, len(in))
	for r := range in {
		out[r] = struct{}{}
	}
	return out
}

func reasonList(in map[model.RejectReason]struct{}) []model.RejectReason {
	out := make([]model.RejectReason, 0, len(in))
	for r := range in {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func reasonsEqual(a, b map[model.RejectReason]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for r := range a {
		if _, ok := b[r]; !ok {
			return false
		}
	}
	return true
}
