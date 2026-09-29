package storage

import (
	"compress/gzip"
	"container/heap"
	"io"
	"os"
	"sort"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

// RangeFilter narrows a time-range query. Empty device/sensor lists match all.
type RangeFilter struct {
	From    time.Time // inclusive
	To      time.Time // inclusive
	Devices []model.DeviceID
	Sensors []model.SensorID
}

// RangeRecord is one record on the merged, deterministic timeline.
type RangeRecord struct {
	Time     time.Time
	Kind     byte // recSample / recEvent / recReject
	Sample   *model.CalibratedSample
	Event    *model.Event
	Rejected *model.RejectedRecord
}

// IncompleteReason describes one period inside the range that is not fully
// trustworthy: gaps, hard rejections, unsealed segments or reclaimed spans.
type IncompleteReason struct {
	From   time.Time
	To     time.Time
	Reason model.RejectReason
	Detail string
}

// RangeSummary is the reproducible completeness accounting for a query.
type RangeSummary struct {
	From              time.Time
	To                time.Time
	ScannedSegments   []string // only segments overlapping the filter
	CompleteIntervals []TimeInterval
	Incomplete        []IncompleteReason
	NoData            []TimeInterval
	CountsByReason    map[model.RejectReason]int
	Samples           int
	Events            int
	Rejected          int
}

// TimeInterval is a half-open [From, To) interval.
type TimeInterval struct {
	From time.Time
	To   time.Time
}

func (f RangeFilter) deviceOK(d string) bool {
	if len(f.Devices) == 0 {
		return true
	}
	for _, x := range f.Devices {
		if string(x) == d {
			return true
		}
	}
	return false
}

func (f RangeFilter) sensorOK(sn string) bool {
	if len(f.Sensors) == 0 {
		return true
	}
	for _, x := range f.Sensors {
		if string(x) == sn {
			return true
		}
	}
	return false
}

func (f RangeFilter) in(t time.Time) bool {
	if t.Before(f.From) {
		return false
	}
	if !f.To.IsZero() && t.After(f.To) {
		return false
	}
	return true
}

// Pin marks a segment as in-use by a reader/exporter; archive/reclaim skip it.
func (s *Store) Pin(id string) {
	s.pinMu.Lock()
	s.pins[id]++
	s.pinMu.Unlock()
}

// Unpin releases one Pin reference.
func (s *Store) Unpin(id string) {
	s.pinMu.Lock()
	if s.pins[id] > 1 {
		s.pins[id]--
	} else {
		delete(s.pins, id)
	}
	s.pinMu.Unlock()
}

// IsPinned reports whether a reader/exporter currently holds the segment.
func (s *Store) IsPinned(id string) bool {
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	return s.pins[id] > 0
}

// segmentStream streams one segment in canonical key order with a filter.
type segmentStream struct {
	s        *Store
	id       string
	f        *os.File
	zr       *gzip.Reader
	fr       *FrameReader
	filter   RangeFilter
	loaded   []keyedEntry // legacy/partial unsorted fallback (buffered once)
	loadedAt int
	torn     bool
	head     *RangeRecord
}

func (s *Store) openStream(id string, f RangeFilter) (*segmentStream, error) {
	m, err := s.SegmentMeta(id)
	if err != nil {
		return nil, err
	}
	st := &segmentStream{s: s, id: id, filter: f}
	if m.State == model.BatchReclaimed {
		return st, nil // tombstone: no data records
	}
	if !m.Sorted {
		// Open/partial or legacy unsealed-on-crash segment: buffer the intact
		// prefix once and merge from memory; bounded by one open segment.
		s.lifeMu.RLock()
		defer s.lifeMu.RUnlock()
		b, err := s.LoadBatch(id)
		if err != nil {
			return nil, err
		}
		var entries []keyedEntry
		for i := range b.Samples {
			js := sampleToJSON(&b.Samples[i])
			raw, _ := marshalFrame(recSample, js)
			e, _ := decodeEntry(raw)
			entries = append(entries, e)
		}
		for i := range b.Events {
			raw, _ := marshalFrame(recEvent, eventToJSON(&b.Events[i]))
			e, _ := decodeEntry(raw)
			entries = append(entries, e)
		}
		for i := range b.Rejected {
			raw, _ := marshalFrame(recReject, rejectToJSON(&b.Rejected[i]))
			e, _ := decodeEntry(raw)
			entries = append(entries, e)
		}
		sort.SliceStable(entries, func(i, j int) bool {
			if !entries[i].t.Equal(entries[j].t) {
				return entries[i].t.Before(entries[j].t)
			}
			return entries[i].sortKey < entries[j].sortKey
		})
		st.loaded = entries
		if err := st.advanceLoaded(); err != nil {
			return nil, err
		}
		return st, nil
	}
	var path string
	var gz bool
	s.lifeMu.RLock()
	defer s.lifeMu.RUnlock()
	if m.State == model.BatchArchived {
		if _, err := os.Stat(s.archivePath(id)); err == nil {
			path, gz = s.archivePath(id), true
		} else {
			// Archived then reclaimed between snapshot and open: tombstone.
			return st, nil
		}
	} else {
		if _, err := os.Stat(s.batchPath(id)); err != nil {
			if _, err2 := os.Stat(s.archivePath(id)); err2 == nil {
				path, gz = s.archivePath(id), true
			} else {
				// Archived/reclaimed concurrently: treat as tombstone.
				return st, nil
			}
		} else {
			path = s.batchPath(id)
		}
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return st, nil // reclaimed between snapshot and open: tombstone
		}
		return nil, err
	}
	st.f = file
	var rdr io.Reader = file
	if gz {
		zr, err := gzip.NewReader(file)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		st.zr = zr
		rdr = zr
	}
	st.fr = NewFrameReader(rdr)
	if err := st.advanceFile(); err != nil {
		st.close()
		return nil, err
	}
	return st, nil
}

func (st *segmentStream) close() {
	if st.zr != nil {
		_ = st.zr.Close()
	}
	if st.f != nil {
		_ = st.f.Close()
	}
}

func recordFromPayload(payload []byte) (*RangeRecord, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var env = envelope{Type: payload[0], Raw: payload[1:]}
	rc := &RangeRecord{Kind: payload[0]}
	switch payload[0] {
	case recSample:
		smp, err := decodeSample(&env)
		if err != nil {
			return nil, err
		}
		rc.Time, rc.Sample = smp.Aligned.UniTime, smp
	case recEvent:
		ev, err := decodeEvent(&env)
		if err != nil {
			return nil, err
		}
		rc.Time, rc.Event = ev.At, ev
	case recReject:
		rj, err := decodeReject(&env)
		if err != nil {
			return nil, err
		}
		rc.Time, rc.Rejected = rj.At, rj
	default:
		return nil, nil
	}
	return rc, nil
}

func (st *segmentStream) matches(rc *RangeRecord) bool {
	if rc == nil || !st.filter.in(rc.Time) {
		return false
	}
	switch rc.Kind {
	case recSample:
		return st.filter.deviceOK(string(rc.Sample.Aligned.Device)) &&
			st.filter.sensorOK(string(rc.Sample.Aligned.Sensor))
	case recEvent:
		return st.filter.deviceOK(string(rc.Event.Device)) &&
			st.filter.sensorOK(string(rc.Event.Sensor))
	case recReject:
		return st.filter.deviceOK(string(rc.Rejected.Sample.Device)) &&
			st.filter.sensorOK(string(rc.Rejected.Sample.Sensor))
	}
	return false
}

func (st *segmentStream) advanceLoaded() error {
	for st.loadedAt < len(st.loaded) {
		e := st.loaded[st.loadedAt]
		st.loadedAt++
		if e.payload[0] == recHeader || e.payload[0] == recIndex {
			continue
		}
		rc, err := recordFromPayload(e.payload)
		if err != nil {
			return err
		}
		if st.matches(rc) {
			st.head = rc
			return nil
		}
	}
	st.head = nil
	return nil
}

func (st *segmentStream) advanceFile() error {
	for {
		payload, err := st.fr.Next()
		if err == io.EOF {
			st.head = nil
			return nil
		}
		if err == io.ErrUnexpectedEOF {
			st.torn = true
			st.head = nil
			return nil
		}
		if err != nil {
			return err
		}
		if len(payload) == 0 || payload[0] == recHeader || payload[0] == recIndex {
			continue
		}
		rc, err := recordFromPayload(payload)
		if err != nil {
			return err
		}
		if st.matches(rc) {
			st.head = rc
			return nil
		}
	}
}

func (st *segmentStream) next() (*RangeRecord, error) {
	cur := st.head
	if st.loaded != nil || (st.f == nil) {
		if cur == nil {
			return nil, nil
		}
		return cur, st.advanceLoaded()
	}
	if cur == nil {
		return nil, nil
	}
	return cur, st.advanceFile()
}

// streamHeap merges per-segment ordered streams by (time, device/sensor/seq).
type streamHeap struct {
	items []*segmentStream
}

func (h *streamHeap) Len() int { return len(h.items) }
func (h *streamHeap) Less(i, j int) bool {
	a, b := h.items[i].head, h.items[j].head
	if a == nil || b == nil {
		return a != nil
	}
	if !a.Time.Equal(b.Time) {
		return a.Time.Before(b.Time)
	}
	return recordSortKey(a) < recordSortKey(b)
}
func (h *streamHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *streamHeap) Push(x any)    { h.items = append(h.items, x.(*segmentStream)) }
func (h *streamHeap) Pop() any {
	n := len(h.items)
	it := h.items[n-1]
	h.items = h.items[:n-1]
	return it
}

func recordSortKey(r *RangeRecord) string {
	switch r.Kind {
	case recSample:
		return entryKey(r.Time, string(r.Sample.Aligned.Device), string(r.Sample.Aligned.Sensor),
			r.Sample.Aligned.Seq, recSample)
	case recEvent:
		return entryKey(r.Time, string(r.Event.Device), string(r.Event.Sensor), 0, recEvent)
	default:
		return entryKey(r.Time, string(r.Rejected.Sample.Device), string(r.Rejected.Sample.Sensor),
			r.Rejected.Sample.Seq, recReject)
	}
}

func (s *Store) overlappingSegments(f RangeFilter) ([]*model.SegmentMeta, error) {
	all, err := s.Segments()
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, m := range all {
		if len(f.Devices) > 0 {
			hit := false
			for _, d := range m.Devices {
				if f.deviceOK(d) {
					hit = true
					break
				}
			}
			if !hit {
				continue
			}
		}
		if len(f.Sensors) > 0 {
			hit := false
			for _, sn := range m.Sensors {
				if f.sensorOK(sn) {
					hit = true
					break
				}
			}
			if !hit {
				continue
			}
		}
		if m.FirstTime.IsZero() {
			out = append(out, m) // conservatively inspect unknown-coverage segments
			continue
		}
		if !f.To.IsZero() && m.FirstTime.After(f.To) {
			continue
		}
		if !m.LastTime.Before(f.From) {
			out = append(out, m)
		}
	}
	return out, nil
}

// ExportRange streams every record in [From, To] matching the device/sensor
// filter in one deterministic order. emit is called one record at a time;
// memory use is bounded by the number of covered segments, not by range size.
// Only segments overlapping the range are opened.
func (s *Store) ExportRange(f RangeFilter, emit func(RangeRecord) error) (*RangeSummary, error) {
	metas, err := s.overlappingSegments(f)
	if err != nil {
		return nil, err
	}
	sum := &RangeSummary{From: f.From, To: f.To, CountsByReason: map[model.RejectReason]int{}}
	var streams []*segmentStream
	opened := map[string]bool{}
	for _, m := range metas {
		if opened[m.ID] {
			continue
		}
		opened[m.ID] = true
		s.Pin(m.ID)
		st, err := s.openStream(m.ID, f)
		if err != nil {
			s.Unpin(m.ID)
			for _, x := range streams {
				x.close()
				s.Unpin(x.id)
			}
			return nil, err
		}
		streams = append(streams, st)
		sum.ScannedSegments = append(sum.ScannedSegments, m.ID)
	}
	h := &streamHeap{}
	for _, st := range streams {
		if st.head != nil {
			heap.Push(h, st)
		}
	}
	for h.Len() > 0 {
		st := h.items[0]
		rc := st.head
		if err := emit(*rc); err != nil {
			break
		}
		switch rc.Kind {
		case recSample:
			sum.Samples++
		case recEvent:
			sum.Events++
		case recReject:
			sum.Rejected++
		}
		if err := st.advanceFileOrLoaded(); err != nil {
			for _, x := range streams {
				x.close()
				s.Unpin(x.id)
			}
			return nil, err
		}
		if st.head == nil {
			heap.Pop(h)
		} else {
			heap.Fix(h, 0)
		}
	}
	for _, st := range streams {
		st.close()
		s.Unpin(st.id)
	}
	s.fillCompleteness(sum, f, metas)
	return sum, nil
}

func (st *segmentStream) advanceFileOrLoaded() error {
	if st.f != nil {
		return st.advanceFile()
	}
	return st.advanceLoaded()
}

// QueryRange buffers only the result records (convenience wrapper; large
// exports should use ExportRange).
func (s *Store) QueryRange(f RangeFilter) ([]RangeRecord, *RangeSummary, error) {
	var out []RangeRecord
	sum, err := s.ExportRange(f, func(r RangeRecord) error {
		out = append(out, r)
		return nil
	})
	return out, sum, err
}

type coverSpan struct{ from, to time.Time }

func mergeSpans(in []coverSpan) []coverSpan {
	if len(in) == 0 {
		return in
	}
	sort.Slice(in, func(i, j int) bool { return in[i].from.Before(in[j].from) })
	out := in[:1]
	for _, sp := range in[1:] {
		last := &out[len(out)-1]
		if !sp.from.After(last.to) {
			if sp.to.After(last.to) {
				last.to = sp.to
			}
		} else {
			out = append(out, sp)
		}
	}
	return out
}

func subtractSpans(base, cut []coverSpan) []coverSpan {
	var out []coverSpan
	for _, b := range base {
		cur := b
		for _, c := range cut {
			if !c.to.After(cur.from) || !c.from.Before(cur.to) {
				continue
			}
			if c.from.After(cur.from) {
				out = append(out, coverSpan{cur.from, c.from})
			}
			if c.to.Before(cur.to) {
				cur.from = c.to
			} else {
				cur = coverSpan{}
				break
			}
		}
		if !cur.from.IsZero() && cur.from.Before(cur.to) {
			out = append(out, cur)
		}
	}
	return out
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func minTime2(a, b time.Time) time.Time {
	if b.IsZero() || a.Before(b) {
		return a
	}
	return b
}

// fillCompleteness partitions [From, To] into complete intervals,
// incomplete periods (tagged by reason) and genuine no-data gaps. Absent
// segments never look like bad data and markers never look like no data.
func (s *Store) fillCompleteness(sum *RangeSummary, f RangeFilter, metas []*model.SegmentMeta) {
	var covered []coverSpan
	var incomplete []IncompleteReason
	addBad := func(a, b time.Time, r model.RejectReason, d string) {
		a, b = maxTime(a, f.From), minTime2(b, f.To)
		if !a.Before(b) {
			return
		}
		incomplete = append(incomplete, IncompleteReason{From: a, To: b, Reason: r, Detail: d})
		sum.CountsByReason[r]++
	}
	addCover := func(a, b time.Time) {
		a, b = maxTime(a, f.From), minTime2(b, f.To)
		if a.Before(b) {
			covered = append(covered, coverSpan{a, b})
		}
	}
	metaMatches := func(m *model.SegmentMeta) bool {
		if len(f.Devices) == 0 {
			return true
		}
		for _, d := range m.Devices {
			if f.deviceOK(d) {
				return true
			}
		}
		return false
	}
	for _, m := range metas {
		if m.FirstTime.IsZero() {
			continue
		}
		if !metaMatches(m) {
			continue
		}
		covFrom, covTo := m.FirstTime, m.LastTime.Add(time.Nanosecond)
		switch m.State {
		case model.BatchReclaimed:
			addCover(covFrom, covTo)
			addBad(covFrom, covTo, model.ReasonSegmentReclaimed,
				"segment reclaimed under quota: "+m.ID)
			continue
		case model.BatchIncomplete:
			addCover(covFrom, covTo)
			addBad(covFrom, covTo, model.ReasonSegmentIncomplete,
				"segment never sealed (crash/unfinished): "+m.ID)
		default:
			addCover(covFrom, covTo)
		}
		for _, g := range m.Markers {
			if len(f.Devices) > 0 && !f.deviceOK(string(g.Device)) {
				continue
			}
			if len(f.Sensors) > 0 && !f.sensorOK(string(g.Sensor)) {
				continue
			}
			if g.At.Before(m.FirstTime) || g.At.After(m.LastTime) {
				continue
			}
			addBad(g.At, g.At.Add(time.Nanosecond), g.Reason, g.Detail)
		}
	}
	covered = mergeSpans(covered)
	bad := make([]coverSpan, 0, len(incomplete))
	for _, r := range incomplete {
		bad = append(bad, coverSpan{r.From, r.To})
	}
	bad = mergeSpans(bad)
	for _, c := range subtractSpans(covered, bad) {
		sum.CompleteIntervals = append(sum.CompleteIntervals, TimeInterval{From: c.from, To: c.to})
	}
	sum.Incomplete = incomplete
	cursor := f.From
	for _, c := range covered {
		if cursor.Before(c.from) {
			sum.NoData = append(sum.NoData, TimeInterval{From: cursor, To: c.from})
		}
		if c.to.After(cursor) {
			cursor = c.to
		}
	}
	if !f.To.IsZero() && cursor.Before(f.To) {
		sum.NoData = append(sum.NoData, TimeInterval{From: cursor, To: f.To})
	}
}
