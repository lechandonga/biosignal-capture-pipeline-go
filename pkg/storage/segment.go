package storage

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

const (
	dirMeta   = "meta"
	extMeta   = ".meta.json"
	extSealed = ".sealed.tmp"
)

// keyedEntry is one domain frame placed on the canonical timeline.
type keyedEntry struct {
	t       time.Time
	sortKey string // device/sensor + seq for deterministic ties
	payload []byte // frame byte (with type prefix)
}

func entryKey(t time.Time, device, sensor string, seq int64, typ byte) string {
	return fmt.Sprintf("%s|%s|%020d|%d", device, sensor, uint64(seq), typ)
}

// decodeEntry decodes one frame payload into a keyed entry.
func decodeEntry(payload []byte) (keyedEntry, error) {
	if len(payload) == 0 {
		return keyedEntry{}, fmt.Errorf("%w: empty frame", ErrCorrupt)
	}
	typ := payload[0]
	raw := payload[1:]
	var t time.Time
	var dev, sen string
	var seq int64
	switch typ {
	case recHeader, recIndex:
		return keyedEntry{payload: payload, t: time.Time{}}, nil
	case recSample:
		var js jsonSample
		if err := json.Unmarshal(raw, &js); err != nil {
			return keyedEntry{}, err
		}
		t, dev, sen, seq = js.UniTime, js.Device, js.Sensor, js.Seq
	case recEvent:
		var je jsonEvent
		if err := json.Unmarshal(raw, &je); err != nil {
			return keyedEntry{}, err
		}
		t, dev, sen = je.At, je.Device, je.Sensor
	case recReject:
		var jr jsonReject
		if err := json.Unmarshal(raw, &jr); err != nil {
			return keyedEntry{}, err
		}
		t, dev, sen, seq = jr.At, jr.Device, jr.Sensor, jr.Seq
	default:
		return keyedEntry{}, fmt.Errorf("%w: unknown record type %d", ErrCorrupt, typ)
	}
	return keyedEntry{t: t, sortKey: entryKey(t, dev, sen, seq, typ), payload: payload}, nil
}

// jsonIndex is the trailing key index of a sealed segment.
type jsonIndex struct {
	SampleTimes []time.Time
	EventTimes  []time.Time
	RejectTimes []time.Time
	SampleCount int
	EventCount  int
	RejectCount int
	FirstTime   time.Time
	LastTime    time.Time
	Markers     []model.GapMarker
	Devices     []string
	Sensors     []string
}

// sortAndSeal rewrites the partial file into canonical key order, appends the
// key index and footer, then atomically renames to .batch. The rewrite goes
// through .sealed.tmp in the same directory and is fsynced before the rename,
// so a crash leaves either the old .partial or the new .batch, never a mix.
func (s *Store) sortAndSeal(h *handle, batchID, sessionID string, startedAt, closedAt time.Time) error {
	partialPath := s.partialPath(batchID)
	sealedPath := partialPath + extSealed
	batchPath := s.batchPath(batchID)

	if err := h.f.Sync(); err != nil {
		return err
	}
	if err := h.f.Close(); err != nil {
		return err
	}

	src, err := os.Open(partialPath)
	if err != nil {
		return err
	}
	entries, torn, header, err := readAllEntries(src)
	if cerr := src.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if torn {
		return fmt.Errorf("%w: %s has torn trailing frame", ErrCorrupt, batchID)
	}

	dataEntries := make([]keyedEntry, 0, len(entries))
	for _, e := range entries {
		if len(e.payload) > 0 && e.payload[0] != recHeader {
			dataEntries = append(dataEntries, e)
		}
	}
	sort.SliceStable(dataEntries, func(i, j int) bool {
		if !dataEntries[i].t.Equal(dataEntries[j].t) {
			return dataEntries[i].t.Before(dataEntries[j].t)
		}
		return dataEntries[i].sortKey < dataEntries[j].sortKey
	})

	idx := buildIndex(dataEntries)

	tmp, err := os.OpenFile(sealedPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	abort := func(e error) error {
		_ = tmp.Close()
		_ = os.Remove(sealedPath)
		return e
	}
	var headerPayload []byte
	if header != nil {
		headerPayload = header
	} else {
		hb, err := json.Marshal(jsonHeader{ID: batchID, SessionID: sessionID, StartedAt: startedAt})
		if err != nil {
			return abort(err)
		}
		headerPayload = append([]byte{recHeader}, hb...)
	}
	if err := writeFrame(tmp, headerPayload); err != nil {
		return abort(err)
	}
	for _, e := range dataEntries {
		if err := writeFrame(tmp, e.payload); err != nil {
			return abort(err)
		}
	}
	indexOffset, err := tmp.Seek(0, io.SeekCurrent)
	if err != nil {
		return abort(err)
	}
	ir, err := json.Marshal(idx)
	if err != nil {
		return abort(err)
	}
	if err := writeFrame(tmp, append([]byte{recIndex}, ir...)); err != nil {
		return abort(err)
	}
	if err := WriteFooter(tmp, indexOffset); err != nil {
		return abort(err)
	}
	if err := tmp.Sync(); err != nil {
		return abort(err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(sealedPath)
		return err
	}
	if err := os.Rename(sealedPath, batchPath); err != nil {
		_ = os.Remove(sealedPath)
		return err
	}
	if err := os.Remove(partialPath); err != nil {
		return err
	}
	fsyncDir(filepath.Join(s.root, dirBatches))

	meta := &model.SegmentMeta{
		ID: batchID, SessionID: sessionID, Index: sessionIndex(batchID),
		State: model.BatchComplete, StartedAt: startedAt, ClosedAt: closedAt,
		FirstTime: idx.FirstTime, LastTime: idx.LastTime,
		SampleCount: idx.SampleCount, EventCount: idx.EventCount,
		RejectedCount: idx.RejectCount, Sorted: true, Markers: idx.Markers,
		Devices: idx.Devices, Sensors: idx.Sensors,
	}
	if fi, err := os.Stat(batchPath); err == nil {
		meta.Bytes = fi.Size()
	}
	s.setMeta(batchID, meta)
	_ = s.writeSidecar(meta)
	return nil
}

func fsyncDir(path string) {
	if d, err := os.Open(path); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

func readAllEntries(r io.Reader) (entries []keyedEntry, torn bool, header []byte, err error) {
	fr := NewFrameReader(r)
	for {
		payload, ferr := fr.Next()
		if ferr == io.EOF {
			return entries, false, header, nil
		}
		if ferr == io.ErrUnexpectedEOF {
			return entries, true, header, nil
		}
		if ferr != nil {
			return nil, false, header, ferr
		}
		if len(payload) == 0 {
			continue
		}
		if payload[0] == recHeader {
			header = payload
			continue
		}
		if payload[0] == recIndex {
			continue
		}
		e, derr := decodeEntry(payload)
		if derr != nil {
			return nil, false, header, derr
		}
		entries = append(entries, e)
	}
}

func buildIndex(entries []keyedEntry) jsonIndex {
	idx := jsonIndex{}
	var times []time.Time
	for _, e := range entries {
		if len(e.payload) == 0 {
			continue
		}
		times = append(times, e.t)
		switch e.payload[0] {
		case recSample:
			idx.SampleCount++
			idx.SampleTimes = append(idx.SampleTimes, e.t)
			var js jsonSample
			_ = json.Unmarshal(e.payload[1:], &js)
			if js.Gap {
				idx.Markers = append(idx.Markers, model.GapMarker{
					At: e.t, Device: model.DeviceID(js.Device), Sensor: model.SensorID(js.Sensor),
					Reason: model.ReasonPacketLoss, Detail: "packet loss gap marker",
				})
			}
		case recEvent:
			idx.EventCount++
			idx.EventTimes = append(idx.EventTimes, e.t)
		case recReject:
			idx.RejectCount++
			idx.RejectTimes = append(idx.RejectTimes, e.t)
			var jr jsonReject
			_ = json.Unmarshal(e.payload[1:], &jr)
			if jr.Action == string(model.ActionRejected) {
				idx.Markers = append(idx.Markers, model.GapMarker{
					At: e.t, Device: model.DeviceID(jr.Device), Sensor: model.SensorID(jr.Sensor),
					Reason: model.RejectReason(jr.Reason), Detail: jr.Detail,
				})
			}
		}
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	if len(times) > 0 {
		idx.FirstTime = times[0]
		idx.LastTime = times[len(times)-1]
	}
	devs := map[string]struct{}{}
	sens := map[string]struct{}{}
	for _, e := range entries {
		var d, sn string
		switch e.payload[0] {
		case recSample:
			var js jsonSample
			_ = json.Unmarshal(e.payload[1:], &js)
			d, sn = js.Device, js.Sensor
		case recEvent:
			var je jsonEvent
			_ = json.Unmarshal(e.payload[1:], &je)
			d, sn = je.Device, je.Sensor
		case recReject:
			var jr jsonReject
			_ = json.Unmarshal(e.payload[1:], &jr)
			d, sn = jr.Device, jr.Sensor
		}
		if d != "" {
			devs[d] = struct{}{}
		}
		if sn != "" {
			sens[sn] = struct{}{}
		}
	}
	for d := range devs {
		idx.Devices = append(idx.Devices, d)
	}
	for sn := range sens {
		idx.Sensors = append(idx.Sensors, sn)
	}
	sort.Strings(idx.Devices)
	sort.Strings(idx.Sensors)
	return idx
}

func sessionIndex(batchID string) int {
	i := strings.LastIndex(batchID, "-")
	if i < 0 {
		return 0
	}
	n, err := strconv.Atoi(batchID[i+1:])
	if err != nil {
		return 0
	}
	return n
}

// --- sidecar metadata ------------------------------------------------------

func (s *Store) metaPath(id string) string {
	return filepath.Join(s.root, dirMeta, id+extMeta)
}

func (s *Store) writeSidecar(m *model.SegmentMeta) error {
	if err := os.MkdirAll(filepath.Join(s.root, dirMeta), 0o755); err != nil {
		return err
	}
	final := s.metaPath(m.ID)
	tmp := final + ".tmp"
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	fsyncDir(filepath.Join(s.root, dirMeta))
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	fsyncDir(filepath.Join(s.root, dirMeta))
	return nil
}

func (s *Store) setMeta(id string, m *model.SegmentMeta) {
	s.metaMu.Lock()
	s.metaCache[id] = m
	s.metaMu.Unlock()
}

// SegmentMeta returns durable metadata for a segment, deriving it once (with
// a full scan of legacy/partial segments) when no sidecar exists yet.
func (s *Store) SegmentMeta(id string) (*model.SegmentMeta, error) {
	s.metaMu.Lock()
	m := s.metaCache[id]
	s.metaMu.Unlock()
	if m != nil {
		return cloneMeta(m), nil
	}
	if raw, err := os.ReadFile(s.metaPath(id)); err == nil {
		var mm model.SegmentMeta
		if err := json.Unmarshal(raw, &mm); err == nil {
			cp := mm
			s.setMeta(id, &cp)
			return cloneMeta(&cp), nil
		}
	}
	mm, err := s.deriveMeta(id)
	if err != nil {
		return nil, err
	}
	s.setMeta(id, mm)
	_ = s.writeSidecar(mm)
	return cloneMeta(mm), nil
}

func cloneMeta(m *model.SegmentMeta) *model.SegmentMeta {
	cp := *m
	cp.Markers = append([]model.GapMarker(nil), m.Markers...)
	cp.Devices = append([]string(nil), m.Devices...)
	cp.Sensors = append([]string(nil), m.Sensors...)
	return &cp
}

func (s *Store) deriveMeta(id string) (*model.SegmentMeta, error) {
	var path string
	var state model.BatchState
	var gz bool
	switch {
	case s.exists(s.archivePath(id)):
		path, state, gz = s.archivePath(id), model.BatchArchived, true
	case s.exists(s.batchPath(id)):
		path, state = s.batchPath(id), model.BatchComplete
	case s.exists(s.partialPath(id)):
		path, state = s.partialPath(id), model.BatchIncomplete
	default:
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var rdr io.Reader = f
	if gz {
		zr, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		rdr = zr
	}
	entries, torn, header, err := readAllEntries(rdr)
	if err != nil {
		return nil, err
	}
	if torn {
		state = model.BatchIncomplete
	}
	idx := buildIndex(entries)
	m := &model.SegmentMeta{
		ID: id, Index: sessionIndex(id), State: state,
		FirstTime: idx.FirstTime, LastTime: idx.LastTime,
		SampleCount: idx.SampleCount, EventCount: idx.EventCount,
		RejectedCount: idx.RejectCount,
		Sorted:        state == model.BatchComplete || state == model.BatchArchived,
		Markers:       idx.Markers, Devices: idx.Devices, Sensors: idx.Sensors,
	}
	if header != nil {
		var h jsonHeader
		_ = json.Unmarshal(header[1:], &h)
		m.SessionID = h.SessionID
		m.StartedAt = h.StartedAt
	}
	if fi, err := f.Stat(); err == nil {
		m.Bytes = fi.Size()
	}
	if state == model.BatchIncomplete && torn && !idx.LastTime.IsZero() {
		m.Markers = append(m.Markers, model.GapMarker{
			At: idx.LastTime, Reason: model.ReasonSegmentIncomplete,
			Detail: "segment not sealed; torn trailing frame dropped",
		})
	}
	return m, nil
}

// Segments returns metadata of every on-disk segment in canonical
// (session, index) order. Only metadata files/dir entries are touched.
func (s *Store) Segments() ([]*model.SegmentMeta, error) {
	ids, err := s.ListBatches()
	if err != nil {
		return nil, err
	}
	out := make([]*model.SegmentMeta, 0, len(ids))
	for _, id := range ids {
		m, err := s.SegmentMeta(id)
		if err != nil {
			continue
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SessionID != out[j].SessionID {
			return out[i].SessionID < out[j].SessionID
		}
		if out[i].Index != out[j].Index {
			return out[i].Index < out[j].Index
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}
