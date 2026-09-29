package storage

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/pkg/model"
)

func readAll(t *testing.T, st *Store, id string) []SegmentRecord {
	t.Helper()
	rdr, err := st.OpenSegmentReader(id)
	if err != nil {
		t.Fatalf("open %s: %v", id, err)
	}
	var out []SegmentRecord
	for rdr.Next() {
		out = append(out, rdr.Record())
	}
	err = rdr.Err()
	_ = rdr.Close()
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("scan %s: %v", id, err)
	}
	return out
}

// TestSegment_FooterDistinguishesSealed verifies the durable footer and that
// header/footer records never surface in streamed output.
func TestSegment_FooterDistinguishesSealed(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	_ = st.CreateBatch("s1", "sess", time.Now())
	_ = st.AppendBatchSample("s1", mkCal(1, 1))
	closed := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	if err := st.CompleteBatch("s1", closed); err != nil {
		t.Fatal(err)
	}
	recs := readAll(t, st, "s1")
	if len(recs) != 1 || recs[0].Sample == nil {
		t.Fatalf("want exactly 1 sample record, got %+v", recs)
	}
	b, err := st.LoadBatch("s1")
	if err != nil {
		t.Fatal(err)
	}
	if b.State != model.BatchComplete || !b.ClosedAt.Equal(closed) {
		t.Fatalf("footer recovery wrong: state=%s closed=%v", b.State, b.ClosedAt)
	}
}

// TestSegment_IndexReconcileOnRestart verifies metadata reconstruction after
// restart and monotonic continuation numbering.
func TestSegment_IndexReconcileOnRestart(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	t0 := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		id := "sess-batch-" + strconv.FormatInt(int64(100001+i), 10)
		_ = st.CreateBatch(id, "sess", t0.Add(time.Duration(i)*time.Hour))
		for k := int64(0); k < 5; k++ {
			c := mkCal(k, 1)
			c.Aligned.UniTime = t0.Add(time.Duration(i)*time.Hour + time.Duration(k)*time.Second)
			_ = st.AppendBatchSample(id, c)
		}
		_ = st.CompleteBatch(id, t0.Add(time.Duration(i+1)*time.Hour))
	}
	_ = st.Close()

	st2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	segs := st2.SessionSegments("sess")
	if len(segs) != 3 {
		t.Fatalf("want 3 indexed segments, got %d", len(segs))
	}
	for _, m := range segs {
		if m.Samples != 5 || m.State != model.BatchComplete || m.FirstTime.IsZero() {
			t.Fatalf("metadata not reconciled: %+v", m)
		}
	}
	if got := st2.NextSessionSeq("sess"); got != 100004 {
		t.Fatalf("NextSessionSeq want 100004, got %d", got)
	}
}

// TestSegment_RestartNeverReopensIncomplete verifies a crashed segment keeps
// its id and content and cannot be reused; continuation needs a fresh id.
func TestSegment_RestartNeverReopensIncomplete(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	_ = st.CreateBatch("sess-batch-000001", "sess", time.Now())
	_ = st.AppendBatchSample("sess-batch-000001", mkCal(1, 1))
	_ = st.Close()

	st2, _ := Open(dir)
	if err := st2.CreateBatch("sess-batch-000001", "sess", time.Now()); !errors.Is(err, ErrBatchOpen) {
		t.Fatalf("reusing existing incomplete id must fail, got %v", err)
	}
	next := st2.NextSessionSeq("sess")
	if next != 2 {
		t.Fatalf("next seq want 2, got %d", next)
	}
	if err := st2.CreateBatch("sess-batch-000002", "sess", time.Now()); err != nil {
		t.Fatal(err)
	}
	b, _ := st2.LoadBatch("sess-batch-000001")
	if b.State != model.BatchIncomplete || len(b.Samples) != 1 {
		t.Fatalf("old incomplete segment altered: %+v", b)
	}
}

// TestSegment_StreamReaderTornTail streams a partial with a torn final frame.
func TestSegment_StreamReaderTornTail(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	_ = st.CreateBatch("p1", "s", time.Now())
	_ = st.AppendBatchSample("p1", mkCal(1, 1))
	_ = st.AppendBatchSample("p1", mkCal(2, 2))
	st.mu.Lock()
	h := st.open["p1"]
	st.mu.Unlock()
	h.mu.Lock()
	if _, err := h.f.Write([]byte{0x05, 0x00, 0x00, 0x00, 0x61}); err != nil {
		t.Fatal(err)
	}
	_ = h.f.Sync()
	h.mu.Unlock()
	_ = st.Close()

	st2, _ := Open(dir)
	rdr, err := st2.OpenSegmentReader("p1")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rdr.Next() {
		n++
	}
	if !errors.Is(rdr.Err(), io.ErrUnexpectedEOF) {
		t.Fatalf("want torn tail, got %v", rdr.Err())
	}
	_ = rdr.Close()
	if n != 2 {
		t.Fatalf("intact prefix must stream, got %d", n)
	}
}

// TestArchive_BusySegmentLeased verifies lease protection against archive
// and reclaim.
func TestArchive_BusySegmentLeased(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	_ = st.CreateBatch("l1", "s", time.Now())
	_ = st.AppendBatchSample("l1", mkCal(1, 1))
	_ = st.CompleteBatch("l1", time.Now())

	rdr, err := st.OpenSegmentReader("l1")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.ArchiveBatch("l1"); !errors.Is(err, ErrSegmentBusy) {
		t.Fatalf("leased archive want ErrSegmentBusy, got %v", err)
	}
	if _, err := st.ReclaimSegment("l1", "test"); !errors.Is(err, ErrSegmentBusy) {
		t.Fatalf("leased reclaim want ErrSegmentBusy, got %v", err)
	}
	_ = rdr.Close()
	if err := st.ArchiveBatch("l1"); err != nil {
		t.Fatalf("archive after release: %v", err)
	}
	if _, err := os.Stat(st.archivePath("l1")); err != nil {
		t.Fatalf("archive file missing: %v", err)
	}
	// Archived segment streams identically to the uncompressed original.
	recs := readAll(t, st, "l1")
	if len(recs) != 1 {
		t.Fatalf("archived readback got %d", len(recs))
	}
}

// TestSegment_LegacyDataNoMigration emulates a pre-upgrade sealed batch
// (header/data frames, no footer, no index dir): it must open and read with
// the same content and COMPLETE state without any migration.
func TestSegment_LegacyDataNoMigration(t *testing.T) {
	tmp := t.TempDir()
	legacy, _ := Open(tmp)
	_ = legacy.CreateBatch("legacy1", "oldsess", time.Now())
	_ = legacy.AppendBatchSample("legacy1", mkCal(1, 42))
	_ = legacy.AppendBatchSample("legacy1", mkCal(2, 43))
	_ = legacy.CompleteBatch("legacy1", time.Now())
	_ = legacy.Close()

	data, err := os.ReadFile(filepath.Join(tmp, dirBatches, "legacy1"+extBatch))
	if err != nil {
		t.Fatal(err)
	}
	// Trim the trailing footer frame by walking frames and remembering where
	// the last frame begins (4-byte len + payload + 4-byte crc).
	br := bufio.NewReader(bytes.NewReader(data))
	fs := newFrameScanner(br)
	prevEnd := 0
	end := 0
	for fs.next() {
		prevEnd = end
		end += lenSize + len(fs.bytes()) + crcSize
	}
	if err := fs.err(); err != nil {
		t.Fatal(err)
	}
	data = data[:prevEnd]
	_ = end

	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, dirBatches), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, dirArchive), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, dirBatches, "legacy1"+extBatch), data, 0o644); err != nil {
		t.Fatal(err)
	}

	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := st.LoadBatch("legacy1")
	if err != nil {
		t.Fatal(err)
	}
	if b.State != model.BatchComplete || len(b.Samples) != 2 {
		t.Fatalf("legacy sealed batch must read COMPLETE: state=%s n=%d", b.State, len(b.Samples))
	}
	segs := st.SessionSegments("oldsess")
	if len(segs) != 1 || segs[0].Samples != 2 {
		t.Fatalf("legacy segment missing from index: %+v", segs)
	}
	recs := readAll(t, st, "legacy1")
	if len(recs) != 2 || recs[0].Sample.Aligned.Seq != 1 {
		t.Fatalf("legacy stream wrong: %+v", recs)
	}
}

// TestSegment_ConcurrentAppendReadReclaim races writers, readers, archivers
// and reclamation: no read ever observes corruption or misalignment.
func TestSegment_ConcurrentAppendReadReclaim(t *testing.T) {
	dir := t.TempDir()
	st, _ := Open(dir)
	for i := 0; i < 6; i++ {
		id := "old-batch-" + strconv.Itoa(i)
		_ = st.CreateBatch(id, "old", time.Now())
		for k := int64(0); k < 4; k++ {
			_ = st.AppendBatchSample(id, mkCal(k, float64(k)))
		}
		_ = st.CompleteBatch(id, time.Now())
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, 4)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for seq := 0; ; seq++ {
			select {
			case <-stop:
				return
			default:
			}
			id := "write-batch-" + strconv.Itoa(seq)
			if err := st.CreateBatch(id, "w", time.Now()); err != nil {
				continue
			}
			for k := 0; k < 3; k++ {
				_ = st.AppendBatchSample(id, mkCal(int64(k), float64(k)))
			}
			_ = st.CompleteBatch(id, time.Now())
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, m := range st.Segments() {
				if m.State == model.BatchIncomplete {
					continue
				}
				rdr, err := st.OpenSegmentReader(m.ID)
				if err != nil {
					continue
				}
				for rdr.Next() {
					rec := rdr.Record()
					if rec.Sample == nil && rec.Event == nil && rec.Rejected == nil {
						errCh <- errors.New("empty record leaked")
						_ = rdr.Close()
						return
					}
				}
				if err := rdr.Err(); err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
					errCh <- err
					_ = rdr.Close()
					return
				}
				_ = rdr.Close()
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		st.SetQuota(1)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _, _, _ = st.EnforceQuota()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, m := range st.Segments() {
				if m.State == model.BatchComplete {
					_ = st.ArchiveBatch(m.ID)
				}
			}
		}
	}()

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	for _, m := range st.Segments() {
		rdr, err := st.OpenSegmentReader(m.ID)
		if err != nil {
			t.Fatalf("index points at unreadable %s: %v", m.ID, err)
		}
		for rdr.Next() {
		}
		_ = rdr.Err()
		_ = rdr.Close()
	}
}
