package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// FooterMagic marks a sealed segment carrying a trailing key index.
var FooterMagic = [4]byte{'B', 'S', 'P', 'F'}

const footerSize = 4 + 8 // magic + uint64 indexOffset

// WriteFooter appends the fixed-size footer that locates the index frame.
func WriteFooter(w io.Writer, indexOffset int64) error {
	var buf [footerSize]byte
	copy(buf[:4], FooterMagic[:])
	binary.LittleEndian.PutUint64(buf[4:], uint64(indexOffset))
	_, err := w.Write(buf[:])
	return err
}

// ReadFooter validates the trailing footer of a file of the given size.
func ReadFooter(r io.ReaderAt, size int64) (indexOffset int64, ok bool) {
	if size < footerSize {
		return 0, false
	}
	var buf [footerSize]byte
	if _, err := r.ReadAt(buf[:], size-footerSize); err != nil {
		return 0, false
	}
	var magic [4]byte
	copy(magic[:], buf[:4])
	if magic != FooterMagic {
		return 0, false
	}
	return int64(binary.LittleEndian.Uint64(buf[4:])), true
}

// Record framing: each record is
//
//	uint32 LE payload length
//	bytes  payload
//	uint32 LE crc32-IEEE(payload)
//
// This makes half-written or torn trailing records detectable and prevents
// record misalignment during concurrent append / sequential read.
var (
	errShortFrame = errors.New("storage: truncated record frame")
	errCRC        = errors.New("storage: record CRC mismatch")
)

const (
	lenSize   = 4
	crcSize   = 4
	maxRecord = 8 << 20 // 8 MiB hard cap per record
)

func writeFrame(w io.Writer, payload []byte) error {
	var hdr [lenSize]byte
	var ftr [crcSize]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(ftr[:], crc32.ChecksumIEEE(payload))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	if _, err := w.Write(ftr[:]); err != nil {
		return err
	}
	return nil
}

// readFrames returns all intact payloads in order. A torn final frame (crash
// mid-write) causes io.ErrUnexpectedEOF to be returned together with the
// intact prefix; callers treat such a batch as INCOMPLETE rather than
// silently joining the partial suffix.
func readFrames(r io.Reader) ([][]byte, error) {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	var out [][]byte
	var hdr [lenSize]byte
	var ftr [crcSize]byte
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return out, err
		}
		n := binary.LittleEndian.Uint32(hdr[:])
		if n == 0 || n > maxRecord {
			return out, errors.New("storage: implausible record length")
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(br, payload); err != nil {
			return out, io.ErrUnexpectedEOF // torn payload
		}
		if _, err := io.ReadFull(br, ftr[:]); err != nil {
			return out, io.ErrUnexpectedEOF // torn CRC
		}
		if got := crc32.ChecksumIEEE(payload); got != binary.LittleEndian.Uint32(ftr[:]) {
			return out, errCRC
		}
		out = append(out, payload)
	}
}

// FrameReader streams CRC-framed payloads without buffering the whole file,
// so range exports can process multi-hour segments with constant memory.
type FrameReader struct {
	br  *bufio.Reader
	hdr [lenSize]byte
	ftr [crcSize]byte
}

// NewFrameReader wraps r with a streaming frame reader.
func NewFrameReader(r io.Reader) *FrameReader {
	return &FrameReader{br: bufio.NewReader(r)}
}

// Next returns the next intact payload. io.EOF marks a clean end;
// io.ErrUnexpectedEOF marks a torn trailing frame.
func (f *FrameReader) Next() ([]byte, error) {
	if _, err := io.ReadFull(f.br, f.hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, err
	}
	{
		var magic [4]byte
		copy(magic[:], f.hdr[:4])
		if magic == FooterMagic {
			// Trailing footer at a frame boundary: skip offset bytes, clean EOF.
			var off [8]byte
			if _, err := io.ReadFull(f.br, off[:]); err != nil {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, io.EOF
		}
	}
	n := binary.LittleEndian.Uint32(f.hdr[:])
	if n == 0 || n > maxRecord {
		return nil, errors.New("storage: implausible record length")
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(f.br, payload); err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	if _, err := io.ReadFull(f.br, f.ftr[:]); err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(f.ftr[:]) {
		return nil, errCRC
	}
	return payload, nil
}
