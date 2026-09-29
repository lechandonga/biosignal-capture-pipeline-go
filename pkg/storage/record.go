package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

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

// frameScanner streams length-prefixed CRC frames from an underlying reader
// one frame at a time, so exports never have to hold a whole segment in
// memory. A torn final frame (crash mid-write) is reported as
// io.ErrUnexpectedEOF after every intact frame has been yielded.
type frameScanner struct {
	br      *bufio.Reader
	payload []byte
	scanErr error
}

func newFrameScanner(r io.Reader) *frameScanner {
	if br, ok := r.(*bufio.Reader); ok {
		return &frameScanner{br: br}
	}
	return &frameScanner{br: bufio.NewReader(r)}
}

// next advances to the next intact frame. It returns false when the stream is
// exhausted; err() distinguishes a clean EOF (nil) from a torn/corrupt tail.
func (fs *frameScanner) next() bool {
	if fs.scanErr != nil {
		return false
	}
	var hdr [lenSize]byte
	if _, err := io.ReadFull(fs.br, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			fs.scanErr = nil
		} else {
			fs.scanErr = err
		}
		return false
	}
	n := binary.LittleEndian.Uint32(hdr[:])
	if n == 0 || n > maxRecord {
		fs.scanErr = errors.New("storage: implausible record length")
		return false
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(fs.br, payload); err != nil {
		fs.scanErr = io.ErrUnexpectedEOF // torn payload
		return false
	}
	var ftr [crcSize]byte
	if _, err := io.ReadFull(fs.br, ftr[:]); err != nil {
		fs.scanErr = io.ErrUnexpectedEOF // torn CRC
		return false
	}
	if got := crc32.ChecksumIEEE(payload); got != binary.LittleEndian.Uint32(ftr[:]) {
		fs.scanErr = errCRC
		return false
	}
	fs.payload = payload
	return true
}

func (fs *frameScanner) bytes() []byte { return fs.payload }

func (fs *frameScanner) err() error { return fs.scanErr }
