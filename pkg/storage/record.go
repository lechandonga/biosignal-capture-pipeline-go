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
