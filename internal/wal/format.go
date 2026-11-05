// Package wal is the input log of §4.3: segments of CRC-framed records on a
// vfs.FS, with group-commit friendly buffering, rotation, sequential reading
// and crash recovery.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
)

// Record types of §4.3.
const (
	TypeBatch byte = 1
	TypeTick  byte = 2
	TypeLease byte = 3
)

const (
	// Magic opens every segment.
	Magic = "OTLOG001"
	// HeaderSize is the size of the segment header: magic plus log id.
	HeaderSize = 16
	// DefaultSegmentBytes is the rotation size of D05.
	DefaultSegmentBytes = 256 << 20

	frameSize = 8 // u32 len + u32 crc
	segSuffix = ".seg"
)

// ErrCorrupt reports a log that does not follow §4.3 at a point recovery
// does not repair.
var ErrCorrupt = errors.New("wal: corrupt log")

// Pos is a point between records: the segment (by its first offset), the
// byte inside it, and the offset of the next event at that point.
type Pos struct {
	Segment uint64
	Byte    int64
	Next    uint64
}

// Record is one decoded log record.
type Record struct {
	Type    byte
	Payload []byte
	// Offset is the offset of the first event of a BATCH; for TICK and
	// LEASE it is the offset of the next event.
	Offset uint64
	// Events is the number of events of a BATCH, zero otherwise.
	Events uint64
	// Pos is where the record starts.
	Pos Pos
}

func segName(base uint64) string {
	return fmt.Sprintf("%020d%s", base, segSuffix)
}

func parseSegName(name string) (uint64, bool) {
	digits, ok := strings.CutSuffix(name, segSuffix)
	if !ok || len(digits) != 20 {
		return 0, false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseUint(digits, 10, 64)
	return v, err == nil
}

func appendHeader(b []byte, id uint64) []byte {
	b = append(b, Magic...)
	return binary.LittleEndian.AppendUint64(b, id)
}

func parseHeader(h []byte) (uint64, error) {
	if len(h) != HeaderSize || string(h[:len(Magic)]) != Magic {
		return 0, fmt.Errorf("%w: bad segment header", ErrCorrupt)
	}
	return binary.LittleEndian.Uint64(h[len(Magic):]), nil
}

func appendRecord(b []byte, typ byte, payload []byte) []byte {
	crc := crc32.Update(crc32.ChecksumIEEE([]byte{typ}), crc32.IEEETable, payload)
	b = binary.LittleEndian.AppendUint32(b, uint32(1+len(payload)))
	b = binary.LittleEndian.AppendUint32(b, crc)
	b = append(b, typ)
	return append(b, payload...)
}

// BatchEvents returns the event count n from the header of a BATCH payload
// (§4.3) without decoding the events.
func BatchEvents(p []byte) (uint64, error) {
	tl, k := binary.Uvarint(p)
	if k <= 0 || tl > uint64(len(p)-k) {
		return 0, fmt.Errorf("%w: bad batch tenant", ErrCorrupt)
	}
	p = p[k+int(tl):]
	if len(p) < 9 || (p[0] != 1 && p[0] != 2) {
		return 0, fmt.Errorf("%w: bad batch kind", ErrCorrupt)
	}
	n, k := binary.Uvarint(p[9:])
	if k <= 0 {
		return 0, fmt.Errorf("%w: bad batch count", ErrCorrupt)
	}
	return n, nil
}

// EncodeTick returns the payload of a TICK record.
func EncodeTick(tsMs int64) []byte {
	return binary.LittleEndian.AppendUint64(nil, uint64(tsMs))
}

// DecodeTick parses the payload of a TICK record.
func DecodeTick(p []byte) (int64, error) {
	if len(p) != 8 {
		return 0, fmt.Errorf("%w: bad tick", ErrCorrupt)
	}
	return int64(binary.LittleEndian.Uint64(p)), nil
}

// Lease is the payload of a LEASE record.
type Lease struct {
	Epoch     uint64
	Holder    string
	ExpiresMs int64
}

// EncodeLease returns the payload of a LEASE record.
func EncodeLease(l Lease) []byte {
	b := binary.LittleEndian.AppendUint64(nil, l.Epoch)
	b = binary.AppendUvarint(b, uint64(len(l.Holder)))
	b = append(b, l.Holder...)
	return binary.LittleEndian.AppendUint64(b, uint64(l.ExpiresMs))
}

// DecodeLease parses the payload of a LEASE record.
func DecodeLease(p []byte) (Lease, error) {
	bad := fmt.Errorf("%w: bad lease", ErrCorrupt)
	if len(p) < 8 {
		return Lease{}, bad
	}
	l := Lease{Epoch: binary.LittleEndian.Uint64(p)}
	p = p[8:]
	hl, k := binary.Uvarint(p)
	if k <= 0 || hl > uint64(len(p)-k) || uint64(len(p)-k)-hl != 8 {
		return Lease{}, bad
	}
	l.Holder = string(p[k : k+int(hl)])
	l.ExpiresMs = int64(binary.LittleEndian.Uint64(p[k+int(hl):]))
	return l, nil
}
