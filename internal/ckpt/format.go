// Package ckpt is the checkpoint of D05: a barrier through the sequencer
// and the shards at a consistent point of the log, an exact binary image of
// the state, atomic files under <data>/ckpt/ and recovery by replaying the
// log from the newest valid one.
package ckpt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/CesarManzoCode/oteadero/internal/agg"
	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/ingest"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

const (
	// Magic opens every checkpoint file.
	Magic = "OTCKPT01"
	// Version is the format of the state after the header.
	Version = 3

	// headerSize is magic + u32 version + u32 CRC-32 IEEE of the body.
	headerSize = len(Magic) + 4 + 4
)

// ErrInvalid reports a checkpoint that cannot be trusted: wrong magic or
// version, wrong CRC or a body that does not follow the format.
var ErrInvalid = errors.New("ckpt: invalid checkpoint")

// State is what a checkpoint holds: the sequencer's snapshot, the open
// buckets of the shards at the same point of the log, the largest sketch
// error observed so far and whatever else the process keeps at that point,
// opaque here: an evaluator stores its rules and outbox in Extra.
type State struct {
	Snapshot       ingest.Snapshot
	Buckets        []agg.OpenBucket
	SketchErrorMax float64
	Extra          []byte
}

// Encode returns the file image of st. Integers are little-endian, strings
// are uvarint length plus bytes and floats are their IEEE 754 bits.
func Encode(st *State) []byte {
	snap := &st.Snapshot
	b := make([]byte, headerSize, 4096)
	copy(b, Magic)
	b = binary.LittleEndian.AppendUint64(b, snap.LogID)
	b = binary.LittleEndian.AppendUint64(b, snap.Pos.Segment)
	b = appendI64(b, snap.Pos.Byte)
	b = binary.LittleEndian.AppendUint64(b, snap.Pos.Next)
	b = appendI64(b, snap.BucketMs)
	b = appendI64(b, snap.LatenessMs)
	b = appendI64(b, snap.IdleGraceMs)
	b = binary.LittleEndian.AppendUint64(b, snap.Lease.Epoch)
	b = appendString(b, snap.Lease.Holder)
	b = appendI64(b, snap.Lease.ExpiresMs)

	b = binary.AppendUvarint(b, uint64(len(snap.Marks)))
	for _, m := range snap.Marks {
		b = appendString(b, m.Tenant)
		b = appendI64(b, m.MaxTs)
	}

	b = binary.AppendUvarint(b, uint64(len(snap.Tenants)))
	for i := range snap.Tenants {
		s := &snap.Tenants[i]
		b = appendString(b, s.Tenant)
		b = binary.LittleEndian.AppendUint64(b, s.Received)
		b = binary.LittleEndian.AppendUint64(b, s.Accepted)
		b = binary.AppendUvarint(b, uint64(len(s.Dropped)))
		for _, n := range s.Dropped {
			b = binary.LittleEndian.AppendUint64(b, n)
		}
		b = binary.LittleEndian.AppendUint64(b, s.Late)
		b = appendI64(b, s.QueuedEvents)
	}

	b = binary.AppendUvarint(b, uint64(len(st.Buckets)))
	for i := range st.Buckets {
		o := &st.Buckets[i]
		b = appendString(b, o.Tenant)
		b = appendString(b, o.Series)
		b = append(b, byte(o.Kind))
		b = appendI64(b, o.End)
		b = binary.LittleEndian.AppendUint64(b, o.Count)
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(o.Sum))
		b = binary.AppendUvarint(b, uint64(len(o.Ext)))
		b = append(b, o.Ext...)
	}
	b = binary.LittleEndian.AppendUint64(b, math.Float64bits(st.SketchErrorMax))
	b = binary.AppendUvarint(b, uint64(len(st.Extra)))
	b = append(b, st.Extra...)

	binary.LittleEndian.PutUint32(b[len(Magic):], Version)
	binary.LittleEndian.PutUint32(b[len(Magic)+4:], crc32.ChecksumIEEE(b[headerSize:]))
	return b
}

func appendI64(b []byte, v int64) []byte {
	return binary.LittleEndian.AppendUint64(b, uint64(v))
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// Decode parses a file image; every failure wraps ErrInvalid.
func Decode(data []byte) (*State, error) {
	if len(data) < headerSize || string(data[:len(Magic)]) != Magic {
		return nil, fmt.Errorf("%w: bad header", ErrInvalid)
	}
	if v := binary.LittleEndian.Uint32(data[len(Magic):]); v != Version {
		return nil, fmt.Errorf("%w: version %d", ErrInvalid, v)
	}
	if crc32.ChecksumIEEE(data[headerSize:]) != binary.LittleEndian.Uint32(data[len(Magic)+4:]) {
		return nil, fmt.Errorf("%w: bad crc", ErrInvalid)
	}

	r := reader{p: data[headerSize:]}
	st := &State{}
	snap := &st.Snapshot
	snap.LogID = r.u64()
	snap.Pos = wal.Pos{Segment: r.u64(), Byte: r.i64(), Next: r.u64()}
	snap.BucketMs = r.i64()
	snap.LatenessMs = r.i64()
	snap.IdleGraceMs = r.i64()
	snap.Lease = wal.Lease{Epoch: r.u64(), Holder: r.string(), ExpiresMs: r.i64()}

	if n := r.count(9); n > 0 {
		snap.Marks = make([]ingest.Mark, n)
		for i := range snap.Marks {
			snap.Marks[i] = ingest.Mark{Tenant: r.string(), MaxTs: r.i64()}
		}
	}

	if n := r.count(26); n > 0 {
		snap.Tenants = make([]ingest.Stats, n)
		for i := range snap.Tenants {
			s := &snap.Tenants[i]
			s.Tenant = r.string()
			s.Received = r.u64()
			s.Accepted = r.u64()
			if r.uvarint() != uint64(len(s.Dropped)) {
				return nil, fmt.Errorf("%w: wrong number of drop causes", ErrInvalid)
			}
			for c := range s.Dropped {
				s.Dropped[c] = r.u64()
			}
			s.Late = r.u64()
			if s.QueuedEvents = r.i64(); s.QueuedEvents != 0 {
				return nil, fmt.Errorf("%w: non-empty queue", ErrInvalid)
			}
		}
	}

	if n := r.count(28); n > 0 {
		st.Buckets = make([]agg.OpenBucket, n)
		for i := range st.Buckets {
			o := &st.Buckets[i]
			o.Tenant = r.string()
			o.Series = r.string()
			o.Kind = event.Kind(r.u8())
			o.End = r.i64()
			o.Count = r.u64()
			o.Sum = math.Float64frombits(r.u64())
			if ext := r.bytes(); len(ext) > 0 {
				o.Ext = ext
			}
		}
	}

	st.SketchErrorMax = math.Float64frombits(r.u64())
	if extra := r.bytes(); len(extra) > 0 {
		st.Extra = extra
	}

	if r.bad {
		return nil, fmt.Errorf("%w: truncated body", ErrInvalid)
	}
	if len(r.p) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrInvalid, len(r.p))
	}
	return st, nil
}

// reader consumes a body; after the first short read it sets bad and
// returns zero values.
type reader struct {
	p   []byte
	bad bool
}

func (r *reader) u8() byte {
	if r.bad || len(r.p) < 1 {
		r.bad = true
		return 0
	}
	c := r.p[0]
	r.p = r.p[1:]
	return c
}

func (r *reader) u64() uint64 {
	if r.bad || len(r.p) < 8 {
		r.bad = true
		return 0
	}
	v := binary.LittleEndian.Uint64(r.p)
	r.p = r.p[8:]
	return v
}

func (r *reader) i64() int64 { return int64(r.u64()) }

func (r *reader) uvarint() uint64 {
	if r.bad {
		return 0
	}
	v, k := binary.Uvarint(r.p)
	if k <= 0 {
		r.bad = true
		return 0
	}
	r.p = r.p[k:]
	return v
}

// count reads an element count, bounded by what the remaining bytes can
// hold at minSize bytes per element.
func (r *reader) count(minSize uint64) uint64 {
	n := r.uvarint()
	if r.bad || n > uint64(len(r.p))/minSize {
		r.bad = true
		return 0
	}
	return n
}

func (r *reader) bytes() []byte {
	n := r.uvarint()
	if r.bad || n > uint64(len(r.p)) {
		r.bad = true
		return nil
	}
	v := r.p[:n:n]
	r.p = r.p[n:]
	return v
}

func (r *reader) string() string { return string(r.bytes()) }
