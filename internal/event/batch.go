package event

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// ErrBadBatch reports a BATCH payload that does not follow §4.3.
var ErrBadBatch = errors.New("event: bad batch payload")

// Minimum encoded sizes of one event, used to bound allocations on decode.
const (
	minMetricSize = 1 + 8 + 8     // empty key, f64, i64
	minLogSize    = 8 + 1 + 1 + 1 // i64, three empty strings
)

// AppendBatch appends the BATCH payload of b (§4.3) to dst. Its event count,
// b.Len(), is the n the log needs to assign offsets.
func AppendBatch(dst []byte, b *Batch) []byte {
	dst = appendString(dst, b.Tenant)
	dst = append(dst, byte(b.Kind))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(b.IngestMs))
	dst = binary.AppendUvarint(dst, uint64(b.Len()))
	if b.Kind == KindLogs {
		for _, e := range b.Logs {
			dst = binary.LittleEndian.AppendUint64(dst, uint64(e.TsMs))
			dst = appendString(dst, e.Labels)
			dst = appendString(dst, e.Fields)
			dst = appendString(dst, e.Msg)
		}
		return dst
	}
	for _, e := range b.Metrics {
		dst = appendString(dst, e.Key)
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(e.Value))
		dst = binary.LittleEndian.AppendUint64(dst, uint64(e.TsMs))
	}
	return dst
}

func appendString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// DecodeBatch parses a BATCH payload. The payload must hold exactly the
// declared events, with nothing after them.
func DecodeBatch(p []byte) (Batch, error) {
	r := reader{p: p}
	b := Batch{Tenant: r.string()}
	b.Kind = Kind(r.u8())
	b.IngestMs = int64(r.u64())
	n := r.uvarint()
	if r.bad {
		return Batch{}, fmt.Errorf("%w: truncated header", ErrBadBatch)
	}
	switch b.Kind {
	case KindMetrics:
		if n > uint64(len(r.p)/minMetricSize) {
			return Batch{}, fmt.Errorf("%w: %d events do not fit", ErrBadBatch, n)
		}
		b.Metrics = make([]Metric, n)
		for i := range b.Metrics {
			e := &b.Metrics[i]
			e.Key = r.string()
			e.Value = math.Float64frombits(r.u64())
			e.TsMs = int64(r.u64())
		}
	case KindLogs:
		if n > uint64(len(r.p)/minLogSize) {
			return Batch{}, fmt.Errorf("%w: %d events do not fit", ErrBadBatch, n)
		}
		b.Logs = make([]Log, n)
		for i := range b.Logs {
			e := &b.Logs[i]
			e.TsMs = int64(r.u64())
			e.Labels = r.string()
			e.Fields = r.string()
			e.Msg = r.string()
		}
	default:
		return Batch{}, fmt.Errorf("%w: unknown kind %d", ErrBadBatch, b.Kind)
	}
	if r.bad {
		return Batch{}, fmt.Errorf("%w: truncated events", ErrBadBatch)
	}
	if len(r.p) != 0 {
		return Batch{}, fmt.Errorf("%w: %d trailing bytes", ErrBadBatch, len(r.p))
	}
	return b, nil
}

// reader consumes a payload; after the first short read it sets bad and
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

func (r *reader) string() string {
	n := r.uvarint()
	if r.bad || n > uint64(len(r.p)) {
		r.bad = true
		return ""
	}
	s := string(r.p[:n])
	r.p = r.p[n:]
	return s
}
