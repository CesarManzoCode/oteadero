// Package sketch holds the mergeable summaries of D07: DDSketch for quantiles
// and HyperLogLog for cardinality. Both are plain values with no I/O and no
// locks, and their state serializes exactly for checkpoints.
package sketch

import (
	"encoding/binary"
	"errors"
	"maps"
	"math"
	"slices"
)

const (
	// Alpha is the published relative error bound of the quantiles (D07).
	Alpha = 0.01

	// internalAlpha builds gamma; the margin up to Alpha absorbs the rounding
	// of the logarithm, which can move a value at a bucket edge one bucket up.
	internalAlpha = 0.0099
	gamma         = (1 + internalAlpha) / (1 - internalAlpha)
)

var (
	lnGamma = math.Log(gamma)
	lnScale = math.Log(2 / (gamma + 1))
)

// errBadState reports a serialized state that does not follow the format.
var errBadState = errors.New("sketch: bad state")

// DDSketch is the sketch of D07: positive, negative and zero stores with no
// collapsing. ±Inf have counters of their own, below and above every bucket,
// so that the order and the bound hold for them too. NaN has no order: it is
// counted apart and takes no rank.
type DDSketch struct {
	pos, neg map[int32]uint64
	zeros    uint64
	posInf   uint64
	negInf   uint64
	nans     uint64
}

// NewDDSketch returns an empty sketch.
func NewDDSketch() *DDSketch {
	return &DDSketch{pos: map[int32]uint64{}, neg: map[int32]uint64{}}
}

// index returns ⌈log_γ x⌉ for a positive finite x.
func index(x float64) int32 {
	return int32(math.Ceil(math.Log(x) / lnGamma))
}

// estimate returns 2γ^i/(γ+1). It works in log space because γ^i overflows
// for the top buckets, where the value itself is still finite.
func estimate(i int32) float64 {
	return min(math.Exp(float64(i)*lnGamma+lnScale), math.MaxFloat64)
}

// Add adds one value.
func (d *DDSketch) Add(x float64) {
	switch {
	case math.IsInf(x, 1):
		d.posInf++
	case math.IsInf(x, -1):
		d.negInf++
	case x > 0:
		d.pos[index(x)]++
	case x < 0:
		d.neg[index(-x)]++
	case x == 0:
		d.zeros++
	default:
		d.nans++
	}
}

// Count returns how many values have a rank: all but NaN.
func (d *DDSketch) Count() uint64 {
	n := d.zeros + d.posInf + d.negInf
	for _, c := range d.pos {
		n += c
	}
	for _, c := range d.neg {
		n += c
	}
	return n
}

// NaNs returns how many NaN values were added.
func (d *DDSketch) NaNs() uint64 { return d.nans }

// Rank returns ⌊q(n−1)⌋, the 0-based rank of the lower quantile q of n > 0
// values (D07).
func Rank(q float64, n uint64) uint64 {
	return uint64(math.Floor(q * float64(n-1)))
}

// Quantile returns the estimate of quantile q in [0, 1], or NaN when no value
// has a rank.
func (d *DDSketch) Quantile(q float64) float64 {
	n := d.Count()
	if n == 0 {
		return math.NaN()
	}
	r := Rank(q, n)
	if r < d.negInf {
		return math.Inf(-1)
	}
	r -= d.negInf
	for _, i := range slices.Backward(slices.Sorted(maps.Keys(d.neg))) {
		if r < d.neg[i] {
			return -estimate(i)
		}
		r -= d.neg[i]
	}
	if r < d.zeros {
		return 0
	}
	r -= d.zeros
	for _, i := range slices.Sorted(maps.Keys(d.pos)) {
		if r < d.pos[i] {
			return estimate(i)
		}
		r -= d.pos[i]
	}
	return math.Inf(1)
}

// Merge adds the values of o, with the same result as adding them one by one.
func (d *DDSketch) Merge(o *DDSketch) {
	for i, c := range o.pos {
		d.pos[i] += c
	}
	for i, c := range o.neg {
		d.neg[i] += c
	}
	d.zeros += o.zeros
	d.posInf += o.posInf
	d.negInf += o.negInf
	d.nans += o.nans
}

// AppendState appends the exact state: the four counters as uvarints, then
// the positive and the negative store. Equal sketches have equal bytes.
func (d *DDSketch) AppendState(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, d.zeros)
	dst = binary.AppendUvarint(dst, d.posInf)
	dst = binary.AppendUvarint(dst, d.negInf)
	dst = binary.AppendUvarint(dst, d.nans)
	dst = appendStore(dst, d.pos)
	return appendStore(dst, d.neg)
}

// appendStore writes the number of buckets and, by ascending index, each one
// as varint distance from the previous index and uvarint count.
func appendStore(dst []byte, store map[int32]uint64) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(store)))
	prev := int64(0)
	for _, i := range slices.Sorted(maps.Keys(store)) {
		dst = binary.AppendVarint(dst, int64(i)-prev)
		dst = binary.AppendUvarint(dst, store[i])
		prev = int64(i)
	}
	return dst
}

// LoadState replaces the sketch with what AppendState wrote.
func (d *DDSketch) LoadState(state []byte) error {
	r := stateReader{p: state}
	loaded := DDSketch{zeros: r.uvarint(), posInf: r.uvarint(), negInf: r.uvarint(), nans: r.uvarint()}
	loaded.pos = r.store()
	loaded.neg = r.store()
	if r.bad || len(r.p) != 0 {
		return errBadState
	}
	*d = loaded
	return nil
}

// stateReader consumes a state; after the first failure it sets bad and
// returns zero values.
type stateReader struct {
	p   []byte
	bad bool
}

func (r *stateReader) uvarint() uint64 {
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

func (r *stateReader) varint() int64 {
	if r.bad {
		return 0
	}
	v, k := binary.Varint(r.p)
	if k <= 0 {
		r.bad = true
		return 0
	}
	r.p = r.p[k:]
	return v
}

func (r *stateReader) store() map[int32]uint64 {
	n := r.uvarint()
	// A bucket takes at least two bytes.
	if r.bad || n > uint64(len(r.p))/2 {
		r.bad = true
		return nil
	}
	store := make(map[int32]uint64, n)
	prev := int64(0)
	for k := range n {
		delta := r.varint()
		count := r.uvarint()
		// Plain addition could wrap around on a hostile delta; the range check
		// comes before it.
		if r.bad || count == 0 || (k > 0 && delta <= 0) ||
			delta < math.MinInt32-prev || delta > math.MaxInt32-prev {
			r.bad = true
			return nil
		}
		prev += delta
		store[int32(prev)] = count
	}
	return store
}
