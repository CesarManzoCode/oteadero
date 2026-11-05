package sketch

import (
	"encoding/binary"
	"math"
	"math/bits"
)

const (
	hllP = 14
	hllM = 1 << hllP
	// hllMaxRank is the largest rank: all 50 bits after the index are zero.
	hllMaxRank = 64 - hllP + 1
	hllAlphaM  = 0.7213 / (1 + 1.079/hllM)

	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// hllPowers[r] is 2^−r.
var hllPowers = func() (t [hllMaxRank + 1]float64) {
	for r := range t {
		t[r] = math.Ldexp(1, -r)
	}
	return t
}()

// Mix64 is the fmix64 finalizer of MurmurHash3.
func Mix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// Hash is FNV-1a 64 followed by fmix64 (D07).
func Hash(s string) uint64 {
	h := uint64(fnvOffset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	return Mix64(h)
}

// HLL is the HyperLogLog of D07: p = 14 and 8-bit registers. The top 14 bits
// of the hash pick the register and the rest give the rank. The zero value is
// an empty HLL.
type HLL struct {
	regs [hllM]uint8
}

// Add adds one value.
func (h *HLL) Add(s string) {
	x := Hash(s)
	rank := uint8(bits.LeadingZeros64(x<<hllP|1<<(hllP-1))) + 1
	if i := x >> (64 - hllP); rank > h.regs[i] {
		h.regs[i] = rank
	}
}

// Estimate returns the cardinality estimate: αm·m²/Σ2^−reg, or linear
// counting when that is at most 5m/2 and a register is still zero.
func (h *HLL) Estimate() uint64 {
	sum := 0.0
	zeros := 0
	for _, r := range h.regs {
		sum += hllPowers[r]
		if r == 0 {
			zeros++
		}
	}
	m := float64(hllM)
	e := hllAlphaM * m * m / sum
	if e <= 5*m/2 && zeros > 0 {
		e = m * math.Log(m/float64(zeros))
	}
	return uint64(math.Round(e))
}

// Merge takes the maximum of each register: the result is the HLL of the
// union.
func (h *HLL) Merge(o *HLL) {
	for i, r := range o.regs {
		if r > h.regs[i] {
			h.regs[i] = r
		}
	}
}

// AppendState appends the exact state: the number of non-zero registers and,
// by ascending index, each one as uvarint distance from the previous index
// and its byte. Windows of a second rarely fill the registers.
func (h *HLL) AppendState(dst []byte) []byte {
	n := 0
	for _, r := range h.regs {
		if r != 0 {
			n++
		}
	}
	dst = binary.AppendUvarint(dst, uint64(n))
	prev := 0
	for i, r := range h.regs {
		if r != 0 {
			dst = binary.AppendUvarint(dst, uint64(i-prev))
			dst = append(dst, r)
			prev = i
		}
	}
	return dst
}

// LoadState replaces the HLL with what AppendState wrote.
func (h *HLL) LoadState(state []byte) error {
	r := stateReader{p: state}
	n := r.uvarint()
	if r.bad || n > hllM {
		return errBadState
	}
	var regs [hllM]uint8
	i := uint64(0)
	for k := range n {
		gap := r.uvarint()
		if r.bad || len(r.p) == 0 || (k > 0 && gap == 0) || gap >= hllM-i {
			return errBadState
		}
		i += gap
		rank := r.p[0]
		r.p = r.p[1:]
		if rank == 0 || rank > hllMaxRank {
			return errBadState
		}
		regs[i] = rank
	}
	if len(r.p) != 0 {
		return errBadState
	}
	h.regs = regs
	return nil
}
