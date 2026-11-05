package sketch

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/CesarManzoCode/oteadero/internal/rng"
)

func TestHashIsFNV1aThenFmix64(t *testing.T) {
	// FNV-1a 64 of the empty string is its offset basis, and of "a" the
	// published test vector.
	if got := Hash(""); got != Mix64(0xcbf29ce484222325) {
		t.Errorf("Hash(\"\") = %x", got)
	}
	if got := Hash("a"); got != Mix64(0xaf63dc4c8601ec8c) {
		t.Errorf("Hash(\"a\") = %x", got)
	}
	if Mix64(0) != 0 || Mix64(1) != 0xb456bcfc34c2cb2c {
		t.Errorf("Mix64 = %x, %x", Mix64(0), Mix64(1))
	}
}

func TestHLLWithinBound(t *testing.T) {
	checkpoints := []int{1, 2, 3, 5, 10, 30, 100, 300, 1000, 3000, 10_000, 20_000, 30_000, 38_000, 41_000, 44_000, 50_000, 60_000, 80_000, 100_000, 200_000, 500_000, 1_000_000}
	const bound = 0.05
	for _, prefix := range []string{"user-", "trace/", ""} {
		h := &HLL{}
		next := 0
		for n := 1; next < len(checkpoints); n++ {
			h.Add(prefix + strconv.Itoa(n))
			if n != checkpoints[next] {
				continue
			}
			next++
			if rel := math.Abs(float64(h.Estimate())-float64(n)) / float64(n); rel > bound {
				t.Errorf("prefix %q, n=%d: estimate %d, relative error %v", prefix, n, h.Estimate(), rel)
			}
		}
	}
}

func TestHLLEmptyAndRepeats(t *testing.T) {
	h := &HLL{}
	if got := h.Estimate(); got != 0 {
		t.Errorf("empty = %d", got)
	}
	for range 1000 {
		h.Add("same")
		h.Add("")
	}
	if got := h.Estimate(); got != 2 {
		t.Errorf("two values repeated = %d", got)
	}
}

func TestHLLMergeEqualsUnion(t *testing.T) {
	r := rng.New(3)
	const parts = 8
	union := &HLL{}
	hlls := make([]HLL, parts)
	for range 300_000 {
		v := strconv.Itoa(r.IntN(200_000))
		union.Add(v)
		hlls[r.IntN(parts)].Add(v)
	}
	merged := &HLL{}
	for i := range hlls {
		merged.Merge(&hlls[i])
	}
	if merged.regs != union.regs {
		t.Fatal("the merge differs from the HLL of the union")
	}
	if merged.Estimate() != union.Estimate() {
		t.Fatalf("estimates %d and %d", merged.Estimate(), union.Estimate())
	}
}

func TestHLLStateRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 1000, 500_000} {
		h := &HLL{}
		for i := range n {
			h.Add(strconv.Itoa(i))
		}
		state := h.AppendState(nil)
		got := &HLL{}
		got.Add("replaced by the load")
		if err := got.LoadState(state); err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if got.regs != h.regs {
			t.Fatalf("n=%d: registers differ", n)
		}
		if !bytes.Equal(got.AppendState(nil), state) {
			t.Fatalf("n=%d: the bytes changed", n)
		}
	}
}

func TestHLLLoadStateRejectsDamage(t *testing.T) {
	h := &HLL{}
	for i := range 100 {
		h.Add(strconv.Itoa(i))
	}
	state := h.AppendState(nil)
	for n := range len(state) {
		if err := (&HLL{}).LoadState(state[:n]); err == nil {
			t.Errorf("truncated to %d bytes: loaded", n)
		}
	}
	if err := (&HLL{}).LoadState(append(slices.Clone(state), 0)); err == nil {
		t.Error("trailing byte: loaded")
	}
	regs := func(pairs ...uint64) []byte {
		b := binary.AppendUvarint(nil, uint64(len(pairs)/2))
		for i := 0; i < len(pairs); i += 2 {
			b = binary.AppendUvarint(b, pairs[i])
			b = append(b, byte(pairs[i+1]))
		}
		return b
	}
	if err := (&HLL{}).LoadState(regs(0, 1, hllM-1, hllMaxRank)); err != nil {
		t.Errorf("first and last register: %v", err)
	}
	for name, bad := range map[string][]byte{
		"zero rank":        regs(5, 0),
		"rank too large":   regs(5, hllMaxRank+1),
		"index too large":  regs(hllM, 1),
		"repeated index":   regs(5, 1, 0, 1),
		"past the end":     regs(hllM-1, 1, 1, 1),
		"too many entries": binary.AppendUvarint(nil, hllM+1),
	} {
		if err := (&HLL{}).LoadState(bad); err == nil {
			t.Errorf("%s: loaded", name)
		}
	}
}
