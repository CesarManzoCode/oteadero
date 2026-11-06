package sketch

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/CesarManzoCode/oteadero/internal/rng"
)

var testQuantiles = []float64{0, 0.01, 0.25, 0.5, 0.9, 0.99, 0.999, 1}

// checkBound compares the sketch of values with the exact quantile of the
// same rank.
func checkBound(t *testing.T, name string, values []float64) {
	t.Helper()
	d := NewDDSketch()
	for _, v := range values {
		d.Add(v)
	}
	sorted := slices.Sorted(slices.Values(values))
	for _, q := range testQuantiles {
		exact := sorted[Rank(q, uint64(len(sorted)))]
		got := d.Quantile(q)
		if got == exact {
			continue
		}
		if rel := math.Abs(got-exact) / math.Abs(exact); !(rel <= Alpha) {
			t.Errorf("%s: q=%v: got %v, exact %v, relative error %v", name, q, got, exact, rel)
		}
	}
}

func TestRelativeErrorOnSeededDistributions(t *testing.T) {
	const n = 200_000
	for _, tc := range []struct {
		name string
		draw func(r *rand.Rand) float64
	}{
		{"uniform", func(r *rand.Rand) float64 {
			return 1000 * (1 - r.Float64())
		}},
		{"lognormal", func(r *rand.Rand) float64 {
			return math.Exp(2 * r.NormFloat64())
		}},
		{"pareto", func(r *rand.Rand) float64 {
			return math.Pow(1-r.Float64(), -1/1.5)
		}},
	} {
		for seed := uint64(1); seed <= 3; seed++ {
			r := rng.New(seed)
			values := make([]float64, n)
			for i := range values {
				values[i] = tc.draw(r)
			}
			checkBound(t, tc.name, values)
		}
	}
}

func TestRelativeErrorAtBucketEdges(t *testing.T) {
	var all []float64
	for i := -2000; i <= 2000; i += 7 {
		edge := math.Pow(gamma, float64(i))
		for _, x := range []float64{edge, math.Nextafter(edge, 0), math.Nextafter(edge, math.Inf(1))} {
			checkBound(t, "edge", []float64{x})
			checkBound(t, "negative edge", []float64{-x})
			all = append(all, x)
		}
	}
	checkBound(t, "all edges", all)
}

func TestRelativeErrorAcrossTheNormalRange(t *testing.T) {
	const smallestNormal = 0x1p-1022
	for _, x := range []float64{math.MaxFloat64, math.Nextafter(math.MaxFloat64, 0), 1e308, 1e-300, smallestNormal, 1} {
		checkBound(t, "extreme", []float64{x})
		checkBound(t, "negative extreme", []float64{-x})
	}
	d := NewDDSketch()
	d.Add(math.SmallestNonzeroFloat64)
	if got := d.Quantile(0.5); got <= 0 || math.IsInf(got, 0) {
		t.Errorf("smallest value estimated as %v", got)
	}
}

func TestIndexAndEstimator(t *testing.T) {
	if got := index(1); got != 0 {
		t.Errorf("index(1) = %d", got)
	}
	if got := index(1.5); got != int32(math.Ceil(math.Log(1.5)/math.Log(gamma))) {
		t.Errorf("index(1.5) = %d", got)
	}
	want := 2 * math.Pow(gamma, 10) / (gamma + 1)
	if got := estimate(10); math.Abs(got-want)/want > 1e-12 {
		t.Errorf("estimate(10) = %v, want %v", got, want)
	}
}

func TestLowerQuantileRank(t *testing.T) {
	d := NewDDSketch()
	for _, v := range []float64{1, 100} {
		d.Add(v)
	}
	// With two values, ⌊0.999·1⌋ = 0: every q below 1 is the lower one.
	if got := d.Quantile(0.999); math.Abs(got-1) > Alpha {
		t.Errorf("q=0.999 of {1, 100} = %v", got)
	}
	if got := d.Quantile(1); math.Abs(got-100) > 100*Alpha {
		t.Errorf("q=1 of {1, 100} = %v", got)
	}
}

func TestNegativesAndZeros(t *testing.T) {
	values := []float64{-1000, -10, -0.5, 0, math.Copysign(0, -1), 0, 0.5, 10, 1000}
	checkBound(t, "mixed", values)

	d := NewDDSketch()
	for _, v := range values {
		d.Add(v)
	}
	if got := d.Quantile(0); math.Abs(got+1000) > 1000*Alpha {
		t.Errorf("minimum = %v", got)
	}
	if got := d.Quantile(0.5); got != 0 {
		t.Errorf("median = %v, want exactly 0", got)
	}
	if got := d.Quantile(1); math.Abs(got-1000) > 1000*Alpha {
		t.Errorf("maximum = %v", got)
	}
}

func TestNonFiniteValues(t *testing.T) {
	d := NewDDSketch()
	if !math.IsNaN(d.Quantile(0.5)) {
		t.Error("an empty sketch has a quantile")
	}
	d.Add(math.NaN())
	if !math.IsNaN(d.Quantile(0.5)) || d.Count() != 0 || d.NaNs() != 1 {
		t.Errorf("NaN took a rank: count %d, NaNs %d", d.Count(), d.NaNs())
	}
	for _, v := range []float64{math.Inf(1), math.Inf(-1), 1} {
		d.Add(v)
	}
	if got := d.Quantile(0); !math.IsInf(got, -1) {
		t.Errorf("q=0 = %v", got)
	}
	if got := d.Quantile(0.5); math.Abs(got-1) > Alpha {
		t.Errorf("q=0.5 = %v", got)
	}
	if got := d.Quantile(1); !math.IsInf(got, 1) {
		t.Errorf("q=1 = %v", got)
	}
}

func TestMergeEqualsSketchOfTheUnion(t *testing.T) {
	r := rng.New(7)
	const parts = 16
	union := NewDDSketch()
	sketches := make([]*DDSketch, parts)
	for i := range sketches {
		sketches[i] = NewDDSketch()
	}
	specials := []float64{0, math.NaN(), math.Inf(1), math.Inf(-1)}
	for i := range 100_000 {
		v := math.Exp(3 * r.NormFloat64())
		switch {
		case i%2 == 0:
			v = -v
		case i%1000 == 1:
			v = specials[r.IntN(len(specials))]
		}
		union.Add(v)
		sketches[r.IntN(parts)].Add(v)
	}
	merged := NewDDSketch()
	for _, s := range sketches {
		merged.Merge(s)
	}
	if !bytes.Equal(merged.AppendState(nil), union.AppendState(nil)) {
		t.Fatal("the merge differs from the sketch of the union")
	}
	for _, q := range testQuantiles {
		if merged.Quantile(q) != union.Quantile(q) {
			t.Errorf("q=%v: %v, want %v", q, merged.Quantile(q), union.Quantile(q))
		}
	}
}

func TestStateRoundTrip(t *testing.T) {
	r := rng.New(9)
	d := NewDDSketch()
	for range 10_000 {
		d.Add(r.NormFloat64() * 1e3)
	}
	for _, v := range []float64{0, 0, math.NaN(), math.Inf(1), math.Inf(-1), math.Inf(-1), math.MaxFloat64, math.SmallestNonzeroFloat64} {
		d.Add(v)
	}
	state := d.AppendState(nil)
	got := NewDDSketch()
	got.Add(1) // replaced by the load
	if err := got.LoadState(state); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.AppendState(nil), state) {
		t.Fatal("the bytes changed")
	}
	for _, q := range testQuantiles {
		if got.Quantile(q) != d.Quantile(q) {
			t.Errorf("q=%v: %v, want %v", q, got.Quantile(q), d.Quantile(q))
		}
	}
	got.Add(5) // the loaded stores are usable
	d.Add(5)
	if !bytes.Equal(got.AppendState(nil), d.AppendState(nil)) {
		t.Fatal("the loaded sketch differs after one more value")
	}
	empty := NewDDSketch()
	if err := empty.LoadState(NewDDSketch().AppendState(nil)); err != nil || empty.Count() != 0 {
		t.Fatalf("empty state: %v, count %d", err, empty.Count())
	}
}

func TestLoadStateRejectsDamage(t *testing.T) {
	d := NewDDSketch()
	for _, v := range []float64{1, 2, 3, -1, -2, 0} {
		d.Add(v)
	}
	state := d.AppendState(nil)
	for n := range len(state) {
		if err := NewDDSketch().LoadState(state[:n]); err == nil {
			t.Errorf("truncated to %d bytes: loaded", n)
		}
	}
	if err := NewDDSketch().LoadState(append(slices.Clone(state), 0)); err == nil {
		t.Error("trailing byte: loaded")
	}

	header := []byte{0, 0, 0, 0}
	store := func(pairs ...int64) []byte {
		b := binary.AppendUvarint(slices.Clone(header), uint64(len(pairs)/2))
		for i := 0; i < len(pairs); i += 2 {
			b = binary.AppendVarint(b, pairs[i])
			b = binary.AppendUvarint(b, uint64(pairs[i+1]))
		}
		return append(b, 0) // empty negative store
	}
	if err := NewDDSketch().LoadState(store(5, 1, 3, 1)); err != nil {
		t.Errorf("valid crafted state: %v", err)
	}
	for name, bad := range map[string][]byte{
		"empty bucket":    store(5, 0),
		"repeated index":  store(5, 1, 0, 1),
		"descending":      store(5, 1, -1, 1),
		"index too large": store(math.MaxInt32+1, 1),
		"index too small": store(math.MinInt32-1, 1),
		"more than fits":  append(slices.Clone(header), 200, 1),
	} {
		if err := NewDDSketch().LoadState(bad); err == nil {
			t.Errorf("%s: loaded", name)
		}
	}
}
