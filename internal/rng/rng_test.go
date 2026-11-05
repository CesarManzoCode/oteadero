package rng

import "testing"

func TestSameSeedSameSequence(t *testing.T) {
	var a, b Source = New(42), New(42)
	for i := range 1000 {
		if x, y := a.Uint64(), b.Uint64(); x != y {
			t.Fatalf("step %d: %d != %d", i, x, y)
		}
	}
}

func TestDifferentSeedsDiffer(t *testing.T) {
	a, b := New(1), New(2)
	same := 0
	for range 100 {
		if a.Uint64() == b.Uint64() {
			same++
		}
	}
	if same == 100 {
		t.Fatal("seeds 1 and 2 produced the same sequence")
	}
}

func TestIntNRange(t *testing.T) {
	s := New(7)
	for range 1000 {
		if v := s.IntN(5); v < 0 || v >= 5 {
			t.Fatalf("IntN(5) = %d", v)
		}
	}
}
