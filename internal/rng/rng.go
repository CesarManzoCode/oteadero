// Package rng is the only source of randomness of the engine (D03, R11).
// Every source is seeded explicitly; nothing uses the global generator.
package rng

import "math/rand/v2"

// Source is the randomness the engine components consume.
type Source interface {
	Uint64() uint64
	IntN(n int) int
	Float64() float64
}

// New returns a PCG generator seeded with seed. The same seed always yields
// the same sequence.
func New(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
}
