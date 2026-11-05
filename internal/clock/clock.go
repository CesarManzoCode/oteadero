// Package clock is the only way the engine reads time (D03, R11).
package clock

import (
	"sync"
	"time"
)

// Clock gives wall-clock time in Unix milliseconds and a monotonic time
// measured from an arbitrary fixed origin.
type Clock interface {
	NowMs() int64
	Monotonic() time.Duration
}

// Real reads the operating system clocks.
type Real struct {
	origin time.Time
}

// NewReal returns a Real clock whose monotonic origin is now.
func NewReal() *Real {
	return &Real{origin: time.Now()}
}

// NowMs returns the wall-clock time in Unix milliseconds.
func (c *Real) NowMs() int64 {
	return time.Now().UnixMilli()
}

// Monotonic returns the time elapsed since the clock was created.
func (c *Real) Monotonic() time.Duration {
	return time.Since(c.origin)
}

// Manual is a clock that only moves when Advance is called. Tests and the
// deterministic simulation use it.
type Manual struct {
	mu     sync.Mutex
	wallNs int64
	mono   time.Duration
}

// NewManual returns a Manual clock at wall time wallMs and monotonic zero.
func NewManual(wallMs int64) *Manual {
	return &Manual{wallNs: wallMs * int64(time.Millisecond)}
}

// NowMs returns the wall-clock time in Unix milliseconds.
func (c *Manual) NowMs() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wallNs / int64(time.Millisecond)
}

// Monotonic returns the total time advanced so far.
func (c *Manual) Monotonic() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mono
}

// Advance moves both the wall and the monotonic clock forward by d.
// A negative d panics: neither clock goes backwards.
func (c *Manual) Advance(d time.Duration) {
	if d < 0 {
		panic("clock: negative advance")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wallNs += int64(d)
	c.mono += d
}
