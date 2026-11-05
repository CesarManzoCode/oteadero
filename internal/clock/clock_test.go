package clock

import (
	"testing"
	"time"
)

func TestManualAdvance(t *testing.T) {
	c := NewManual(1_700_000_000_000)
	if got := c.NowMs(); got != 1_700_000_000_000 {
		t.Fatalf("NowMs = %d", got)
	}
	if got := c.Monotonic(); got != 0 {
		t.Fatalf("Monotonic = %v", got)
	}
	c.Advance(1500 * time.Microsecond)
	c.Advance(500 * time.Microsecond)
	if got := c.NowMs(); got != 1_700_000_000_002 {
		t.Fatalf("NowMs after advance = %d", got)
	}
	if got := c.Monotonic(); got != 2*time.Millisecond {
		t.Fatalf("Monotonic after advance = %v", got)
	}
}

func TestManualNegativeAdvancePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("negative advance did not panic")
		}
	}()
	NewManual(0).Advance(-time.Millisecond)
}

func TestRealIsMonotonic(t *testing.T) {
	var c Clock = NewReal()
	a := c.Monotonic()
	b := c.Monotonic()
	if b < a {
		t.Fatalf("monotonic went backwards: %v then %v", a, b)
	}
	if c.NowMs() <= 0 {
		t.Fatal("wall clock not positive")
	}
}
