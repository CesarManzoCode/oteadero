package eval

import (
	"testing"
)

// TestCoreStepsWithoutGoroutines drives an evaluator the way the simulation
// does: steps on the caller's goroutine, lease replies handed to the notifier,
// and a log cut behind the reader taken back without an error.
func TestCoreStepsWithoutGoroutines(t *testing.T) {
	w := newWorld(t)
	w.load(10, alertCounts...)
	tr := &fakeTransport{}
	c, err := NewCore(w.cfg, "e1", w.set, Deps{FS: w.mem, Clock: w.clk, Rand: w.rnd}, tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Recover(); err != nil {
		t.Fatal(err)
	}
	if more, err := c.Step(); err != nil || more {
		t.Fatalf("step at the end of the log: more=%v, %v", more, err)
	}
	if len(tr.leases) != 1 || len(tr.sent) != 0 {
		t.Fatalf("after the first step: %d lease requests, %d sends", len(tr.leases), len(tr.sent))
	}
	c.Notifier().LeaseGranted(tr.leases[0].SentAt, Grant{Epoch: 1, Holder: "e1", TTLMs: 2000})
	if _, err := c.Step(); err != nil {
		t.Fatal(err)
	}
	if len(tr.sent) == 0 {
		t.Fatal("the leader sent nothing")
	}

	w.truncateLog(c.replica.tail.start)
	if _, err := c.Step(); err != nil {
		t.Fatalf("step after the log was cut: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
