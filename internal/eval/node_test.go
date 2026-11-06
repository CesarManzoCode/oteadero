package eval

import (
	"bytes"
	"fmt"
	"strings"
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

// TestCoreMetrics checks the evaluator's gauges of §4.10: the watermark lag of
// what the replica read, the observed sketch error and the notifications.
func TestCoreMetrics(t *testing.T) {
	w := newWorld(t)
	w.load(10, 1, 1)
	c, err := NewCore(w.cfg, "e1", w.set, Deps{FS: w.mem, Clock: w.clk, Rand: w.rnd}, &fakeTransport{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	c.WriteMetrics(&out)
	if strings.Contains(out.String(), "oteadero_watermark_lag_ms{") {
		t.Fatalf("watermark lag before reading the log:\n%s", out.String())
	}
	if err := c.Recover(); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	c.WriteMetrics(&out)
	// The last event is at 11 000 ms, so W = 9 000.
	for _, l := range []string{
		fmt.Sprintf(`oteadero_watermark_lag_ms{tenant="a"} %d`, w.clk.NowMs()-9000),
		"oteadero_sketch_observed_rel_error_max 0",
		`oteadero_notifications_total{status="confirmed"} 0`,
	} {
		if !strings.Contains(out.String(), l+"\n") {
			t.Errorf("metrics lack %q:\n%s", l, out.String())
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
