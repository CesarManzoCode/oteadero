package eval

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/clock"
)

// reply waits for the reply of the one request in flight.
func reply(t *testing.T, tr *HTTPTransport, n *Notifier) {
	t.Helper()
	select {
	case f := <-tr.Replies():
		f(n)
	case <-time.After(30 * time.Second):
		t.Fatal("no reply")
	}
}

func TestHTTPTransportLease(t *testing.T) {
	code := http.StatusOK
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		if r.URL.Path != "/v1/lease" || string(data) != `{"holder":"e1"}` {
			t.Errorf("request %s %s", r.URL.Path, data)
		}
		w.WriteHeader(code)
		io.WriteString(w, body)
	}))
	defer srv.Close()
	tr := NewHTTPTransport(srv.URL+"/", "http://unused")
	defer tr.Close()
	n := NewNotifier("e1", newClock(), tr, nil)

	body = `{"epoch":4,"holder":"e1","ttl_ms":2000}`
	tr.RequestLease(LeaseRequest{Holder: "e1", SentAt: 0})
	reply(t, tr, n)
	if !n.leader || n.epoch != 4 {
		t.Fatalf("after the grant: leader=%v epoch=%d", n.leader, n.epoch)
	}

	code, body = http.StatusConflict, `{"holder":"e2"}`
	tr.RequestLease(LeaseRequest{Holder: "e1", SentAt: time.Millisecond})
	reply(t, tr, n)
	if n.leader {
		t.Fatal("still leader after a 409")
	}
}

func TestHTTPTransportNotify(t *testing.T) {
	code := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		if r.URL.Path != "/notify" || r.Header.Get("Idempotency-Key") != "abc/firing" || r.Header.Get("X-Oteadero-Epoch") != "3" {
			t.Errorf("request %s %v", r.URL.Path, r.Header)
		}
		if !strings.Contains(string(data), `"incident":"abc"`) {
			t.Errorf("body %s", data)
		}
		w.WriteHeader(code)
	}))
	defer srv.Close()
	tr := NewHTTPTransport("http://unused", srv.URL)
	defer tr.Close()
	n := NewNotifier("e1", newClock(), tr, nil)
	x := &Notification{Key: "abc/firing", Incident: "abc", Kind: "firing", Epoch: 3}

	n.LeaseGranted(0, Grant{Epoch: 3, Holder: "e1", TTLMs: 60_000})
	tr.Send(x)
	reply(t, tr, n)
	if got := n.counts[statusConfirmed].Load(); got != 1 {
		t.Fatalf("confirmed = %d", got)
	}

	code = http.StatusInternalServerError
	e := &entry{key: x.Key, inflight: true, sentEpoch: 3, backoff: BackoffMin}
	n.entries[x.Key] = e
	n.order = append(n.order, e)
	n.inflight = 1
	tr.Send(x)
	reply(t, tr, n)
	if got := n.counts[statusError].Load(); got != 1 {
		t.Fatalf("errors = %d", got)
	}

	code = http.StatusConflict
	tr.Send(x)
	reply(t, tr, n)
	if got := n.counts[statusStale].Load(); got != 1 || n.leader {
		t.Fatalf("stale = %d, leader = %v", got, n.leader)
	}
}

// newClock returns a manual clock, for tests where time does not move.
func newClock() *clock.Manual { return clock.NewManual(t0) }
