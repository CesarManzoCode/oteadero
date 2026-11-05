package eval

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/clock"
	"github.com/CesarManzoCode/oteadero/internal/rules"
)

// fakeTransport keeps what the Notifier sends: the test plays the network, so it
// decides what arrives, when, and what is lost.
type fakeTransport struct {
	leases []LeaseRequest
	sent   []*Notification
}

func (f *fakeTransport) RequestLease(r LeaseRequest) { f.leases = append(f.leases, r) }
func (f *fakeTransport) Send(x *Notification)        { f.sent = append(f.sent, x) }

func (f *fakeTransport) keys() []string {
	var out []string
	for _, x := range f.sent {
		out = append(out, x.Key)
	}
	return out
}

type testNotifier struct {
	*Notifier
	tr    *fakeTransport
	roles []string
}

func newTestNotifier(id string, clk clock.Clock) *testNotifier {
	n := &testNotifier{tr: &fakeTransport{}}
	n.Notifier = NewNotifier(id, clk, n.tr, func(leader bool, epoch uint64) {
		if leader {
			n.roles = append(n.roles, fmt.Sprintf("leader epoch=%d", epoch))
		} else {
			n.roles = append(n.roles, "follower")
		}
	})
	return n
}

func firing(rule string, t int64) *rules.Transition {
	return &rules.Transition{
		Rule: rule, Group: "{}", T: t, State: rules.StateFiring, Value: 7, Present: true,
		Incident: rules.Incident{Rule: rule, Group: "{}", FiredAt: t},
	}
}

func resolved(rule string, firedAt, t int64) *rules.Transition {
	return &rules.Transition{
		Rule: rule, Group: "{}", T: t, State: rules.StateResolved,
		Incident: rules.Incident{Rule: rule, Group: "{}", FiredAt: firedAt},
	}
}

func keyOf(tr *rules.Transition) string { return tr.Incident.IdempotencyKey(tr.State) }

func TestLeaderStopsBeforeTheCoordinatorGivesAnotherEpoch(t *testing.T) {
	clk := clock.NewManual(t0)
	coord := NewCoordinator(clk, &syncedLog{}, ttlMs)
	pump := func(n *testNotifier) {
		for _, r := range n.tr.leases {
			coord.Acquire(r.Holder, func(d Decision) {
				if d.Granted {
					n.LeaseGranted(r.SentAt, Grant{Epoch: d.Epoch, Holder: d.Holder, TTLMs: d.TTLMs})
				} else {
					n.LeaseDenied(r.SentAt, d.Holder)
				}
			})
		}
		n.tr.leases = nil
	}
	e1 := newTestNotifier("e1", clk)
	e2 := newTestNotifier("e2", clk)
	e1.Poll()
	pump(e1)
	if !e1.leader || e1.epoch != 1 {
		t.Fatalf("e1 did not get the lease: %v", e1.roles)
	}

	// From here on e1 is cut off from the coordinator but reaches the receiver,
	// and it has something new to say all the time.
	var lastSend, grantedAt time.Duration
	for i := 1; grantedAt == 0 && i < 1000; i++ {
		clk.Advance(10 * time.Millisecond)
		e1.Add("a", firing("r", int64(i)))
		before := len(e1.tr.sent)
		e1.Poll()
		e1.tr.leases = nil
		if len(e1.tr.sent) > before {
			lastSend = clk.Monotonic()
		}
		for _, x := range e1.tr.sent[before:] {
			e1.Delivered(x.Key, 1, ResultConfirmed)
		}
		e2.Poll()
		pump(e2)
		if e2.leader {
			grantedAt = clk.Monotonic()
		}
	}
	if lastSend < 1700*time.Millisecond || lastSend >= 1750*time.Millisecond {
		t.Errorf("last send of e1 at %v, want it to send until just before 1750ms", lastSend)
	}
	if grantedAt < 2000*time.Millisecond {
		t.Errorf("e2 got the lease at %v, want at 2000ms or later", grantedAt)
	}
	if lastSend >= grantedAt {
		t.Errorf("e1 sent at %v, after e2 got the lease at %v", lastSend, grantedAt)
	}
	if e2.epoch != 2 {
		t.Errorf("e2 has epoch %d", e2.epoch)
	}
	if want := []string{"leader epoch=1", "follower"}; !slices.Equal(e1.roles, want) {
		t.Errorf("roles of e1 = %v, want %v", e1.roles, want)
	}
}

func TestLeaseCountsFromTheSend(t *testing.T) {
	clk := clock.NewManual(t0)
	n := newTestNotifier("e1", clk)
	n.Poll()
	sent := n.tr.leases[0].SentAt
	grant := Grant{Epoch: 1, Holder: "e1", TTLMs: ttlMs}

	// A reply delayed by 1 s leaves 750 ms of lease.
	clk.Advance(time.Second)
	n.LeaseGranted(sent, grant)
	if !n.leader {
		t.Fatal("not leader after the grant")
	}
	clk.Advance(749 * time.Millisecond)
	n.Poll()
	if !n.leader {
		t.Fatal("lost the lease early")
	}
	clk.Advance(time.Millisecond)
	n.Poll()
	if n.leader {
		t.Fatal("still leader at send + ttl − margin")
	}

	// A reply delayed past its lease makes no leader.
	sent = clk.Monotonic()
	clk.Advance(1750 * time.Millisecond)
	n.LeaseGranted(sent, Grant{Epoch: 2, Holder: "e1", TTLMs: ttlMs})
	if n.leader {
		t.Fatal("leader from an expired grant")
	}
	if want := []string{"leader epoch=1", "follower"}; !slices.Equal(n.roles, want) {
		t.Fatalf("roles = %v, want %v", n.roles, want)
	}
}

func TestRenewsEvery500ms(t *testing.T) {
	clk := clock.NewManual(t0)
	n := newTestNotifier("e1", clk)
	for range 201 {
		n.Poll()
		clk.Advance(10 * time.Millisecond)
	}
	if got := len(n.tr.leases); got != 5 {
		t.Fatalf("%d requests in 2 s, want 5", got)
	}
	for i, r := range n.tr.leases {
		if want := time.Duration(i) * 500 * time.Millisecond; r.SentAt != want || r.Holder != "e1" {
			t.Errorf("request %d = %+v, want at %v", i, r, want)
		}
	}
}

func TestLateRepliesDoNotChangeTheLease(t *testing.T) {
	clk := clock.NewManual(t0)
	n := newTestNotifier("e1", clk)
	n.Poll()
	denied := clk.Monotonic()
	clk.Advance(RenewEvery)
	n.Poll()
	n.LeaseGranted(clk.Monotonic(), Grant{Epoch: 5, Holder: "e1", TTLMs: ttlMs})

	// The denial of an older request and a grant of an older epoch arrive now.
	n.LeaseDenied(denied, "e2")
	n.LeaseGranted(denied, Grant{Epoch: 4, Holder: "e1", TTLMs: ttlMs})
	if !n.leader || n.epoch != 5 {
		t.Fatalf("leader=%v epoch=%d", n.leader, n.epoch)
	}

	// A denial of a later request does end it.
	clk.Advance(RenewEvery)
	n.LeaseDenied(clk.Monotonic(), "e2")
	if n.leader {
		t.Fatal("still leader after a later denial")
	}
	if want := []string{"leader epoch=5", "follower"}; !slices.Equal(n.roles, want) {
		t.Fatalf("roles = %v, want %v", n.roles, want)
	}
}

func TestPromotionResendsWithTheSameKeys(t *testing.T) {
	clk := clock.NewManual(t0)
	transitions := []*rules.Transition{firing("r", 1000), firing("s", 2000), resolved("r", 1000, 3000)}
	e1 := newTestNotifier("e1", clk)
	e2 := newTestNotifier("e2", clk)
	for _, tr := range transitions {
		e1.Add("a", tr)
		e2.Add("a", tr)
	}
	if len(e2.tr.sent) != 0 {
		t.Fatal("a follower sent")
	}

	// e1 leads and gets to confirm only one key.
	e1.LeaseGranted(clk.Monotonic(), Grant{Epoch: 1, Holder: "e1", TTLMs: ttlMs})
	e1.Poll()
	if got, want := e1.tr.keys(), []string{keyOf(transitions[0]), keyOf(transitions[1]), keyOf(transitions[2])}; !slices.Equal(got, want) {
		t.Fatalf("e1 sent %v, want %v", got, want)
	}
	e1.Delivered(keyOf(transitions[0]), 1, ResultConfirmed)

	// e2 is promoted: it has confirmed nothing, so it resends everything.
	clk.Advance(2 * time.Second)
	e2.LeaseGranted(clk.Monotonic(), Grant{Epoch: 2, Holder: "e2", TTLMs: ttlMs})
	e2.Poll()
	if got, want := e2.tr.keys(), e1.tr.keys(); !slices.Equal(got, want) {
		t.Fatalf("e2 sent %v, want the keys of e1 %v", got, want)
	}
	for _, x := range e2.tr.sent {
		if x.Epoch != 2 || x.Evaluator != "e2" {
			t.Errorf("sent %+v", x)
		}
	}

	// e1 comes back with a new epoch and resends only what it did not confirm.
	for _, x := range e2.tr.sent {
		e2.Delivered(x.Key, 2, ResultConfirmed)
	}
	clk.Advance(2 * time.Second)
	e1.tr.sent = nil
	e1.LeaseGranted(clk.Monotonic(), Grant{Epoch: 3, Holder: "e1", TTLMs: ttlMs})
	e1.Poll()
	if got, want := e1.tr.keys(), []string{keyOf(transitions[1]), keyOf(transitions[2])}; !slices.Equal(got, want) {
		t.Fatalf("e1 resent %v, want %v", got, want)
	}
}

func TestPromotionResendsOnlyTheWindow(t *testing.T) {
	clk := clock.NewManual(t0)
	n := newTestNotifier("e1", clk)
	old := firing("old", 1000)
	edge := firing("edge", 1000+ResendWindowMs)
	other := firing("other", 1000)
	n.Add("a", old)
	n.Add("a", edge)
	n.Add("b", other)

	// Event time runs per tenant: b has not moved, so its key stays.
	n.Advance("a", 1000+ResendWindowMs)
	n.Advance("a", 1001+ResendWindowMs)
	n.LeaseGranted(clk.Monotonic(), Grant{Epoch: 1, Holder: "e1", TTLMs: ttlMs})
	n.Poll()
	if got, want := n.tr.keys(), []string{keyOf(edge), keyOf(other)}; !slices.Equal(got, want) {
		t.Fatalf("sent %v, want %v", got, want)
	}
	var out bytes.Buffer
	n.WriteMetrics(&out)
	if !strings.Contains(out.String(), `oteadero_notifications_total{status="expired"} 1`) {
		t.Fatalf("metrics:\n%s", out.String())
	}
}

func TestLeaderKeepsRetryingOutOfTheWindow(t *testing.T) {
	clk := clock.NewManual(t0)
	n := newTestNotifier("e1", clk)
	n.LeaseGranted(clk.Monotonic(), Grant{Epoch: 1, Holder: "e1", TTLMs: ttlMs})
	tr := firing("r", 1000)
	n.Add("a", tr)
	n.Poll()
	n.Delivered(keyOf(tr), 1, ResultFailed)
	n.Advance("a", 2000+ResendWindowMs)
	clk.Advance(BackoffMin)
	n.tr.sent = nil
	n.Poll()
	if got := n.tr.keys(); !slices.Equal(got, []string{keyOf(tr)}) {
		t.Fatalf("sent %v", got)
	}
}

func TestRetriesBackOffWhileTheLeaseLasts(t *testing.T) {
	clk := clock.NewManual(t0)
	n := newTestNotifier("e1", clk)
	tr := firing("r", 1000)
	key := keyOf(tr)
	n.Add("a", tr)

	var attempts []time.Duration
	grant := func() {
		n.LeaseGranted(clk.Monotonic(), Grant{Epoch: 1, Holder: "e1", TTLMs: ttlMs})
	}
	grant()
	for range 1000 {
		before := len(n.tr.sent)
		n.Poll()
		if len(n.tr.sent) > before {
			attempts = append(attempts, clk.Monotonic())
			n.Delivered(key, 1, ResultFailed)
		}
		clk.Advance(10 * time.Millisecond)
		grant()
	}
	var gaps []time.Duration
	for i := 1; i < len(attempts) && i <= 7; i++ {
		gaps = append(gaps, attempts[i]-attempts[i-1])
	}
	want := []time.Duration{100, 200, 400, 800, 1600, 2000, 2000}
	for i := range want {
		want[i] *= time.Millisecond
	}
	if !slices.Equal(gaps, want) {
		t.Fatalf("gaps = %v, want %v", gaps, want)
	}

	// Once confirmed it is over.
	n.Delivered(key, 1, ResultConfirmed)
	sent := len(n.tr.sent)
	clk.Advance(time.Minute)
	grant()
	n.Poll()
	if len(n.tr.sent) != sent {
		t.Fatal("sent a confirmed key")
	}
}

func TestLostReplyCountsAsAFailure(t *testing.T) {
	clk := clock.NewManual(t0)
	n := newTestNotifier("e1", clk)
	n.LeaseGranted(clk.Monotonic(), Grant{Epoch: 1, Holder: "e1", TTLMs: 60_000})
	n.Add("a", firing("r", 1000))
	n.Poll()
	clk.Advance(replyTimeout - time.Millisecond)
	n.Poll()
	if len(n.tr.sent) != 1 {
		t.Fatalf("%d attempts before the timeout", len(n.tr.sent))
	}
	clk.Advance(time.Millisecond)
	n.Poll()
	clk.Advance(BackoffMin)
	n.Poll()
	if len(n.tr.sent) != 2 {
		t.Fatalf("%d attempts after the timeout", len(n.tr.sent))
	}
}

func TestStaleAnswerFencesTheEpoch(t *testing.T) {
	clk := clock.NewManual(t0)
	n := newTestNotifier("e1", clk)
	n.LeaseGranted(clk.Monotonic(), Grant{Epoch: 3, Holder: "e1", TTLMs: ttlMs})
	tr := firing("r", 1000)
	n.Add("a", tr)
	n.Add("a", firing("s", 1000))
	n.Poll()
	n.Delivered(keyOf(tr), 3, ResultStale)
	if n.leader {
		t.Fatal("still leader after the receiver saw a higher epoch")
	}

	// A renewal of the fenced epoch changes nothing; a higher one does.
	n.tr.sent = nil
	n.LeaseGranted(clk.Monotonic(), Grant{Epoch: 3, Holder: "e1", TTLMs: ttlMs})
	n.Poll()
	if n.leader || len(n.tr.sent) != 0 {
		t.Fatalf("leader=%v sent=%d with a fenced epoch", n.leader, len(n.tr.sent))
	}
	n.LeaseGranted(clk.Monotonic(), Grant{Epoch: 4, Holder: "e1", TTLMs: ttlMs})
	n.Poll()
	if !n.leader || len(n.tr.sent) != 2 || n.tr.sent[0].Epoch != 4 {
		t.Fatalf("leader=%v sent=%v", n.leader, n.tr.keys())
	}
}

func TestInflightIsBounded(t *testing.T) {
	clk := clock.NewManual(t0)
	n := newTestNotifier("e1", clk)
	n.LeaseGranted(clk.Monotonic(), Grant{Epoch: 1, Holder: "e1", TTLMs: ttlMs})
	for i := range 2 * maxInflight {
		n.Add("a", firing("r", int64(i)))
	}
	n.Poll()
	if len(n.tr.sent) != maxInflight {
		t.Fatalf("%d in flight", len(n.tr.sent))
	}
	n.Delivered(n.tr.sent[0].Key, 1, ResultConfirmed)
	n.Poll()
	if len(n.tr.sent) != maxInflight+1 {
		t.Fatalf("%d sent after one confirmation", len(n.tr.sent))
	}
}

func TestAddKeepsOnlyIncidentsOnce(t *testing.T) {
	n := newTestNotifier("e1", clock.NewManual(t0))
	n.Add("a", &rules.Transition{Rule: "r", Group: "{}", T: 1, State: rules.StatePending})
	n.Add("a", &rules.Transition{Rule: "r", Group: "{}", T: 2, State: rules.StateInactive})
	tr := firing("r", 3)
	n.Add("a", tr)
	n.Delivered(keyOf(tr), 1, ResultConfirmed)
	n.Add("a", tr)
	if len(n.order) != 1 || !n.order[0].confirmed {
		t.Fatalf("outbox = %+v", n.order)
	}
}

func TestOutboxStateRoundTrip(t *testing.T) {
	clk := clock.NewManual(t0)
	n := newTestNotifier("e1", clk)
	a, b := firing("r", 1000), resolved("r", 1000, 2000)
	n.Add("a", a)
	n.Add("b", b)
	n.Advance("a", 1000)
	n.Advance("b", -5)
	n.Delivered(keyOf(a), 1, ResultConfirmed)
	state := n.AppendState(nil)

	m := newTestNotifier("e1", clk)
	if err := m.LoadState(state); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m.AppendState(nil), state) {
		t.Fatal("re-encoding changed the bytes")
	}
	m.LeaseGranted(clk.Monotonic(), Grant{Epoch: 1, Holder: "e1", TTLMs: ttlMs})
	m.Poll()
	got := m.tr.sent
	if len(got) != 1 || got[0].Key != keyOf(b) || got[0].Present || got[0].T != 2000 {
		t.Fatalf("sent %+v", got)
	}

	for i := range state {
		before := m.AppendState(nil)
		if err := m.LoadState(state[:i]); err == nil {
			t.Fatalf("truncated to %d bytes: no error", i)
		}
		if !bytes.Equal(m.AppendState(nil), before) {
			t.Fatalf("truncated to %d bytes: the outbox changed", i)
		}
	}
}

func TestNotificationBody(t *testing.T) {
	x := &Notification{Incident: "abc", Kind: "firing", Rule: "r", Group: `{a="1"}`, T: 5000, Value: 2.5, Present: true, Epoch: 3, Evaluator: "e1"}
	want := `{"incident":"abc","kind":"firing","rule":"r","group":"{a=\"1\"}","t":5000,"value":2.5,"epoch":3,"evaluator":"e1"}`
	if got := string(x.Body()); got != want {
		t.Errorf("body = %s, want %s", got, want)
	}
	for _, y := range []Notification{{Value: 1}, {Value: math.NaN(), Present: true}, {Value: math.Inf(1), Present: true}} {
		if got := string(y.Body()); !strings.Contains(got, `"value":null`) {
			t.Errorf("body of %+v = %s", y, got)
		}
	}
}
