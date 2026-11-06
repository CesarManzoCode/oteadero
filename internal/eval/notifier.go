package eval

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"sync/atomic"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/clock"
	"github.com/CesarManzoCode/oteadero/internal/rules"
)

// Timing of D09 and D10.
const (
	// RenewEvery is how often the lease is requested.
	RenewEvery = 500 * time.Millisecond
	// LeaseMargin is what a holder takes off its lease, for the drift between
	// the local clocks.
	LeaseMargin = 250 * time.Millisecond
	// BackoffMin and BackoffMax bound the wait between attempts of a key.
	BackoffMin = 100 * time.Millisecond
	BackoffMax = 2 * time.Second
	// ResendWindowMs is how far back in event time a promotion resends.
	ResendWindowMs = 10 * 60 * 1000

	// replyTimeout is when an attempt with no reply counts as failed, for a
	// transport that can lose replies. HTTP gives up before it.
	replyTimeout = 3 * time.Second
	// maxInflight bounds the attempts without a reply, so that a promotion
	// with a long outbox does not open a connection per key.
	maxInflight = 32
)

// LeaseRequest is a lease request and when it was sent, which the reply brings
// back: the lease counts from there.
type LeaseRequest struct {
	Holder string
	SentAt time.Duration
}

// Grant is the answer to a lease request that got the lease.
type Grant struct {
	Epoch  uint64
	Holder string
	TTLMs  int64
}

// Notification is one attempt to deliver a key (§4.7).
type Notification struct {
	Key       string
	Incident  string
	Kind      string
	Rule      string
	Group     string
	T         int64
	Value     float64
	Present   bool
	Epoch     uint64
	Evaluator string
}

// Body returns the body of §4.7. The value of an absent group is null, and so is
// a NaN or an infinity, which JSON cannot hold.
func (x *Notification) Body() []byte {
	var value *float64
	if x.Present && !math.IsNaN(x.Value) && !math.IsInf(x.Value, 0) {
		value = &x.Value
	}
	// A struct of strings and finite numbers always marshals.
	body, _ := json.Marshal(struct {
		Incident  string   `json:"incident"`
		Kind      string   `json:"kind"`
		Rule      string   `json:"rule"`
		Group     string   `json:"group"`
		T         int64    `json:"t"`
		Value     *float64 `json:"value"`
		Epoch     uint64   `json:"epoch"`
		Evaluator string   `json:"evaluator"`
	}{x.Incident, x.Kind, x.Rule, x.Group, x.T, value, x.Epoch, x.Evaluator})
	return body
}

// Result is how a delivery attempt ended.
type Result uint8

const (
	// ResultConfirmed: the receiver has the key (accepted or duplicate).
	ResultConfirmed Result = iota
	// ResultStale: the receiver has seen a higher epoch than the attempt's.
	ResultStale
	// ResultFailed: no answer that says anything about the key.
	ResultFailed
)

// Transport is the network as the Notifier sees it. Both calls start a request
// and return at once, without calling back into the Notifier; the reply, if one
// ever comes, arrives through LeaseGranted, LeaseDenied or Delivered, on the
// goroutine that owns the Notifier. A simulation can delay, reorder or lose it.
type Transport interface {
	RequestLease(r LeaseRequest)
	Send(x *Notification)
}

// status labels oteadero_notifications_total. Expired counts the keys that left
// the window unconfirmed by this evaluator, which on a follower is the normal
// end of a key the leader delivered.
type status uint8

const (
	statusConfirmed status = iota
	statusStale
	statusError
	statusExpired
	numStatus
)

var statusNames = [numStatus]string{"confirmed", "stale", "error", "expired"}

// entry is a notification of the outbox. Only what is above the blank line goes
// in a checkpoint: a recovered evaluator tries again from the start.
type entry struct {
	tenant    string
	incident  rules.Incident
	kind      string
	key       string
	t         int64
	value     float64
	present   bool
	confirmed bool

	inflight  bool
	sentEpoch uint64
	nextAt    time.Duration
	backoff   time.Duration
}

// Notifier is the leadership of D09 and the outbox of D10 as one synchronous
// state machine. All its methods belong to one goroutine, except WriteMetrics;
// Poll does the work that is due by the injected clock.
//
// Every replica feeds the same transitions into its outbox, and only the leader
// sends. Being leader is a local decision made from the monotonic clock: the
// lease of a grant lasts until SentAt + ttl − margin, so the holder stops before
// the coordinator, which counted the ttl from a later instant, can give the
// lease to anyone else.
type Notifier struct {
	id     string
	clk    clock.Clock
	tr     Transport
	onRole func(leader bool, epoch uint64)

	nextLease time.Duration
	epoch     uint64
	until     time.Duration
	grantSent time.Duration
	// fenced is the highest epoch the receiver has rejected.
	fenced      uint64
	leader      bool
	leaderEpoch uint64

	entries  map[string]*entry
	order    []*entry
	heads    map[string]int64
	inflight int
	counts   [numStatus]atomic.Uint64
}

// NewNotifier returns a follower with an empty outbox. onRole, which may be nil,
// runs at each change of role and of the epoch led.
func NewNotifier(id string, clk clock.Clock, tr Transport, onRole func(leader bool, epoch uint64)) *Notifier {
	if onRole == nil {
		onRole = func(bool, uint64) {}
	}
	return &Notifier{id: id, clk: clk, tr: tr, onRole: onRole, entries: map[string]*entry{}, heads: map[string]int64{}}
}

// Poll requests the lease when due and, if the lease is still good, sends what
// is due. It reads the clock first so that a process that was paused for longer
// than its lease sends nothing when it wakes up.
func (n *Notifier) Poll() {
	now := n.clk.Monotonic()
	n.syncRole(now)
	if now >= n.nextLease {
		n.nextLease = now + RenewEvery
		n.tr.RequestLease(LeaseRequest{Holder: n.id, SentAt: now})
	}
	if n.leader {
		n.sendDue(now)
	}
}

// LeaseGranted takes the reply to the request sent at sentAt. Replies can come
// late and out of order: one of an older epoch says nothing, and one that was
// delayed past its lease does not make a leader.
func (n *Notifier) LeaseGranted(sentAt time.Duration, g Grant) {
	if g.Holder != n.id || g.Epoch < n.epoch {
		return
	}
	until := sentAt + time.Duration(g.TTLMs)*time.Millisecond - LeaseMargin
	if g.Epoch > n.epoch {
		n.epoch = g.Epoch
		n.until = until
		n.grantSent = sentAt
	} else {
		n.until = max(n.until, until)
		n.grantSent = max(n.grantSent, sentAt)
	}
	n.syncRole(n.clk.Monotonic())
}

// LeaseDenied takes the reply of a request that found another holder. It ends
// the lease unless a later request has been granted since.
func (n *Notifier) LeaseDenied(sentAt time.Duration, _ string) {
	if sentAt <= n.grantSent {
		return
	}
	n.until = 0
	n.syncRole(n.clk.Monotonic())
}

// Delivered takes the end of an attempt sent with epoch. A failure that belongs
// to an attempt already given up on changes nothing.
func (n *Notifier) Delivered(key string, epoch uint64, res Result) {
	now := n.clk.Monotonic()
	e := n.entries[key]
	switch res {
	case ResultConfirmed:
		n.counts[statusConfirmed].Add(1)
		if e != nil {
			e.confirmed = true
			n.settle(e)
		}
	case ResultStale:
		n.counts[statusStale].Add(1)
		if e != nil && e.sentEpoch == epoch {
			n.settle(e)
		}
		n.fenced = max(n.fenced, epoch)
		n.syncRole(now)
	default:
		if e != nil && e.inflight && e.sentEpoch == epoch {
			n.fail(e, now)
		}
	}
}

// Add puts the notification of a firing or resolved transition in the outbox,
// unless it is there already, as it is when a replay crosses it again.
func (n *Notifier) Add(tenant string, tr *rules.Transition) {
	if tr.State != rules.StateFiring && tr.State != rules.StateResolved {
		return
	}
	key := tr.Incident.IdempotencyKey(tr.State)
	if _, ok := n.entries[key]; ok {
		return
	}
	e := &entry{
		tenant:   tenant,
		incident: tr.Incident,
		kind:     tr.State,
		key:      key,
		t:        tr.T,
		value:    tr.Value,
		present:  tr.Present,
		backoff:  BackoffMin,
	}
	n.entries[key] = e
	n.order = append(n.order, e)
}

// Advance moves the event time of tenant and drops what falls out of the window.
// The leader keeps retrying what it has not confirmed (D10): only losing the
// lease ends that.
func (n *Notifier) Advance(tenant string, upTo int64) {
	if head, ok := n.heads[tenant]; ok && head >= upTo {
		return
	}
	n.heads[tenant] = upTo
	n.prune(n.leader)
}

func (n *Notifier) prune(keepUnconfirmed bool) {
	n.order = slices.DeleteFunc(n.order, func(e *entry) bool {
		head, ok := n.heads[e.tenant]
		if !ok || e.t >= head-ResendWindowMs || (keepUnconfirmed && !e.confirmed) {
			return false
		}
		n.settle(e)
		delete(n.entries, e.key)
		if !e.confirmed {
			n.counts[statusExpired].Add(1)
		}
		return true
	})
}

// syncRole applies what the clock and the replies say about the lease. A rise of
// the epoch while leading is a promotion too: another evaluator may have led
// between the two grants.
func (n *Notifier) syncRole(now time.Duration) {
	leading := n.epoch > n.fenced && now < n.until
	switch {
	case leading && (!n.leader || n.epoch != n.leaderEpoch):
		n.leader = true
		n.leaderEpoch = n.epoch
		n.promote(now)
		n.onRole(true, n.epoch)
	case !leading && n.leader:
		n.leader = false
		n.onRole(false, n.epoch)
	}
}

// promote schedules what this evaluator has not confirmed in the window, at once
// and with the keys it always had.
func (n *Notifier) promote(now time.Duration) {
	n.prune(false)
	n.inflight = 0
	for _, e := range n.order {
		e.inflight = false
		e.nextAt = now
		e.backoff = BackoffMin
	}
}

func (n *Notifier) sendDue(now time.Duration) {
	for _, e := range n.order {
		if e.confirmed {
			continue
		}
		if e.inflight {
			if now < e.nextAt {
				continue
			}
			n.fail(e, now)
		}
		if now < e.nextAt || n.inflight >= maxInflight {
			continue
		}
		e.inflight = true
		e.sentEpoch = n.epoch
		e.nextAt = now + replyTimeout
		n.inflight++
		n.tr.Send(&Notification{
			Key:       e.key,
			Incident:  e.incident.ID(),
			Kind:      e.kind,
			Rule:      e.incident.Rule,
			Group:     e.incident.Group,
			T:         e.t,
			Value:     e.value,
			Present:   e.present,
			Epoch:     n.epoch,
			Evaluator: n.id,
		})
	}
}

// fail counts a failed attempt and schedules the next one.
func (n *Notifier) fail(e *entry, now time.Duration) {
	n.counts[statusError].Add(1)
	n.settle(e)
	e.nextAt = now + e.backoff
	e.backoff = min(2*e.backoff, BackoffMax)
}

// settle marks the attempt of e as over.
func (n *Notifier) settle(e *entry) {
	if e.inflight {
		e.inflight = false
		n.inflight--
	}
}

// WriteMetrics writes oteadero_notifications_total. The counters start at zero
// with each process. Safe for concurrent use.
func (n *Notifier) WriteMetrics(w io.Writer) {
	fmt.Fprintln(w, "# TYPE oteadero_notifications_total counter")
	for s := range numStatus {
		fmt.Fprintf(w, "oteadero_notifications_total{status=%q} %d\n", statusNames[s], n.counts[s].Load())
	}
}

// ResetOutbox empties the outbox, for a replay from the start of the log.
func (n *Notifier) ResetOutbox() {
	n.entries = map[string]*entry{}
	n.order = nil
	n.heads = map[string]int64{}
	n.inflight = 0
}

// AppendState appends the outbox for a checkpoint. Equal outboxes have equal
// bytes.
func (n *Notifier) AppendState(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(n.heads)))
	for _, tenant := range slices.Sorted(maps.Keys(n.heads)) {
		dst = appendString(dst, tenant)
		dst = binary.AppendVarint(dst, n.heads[tenant])
	}
	dst = binary.AppendUvarint(dst, uint64(len(n.order)))
	for _, e := range n.order {
		dst = appendString(dst, e.tenant)
		dst = appendString(dst, e.incident.Rule)
		dst = appendString(dst, e.incident.Group)
		dst = binary.AppendVarint(dst, e.incident.FiredAt)
		dst = appendString(dst, e.kind)
		dst = binary.AppendVarint(dst, e.t)
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(e.value))
		dst = appendBool(dst, e.present)
		dst = appendBool(dst, e.confirmed)
	}
	return dst
}

// LoadState replaces the outbox with what AppendState wrote; the lease is not
// part of it. On error the outbox stays as it was.
func (n *Notifier) LoadState(state []byte) error {
	r := stateReader{p: state}
	heads := map[string]int64{}
	for range r.count(2) {
		tenant := r.string()
		heads[tenant] = r.varint()
	}
	entries := map[string]*entry{}
	var order []*entry
	for range r.count(8) {
		e := &entry{backoff: BackoffMin}
		e.tenant = r.string()
		e.incident = rules.Incident{Rule: r.string(), Group: r.string(), FiredAt: r.varint()}
		e.kind = r.string()
		e.t = r.varint()
		e.value = math.Float64frombits(r.u64())
		e.present = r.bool()
		e.confirmed = r.bool()
		if r.bad {
			break
		}
		e.key = e.incident.IdempotencyKey(e.kind)
		entries[e.key] = e
		order = append(order, e)
	}
	if r.bad || len(r.p) != 0 {
		return errBadState
	}
	n.heads, n.entries, n.order, n.inflight = heads, entries, order, 0
	return nil
}

var errBadState = errors.New("eval: bad evaluator state")

func appendString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

func appendBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, 1)
	}
	return append(dst, 0)
}

// stateReader consumes a state; after the first short read it sets bad and
// returns zero values.
type stateReader struct {
	p   []byte
	bad bool
}

func (r *stateReader) uvarint() uint64 {
	v, n := binary.Uvarint(r.p)
	if r.bad || n <= 0 {
		r.bad = true
		return 0
	}
	r.p = r.p[n:]
	return v
}

func (r *stateReader) varint() int64 {
	v, n := binary.Varint(r.p)
	if r.bad || n <= 0 {
		r.bad = true
		return 0
	}
	r.p = r.p[n:]
	return v
}

func (r *stateReader) u64() uint64 {
	if r.bad || len(r.p) < 8 {
		r.bad = true
		return 0
	}
	v := binary.LittleEndian.Uint64(r.p)
	r.p = r.p[8:]
	return v
}

func (r *stateReader) u32() uint32 {
	if r.bad || len(r.p) < 4 {
		r.bad = true
		return 0
	}
	v := binary.LittleEndian.Uint32(r.p)
	r.p = r.p[4:]
	return v
}

func (r *stateReader) bool() bool {
	if r.bad || len(r.p) < 1 || r.p[0] > 1 {
		r.bad = true
		return false
	}
	v := r.p[0] == 1
	r.p = r.p[1:]
	return v
}

func (r *stateReader) bytes() []byte {
	n := r.uvarint()
	if r.bad || n > uint64(len(r.p)) {
		r.bad = true
		return nil
	}
	v := r.p[:n]
	r.p = r.p[n:]
	return v
}

func (r *stateReader) string() string { return string(r.bytes()) }

// count reads a number of entries of at least minBytes each, so that a damaged
// count cannot run for longer than the state lasts.
func (r *stateReader) count(minBytes uint64) int {
	n := r.uvarint()
	if r.bad || n > uint64(len(r.p))/minBytes {
		r.bad = true
		return 0
	}
	return int(n)
}
