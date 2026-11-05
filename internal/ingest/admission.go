package ingest

import (
	"cmp"
	"slices"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/event"
)

const (
	// nanoEvent is one event in token units: a rate in events per second
	// times nanoseconds gives tokens with no rounding.
	nanoEvent = int64(time.Second)
	// quantumEvents is the DRR quantum of a weight-1 tenant, in events.
	quantumEvents = 10_000
	// maxUnknownTenants bounds the distinct unknown tenant ids counted by
	// name; the rest are counted together under otherTenant.
	maxUnknownTenants = 1024
	otherTenant       = "_other"
)

// Stats are the counters of one tenant. Received = Accepted + the sum of
// Dropped at every instant Stats can observe.
type Stats struct {
	Tenant   string
	Received uint64
	// Accepted counts admitted events. An admitted batch is always written
	// and synced before its ack; a log failure stops the sequencer.
	Accepted uint64
	Dropped  [NumCauses]uint64
	// Late counts committed events classified late (§4.4); they are also
	// accepted.
	Late uint64
	// QueuedEvents is the current depth of the tenant queue.
	QueuedEvents int64
}

// bucket is a token bucket in nano-events driven by the monotonic clock.
type bucket struct {
	rate   int64 // events per second
	cap    int64
	tokens int64
	last   time.Duration
}

func newBucket(rate, burst int64, now time.Duration) bucket {
	return bucket{rate: rate, cap: burst * nanoEvent, tokens: burst * nanoEvent, last: now}
}

func (b *bucket) refill(now time.Duration) {
	if now <= b.last {
		return
	}
	el := int64(now - b.last)
	b.last = now
	if b.rate == 0 {
		return
	}
	room := b.cap - b.tokens
	if el > room/b.rate {
		b.tokens = b.cap
		return
	}
	b.tokens += b.rate * el
}

func (b *bucket) has(n int64) bool {
	return n <= b.cap/nanoEvent && n*nanoEvent <= b.tokens
}

func (b *bucket) take(n int64) { b.tokens -= n * nanoEvent }

// entry is an admitted batch waiting for the sequencer.
type entry struct {
	t   *tenant
	b   *event.Batch
	n   int64
	ack func(Ack)
}

type tenant struct {
	cfg    TenantConfig
	bucket bucket
	queue  []*entry
	queued int64
	// DRR state: deficit in events and whether its turn is in progress.
	deficit int64
	inTurn  bool
	stats   Stats
}

// validTimes reports whether every event time of b is non-negative and at
// most MaxFutureMs ahead of nowMs (§4.1).
func validTimes(b *event.Batch, nowMs int64) bool {
	limit := nowMs + MaxFutureMs
	if b.Kind == event.KindLogs {
		for i := range b.Logs {
			if ts := b.Logs[i].TsMs; ts < 0 || ts > limit {
				return false
			}
		}
		return true
	}
	for i := range b.Metrics {
		if ts := b.Metrics[i].TsMs; ts < 0 || ts > limit {
			return false
		}
	}
	return true
}

// Submit offers a parsed batch. When it is admitted Submit returns ok and
// ack is called exactly once, from the sequencer, after the batch is synced
// to the log; ack must not block. Otherwise the whole batch is dropped,
// counted, ack is never called and the cause is returned. Submit sets
// b.IngestMs and takes ownership of b. Safe for concurrent use.
func (s *Sequencer) Submit(b *event.Batch, ack func(Ack)) (cause Cause, ok bool) {
	n := int64(b.Len())
	nowMs := s.clk.NowMs()
	mono := s.clk.Monotonic()
	valid := validTimes(b, nowMs)

	s.mu.Lock()
	t := s.tenants[b.Tenant]
	if t == nil {
		st := s.unknownStats(b.Tenant)
		st.Received += uint64(n)
		st.Dropped[CauseUnknownTenant] += uint64(n)
		s.mu.Unlock()
		return CauseUnknownTenant, false
	}
	t.stats.Received += uint64(n)
	switch {
	case s.closing:
		cause = CauseShuttingDown
	case !valid:
		cause = CauseInvalid
	default:
		t.bucket.refill(mono)
		switch {
		case !t.bucket.has(n):
			cause = CauseQuota
		case t.queued+n > t.cfg.QueueEvents:
			cause = CauseQueueFull
		default:
			ok = true
		}
	}
	if !ok {
		t.stats.Dropped[cause] += uint64(n)
		s.mu.Unlock()
		return cause, false
	}
	t.bucket.take(n)
	t.stats.Accepted += uint64(n)
	b.IngestMs = nowMs
	if len(t.queue) == 0 {
		s.active = append(s.active, t)
	}
	t.queue = append(t.queue, &entry{t: t, b: b, n: n, ack: ack})
	t.queued += n
	s.mu.Unlock()
	s.notify()
	return 0, true
}

// CountDrop counts n events of tenant dropped before Submit, such as a
// body that is too large or does not parse. Safe for concurrent use.
func (s *Sequencer) CountDrop(tenantID string, n int, cause Cause) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.unknownStats(tenantID)
	if t := s.tenants[tenantID]; t != nil {
		st = &t.stats
	}
	st.Received += uint64(n)
	st.Dropped[cause] += uint64(n)
}

// unknownStats returns the counters of an unknown tenant id. Called with
// s.mu held.
func (s *Sequencer) unknownStats(id string) *Stats {
	if st := s.unknown[id]; st != nil {
		return st
	}
	if len(s.unknown) >= maxUnknownTenants {
		id = otherTenant
		if st := s.unknown[id]; st != nil {
			return st
		}
	}
	st := &Stats{Tenant: id}
	s.unknown[id] = st
	return st
}

// Stats returns the counters of the configured tenants, in configuration
// order, followed by those of unknown tenant ids, sorted by id. Safe for
// concurrent use.
func (s *Sequencer) Stats() []Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Stats, 0, len(s.ordered)+len(s.unknown))
	for _, t := range s.ordered {
		st := t.stats
		st.QueuedEvents = t.queued
		out = append(out, st)
	}
	start := len(out)
	for _, st := range s.unknown {
		out = append(out, *st)
	}
	slices.SortFunc(out[start:], func(a, b Stats) int { return cmp.Compare(a.Tenant, b.Tenant) })
	return out
}

// pick removes the next batch in weighted deficit round robin order, or
// returns nil when every queue is empty. A tenant's turn adds
// weight·quantumEvents to its deficit and serves head batches while they
// fit; an emptied queue leaves the round with its deficit reset.
func (s *Sequencer) pick() *entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.active) > 0 {
		if s.cur >= len(s.active) {
			s.cur = 0
		}
		t := s.active[s.cur]
		if !t.inTurn {
			t.inTurn = true
			t.deficit += t.cfg.Weight * quantumEvents
		}
		e := t.queue[0]
		if e.n <= t.deficit {
			t.deficit -= e.n
			t.queue[0] = nil
			t.queue = t.queue[1:]
			t.queued -= e.n
			if len(t.queue) == 0 {
				t.queue = nil
				t.deficit = 0
				t.inTurn = false
				s.active = slices.Delete(s.active, s.cur, s.cur+1)
			}
			return e
		}
		t.inTurn = false
		s.cur++
	}
	return nil
}
