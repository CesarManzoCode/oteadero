package ingest

import (
	"sync"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/clock"
	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

// tickInterval paces TICK records while some tenant is idle (§4.4).
const tickInterval = time.Second

// Ack is what an admitted batch's producer receives once the batch is
// durable, or once the log has failed (Err set, durability unknown).
type Ack struct {
	FirstOffset uint64
	Events      int
	Err         error
}

// CommittedBatch is a batch that is durable in the log, with the offset of
// its first event and the §4.4 classification of each event.
type CommittedBatch struct {
	Batch       *event.Batch
	FirstOffset uint64
	// Late[i] reports event i late; Watermark[i] is the tenant watermark
	// right after event i was processed.
	Late      []bool
	Watermark []int64
	LateCount int
}

// WatermarkAdvance is a tenant watermark raised by a TICK.
type WatermarkAdvance struct {
	Tenant    string
	Watermark int64
}

// TickEffect is a durable TICK record and the watermarks it raised.
type TickEffect struct {
	TsMs int64
	// NextOffset is the offset of the next event after the TICK; the last
	// event before it is NextOffset − 1.
	NextOffset uint64
	Advances   []WatermarkAdvance
}

// Sink receives what the sequencer commits, in log order, after the fsync
// and after the producers were acked. It is called from the goroutine that
// calls Poll and may keep what it receives.
type Sink interface {
	Batch(c *CommittedBatch)
	Tick(t *TickEffect)
}

type delivery struct {
	e     *entry
	stats *Stats
	batch *CommittedBatch
	tick  *TickEffect
	lease *leaseEntry
}

// Sequencer is the single writer of the log (D03). It is a synchronous
// state machine: Submit, CountDrop, Stats and Shutdown are safe for
// concurrent use, while Poll, NextWake, Drained and Run belong to one
// goroutine, the sequencer's own in production or the simulation's single
// thread. Time comes only from the injected clock and disk only from the
// log.
type Sequencer struct {
	clk         clock.Clock
	log         *wal.Log
	sink        Sink
	maxBytes    int
	maxDelay    time.Duration
	idleGraceMs int64
	wake        chan struct{}

	mu      sync.Mutex
	tenants map[string]*tenant
	ordered []*tenant
	unknown map[string]*Stats
	active  []*tenant // DRR round: tenants with a non-empty queue
	cur     int
	closing bool
	leases  []*leaseEntry
	// delivered is each tenant's watermark as of what reached the sink.
	delivered map[string]int64

	// Owned by the Poll goroutine.
	wm            *watermarks
	buf           []byte
	pending       []delivery
	unsynced      bool
	firstUnsynced time.Duration
	nextTick      time.Duration
	err           error
	ckpt          Checkpointer
	ckptEvery     time.Duration
	ckptEvents    uint64
	ckptAt        time.Duration
	ckptNext      uint64
	lease         wal.Lease
}

// New returns a sequencer that appends to log, which must be open and
// owned by it from now on.
func New(cfg Config, clk clock.Clock, log *wal.Log, sink Sink) (*Sequencer, error) {
	cfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	s := &Sequencer{
		clk:         clk,
		log:         log,
		sink:        sink,
		maxBytes:    cfg.FsyncMaxBytes,
		maxDelay:    cfg.FsyncMaxDelay,
		idleGraceMs: cfg.IdleGraceMs,
		wake:        make(chan struct{}, 1),
		tenants:     make(map[string]*tenant, len(cfg.Tenants)),
		unknown:     map[string]*Stats{},
		delivered:   map[string]int64{},
		wm:          newWatermarks(cfg.BucketMs, cfg.LatenessMs, cfg.IdleGraceMs),
	}
	now := clk.Monotonic()
	for _, tc := range cfg.Tenants {
		t := &tenant{cfg: tc, bucket: newBucket(tc.QuotaEPS, tc.Burst, now), stats: Stats{Tenant: tc.ID}}
		s.tenants[tc.ID] = t
		s.ordered = append(s.ordered, t)
	}
	return s, nil
}

func (s *Sequencer) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Sequencer) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

// Shutdown stops admission: later batches are dropped as shutting_down,
// while the admitted ones are still written, synced and acked without
// waiting for the group commit delay. Safe for concurrent use.
func (s *Sequencer) Shutdown() {
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	s.notify()
}

// Drained reports that the sequencer is shut down and has nothing left to
// write or ack.
func (s *Sequencer) Drained() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing && len(s.active) == 0 && len(s.leases) == 0 && len(s.pending) == 0
}

// Poll does the work that is due now: it appends queued batches in DRR
// order, commits when FsyncMaxBytes are pending (at most one such commit
// per call, NextWake reports the rest), writes a TICK when due and commits
// when FsyncMaxDelay has passed since the first unsynced record, or at
// once while shutting down. After a log error every call returns it.
func (s *Sequencer) Poll() error {
	if s.err != nil {
		return s.err
	}
	if err := s.writeLeases(); err != nil {
		return s.fail(err)
	}
	for {
		e := s.pick()
		if e == nil {
			break
		}
		if err := s.write(e); err != nil {
			return s.fail(err)
		}
		if s.log.Pending() >= s.maxBytes {
			if err := s.commit(); err != nil {
				return s.fail(err)
			}
			break
		}
	}
	if err := s.maybeTick(); err != nil {
		return s.fail(err)
	}
	if len(s.pending) > 0 && (s.isClosing() || s.clk.Monotonic()-s.firstUnsynced >= s.maxDelay) {
		if err := s.commit(); err != nil {
			return s.fail(err)
		}
	}
	if s.checkpointDue() {
		return s.Barrier()
	}
	return nil
}

func (s *Sequencer) markUnsynced() {
	if !s.unsynced {
		s.unsynced = true
		s.firstUnsynced = s.clk.Monotonic()
	}
}

// write appends e as a BATCH record and classifies its events.
func (s *Sequencer) write(e *entry) error {
	s.buf = event.AppendBatch(s.buf[:0], e.b)
	first, err := s.log.AppendBatch(s.buf, uint64(e.n))
	if err != nil {
		return err
	}
	s.markUnsynced()
	s.pending = append(s.pending, delivery{e: e, stats: &e.t.stats, batch: s.classify(e.b, first)})
	return nil
}

// classify applies the §4.4 rules to b, whose first event has offset first.
// Live writes and recovery replay both go through it.
func (s *Sequencer) classify(b *event.Batch, first uint64) *CommittedBatch {
	return s.wm.classify(b, first)
}

// applyTick applies a TICK at server time tsMs, followed by the event with
// offset next. Live writes and recovery replay both go through it.
func (s *Sequencer) applyTick(tsMs int64, next uint64) *TickEffect {
	return s.wm.applyTick(tsMs, next)
}

// maybeTick appends a TICK when some tenant with a defined watermark has
// maxTs < now − G and the previous TICK is at least tickInterval old.
func (s *Sequencer) maybeTick() error {
	if s.idleGraceMs == 0 || s.isClosing() {
		return nil
	}
	now := s.clk.Monotonic()
	if now < s.nextTick {
		return nil
	}
	nowMs := s.clk.NowMs()
	lo, ok := s.wm.minMaxTs()
	if !ok || lo >= nowMs-s.idleGraceMs {
		return nil
	}
	if err := s.log.AppendTick(nowMs); err != nil {
		return err
	}
	s.nextTick = now + tickInterval
	s.markUnsynced()
	s.pending = append(s.pending, delivery{tick: s.applyTick(nowMs, s.log.NextOffset())})
	return nil
}

// commit syncs the log, then acks the producers and then hands everything
// pending to the sink, in log order.
func (s *Sequencer) commit() error {
	if err := s.log.Sync(); err != nil {
		return err
	}
	ds := s.pending
	s.pending = nil
	s.unsynced = false
	s.deliver(ds)
	return nil
}

// deliver counts the late events of ds, acks the producers of those that
// have one (replayed records have none) and hands ds to the sink, in order.
func (s *Sequencer) deliver(ds []delivery) {
	s.mu.Lock()
	for _, d := range ds {
		switch {
		case d.batch != nil:
			d.stats.Late += uint64(d.batch.LateCount)
			if n := len(d.batch.Watermark); n > 0 {
				s.delivered[d.batch.Batch.Tenant] = d.batch.Watermark[n-1]
			}
		case d.tick != nil:
			for _, adv := range d.tick.Advances {
				s.delivered[adv.Tenant] = adv.Watermark
			}
		default:
			s.lease = d.lease.lease
		}
	}
	s.mu.Unlock()
	for _, d := range ds {
		if d.e != nil && d.e.ack != nil {
			d.e.ack(Ack{FirstOffset: d.batch.FirstOffset, Events: int(d.e.n)})
		}
		if d.lease != nil {
			d.lease.done(nil)
		}
	}
	for _, d := range ds {
		switch {
		case d.batch != nil:
			s.sink.Batch(d.batch)
		case d.tick != nil:
			s.sink.Tick(d.tick)
		}
	}
}

// fail stops the sequencer after a log error: admission closes and every
// written or queued batch is acked with err.
func (s *Sequencer) fail(err error) error {
	s.err = err
	s.mu.Lock()
	s.closing = true
	var queued []*entry
	for _, t := range s.ordered {
		queued = append(queued, t.queue...)
		t.queue = nil
		t.queued = 0
		t.deficit = 0
		t.inTurn = false
	}
	s.active = nil
	leases := s.leases
	s.leases = nil
	s.mu.Unlock()
	ds := s.pending
	s.pending = nil
	for _, d := range ds {
		if d.e != nil && d.e.ack != nil {
			d.e.ack(Ack{Events: int(d.e.n), Err: err})
		}
		if d.lease != nil {
			d.lease.done(err)
		}
	}
	for _, l := range leases {
		l.done(err)
	}
	for _, e := range queued {
		if e.ack != nil {
			e.ack(Ack{Events: int(e.n), Err: err})
		}
	}
	return err
}

// NextWake reports how long from now Poll has work without new input: 0
// while batches are queued, otherwise the earliest of the group commit
// deadline, the next TICK and the next checkpoint. ok is false when only
// new input or Shutdown can create work.
func (s *Sequencer) NextWake() (d time.Duration, ok bool) {
	if s.err != nil {
		return 0, false
	}
	s.mu.Lock()
	busy, closing := len(s.active) > 0 || len(s.leases) > 0, s.closing
	s.mu.Unlock()
	if busy || (closing && len(s.pending) > 0) {
		return 0, true
	}
	now := s.clk.Monotonic()
	if len(s.pending) > 0 {
		d, ok = max(s.firstUnsynced+s.maxDelay-now, 0), true
	}
	if s.idleGraceMs > 0 && !closing {
		if lo, has := s.wm.minMaxTs(); has {
			idle := time.Duration(lo+s.idleGraceMs+1-s.clk.NowMs()) * time.Millisecond
			t := max(idle, s.nextTick-now, 0)
			if !ok || t < d {
				d, ok = t, true
			}
		}
	}
	if s.ckpt != nil && !closing {
		t := max(s.ckptAt+s.ckptEvery-now, 0)
		if !ok || t < d {
			d, ok = t, true
		}
	}
	return d, ok
}

// Run drives the sequencer in the calling goroutine until it is drained
// after Shutdown (nil) or the log fails (the error). Submit and Shutdown
// wake it through a one-slot channel; the timer only wakes it up when
// NextWake says so, every decision reads the injected clock.
func (s *Sequencer) Run() error {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		if err := s.Poll(); err != nil {
			return err
		}
		if s.Drained() {
			return nil
		}
		d, ok := s.NextWake()
		if ok && d == 0 {
			continue
		}
		var fire <-chan time.Time
		if ok {
			timer.Reset(d)
			fire = timer.C
		}
		select {
		case <-s.wake:
		case <-fire:
		}
	}
}
