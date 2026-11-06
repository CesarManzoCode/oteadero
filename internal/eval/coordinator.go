// Package eval is the replicated evaluator of D09 and D10: the lease
// coordinator that serve runs, the notifier that decides who leads and what to
// send, and the replica that follows the log. The coordinator, the notifier and
// the replica are synchronous state machines with clock, disk and network
// injected (D03); Node is what runs them in production.
package eval

import (
	"errors"
	"sync"

	"github.com/CesarManzoCode/oteadero/internal/clock"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

// ErrClosing reports a request the log no longer takes because of a shutdown.
var ErrClosing = errors.New("eval: coordinator is shutting down")

// LeaseLog makes a LEASE record durable; ingest.Sequencer is the one in
// production. done runs once, after the fsync or with the failure, unless
// SubmitLease returns false.
type LeaseLog interface {
	SubmitLease(l wal.Lease, done func(error)) bool
}

// Decision answers a lease request. A request that is not granted and has no
// Err names the current holder.
type Decision struct {
	Granted bool
	Epoch   uint64
	Holder  string
	TTLMs   int64
	Err     error
}

// Coordinator grants the lease of D09. The decision and the order of the LEASE
// records are taken together under a lock, so epochs in the log only grow; the
// answer waits for the record, so nobody acts on an epoch a crash can undo.
// Safe for concurrent use.
type Coordinator struct {
	clk   clock.Clock
	log   LeaseLog
	ttlMs int64

	mu  sync.Mutex
	cur wal.Lease
}

// NewCoordinator returns a Coordinator with no lease granted.
func NewCoordinator(clk clock.Clock, log LeaseLog, ttlMs int64) *Coordinator {
	return &Coordinator{clk: clk, log: log, ttlMs: ttlMs}
}

// Restore loads the last LEASE record of the log. A lease that has not expired
// keeps excluding other holders across the restart.
func (c *Coordinator) Restore(l wal.Lease) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cur = l
}

// Acquire grants or renews the lease of holder if it is free, expired or
// already holds it; only a change of holder raises the epoch. done receives the
// decision, after the fsync for a grant. It must not block nor call Acquire.
//
// The expiry starts when the request is decided, which is after the holder sent
// it: that is what lets the holder count its lease from the moment it sent.
func (c *Coordinator) Acquire(holder string, done func(Decision)) {
	c.mu.Lock()
	now := c.clk.NowMs()
	epoch := c.cur.Epoch
	switch {
	case epoch > 0 && holder == c.cur.Holder:
	case epoch == 0 || now >= c.cur.ExpiresMs:
		epoch++
	default:
		current := c.cur.Holder
		c.mu.Unlock()
		done(Decision{Holder: current})
		return
	}
	// An expiry never moves back, not even if the wall clock does.
	lease := wal.Lease{Epoch: epoch, Holder: holder, ExpiresMs: max(now+c.ttlMs, c.cur.ExpiresMs)}
	ok := c.log.SubmitLease(lease, func(err error) {
		done(Decision{Granted: err == nil, Epoch: lease.Epoch, Holder: lease.Holder, TTLMs: c.ttlMs, Err: err})
	})
	if ok {
		c.cur = lease
	}
	c.mu.Unlock()
	if !ok {
		done(Decision{Err: ErrClosing})
	}
}
