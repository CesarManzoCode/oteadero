package ingest

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

// ErrForeignSnapshot reports a snapshot of another log, or of a point past
// the end of this one, which an older snapshot may still replace.
var ErrForeignSnapshot = errors.New("ingest: snapshot does not match this log")

// Mark is the watermark state of a tenant.
type Mark struct {
	Tenant string
	MaxTs  int64
}

// Snapshot is the sequencer's state at a barrier: everything before Pos is
// durable and was delivered to the sink, and nothing after it was.
type Snapshot struct {
	LogID uint64
	// Pos is the next record; Pos.Next is the next offset.
	Pos         wal.Pos
	BucketMs    int64
	LatenessMs  int64
	IdleGraceMs int64
	// Marks is in first-event order, the order TICK effects depend on.
	Marks []Mark
	// Tenants holds the counters as of Pos. The batches still queued have
	// no ack and no record, and a recovery forgets them, so the queues are
	// empty here: QueuedEvents is zero and they are not in Received nor
	// Accepted.
	Tenants []Stats
}

// Checkpointer persists a barrier's snapshot together with the state of
// whatever consumes the sink.
type Checkpointer interface {
	Checkpoint(snap *Snapshot) error
}

// SetCheckpointer makes Poll run a Barrier every positive interval every or
// after events new events, whichever comes first. Call it before Poll or
// Run, after any recovery.
func (s *Sequencer) SetCheckpointer(c Checkpointer, every time.Duration, events uint64) {
	s.ckpt = c
	s.ckptEvery = every
	s.ckptEvents = events
	s.ckptAt = s.clk.Monotonic()
	s.ckptNext = s.log.NextOffset()
}

func (s *Sequencer) checkpointDue() bool {
	return s.ckpt != nil && (s.clk.Monotonic()-s.ckptAt >= s.ckptEvery || s.log.NextOffset()-s.ckptNext >= s.ckptEvents)
}

// Barrier commits everything written, which delivers it to the sink, and
// hands the resulting snapshot to the checkpointer. Queued batches stay
// queued. A failure stops the sequencer like a log error does. It belongs
// to the Poll goroutine; a server calls it once more after Run returns.
func (s *Sequencer) Barrier() error {
	if s.err != nil {
		return s.err
	}
	if err := s.commit(); err != nil {
		return s.fail(err)
	}
	if s.ckpt == nil {
		return nil
	}
	snap := s.snapshot()
	if err := s.ckpt.Checkpoint(&snap); err != nil {
		return s.fail(err)
	}
	s.ckptAt = s.clk.Monotonic()
	s.ckptNext = snap.Pos.Next
	return nil
}

func (s *Sequencer) snapshot() Snapshot {
	stats := s.Stats()
	for i := range stats {
		st := &stats[i]
		st.Received -= uint64(st.QueuedEvents)
		st.Accepted -= uint64(st.QueuedEvents)
		st.QueuedEvents = 0
	}
	return Snapshot{
		LogID:       s.log.ID(),
		Pos:         s.log.End(),
		BucketMs:    s.wm.bucketMs,
		LatenessMs:  s.wm.latenessMs,
		IdleGraceMs: s.idleGraceMs,
		Marks:       s.wm.marks(),
		Tenants:     stats,
	}
}

// Restore loads a snapshot into a sequencer that has not started. It fails
// with ErrForeignSnapshot, changing nothing, if the snapshot does not fit
// this log; a snapshot taken with another S, L or G is a plain error, since
// replaying with other rules would not give the same result.
func (s *Sequencer) Restore(snap *Snapshot) error {
	end := s.log.End()
	p := snap.Pos
	if snap.LogID != s.log.ID() || p.Next > end.Next || p.Segment > end.Segment || (p.Segment == end.Segment && p.Byte > end.Byte) {
		return ErrForeignSnapshot
	}
	if snap.BucketMs != s.wm.bucketMs || snap.LatenessMs != s.wm.latenessMs || snap.IdleGraceMs != s.idleGraceMs {
		return fmt.Errorf("ingest: snapshot has S=%d L=%d G=%d, configuration has S=%d L=%d G=%d",
			snap.BucketMs, snap.LatenessMs, snap.IdleGraceMs, s.wm.bucketMs, s.wm.latenessMs, s.idleGraceMs)
	}
	s.wm.restore(snap.Marks)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range snap.Tenants {
		if t := s.tenants[st.Tenant]; t != nil {
			t.stats = st
		} else {
			s.unknown[st.Tenant] = &st
		}
	}
	return nil
}

// Replay feeds the records of r, up to the end of the log, through the code
// the live path uses: classification, counters and sink, with no producer
// to ack. Call it on the Poll goroutine after Restore and before Poll.
func (s *Sequencer) Replay(r *wal.Reader) error {
	for {
		rec, err := r.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch rec.Type {
		case wal.TypeBatch:
			b, err := event.DecodeBatch(rec.Payload)
			if err != nil {
				return fmt.Errorf("replay at offset %d: %w", rec.Offset, err)
			}
			s.deliver([]delivery{{stats: s.countReplayed(&b), batch: s.classify(&b, rec.Offset)}})
		case wal.TypeTick:
			ts, err := wal.DecodeTick(rec.Payload)
			if err != nil {
				return fmt.Errorf("replay at offset %d: %w", rec.Offset, err)
			}
			s.deliver([]delivery{{tick: s.applyTick(ts, rec.Offset)}})
		}
	}
}

// countReplayed counts b as received and accepted and returns the counters
// of its tenant, which may have left the configuration since it was logged.
func (s *Sequencer) countReplayed(b *event.Batch) *Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.statsFor(b.Tenant)
	st.Received += uint64(b.Len())
	st.Accepted += uint64(b.Len())
	return st
}

func (s *Sequencer) statsFor(id string) *Stats {
	if t := s.tenants[id]; t != nil {
		return &t.stats
	}
	return s.unknownStats(id)
}
