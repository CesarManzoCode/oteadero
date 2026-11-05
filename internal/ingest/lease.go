package ingest

import "github.com/CesarManzoCode/oteadero/internal/wal"

// leaseEntry is a LEASE record waiting for the sequencer, the single writer of
// the log.
type leaseEntry struct {
	lease wal.Lease
	done  func(error)
}

// SubmitLease queues a LEASE record. done is called exactly once, from the
// sequencer, after the record is synced or with the log failure; it must not
// block. While shutting down it returns false and done is never called. Safe
// for concurrent use.
func (s *Sequencer) SubmitLease(l wal.Lease, done func(error)) bool {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return false
	}
	s.leases = append(s.leases, &leaseEntry{lease: l, done: done})
	s.mu.Unlock()
	s.notify()
	return true
}

// writeLeases appends the queued LEASE records. A failed append leaves its
// record and the later ones to the caller's fail, back in the queue.
func (s *Sequencer) writeLeases() error {
	s.mu.Lock()
	leases := s.leases
	s.leases = nil
	s.mu.Unlock()
	for i, l := range leases {
		if err := s.log.AppendLease(l.lease); err != nil {
			s.mu.Lock()
			s.leases = append(leases[i:], s.leases...)
			s.mu.Unlock()
			return err
		}
		s.markUnsynced()
		s.pending = append(s.pending, delivery{lease: l})
	}
	return nil
}

// LastLease returns the last LEASE record that is durable, from the live path,
// a restored snapshot or a replay. It belongs to the Poll goroutine; a recovery
// reads it before the sequencer runs.
func (s *Sequencer) LastLease() wal.Lease { return s.lease }
