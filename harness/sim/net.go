package sim

import (
	"errors"
	"slices"
	"time"
)

// NetConfig is the model of the network. Every message draws, in this order,
// whether it is lost, its delay in [MinDelay, MaxDelay] and whether it is
// reordered. A message that is not reordered never overtakes an earlier one
// on its link; a reordered one gets up to ReorderDelay more and lets later
// ones pass.
type NetConfig struct {
	MinDelay, MaxDelay time.Duration
	Loss               float64
	Reorder            float64
	ReorderDelay       time.Duration
}

func (c NetConfig) validate() error {
	switch {
	case c.MinDelay < 0 || c.MaxDelay < c.MinDelay:
		return errors.New("sim: network delay range is empty or negative")
	case c.Loss < 0 || c.Loss > 1 || c.Reorder < 0 || c.Reorder > 1:
		return errors.New("sim: network probabilities must be in [0, 1]")
	case c.ReorderDelay < 0:
		return errors.New("sim: negative reorder delay")
	}
	return nil
}

// link is a directed pair of nodes.
type link struct{ from, to string }

// partition keeps a and b from seeing each other in [from, to).
type partition struct {
	a, b     string
	from, to time.Duration
}

// Partition cuts the pair a, b in both directions in [from, to). Other pairs
// are unaffected.
func (s *Sim) Partition(a, b string, from, to time.Duration) {
	s.node(a)
	s.node(b)
	if to <= from {
		panic("sim: empty partition")
	}
	s.parts = append(s.parts, partition{a: a, b: b, from: from, to: to})
}

func (s *Sim) partitioned(x, y string, t time.Duration) bool {
	for _, p := range s.parts {
		if t >= p.from && t < p.to && (p.a == x && p.b == y || p.a == y && p.b == x) {
			return true
		}
	}
	return false
}

// span draws a duration uniformly in [0, d].
func (s *Sim) span(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(s.rnd.Int64N(int64(d) + 1))
}

func (s *Sim) send(from, to *node, kind string, payload []byte) {
	m := Message{From: from.name, To: to.name, Kind: kind, Payload: slices.Clone(payload)}
	if s.partitioned(m.From, m.To, s.now) {
		s.stats.Partitioned++
		return
	}
	net := s.cfg.Net
	if net.Loss > 0 && s.rnd.Float64() < net.Loss {
		s.stats.Lost++
		return
	}
	at := s.now + net.MinDelay + s.span(net.MaxDelay-net.MinDelay)
	if net.Reorder > 0 && s.rnd.Float64() < net.Reorder {
		at += s.span(net.ReorderDelay)
		s.stats.Reordered++
	} else {
		l := link{from: m.From, to: m.To}
		at = max(at, s.links[l])
		s.links[l] = at
	}
	s.schedule(at, func() { s.deliver(to, m) })
}

// deliver hands m to its node if the pair is not partitioned and the node
// runs; a paused node gets it when it resumes.
func (s *Sim) deliver(n *node, m Message) {
	if s.partitioned(m.From, m.To, s.now) {
		s.stats.Partitioned++
		return
	}
	if !n.up {
		s.stats.Down++
		return
	}
	n.dispatch(held{counted: true, fn: func() {
		s.write(n, m.Kind, m.Payload)
		n.proc.Receive(m)
	}})
}
