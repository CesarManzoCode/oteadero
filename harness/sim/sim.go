// Package sim is the deterministic simulation of D16: a single-threaded
// scheduler over virtual time that runs every node of a scenario, one seeded
// PCG for every decision, a crashable disk per node, a simulated network and
// process faults. It reads no real clock, starts no goroutine and never ranges
// over a map on the path that writes the record, so the same seed yields the
// same record byte for byte.
package sim

import (
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"math/rand/v2"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/clock"
	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

// Kinds of the record lines the simulation writes itself.
const (
	KindStart  = "sim.start"
	KindCrash  = "sim.crash"
	KindPause  = "sim.pause"
	KindResume = "sim.resume"
)

// Config fixes a simulation run.
type Config struct {
	Seed uint64
	// EpochMs is the wall clock, in Unix milliseconds, of every node at
	// virtual time zero before its own offset.
	EpochMs int64
	Net     NetConfig
}

// NodeConfig declares a node.
type NodeConfig struct {
	Name  string
	Start Factory
	// ClockOffset shifts the wall clock of the node.
	ClockOffset time.Duration
	// DriftPPM makes both clocks of the node run faster (positive) or slower
	// (negative) than virtual time, in parts per million.
	DriftPPM int64
	// Shared names a node declared before whose synced disk this node sees
	// read-only through Env.Shared.
	Shared string
}

// Factory builds the in-memory state of a process from its disk. It runs at
// every start, so a crash throws the old state away and the next start
// rebuilds it.
type Factory func(env *Env) Process

// Process is the state a node keeps in memory while it runs.
type Process interface {
	Receive(m Message)
}

// Message is a network message between two nodes.
type Message struct {
	From, To, Kind string
	Payload        []byte
}

// Stats counts what the simulation delivered and every message or timer it
// dropped, by cause.
type Stats struct {
	// Delivered is the number of record lines.
	Delivered uint64
	// Lost counts messages dropped by the loss rate.
	Lost uint64
	// Partitioned counts messages dropped by a partition, at send or at
	// arrival.
	Partitioned uint64
	// Down counts messages that arrived at a node that was not running.
	Down uint64
	// Crashed counts messages and timers held by a pause when the node
	// crashed.
	Crashed uint64
	// Stale counts timers of a process that crashed before they fired.
	Stale uint64
	// Reordered counts messages sent out of the FIFO order of their link.
	Reordered uint64
}

// Sim is one simulation run. Nothing in it is safe for concurrent use, and
// nothing needs to be: everything runs on the caller's goroutine.
type Sim struct {
	cfg    Config
	rnd    *rand.Rand
	now    time.Duration
	seq    uint64
	queue  eventQueue
	byName map[string]*node
	parts  []partition
	links  map[link]time.Duration
	record bytes.Buffer
	sum    hash.Hash
	stats  Stats
}

// New returns an empty simulation at virtual time zero.
func New(cfg Config) *Sim {
	if err := cfg.Net.validate(); err != nil {
		panic(err)
	}
	return &Sim{
		cfg:    cfg,
		rnd:    rng.New(cfg.Seed),
		byName: map[string]*node{},
		links:  map[link]time.Duration{},
		sum:    sha256.New(),
	}
}

type node struct {
	name     string
	start    Factory
	clock    *nodeClock
	disk     *vfs.Mem
	shared   vfs.FS
	inc      uint64 // incarnation: grows at each crash
	up       bool
	proc     Process
	paused   int
	deferred []held
}

// held is an event that arrived while its node was paused.
type held struct {
	counted bool // a message or a timer, as opposed to a start
	fn      func()
}

// AddNode declares a node with an empty disk. It does not start it.
func (s *Sim) AddNode(c NodeConfig) {
	mustToken("node name", c.Name)
	if _, ok := s.byName[c.Name]; ok {
		panic(fmt.Sprintf("sim: node %q declared twice", c.Name))
	}
	if c.Start == nil {
		panic(fmt.Sprintf("sim: node %q has no factory", c.Name))
	}
	if c.DriftPPM <= -1_000_000 {
		panic(fmt.Sprintf("sim: node %q clock would stop or run backwards", c.Name))
	}
	n := &node{
		name:  c.Name,
		start: c.Start,
		clock: &nodeClock{s: s, offset: c.ClockOffset, ppm: c.DriftPPM},
		disk:  vfs.NewMem(),
	}
	if c.Shared != "" {
		n.shared = s.node(c.Shared).disk.Synced()
	}
	s.byName[c.Name] = n
}

func (s *Sim) node(name string) *node {
	n, ok := s.byName[name]
	if !ok {
		panic(fmt.Sprintf("sim: unknown node %q", name))
	}
	return n
}

// Now returns the virtual time.
func (s *Sim) Now() time.Duration { return s.now }

// Rand returns the PCG of the run. Scenarios draw their fault schedule from
// it, so the seed decides that too.
func (s *Sim) Rand() rng.Source { return s.rnd }

// Disk returns the disk of a node, for scenarios that inspect it.
func (s *Sim) Disk(name string) *vfs.Mem { return s.node(name).disk }

// Stats returns the counters so far.
func (s *Sim) Stats() Stats { return s.stats }

// Record returns the record so far, one line per delivered event. The slice
// is valid until the next call that runs events.
func (s *Sim) Record() []byte { return s.record.Bytes() }

// Hash returns the SHA-256 of the record so far, in hex.
func (s *Sim) Hash() string { return hex.EncodeToString(s.sum.Sum(nil)) }

// Start starts the process of a node at virtual time at. Starting a node
// that is running panics.
func (s *Sim) Start(name string, at time.Duration) {
	n := s.node(name)
	s.schedule(at, func() { n.dispatch(held{fn: func() { s.start(n) }}) })
}

// Crash kills the process of a node at virtual time at, as kill -9 together
// with a power loss of its disk: its memory, its pending timers and what a
// pause held are dropped, and its disk loses what was not synced and, as the
// PCG decides, keeps part of the last unsynced write. It does not restart it.
func (s *Sim) Crash(name string, at time.Duration) {
	n := s.node(name)
	s.schedule(at, func() { s.crash(n) })
}

// Pause stops a node from processing anything in [from, to): messages and
// timers that arrive meanwhile run at to, in arrival order. Its clock keeps
// running.
func (s *Sim) Pause(name string, from, to time.Duration) {
	if to <= from {
		panic("sim: empty pause")
	}
	n := s.node(name)
	s.schedule(from, func() {
		n.paused++
		s.write(n, KindPause, nil)
	})
	s.schedule(to, func() { s.resume(n) })
}

func (s *Sim) start(n *node) {
	if n.up {
		panic(fmt.Sprintf("sim: node %q started while running", n.name))
	}
	n.up = true
	s.write(n, KindStart, nil)
	n.proc = n.start(&Env{s: s, n: n, inc: n.inc})
}

func (s *Sim) crash(n *node) {
	for _, h := range n.deferred {
		if h.counted {
			s.stats.Crashed++
		}
	}
	n.deferred = nil
	n.inc++
	n.up = false
	n.proc = nil
	n.disk.Crash(s.rnd)
	s.write(n, KindCrash, nil)
}

func (s *Sim) resume(n *node) {
	n.paused--
	s.write(n, KindResume, nil)
	if n.paused > 0 {
		return
	}
	q := n.deferred
	n.deferred = nil
	for _, h := range q {
		h.fn()
	}
}

// dispatch runs h now, or holds it until the node resumes.
func (n *node) dispatch(h held) {
	if n.paused > 0 {
		n.deferred = append(n.deferred, h)
		return
	}
	h.fn()
}

// RunUntil runs every event scheduled at or before t, in order of time and
// then of scheduling, and leaves the virtual time at t.
func (s *Sim) RunUntil(t time.Duration) {
	for len(s.queue) > 0 && s.queue[0].at <= t {
		ev := heap.Pop(&s.queue).(*event)
		s.now = ev.at
		ev.fn()
	}
	if t > s.now {
		s.now = t
	}
}

func (s *Sim) schedule(at time.Duration, fn func()) {
	if at < s.now {
		panic(fmt.Sprintf("sim: event at %v scheduled in the past (now %v)", at, s.now))
	}
	s.seq++
	heap.Push(&s.queue, &event{at: at, seq: s.seq, fn: fn})
}

// write appends the record line of an event delivered to n.
func (s *Sim) write(n *node, kind string, payload []byte) {
	h := sha256.Sum256(payload)
	line := fmt.Appendf(nil, "t=%d node=%s kind=%s h=%s\n", s.now.Milliseconds(), n.name, kind, hex.EncodeToString(h[:8]))
	s.record.Write(line)
	s.sum.Write(line)
	s.stats.Delivered++
}

// mustToken panics unless v is a non-empty run of printable ASCII without
// spaces, so that record lines stay one field per token.
func mustToken(what, v string) {
	ok := v != ""
	for i := 0; i < len(v) && ok; i++ {
		ok = v[i] > ' ' && v[i] < 0x7f
	}
	if !ok {
		panic(fmt.Sprintf("sim: invalid %s %q", what, v))
	}
}

// Env is what a process sees of the world. Each start gets a new Env; using
// one after its process crashed panics.
type Env struct {
	s   *Sim
	n   *node
	inc uint64
}

func (e *Env) check() {
	if e.inc != e.n.inc {
		panic(fmt.Sprintf("sim: node %q used an environment from before a crash", e.n.name))
	}
}

// Node returns the name of the node.
func (e *Env) Node() string { return e.n.name }

// Clock returns the clock of the node.
func (e *Env) Clock() clock.Clock { return e.n.clock }

// Rand returns the PCG of the run.
func (e *Env) Rand() rng.Source { return e.s.rnd }

// FS returns the disk of the node.
func (e *Env) FS() vfs.FS { return e.n.disk }

// Shared returns the read-only synced disk of NodeConfig.Shared, or nil.
func (e *Env) Shared() vfs.FS { return e.n.shared }

// Send sends a message to another node, or to the same one.
func (e *Env) Send(to, kind string, payload []byte) {
	e.check()
	mustToken("kind", kind)
	e.s.send(e.n, e.s.node(to), kind, payload)
}

// After runs fn after d of virtual time, unless the process crashes first.
// The record line of the timer has the given kind and an empty payload.
func (e *Env) After(d time.Duration, kind string, fn func()) {
	e.check()
	mustToken("kind", kind)
	if d < 0 {
		panic("sim: negative timer")
	}
	s, n, inc := e.s, e.n, e.inc
	s.schedule(s.now+d, func() {
		if n.inc != inc {
			s.stats.Stale++
			return
		}
		n.dispatch(held{counted: true, fn: func() {
			s.write(n, kind, nil)
			fn()
		}})
	})
}

// nodeClock reads virtual time through the offset and drift of its node.
// Both clocks start at virtual time zero and keep running while the node is
// paused or down.
type nodeClock struct {
	s      *Sim
	offset time.Duration
	ppm    int64
}

var _ clock.Clock = (*nodeClock)(nil)

// Monotonic returns virtual time scaled by 1 + ppm/10⁶, without overflow
// for runs of years.
func (c *nodeClock) Monotonic() time.Duration {
	t := int64(c.s.now)
	return time.Duration(t + t/1_000_000*c.ppm + t%1_000_000*c.ppm/1_000_000)
}

// NowMs returns the wall clock in Unix milliseconds.
func (c *nodeClock) NowMs() int64 {
	ns := int64(c.offset + c.Monotonic())
	ms := ns / int64(time.Millisecond)
	if ns%int64(time.Millisecond) < 0 {
		ms--
	}
	return c.s.cfg.EpochMs + ms
}

type event struct {
	at  time.Duration
	seq uint64
	fn  func()
}

// eventQueue is a min-heap by (at, seq); seq is unique, so the order is total.
type eventQueue []*event

func (q eventQueue) Len() int { return len(q) }

func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}

func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *eventQueue) Push(x any) { *q = append(*q, x.(*event)) }

func (q *eventQueue) Pop() any {
	old := *q
	ev := old[len(old)-1]
	old[len(old)-1] = nil
	*q = old[:len(old)-1]
	return ev
}
