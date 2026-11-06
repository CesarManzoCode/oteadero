package eval

import (
	"bytes"
	"errors"
	"net/http"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/config"
	"github.com/CesarManzoCode/oteadero/internal/rules"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

// PollInterval is how long an idle evaluator waits before looking at the log
// and the lease again. The log has no way to announce new records to a reader.
const PollInterval = 10 * time.Millisecond

// Core is one evaluator without its transport loop: the replica that follows
// the log and the notifier, as one synchronous state machine (D03). Node runs it
// in a goroutine with HTTP; the simulation calls it from its single thread with
// its own transport. NewCore, Recover, Step and Close run in that order, and so
// do the replies the transport brings back to Notifier, all on one goroutine.
type Core struct {
	cfg      config.Config
	id       string
	set      *rules.Set
	deps     Deps
	notifier *Notifier
	replica  *Replica
	failed   bool
}

// NewCore wires an evaluator over tr. onRole runs at each change of role.
func NewCore(cfg config.Config, id string, set *rules.Set, d Deps, tr Transport, onRole func(leader bool, epoch uint64)) (*Core, error) {
	notifier := NewNotifier(id, d.Clock, tr, onRole)
	replica, err := NewReplica(cfg, id, set, d, notifier)
	if err != nil {
		return nil, err
	}
	return &Core{cfg: cfg, id: id, set: set, deps: d, notifier: notifier, replica: replica}, nil
}

// Notifier returns the notifier, which takes the replies of the transport.
func (c *Core) Notifier() *Notifier { return c.notifier }

// Recover replays up to the end of the log before the evaluator asks for the
// lease, so that one that just started does not lead with old state.
func (c *Core) Recover() error {
	if _, err := c.replica.Recover(); err != nil {
		c.failed = true
		return err
	}
	return nil
}

// Step reads up to stepRecords records, checkpoints when due and polls the
// notifier. A log cut behind the reader, by a recovery of serve, restarts the
// replica from its checkpoints; the lease and its role are not affected. more
// reports a full step: the log may hold more records right now. After an error
// the evaluator is done.
func (c *Core) Step() (more bool, err error) {
	read, err := c.replica.Step(stepRecords)
	if errors.Is(err, wal.ErrRewound) || errors.Is(err, wal.ErrCorrupt) {
		read, err = 0, c.restartReplica()
	}
	if err == nil {
		err = c.replica.CheckpointIfDue()
	}
	if err != nil {
		c.failed = true
		return false, err
	}
	c.notifier.Poll()
	return read == stepRecords, nil
}

// restartReplica replaces the replica with a new one, with its own export run: a
// new reading of the log is a new start for the scripts of §4.8. Failing again
// on the way back is final.
func (c *Core) restartReplica() error {
	err := c.replica.Release()
	replica, nerr := NewReplica(c.cfg, c.id, c.set, c.deps, c.notifier)
	if err = errors.Join(err, nerr); err != nil {
		return err
	}
	c.replica = replica
	_, err = replica.Recover()
	return err
}

// Close writes the final checkpoint unless Recover or Step failed, and releases
// everything.
func (c *Core) Close() error {
	if c.failed {
		return c.replica.Release()
	}
	return c.replica.Close()
}

// Node is `oteadero evaluate` without the process around it: the Core and the
// HTTP transport in one loop. New, Recover, Run and Close run in that order;
// Handler can be served from Recover until Close.
type Node struct {
	core      *Core
	transport *HTTPTransport
	mux       *http.ServeMux
}

// NewNode wires a node. onRole runs on the goroutine of Run at each change of
// role.
func NewNode(cfg config.Config, id string, set *rules.Set, coordinator, receiver string, d Deps, onRole func(leader bool, epoch uint64)) (*Node, error) {
	transport := NewHTTPTransport(coordinator, receiver)
	core, err := NewCore(cfg, id, set, d, transport, onRole)
	if err != nil {
		transport.Close()
		return nil, err
	}
	n := &Node{core: core, transport: transport, mux: http.NewServeMux()}
	n.mux.HandleFunc("GET /metrics", n.metrics)
	return n, nil
}

// Handler returns the HTTP interface.
func (n *Node) Handler() http.Handler { return n.mux }

func (n *Node) metrics(w http.ResponseWriter, _ *http.Request) {
	var out bytes.Buffer
	n.core.notifier.WriteMetrics(&out)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write(out.Bytes())
}

// Recover replays up to the end of the log; see Core.Recover.
func (n *Node) Recover() error { return n.core.Recover() }

// Run follows the log and drives the notifier until stop closes or something
// fails.
func (n *Node) Run(stop <-chan struct{}) error {
	timer := time.NewTimer(PollInterval)
	defer timer.Stop()
	for {
		select {
		case <-stop:
			return nil
		case reply := <-n.transport.Replies():
			reply(n.core.notifier)
			continue
		default:
		}
		more, err := n.core.Step()
		if err != nil {
			return err
		}
		if more {
			continue
		}
		timer.Reset(PollInterval)
		select {
		case <-stop:
			return nil
		case reply := <-n.transport.Replies():
			reply(n.core.notifier)
		case <-timer.C:
		}
	}
}

// Close stops the transport, writes the final checkpoint unless Recover or Run
// failed, and releases everything.
func (n *Node) Close() error {
	n.transport.Close()
	return n.core.Close()
}
