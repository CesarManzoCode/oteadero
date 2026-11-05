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

// pollInterval is how long an idle node waits before looking at the log and the
// lease again. The log has no way to announce new records to a reader.
const pollInterval = 10 * time.Millisecond

// Node is `oteadero evaluate` without the process around it: the replica, the
// notifier and the HTTP transport in one loop. New, Recover, Run and Close run in
// that order; Handler can be served from Recover until Close.
type Node struct {
	cfg       config.Config
	id        string
	set       *rules.Set
	deps      Deps
	notifier  *Notifier
	replica   *Replica
	transport *HTTPTransport
	mux       *http.ServeMux
	failed    bool
}

// NewNode wires a node. onRole runs on the goroutine of Run at each change of
// role.
func NewNode(cfg config.Config, id string, set *rules.Set, coordinator, receiver string, d Deps, onRole func(leader bool, epoch uint64)) (*Node, error) {
	transport := NewHTTPTransport(coordinator, receiver)
	notifier := NewNotifier(id, d.Clock, transport, onRole)
	replica, err := NewReplica(cfg, id, set, d, notifier)
	if err != nil {
		transport.Close()
		return nil, err
	}
	n := &Node{
		cfg:       cfg,
		id:        id,
		set:       set,
		deps:      d,
		notifier:  notifier,
		replica:   replica,
		transport: transport,
		mux:       http.NewServeMux(),
	}
	n.mux.HandleFunc("GET /metrics", n.metrics)
	return n, nil
}

// Handler returns the HTTP interface.
func (n *Node) Handler() http.Handler { return n.mux }

func (n *Node) metrics(w http.ResponseWriter, _ *http.Request) {
	var out bytes.Buffer
	n.notifier.WriteMetrics(&out)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write(out.Bytes())
}

// Recover replays up to the end of the log before the node asks for the lease,
// so that a node that just started does not lead with old state.
func (n *Node) Recover() error {
	if _, err := n.replica.Recover(); err != nil {
		n.failed = true
		return err
	}
	return nil
}

// Run follows the log and drives the notifier until stop closes or something
// fails. A log cut behind the reader, by a recovery of serve, restarts the
// replica from its checkpoints; the lease and its role are not affected. Failing
// again on the way back is final.
func (n *Node) Run(stop <-chan struct{}) error {
	err := n.loop(stop)
	n.failed = err != nil
	return err
}

func (n *Node) loop(stop <-chan struct{}) error {
	timer := time.NewTimer(pollInterval)
	defer timer.Stop()
	for {
		select {
		case <-stop:
			return nil
		case reply := <-n.transport.Replies():
			reply(n.notifier)
			continue
		default:
		}
		read, err := n.replica.Step(stepRecords)
		if errors.Is(err, wal.ErrRewound) || errors.Is(err, wal.ErrCorrupt) {
			read, err = 0, n.restartReplica()
		}
		if err != nil {
			return err
		}
		if err := n.replica.CheckpointIfDue(); err != nil {
			return err
		}
		n.notifier.Poll()
		if read == stepRecords {
			continue
		}
		timer.Reset(pollInterval)
		select {
		case <-stop:
			return nil
		case reply := <-n.transport.Replies():
			reply(n.notifier)
		case <-timer.C:
		}
	}
}

// restartReplica replaces the replica with a new one, with its own export run: a
// new reading of the log is a new start for the scripts of §4.8.
func (n *Node) restartReplica() error {
	err := n.replica.Release()
	replica, nerr := NewReplica(n.cfg, n.id, n.set, n.deps, n.notifier)
	if err = errors.Join(err, nerr); err != nil {
		return err
	}
	n.replica = replica
	_, err = replica.Recover()
	return err
}

// Close stops the transport, writes the final checkpoint unless Recover or Run
// failed, and releases everything.
func (n *Node) Close() error {
	n.transport.Close()
	if n.failed {
		return n.replica.Release()
	}
	return n.replica.Close()
}
