// Package server is `oteadero serve` without the process around it: it wires
// the log, the sequencer, the aggregator and the checkpoints from the
// configuration and serves the HTTP interface of §4.1, §4.9 and §4.10.
// Handlers read and parse in parallel; the single sequencer does the rest
// (D03).
package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/agg"
	"github.com/CesarManzoCode/oteadero/internal/ckpt"
	"github.com/CesarManzoCode/oteadero/internal/clock"
	"github.com/CesarManzoCode/oteadero/internal/config"
	"github.com/CesarManzoCode/oteadero/internal/eval"
	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/ingest"
	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/stream"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

// TenantHeader names the tenant of a batch (§4.1).
const TenantHeader = "X-Oteadero-Tenant"

// maxLeaseBody bounds the body of a lease request, which only holds an id.
const maxLeaseBody = 4096

// Deps is what the server reads the outside world through (D03).
type Deps struct {
	FS    vfs.FS
	Clock clock.Clock
	Rand  rng.Source
	// ExportDir enables the verification export of §4.8 when not empty.
	ExportDir string
}

// Recovery is what Start found in the data directory.
type Recovery struct {
	NextOffset     uint64
	TruncatedBytes int64
}

// Server is one life of the engine. New, Start, Drain and Close run in that
// order; Handler can be served from Start until Close.
type Server struct {
	clk       clock.Clock
	log       *wal.Log
	opened    wal.Recovery
	agg       *agg.Aggregator
	seq       *ingest.Sequencer
	coord     *eval.Coordinator
	mgr       *ckpt.Manager
	export    *agg.Export
	tail      *stream.Tail
	live      *stream.Live
	ckptEvery time.Duration
	ckptCount uint64
	maxBody   int64
	known     map[string]bool
	mux       *http.ServeMux
	closing   atomic.Bool
	done      chan struct{}
}

// New opens the log, which truncates its broken tail, and wires everything.
func New(cfg config.Config, d Deps) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	icfg := ingest.Config{
		BucketMs:      cfg.BucketMs,
		LatenessMs:    cfg.LatenessMs,
		IdleGraceMs:   cfg.IdleGraceMs,
		FsyncMaxBytes: cfg.FsyncMaxBytes,
		FsyncMaxDelay: time.Duration(cfg.FsyncMaxDelayMs) * time.Millisecond,
	}
	known := make(map[string]bool, len(cfg.Tenants))
	sketches := make(map[string]agg.SketchConfig, len(cfg.Tenants))
	for _, t := range cfg.Tenants {
		known[t.ID] = true
		sketches[t.ID] = agg.SketchConfig{QuantileMetrics: t.QuantileMetrics, CardinalityFields: t.CardinalityFields}
		icfg.Tenants = append(icfg.Tenants, ingest.TenantConfig{
			ID:          t.ID,
			QuotaEPS:    t.QuotaEPS,
			Burst:       t.Burst,
			Weight:      t.Weight,
			QueueEvents: t.QueueEvents,
		})
	}
	a, err := agg.New(agg.Config{BucketMs: cfg.BucketMs, Shards: cfg.Shards, Sketches: sketches})
	if err != nil {
		return nil, err
	}
	opts := ckpt.Options{RetainEvents: uint64(cfg.TailRetentionEvents)}
	var export *agg.Export
	if d.ExportDir != "" {
		runID := fmt.Sprintf("%d-%016x", d.Clock.NowMs(), d.Rand.Uint64())
		if export, err = agg.NewExport(d.FS, d.ExportDir, runID); err != nil {
			return nil, err
		}
		a.Subscribe(export)
		opts.Flush = export.Flush
	}
	logDir := filepath.Join(cfg.DataDir, "log")
	l, opened, err := wal.Open(d.FS, logDir, d.Rand, wal.Options{SegmentBytes: cfg.SegmentBytes})
	if err != nil {
		return nil, err
	}
	tail := stream.NewTail(d.FS, logDir, l.ID(), l.End(), uint64(cfg.TailMaxLagEvents))
	live := stream.NewLive(l.ID(), stream.RingSize)
	a.Subscribe(live)
	opts.Pinned = tail.Pinned
	opts.Extra = live.AppendState
	opts.RestoreExtra = live.LoadState
	seq, err := ingest.New(icfg, d.Clock, l, a)
	if err != nil {
		l.Close()
		return nil, err
	}
	s := &Server{
		clk:       d.Clock,
		log:       l,
		opened:    opened,
		agg:       a,
		seq:       seq,
		coord:     eval.NewCoordinator(d.Clock, seq, cfg.LeaseTTLMs),
		mgr:       ckpt.New(d.FS, cfg.DataDir, a, opts),
		export:    export,
		tail:      tail,
		live:      live,
		ckptEvery: time.Duration(cfg.CheckpointIntervalMs) * time.Millisecond,
		ckptCount: uint64(cfg.CheckpointEvents),
		maxBody:   cfg.MaxBodyBytes,
		known:     known,
		mux:       http.NewServeMux(),
		done:      make(chan struct{}),
	}
	s.mux.HandleFunc("POST /v1/metrics", func(w http.ResponseWriter, r *http.Request) { s.ingest(w, r, event.KindMetrics) })
	s.mux.HandleFunc("POST /v1/logs", func(w http.ResponseWriter, r *http.Request) { s.ingest(w, r, event.KindLogs) })
	s.mux.HandleFunc("POST /v1/lease", s.lease)
	s.mux.Handle("GET /v1/tail", tail)
	s.mux.Handle("GET /v1/live", live)
	s.mux.HandleFunc("GET /metrics", s.metrics)
	s.mux.HandleFunc("GET /healthz", s.healthz)
	return s, nil
}

// Recover recovers from the newest usable checkpoint and the log and wires
// the coordinator, the checkpoints and the tail to the sequencer, which it
// leaves to the caller: Start runs it in its goroutine, and the simulation
// calls Poll from its single thread (D03), with the aggregator inline. After a
// failed Recover the process should exit: a checkpoint now would save a state
// that was not fully recovered.
func (s *Server) Recover() (Recovery, error) {
	if _, err := s.mgr.Recover(s.seq); err != nil {
		return Recovery{}, err
	}
	s.coord.Restore(s.seq.LastLease())
	s.seq.SetCheckpointer(s.mgr, s.ckptEvery, s.ckptCount)
	s.seq.SetSyncHook(s.tail.Synced)
	return Recovery{NextOffset: s.log.NextOffset(), TruncatedBytes: s.opened.TruncatedBytes}, nil
}

// Start recovers, then runs the shards and the sequencer in their
// goroutines. After a failed Start the process should exit, as after a
// failed Recover.
func (s *Server) Start() (Recovery, error) {
	rec, err := s.Recover()
	if err != nil {
		return rec, err
	}
	s.agg.Start()
	go func() {
		// A log error stops admission and acks everyone with it; Close
		// reports it.
		_ = s.seq.Run()
		s.closing.Store(true)
		close(s.done)
	}()
	return rec, nil
}

// Sequencer returns the sequencer, for a caller that drives it after Recover.
func (s *Server) Sequencer() *ingest.Sequencer { return s.seq }

// Coordinator returns the lease coordinator of D09.
func (s *Server) Coordinator() *eval.Coordinator { return s.coord }

// Handler returns the HTTP interface.
func (s *Server) Handler() http.Handler { return s.mux }

// Done is closed when the sequencer stopped, after Drain or a log failure.
func (s *Server) Done() <-chan struct{} { return s.done }

// Drain stops admission, so that later batches get 503 shutting_down, and
// returns once every admitted batch is synced and acked. It also ends the
// live streams, whose clients resume with their tokens, so that the HTTP
// server can shut down. Call it after Start; it can be called again.
func (s *Server) Drain() {
	s.closing.Store(true)
	s.seq.Shutdown()
	<-s.done
	s.tail.Close()
	s.live.Close()
}

// Close drains, writes the final checkpoint and closes everything. Stop the
// HTTP server first: its last requests are counted in that checkpoint. It
// returns the log failure, if there was one, and writes no checkpoint then.
func (s *Server) Close() error {
	s.Drain()
	err := s.seq.Barrier()
	s.agg.Stop()
	if s.export != nil {
		err = errors.Join(err, s.export.Close())
	}
	return errors.Join(err, s.log.Close())
}

// bodies recycles request buffers; no parsed event aliases its body.
var bodies = sync.Pool{New: func() any { return new(bytes.Buffer) }}

// Reply is the answer to a batch of §4.1: the HTTP status and the JSON body,
// without the final newline.
type Reply struct {
	Status int
	Body   string
}

// ingest handles a batch of §4.1 over HTTP.
func (s *Server) ingest(w http.ResponseWriter, r *http.Request, kind event.Kind) {
	buf := bodies.Get().(*bytes.Buffer)
	buf.Reset()
	defer bodies.Put(buf)
	_, err := buf.ReadFrom(http.MaxBytesReader(w, r.Body, s.maxBody))
	done := make(chan Reply, 1)
	s.Ingest(r.Header.Get(TenantHeader), kind, buf.Bytes(), err, func(rep Reply) { done <- rep })
	rep := <-done
	writeJSON(w, rep.Status, "%s", rep.Body)
}

// Ingest takes a batch of §4.1 whose body was read with readErr and answers it
// through reply exactly once: at once for a drop, and from the sequencer after
// the fsync for an admitted batch. reply must not block. Every outcome but the
// 200 and the log failure is a whole-batch drop, counted before the reply. A
// body that is over the limit or cut short is counted by the lines that did
// arrive. The HTTP handlers use it, and so does the simulation, which delivers
// bodies as messages.
func (s *Server) Ingest(tenant string, kind event.Kind, body []byte, readErr error, reply func(Reply)) {
	var tooBig *http.MaxBytesError
	switch {
	case !s.known[tenant]:
		reply(s.drop(tenant, kind, body, ingest.CauseUnknownTenant))
		return
	case s.closing.Load():
		reply(s.drop(tenant, kind, body, ingest.CauseShuttingDown))
		return
	case errors.As(readErr, &tooBig):
		reply(s.drop(tenant, kind, body, ingest.CauseTooLarge))
		return
	case readErr != nil:
		reply(s.drop(tenant, kind, body, ingest.CauseInvalid))
		return
	}

	b := &event.Batch{Tenant: tenant, Kind: kind}
	var err error
	if kind == event.KindLogs {
		b.Logs, err = event.ParseLogs(nil, body)
	} else {
		b.Metrics, err = event.ParseMetrics(nil, body)
	}
	switch {
	case errors.Is(err, event.ErrTooLarge):
		reply(s.drop(tenant, kind, body, ingest.CauseTooLarge))
		return
	case err != nil:
		reply(s.drop(tenant, kind, body, ingest.CauseInvalid))
		return
	}

	n := b.Len()
	cause, ok := s.seq.Submit(b, func(a ingest.Ack) {
		if a.Err != nil {
			reply(Reply{Status: http.StatusInternalServerError, Body: `{"error":"log_failure"}`})
			return
		}
		reply(Reply{Status: http.StatusOK, Body: fmt.Sprintf(`{"accepted":%d,"first_offset":%d}`, a.Events, a.FirstOffset)})
	})
	if !ok {
		reply(dropReply(n, cause))
	}
}

// drop counts a batch that never reached Submit and returns its answer.
func (s *Server) drop(tenant string, kind event.Kind, body []byte, cause ingest.Cause) Reply {
	n := event.CountEvents(kind, body)
	s.seq.CountDrop(tenant, n, cause)
	return dropReply(n, cause)
}

func dropReply(n int, cause ingest.Cause) Reply {
	status := http.StatusBadRequest
	switch cause {
	case ingest.CauseQuota, ingest.CauseQueueFull:
		status = http.StatusTooManyRequests
	case ingest.CauseShuttingDown:
		status = http.StatusServiceUnavailable
	}
	return Reply{Status: status, Body: fmt.Sprintf(`{"dropped":%d,"cause":"%s"}`, n, cause)}
}

func writeJSON(w http.ResponseWriter, status int, format string, args ...any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, format+"\n", args...)
}

// lease handles POST /v1/lease of D09. The coordinator answers once the LEASE
// record is synced.
func (s *Server) lease(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Holder string `json:"holder"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxLeaseBody)).Decode(&req); err != nil || req.Holder == "" {
		writeJSON(w, http.StatusBadRequest, `{"error":"invalid"}`)
		return
	}
	decided := make(chan eval.Decision, 1)
	s.coord.Acquire(req.Holder, func(d eval.Decision) { decided <- d })
	d := <-decided
	holder := agg.AppendJSONString(nil, d.Holder)
	switch {
	case errors.Is(d.Err, eval.ErrClosing):
		writeJSON(w, http.StatusServiceUnavailable, `{"error":"shutting_down"}`)
	case d.Err != nil:
		writeJSON(w, http.StatusInternalServerError, `{"error":"log_failure"}`)
	case !d.Granted:
		writeJSON(w, http.StatusConflict, `{"holder":%s}`, holder)
	default:
		writeJSON(w, http.StatusOK, `{"epoch":%d,"holder":%s,"ttl_ms":%d}`, d.Epoch, holder, d.TTLMs)
	}
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	if s.closing.Load() {
		writeJSON(w, http.StatusServiceUnavailable, `{"status":"shutting_down"}`)
		return
	}
	writeJSON(w, http.StatusOK, `{"status":"ok"}`)
}

// metrics serves the counters of §4.10 that exist so far, in the exposition
// format: all the lines of a metric together, after its TYPE line.
func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	stats := s.seq.Stats()
	now := s.clk.NowMs()
	var out bytes.Buffer
	line := func(name string, value any, labels ...event.Label) {
		fmt.Fprintf(&out, "%s%s %v\n", name, event.CanonicalLabels(labels), value)
	}
	counter := func(name string, value func(*ingest.Stats) uint64) {
		fmt.Fprintf(&out, "# TYPE %s counter\n", name)
		for i := range stats {
			line(name, value(&stats[i]), event.Label{Name: "tenant", Value: stats[i].Tenant})
		}
	}

	counter("oteadero_events_received_total", func(st *ingest.Stats) uint64 { return st.Received })
	counter("oteadero_events_accepted_total", func(st *ingest.Stats) uint64 { return st.Accepted })
	out.WriteString("# TYPE oteadero_events_dropped_total counter\n")
	for i := range stats {
		for c := range ingest.NumCauses {
			line("oteadero_events_dropped_total", stats[i].Dropped[c],
				event.Label{Name: "tenant", Value: stats[i].Tenant}, event.Label{Name: "cause", Value: c.String()})
		}
	}
	counter("oteadero_events_late_total", func(st *ingest.Stats) uint64 { return st.Late })

	out.WriteString("# TYPE oteadero_watermark_lag_ms gauge\n")
	for i := range stats {
		if wm, ok := s.seq.Watermark(stats[i].Tenant); ok {
			line("oteadero_watermark_lag_ms", now-wm, event.Label{Name: "tenant", Value: stats[i].Tenant})
		}
	}
	out.WriteString("# TYPE oteadero_queue_depth_events gauge\n")
	for i := range stats {
		if s.known[stats[i].Tenant] {
			line("oteadero_queue_depth_events", stats[i].QueuedEvents, event.Label{Name: "queue", Value: stats[i].Tenant})
		}
	}
	line("oteadero_queue_depth_events", s.tail.Active()+s.live.Active(), event.Label{Name: "queue", Value: "tail"})
	out.WriteString("# TYPE oteadero_sketch_observed_rel_error_max gauge\n")
	fmt.Fprintf(&out, "oteadero_sketch_observed_rel_error_max %v\n", s.agg.SketchErrorMax())

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write(out.Bytes())
}
