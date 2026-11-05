// Package server is `oteadero serve` without the process around it: it wires
// the log, the sequencer, the aggregator and the checkpoints from the
// configuration and serves the HTTP interface of §4.1 and §4.10. Handlers
// read and parse in parallel; the single sequencer does the rest (D03).
package server

import (
	"bytes"
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
	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/ingest"
	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

// TenantHeader names the tenant of a batch (§4.1).
const TenantHeader = "X-Oteadero-Tenant"

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
	mgr       *ckpt.Manager
	export    *agg.Export
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
	l, opened, err := wal.Open(d.FS, filepath.Join(cfg.DataDir, "log"), d.Rand, wal.Options{SegmentBytes: cfg.SegmentBytes})
	if err != nil {
		return nil, err
	}
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
		mgr:       ckpt.New(d.FS, cfg.DataDir, a, opts),
		export:    export,
		ckptEvery: time.Duration(cfg.CheckpointIntervalMs) * time.Millisecond,
		ckptCount: uint64(cfg.CheckpointEvents),
		maxBody:   cfg.MaxBodyBytes,
		known:     known,
		mux:       http.NewServeMux(),
		done:      make(chan struct{}),
	}
	s.mux.HandleFunc("POST /v1/metrics", func(w http.ResponseWriter, r *http.Request) { s.ingest(w, r, event.KindMetrics) })
	s.mux.HandleFunc("POST /v1/logs", func(w http.ResponseWriter, r *http.Request) { s.ingest(w, r, event.KindLogs) })
	s.mux.HandleFunc("GET /metrics", s.metrics)
	s.mux.HandleFunc("GET /healthz", s.healthz)
	return s, nil
}

// Start recovers from the newest usable checkpoint and the log, then runs
// the shards and the sequencer. After a failed Start the process should
// exit: Close would checkpoint a state that was not fully recovered.
func (s *Server) Start() (Recovery, error) {
	if _, err := s.mgr.Recover(s.seq); err != nil {
		return Recovery{}, err
	}
	s.seq.SetCheckpointer(s.mgr, s.ckptEvery, s.ckptCount)
	rec := Recovery{NextOffset: s.log.NextOffset(), TruncatedBytes: s.opened.TruncatedBytes}
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

// Handler returns the HTTP interface.
func (s *Server) Handler() http.Handler { return s.mux }

// Done is closed when the sequencer stopped, after Drain or a log failure.
func (s *Server) Done() <-chan struct{} { return s.done }

// Drain stops admission, so that later batches get 503 shutting_down, and
// returns once every admitted batch is synced and acked. Call it after
// Start; it can be called again.
func (s *Server) Drain() {
	s.closing.Store(true)
	s.seq.Shutdown()
	<-s.done
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

// ingest handles a batch of §4.1. Every outcome but the 200 and the log
// failure is a whole-batch drop, counted before the response. A body that is
// over the limit or cut short is counted by the lines that did arrive.
func (s *Server) ingest(w http.ResponseWriter, r *http.Request, kind event.Kind) {
	tenant := r.Header.Get(TenantHeader)
	buf := bodies.Get().(*bytes.Buffer)
	buf.Reset()
	defer bodies.Put(buf)
	_, err := buf.ReadFrom(http.MaxBytesReader(w, r.Body, s.maxBody))
	body := buf.Bytes()

	var tooBig *http.MaxBytesError
	switch {
	case !s.known[tenant]:
		s.drop(w, tenant, kind, body, ingest.CauseUnknownTenant)
		return
	case s.closing.Load():
		s.drop(w, tenant, kind, body, ingest.CauseShuttingDown)
		return
	case errors.As(err, &tooBig):
		s.drop(w, tenant, kind, body, ingest.CauseTooLarge)
		return
	case err != nil:
		s.drop(w, tenant, kind, body, ingest.CauseInvalid)
		return
	}

	b := &event.Batch{Tenant: tenant, Kind: kind}
	if kind == event.KindLogs {
		b.Logs, err = event.ParseLogs(nil, body)
	} else {
		b.Metrics, err = event.ParseMetrics(nil, body)
	}
	switch {
	case errors.Is(err, event.ErrTooLarge):
		s.drop(w, tenant, kind, body, ingest.CauseTooLarge)
		return
	case err != nil:
		s.drop(w, tenant, kind, body, ingest.CauseInvalid)
		return
	}

	n := b.Len()
	ack := make(chan ingest.Ack, 1)
	if cause, ok := s.seq.Submit(b, func(a ingest.Ack) { ack <- a }); !ok {
		writeDrop(w, n, cause)
		return
	}
	a := <-ack
	if a.Err != nil {
		writeJSON(w, http.StatusInternalServerError, `{"error":"log_failure"}`)
		return
	}
	writeJSON(w, http.StatusOK, `{"accepted":%d,"first_offset":%d}`, a.Events, a.FirstOffset)
}

// drop counts a batch that never reached Submit and answers it.
func (s *Server) drop(w http.ResponseWriter, tenant string, kind event.Kind, body []byte, cause ingest.Cause) {
	n := event.CountEvents(kind, body)
	s.seq.CountDrop(tenant, n, cause)
	writeDrop(w, n, cause)
}

func writeDrop(w http.ResponseWriter, n int, cause ingest.Cause) {
	status := http.StatusBadRequest
	switch cause {
	case ingest.CauseQuota, ingest.CauseQueueFull:
		status = http.StatusTooManyRequests
	case ingest.CauseShuttingDown:
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, `{"dropped":%d,"cause":"%s"}`, n, cause)
}

func writeJSON(w http.ResponseWriter, status int, format string, args ...any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, format+"\n", args...)
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
	out.WriteString("# TYPE oteadero_sketch_observed_rel_error_max gauge\n")
	fmt.Fprintf(&out, "oteadero_sketch_observed_rel_error_max %v\n", s.agg.SketchErrorMax())

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write(out.Bytes())
}
