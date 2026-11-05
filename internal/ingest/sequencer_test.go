package ingest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/clock"
	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

const (
	logDir = "data/log"
	t0     = int64(1_700_000_000_000)
)

// recSink records what the sequencer delivers, in order.
type recSink struct {
	items []any
	trace *[]string
}

func (r *recSink) Batch(c *CommittedBatch) {
	r.items = append(r.items, c)
	if r.trace != nil {
		*r.trace = append(*r.trace, "sink")
	}
}

func (r *recSink) Tick(t *TickEffect) {
	r.items = append(r.items, t)
	if r.trace != nil {
		*r.trace = append(*r.trace, "sink")
	}
}

func (r *recSink) batches() []*CommittedBatch {
	var out []*CommittedBatch
	for _, it := range r.items {
		if c, ok := it.(*CommittedBatch); ok {
			out = append(out, c)
		}
	}
	return out
}

func (r *recSink) tenants() []string {
	var out []string
	for _, c := range r.batches() {
		out = append(out, c.Batch.Tenant)
	}
	return out
}

// traceFS records every file Sync and fails them while *fail is set.
type traceFS struct {
	vfs.FS
	trace *[]string
	fail  *bool
}

func (f traceFS) Create(name string) (vfs.File, error)     { return f.wrap(f.FS.Create(name)) }
func (f traceFS) OpenAppend(name string) (vfs.File, error) { return f.wrap(f.FS.OpenAppend(name)) }

func (f traceFS) wrap(file vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	return traceFile{File: file, fs: f}, nil
}

type traceFile struct {
	vfs.File
	fs traceFS
}

var errSyncFailed = errors.New("test: sync failed")

func (f traceFile) Sync() error {
	if f.fs.fail != nil && *f.fs.fail {
		return errSyncFailed
	}
	*f.fs.trace = append(*f.fs.trace, "sync")
	return f.File.Sync()
}

type fixture struct {
	t    *testing.T
	cfg  Config
	clk  *clock.Manual
	fsys vfs.FS
	sink *recSink
	seq  *Sequencer
}

func newFixture(t *testing.T, cfg Config) *fixture {
	return newFixtureFS(t, cfg, vfs.NewMem())
}

func newFixtureFS(t *testing.T, cfg Config, fsys vfs.FS) *fixture {
	t.Helper()
	l, _, err := wal.Open(fsys, logDir, rng.New(1), wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	clk := clock.NewManual(t0)
	sink := &recSink{}
	seq, err := New(cfg, clk, l, sink)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, cfg: cfg, clk: clk, fsys: fsys, sink: sink, seq: seq}
}

func config(tenants ...TenantConfig) Config {
	cfg := DefaultConfig()
	cfg.Tenants = tenants
	return cfg
}

// open is a tenant whose quota never limits a test.
func open(id string) TenantConfig {
	return TenantConfig{ID: id, QuotaEPS: maxBurst}
}

func metrics(tenant string, ts ...int64) *event.Batch {
	b := &event.Batch{Tenant: tenant, Kind: event.KindMetrics}
	for i, x := range ts {
		b.Metrics = append(b.Metrics, event.Metric{Key: fmt.Sprintf(`m{i="%d"}`, i), Value: float64(i), TsMs: x})
	}
	return b
}

func logs(tenant string, ts ...int64) *event.Batch {
	b := &event.Batch{Tenant: tenant, Kind: event.KindLogs}
	for i, x := range ts {
		b.Logs = append(b.Logs, event.Log{TsMs: x, Labels: `{app="x"}`, Fields: fmt.Sprintf(`{n="%d"}`, i), Msg: "hello"})
	}
	return b
}

// sized returns a metrics batch of n events at t0.
func sized(tenant string, n int) *event.Batch {
	return metrics(tenant, slices.Repeat([]int64{t0}, n)...)
}

type ackBox struct {
	calls int
	ack   Ack
}

func (f *fixture) submit(b *event.Batch) *ackBox {
	f.t.Helper()
	box := &ackBox{}
	if c, ok := f.seq.Submit(b, func(a Ack) { box.calls++; box.ack = a }); !ok {
		f.t.Fatalf("batch of %s dropped: %v", b.Tenant, c)
	}
	f.checkInvariant()
	return box
}

func (f *fixture) drop(b *event.Batch, want Cause) {
	f.t.Helper()
	c, ok := f.seq.Submit(b, func(Ack) { f.t.Errorf("ack for a dropped batch") })
	if ok || c != want {
		f.t.Fatalf("Submit(%s, %d events) = %v, %v; want drop %v", b.Tenant, b.Len(), c, ok, want)
	}
	f.checkInvariant()
}

func (f *fixture) poll() {
	f.t.Helper()
	if err := f.seq.Poll(); err != nil {
		f.t.Fatal(err)
	}
	f.checkInvariant()
}

func (f *fixture) advance(d time.Duration) {
	f.t.Helper()
	f.clk.Advance(d)
	f.poll()
}

// flush appends everything queued and commits it at the group commit
// deadline.
func (f *fixture) flush() {
	f.t.Helper()
	f.poll()
	f.advance(f.cfg.FsyncMaxDelay)
}

func (f *fixture) stats(id string) Stats {
	f.t.Helper()
	for _, st := range f.seq.Stats() {
		if st.Tenant == id {
			return st
		}
	}
	f.t.Fatalf("no stats for tenant %q", id)
	return Stats{}
}

func (f *fixture) checkInvariant() {
	f.t.Helper()
	for _, st := range f.seq.Stats() {
		var dropped uint64
		for _, n := range st.Dropped {
			dropped += n
		}
		if st.Received != st.Accepted+dropped {
			f.t.Fatalf("tenant %q: received %d != accepted %d + dropped %d", st.Tenant, st.Received, st.Accepted, dropped)
		}
	}
}

// records reads the durable part of the log from the start.
func (f *fixture) records() []wal.Record {
	f.t.Helper()
	r, err := wal.OpenReader(f.fsys, logDir, wal.Pos{Byte: wal.HeaderSize})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	var out []wal.Record
	for {
		rec, err := r.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			f.t.Fatal(err)
		}
		rec.Payload = slices.Clone(rec.Payload)
		out = append(out, rec)
	}
}

func TestBucketEnd(t *testing.T) {
	for _, c := range []struct{ ts, want int64 }{
		{0, 0}, {1, 1000}, {999, 1000}, {1000, 1000}, {1001, 2000},
		{-1, 0}, {-1000, -1000}, {-1001, -1000},
	} {
		if got := BucketEnd(c.ts, 1000); got != c.want {
			t.Errorf("BucketEnd(%d) = %d, want %d", c.ts, got, c.want)
		}
	}
}

func TestLateBoundaries(t *testing.T) {
	cfg := config(open("a"), open("b"))
	cfg.IdleGraceMs = 0
	f := newFixture(t, cfg)
	// a: the first event is never late and sets W = 9999 − 2000 = 7999.
	// Then end(7999) = 8000 = W + 1 is on time, end(7000) = 7000 ≤ W is
	// late, 10000 raises W to 8000 and end(7500) = 8000 = W is late.
	f.submit(metrics("a", 9999))
	f.submit(metrics("a", 7999, 7000, 10000, 7500))
	// b has its own watermark: its first event at 5000 is on time although
	// a's watermark is past it, and 0 is late against b's W = 3000.
	f.submit(logs("b", 5000, 0))
	f.flush()

	want := []struct {
		tenant string
		first  uint64
		late   []bool
		wm     []int64
	}{
		{"a", 0, []bool{false}, []int64{7999}},
		{"a", 1, []bool{false, true, false, true}, []int64{7999, 7999, 8000, 8000}},
		{"b", 5, []bool{false, true}, []int64{3000, 3000}},
	}
	got := f.sink.batches()
	if len(got) != len(want) {
		t.Fatalf("sink got %d batches, want %d", len(got), len(want))
	}
	for i, w := range want {
		c := got[i]
		if c.Batch.Tenant != w.tenant || c.FirstOffset != w.first || !slices.Equal(c.Late, w.late) || !slices.Equal(c.Watermark, w.wm) {
			t.Errorf("batch %d = %s first %d late %v wm %v; want %s first %d late %v wm %v",
				i, c.Batch.Tenant, c.FirstOffset, c.Late, c.Watermark, w.tenant, w.first, w.late, w.wm)
		}
	}
	if got := f.stats("a").Late; got != 2 {
		t.Errorf("a late = %d, want 2", got)
	}
	if got := f.stats("b").Late; got != 1 {
		t.Errorf("b late = %d, want 1", got)
	}
}

func TestTickIdleGrace(t *testing.T) {
	f := newFixture(t, config(open("a"), open("b"))) // G = 5000, L = 2000
	f.submit(metrics("a", t0))
	f.poll()
	if d, ok := f.seq.NextWake(); !ok || d != 2*time.Millisecond {
		t.Fatalf("NextWake with a pending batch = %v, %v; want 2ms", d, ok)
	}
	f.advance(2 * time.Millisecond) // committed
	if d, ok := f.seq.NextWake(); !ok || d != 4999*time.Millisecond {
		t.Fatalf("NextWake while a is active = %v, %v; want 4999ms", d, ok)
	}

	// At t0+5000, maxTs = t0 is not below now − G yet.
	f.advance(4998 * time.Millisecond)
	if n := len(f.records()); n != 1 {
		t.Fatalf("log has %d records at now − G = maxTs, want 1", n)
	}
	// At t0+5001 it is: a TICK is written, and delivered once synced.
	f.advance(time.Millisecond)
	if n := len(f.sink.items); n != 1 {
		t.Fatalf("sink has %d items before the TICK is synced, want 1", n)
	}
	f.advance(2 * time.Millisecond)
	wantTick := func(item any, ts int64) {
		t.Helper()
		tk, ok := item.(*TickEffect)
		want := []WatermarkAdvance{{Tenant: "a", Watermark: ts - 5000 - 2000}}
		if !ok || tk.TsMs != ts || tk.NextOffset != 1 || !slices.Equal(tk.Advances, want) {
			t.Fatalf("sink item = %+v, want TICK at %d advancing %v", item, ts, want)
		}
	}
	if len(f.sink.items) != 2 {
		t.Fatalf("sink has %d items, want 2", len(f.sink.items))
	}
	wantTick(f.sink.items[1], t0+5001)

	// One TICK per second: none before 1 s after the previous one.
	f.advance(996 * time.Millisecond)
	if len(f.sink.items) != 2 || len(f.records()) != 2 {
		t.Fatalf("a second TICK was written within 1 s")
	}
	f.advance(2 * time.Millisecond) // t0+6001
	f.advance(2 * time.Millisecond)
	if len(f.sink.items) != 3 {
		t.Fatalf("sink has %d items, want 3", len(f.sink.items))
	}
	wantTick(f.sink.items[2], t0+6001)

	var types []byte
	for _, r := range f.records() {
		types = append(types, r.Type)
	}
	if want := []byte{wal.TypeBatch, wal.TypeTick, wal.TypeTick}; !bytes.Equal(types, want) {
		t.Fatalf("log record types = %v, want %v", types, want)
	}
	if ts, err := wal.DecodeTick(f.records()[1].Payload); err != nil || ts != t0+5001 {
		t.Fatalf("first TICK = %d, %v; want %d", ts, err, t0+5001)
	}
}

func TestNoTicksWithZeroGrace(t *testing.T) {
	cfg := config(open("a"))
	cfg.IdleGraceMs = 0
	f := newFixture(t, cfg)
	f.submit(metrics("a", t0))
	f.flush()
	for range 5 {
		f.advance(time.Minute)
	}
	if n := len(f.records()); n != 1 {
		t.Fatalf("log has %d records with G = 0, want only the batch", n)
	}
	if d, ok := f.seq.NextWake(); ok {
		t.Fatalf("NextWake = %v with nothing to do, want none", d)
	}
}

func TestDRRNoisyTenantKeepsOthersShare(t *testing.T) {
	var tenants []TenantConfig
	for _, id := range []string{"noisy", "a", "b", "c"} {
		tc := open(id)
		tc.QueueEvents = 20_000
		tenants = append(tenants, tc)
	}
	f := newFixture(t, config(tenants...))
	const size = 1000
	full := 0
	for range 30 {
		if c, ok := f.seq.Submit(sized("noisy", size), nil); !ok {
			if c != CauseQueueFull {
				t.Fatalf("noisy dropped with %v, want queue_full", c)
			}
			full++
		}
	}
	if full != 10 {
		t.Fatalf("noisy had %d batches dropped by its full queue, want 10", full)
	}
	for _, id := range []string{"a", "b", "c"} {
		for range 3 {
			f.submit(sized(id, size))
		}
	}
	f.flush()

	// Each turn gives a weight-1 tenant quantumEvents events: the noisy
	// tenant's backlog does not delay the others past their turn.
	per := quantumEvents / size
	var want []string
	want = append(want, slices.Repeat([]string{"noisy"}, per)...)
	for _, id := range []string{"a", "b", "c"} {
		want = append(want, slices.Repeat([]string{id}, 3)...)
	}
	want = append(want, slices.Repeat([]string{"noisy"}, 20-per)...)
	if got := f.sink.tenants(); !slices.Equal(got, want) {
		t.Fatalf("log order = %v, want %v", got, want)
	}
	if st := f.stats("noisy"); st.Accepted != 20*size || st.Dropped[CauseQueueFull] != 10*size {
		t.Fatalf("noisy stats = %+v", st)
	}
	for _, id := range []string{"a", "b", "c"} {
		if st := f.stats(id); st.Accepted != 3*size || st.Received != 3*size {
			t.Fatalf("%s stats = %+v, want all 3000 events accepted", id, st)
		}
	}
}

func TestDRRWeights(t *testing.T) {
	heavy := open("h")
	heavy.Weight = 2
	cfg := config(heavy, open("l"))
	cfg.FsyncMaxBytes = 64 << 20
	f := newFixture(t, cfg)
	for range 6 {
		f.submit(sized("h", quantumEvents))
		f.submit(sized("l", quantumEvents))
	}
	f.flush()
	want := []string{"h", "h", "l", "h", "h", "l", "h", "h", "l", "l", "l", "l"}
	if got := f.sink.tenants(); !slices.Equal(got, want) {
		t.Fatalf("log order = %v, want %v", got, want)
	}
}

func TestTokenBucketBurst(t *testing.T) {
	f := newFixture(t, config(
		TenantConfig{ID: "a", QuotaEPS: 1000, Burst: 3000},
		TenantConfig{ID: "d", QuotaEPS: 1000},
	))
	// The bucket starts full with the burst.
	f.submit(sized("a", 3000))
	f.drop(sized("a", 1), CauseQuota)
	// 500 ms refill 500 events.
	f.clk.Advance(500 * time.Millisecond)
	f.submit(sized("a", 500))
	f.drop(sized("a", 1), CauseQuota)
	// A long pause refills up to the burst and no further; a batch larger
	// than the burst never fits and does not consume tokens.
	f.clk.Advance(time.Hour)
	f.drop(sized("a", 3001), CauseQuota)
	f.submit(sized("a", 3000))
	f.drop(sized("a", 1), CauseQuota)
	// Burst defaults to the quota.
	f.drop(sized("d", 1001), CauseQuota)
	f.submit(sized("d", 1000))

	if st := f.stats("a"); st.Accepted != 6500 || st.Dropped[CauseQuota] != 3004 {
		t.Fatalf("a stats = %+v", st)
	}
	if st := f.stats("d"); st.Accepted != 1000 || st.Dropped[CauseQuota] != 1001 {
		t.Fatalf("d stats = %+v", st)
	}
}

func TestEveryDropCauseCounted(t *testing.T) {
	f := newFixture(t, config(TenantConfig{ID: "a", QuotaEPS: 1000, QueueEvents: 100}))
	f.drop(metrics("zz", t0), CauseUnknownTenant)
	f.drop(metrics("a", t0+MaxFutureMs+1), CauseInvalid)
	f.drop(metrics("a", t0, -1), CauseInvalid)
	first := f.submit(metrics("a", t0+MaxFutureMs))
	f.drop(sized("a", 1001), CauseQuota)
	f.drop(sized("a", 100), CauseQueueFull) // 1 queued + 100 > 100
	second := f.submit(sized("a", 99))
	f.seq.CountDrop("a", 7, CauseTooLarge)
	f.seq.CountDrop("zz", 2, CauseInvalid)
	f.checkInvariant()
	f.seq.Shutdown()
	f.drop(metrics("a", t0), CauseShuttingDown)

	st := f.stats("a")
	var want [NumCauses]uint64
	want[CauseInvalid] = 3
	want[CauseQuota] = 1001
	want[CauseQueueFull] = 100
	want[CauseTooLarge] = 7
	want[CauseShuttingDown] = 1
	if st.Received != 1212 || st.Accepted != 100 || st.Dropped != want || st.QueuedEvents != 100 {
		t.Fatalf("a stats = %+v, want received 1212, accepted 100, dropped %v", st, want)
	}
	zz := f.stats("zz")
	if zz.Received != 3 || zz.Dropped[CauseUnknownTenant] != 1 || zz.Dropped[CauseInvalid] != 2 {
		t.Fatalf("zz stats = %+v", zz)
	}
	if ids := []string{f.seq.Stats()[0].Tenant, f.seq.Stats()[1].Tenant}; !slices.Equal(ids, []string{"a", "zz"}) {
		t.Fatalf("stats order = %v", ids)
	}

	// Shutting down commits the admitted batches at once.
	if first.calls != 0 || second.calls != 0 {
		t.Fatal("acked before Poll")
	}
	f.poll()
	if first.calls != 1 || second.calls != 1 || second.ack.FirstOffset != 1 || second.ack.Events != 99 {
		t.Fatalf("acks = %+v, %+v", first, second)
	}
	if !f.seq.Drained() {
		t.Fatal("not drained after shutdown")
	}
	var names []string
	for c := range NumCauses {
		names = append(names, c.String())
	}
	if want := []string{"quota", "queue_full", "invalid", "too_large", "unknown_tenant", "shutting_down"}; !slices.Equal(names, want) {
		t.Errorf("cause names = %v, want %v", names, want)
	}
}

func TestUnknownTenantsBounded(t *testing.T) {
	f := newFixture(t, config())
	for i := range maxUnknownTenants + 76 {
		f.drop(metrics(fmt.Sprintf("t%04d", i), t0), CauseUnknownTenant)
	}
	st := f.seq.Stats()
	if len(st) != maxUnknownTenants+1 {
		t.Fatalf("%d stats entries, want %d", len(st), maxUnknownTenants+1)
	}
	if other := f.stats(otherTenant); other.Received != 76 || other.Dropped[CauseUnknownTenant] != 76 {
		t.Fatalf("overflow stats = %+v", other)
	}
}

func TestAckOnlyAfterSync(t *testing.T) {
	t.Run("delay", func(t *testing.T) {
		var trace []string
		mem := vfs.NewMem()
		f := newFixtureFS(t, config(open("a")), traceFS{FS: mem, trace: &trace})
		f.sink.trace = &trace
		trace = nil
		for range 2 {
			if _, ok := f.seq.Submit(sized("a", 10), func(Ack) { trace = append(trace, "ack") }); !ok {
				t.Fatal("dropped")
			}
		}
		f.poll()
		f.advance(time.Millisecond)
		if len(trace) != 0 || len(f.sink.items) != 0 {
			t.Fatalf("before the 2 ms deadline: trace %v", trace)
		}
		f.advance(time.Millisecond)
		if want := []string{"sync", "ack", "ack", "sink", "sink"}; !slices.Equal(trace, want) {
			t.Fatalf("trace = %v, want %v", trace, want)
		}
		// What was acked survives a crash.
		mem.Crash(rng.New(2))
		_, rec, err := wal.Open(mem, logDir, rng.New(3), wal.Options{})
		if err != nil || rec.NextOffset != 20 {
			t.Fatalf("after crash: next offset %d, %v; want 20", rec.NextOffset, err)
		}
	})
	t.Run("bytes", func(t *testing.T) {
		var trace []string
		cfg := config(open("a"))
		cfg.FsyncMaxBytes = 1
		f := newFixtureFS(t, cfg, traceFS{FS: vfs.NewMem(), trace: &trace})
		f.sink.trace = &trace
		trace = nil
		if _, ok := f.seq.Submit(sized("a", 10), func(Ack) { trace = append(trace, "ack") }); !ok {
			t.Fatal("dropped")
		}
		f.poll()
		if want := []string{"sync", "ack", "sink"}; !slices.Equal(trace, want) {
			t.Fatalf("trace = %v, want %v", trace, want)
		}
	})
}

func TestLogFailureAcksWithError(t *testing.T) {
	var trace []string
	fail := false
	f := newFixtureFS(t, config(open("a")), traceFS{FS: vfs.NewMem(), trace: &trace, fail: &fail})
	var acks []Ack
	for range 2 {
		if _, ok := f.seq.Submit(sized("a", 5), func(a Ack) { acks = append(acks, a) }); !ok {
			t.Fatal("dropped")
		}
	}
	f.poll()
	fail = true
	f.clk.Advance(2 * time.Millisecond)
	if err := f.seq.Poll(); !errors.Is(err, errSyncFailed) {
		t.Fatalf("Poll = %v, want the sync error", err)
	}
	if len(acks) != 2 || !errors.Is(acks[0].Err, errSyncFailed) || !errors.Is(acks[1].Err, errSyncFailed) {
		t.Fatalf("acks = %+v, want two with the sync error", acks)
	}
	if len(f.sink.items) != 0 {
		t.Fatal("sink received a batch that was never synced")
	}
	f.drop(sized("a", 1), CauseShuttingDown)
	if err := f.seq.Poll(); !errors.Is(err, errSyncFailed) {
		t.Fatalf("second Poll = %v", err)
	}
	if !f.seq.Drained() {
		t.Fatal("not drained after failure")
	}
}

// logBytes runs a fixed input sequence and returns every byte of the log.
func logBytes(t *testing.T) []byte {
	cfg := config(open("a"), TenantConfig{ID: "b", QuotaEPS: 10, Weight: 2})
	mem := vfs.NewMem()
	f := newFixtureFS(t, cfg, mem)
	f.submit(metrics("a", t0, t0-5000, t0+10))
	f.submit(logs("b", t0-1000, t0-9000))
	f.drop(sized("b", 11), CauseQuota)
	f.submit(sized("a", 3))
	f.poll()
	f.advance(3 * time.Millisecond)
	f.advance(6 * time.Second) // TICK
	f.submit(metrics("a", t0+6000))
	f.advance(2 * time.Millisecond)
	f.advance(2 * time.Second) // TICK
	f.seq.Shutdown()
	f.poll()

	names, err := mem.ReadDir(logDir)
	if err != nil {
		t.Fatal(err)
	}
	var out []byte
	for _, n := range names {
		file, err := mem.Open(filepath.Join(logDir, n))
		if err != nil {
			t.Fatal(err)
		}
		size, err := file.Size()
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, size)
		if _, err := file.ReadAt(buf, 0); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		file.Close()
		out = append(out, buf...)
	}
	if ticks := len(f.records()) - len(f.sink.batches()); ticks < 2 {
		t.Fatalf("scenario wrote %d TICKs, want at least 2", ticks)
	}
	return out
}

func TestSameInputSameLog(t *testing.T) {
	a, b := logBytes(t), logBytes(t)
	if len(a) <= wal.HeaderSize {
		t.Fatalf("log has only %d bytes", len(a))
	}
	if !bytes.Equal(a, b) {
		t.Fatal("same input produced different logs")
	}
}

func TestRunDrainsOnShutdown(t *testing.T) {
	f := newFixture(t, config(open("a")))
	acks := make(chan Ack, 2)
	for range 2 {
		if _, ok := f.seq.Submit(sized("a", 4), func(a Ack) { acks <- a }); !ok {
			t.Fatal("dropped")
		}
	}
	f.seq.Shutdown()
	done := make(chan error, 1)
	go func() { done <- f.seq.Run() }()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if a, b := <-acks, <-acks; a.FirstOffset != 0 || b.FirstOffset != 4 || a.Err != nil || b.Err != nil {
		t.Fatalf("acks = %+v, %+v", a, b)
	}
	if n := len(f.sink.batches()); n != 2 {
		t.Fatalf("sink got %d batches, want 2", n)
	}
}

func TestRunCommitsAfterDelay(t *testing.T) {
	l, _, err := wal.Open(vfs.NewMem(), logDir, rng.New(1), wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	clk := clock.NewReal()
	sink := &recSink{}
	seq, err := New(config(open("a")), clk, l, sink)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- seq.Run() }()
	acks := make(chan Ack, 1)
	if _, ok := seq.Submit(metrics("a", clk.NowMs()), func(a Ack) { acks <- a }); !ok {
		t.Fatal("dropped")
	}
	if a := <-acks; a.Err != nil || a.FirstOffset != 0 || a.Events != 1 {
		t.Fatalf("ack = %+v", a)
	}
	seq.Shutdown()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := len(sink.batches()); n != 1 {
		t.Fatalf("sink got %d batches, want 1", n)
	}
}

func TestConfig(t *testing.T) {
	f := newFixture(t, config(TenantConfig{ID: "a", QuotaEPS: 500}))
	got := f.seq.tenants["a"].cfg
	if got.Burst != 500 || got.Weight != 1 || got.QueueEvents != DefaultQueueEvents {
		t.Fatalf("defaults = %+v", got)
	}
	bad := []Config{
		config(open("a"), open("a")),
		config(TenantConfig{}),
		config(TenantConfig{ID: "a", QuotaEPS: -1}),
		config(TenantConfig{ID: "a", Weight: maxWeight + 1}),
		{BucketMs: 0, FsyncMaxBytes: 1},
		{BucketMs: 1000, FsyncMaxBytes: 0},
		{BucketMs: 1000, FsyncMaxBytes: 1, IdleGraceMs: -1},
	}
	for i, cfg := range bad {
		if _, err := New(cfg, f.clk, nil, f.sink); err == nil {
			t.Errorf("config %d accepted", i)
		}
	}
}

func TestWatermarkFollowsCommits(t *testing.T) {
	f := newFixture(t, config(open("a")))
	want := func(w int64, defined bool) {
		t.Helper()
		if got, ok := f.seq.Watermark("a"); got != w || ok != defined {
			t.Fatalf("Watermark = %d, %v; want %d, %v", got, ok, w, defined)
		}
	}
	want(0, false)
	f.submit(metrics("a", t0+500, t0))
	want(0, false) // not committed yet
	f.flush()
	want(t0+500-DefaultLatenessMs, true)

	f.advance(6 * time.Second) // writes a TICK
	tick := f.clk.NowMs()
	f.flush()
	want(tick-DefaultIdleGraceMs-DefaultLatenessMs, true)

	g := newFixture(t, config(open("a")))
	snap := g.seq.snapshot()
	snap.Marks = []Mark{{Tenant: "a", MaxTs: t0}}
	if err := g.seq.Restore(&snap); err != nil {
		t.Fatal(err)
	}
	if got, ok := g.seq.Watermark("a"); !ok || got != t0-DefaultLatenessMs {
		t.Fatalf("after Restore: %d, %v", got, ok)
	}
}

func TestSyncHookSeesDurableEndBeforeAck(t *testing.T) {
	f := newFixture(t, config(open("a")))
	var seen []wal.Pos
	var box *ackBox
	f.seq.SetSyncHook(func(p wal.Pos) {
		if box != nil && box.calls != 0 {
			t.Errorf("hook ran after the ack")
		}
		seen = append(seen, p)
	})
	box = f.submit(metrics("a", t0, t0))
	f.flush()
	if box.calls != 1 {
		t.Fatalf("acks = %d", box.calls)
	}
	if len(seen) != 1 || seen[0].Next != 2 || seen[0] != f.seq.log.End() {
		t.Fatalf("hook saw %+v, log end %+v", seen, f.seq.log.End())
	}
}
