package agg

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/clock"
	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/ingest"
	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

// base is a multiple of S, so base+1000 is a bucket end.
const base = int64(1_700_000_000_000)

type lateRec struct {
	tenant string
	offset uint64
}

type recorder struct {
	buckets []Bucket
	late    []lateRec
}

func (r *recorder) Closed(b *Bucket) { r.buckets = append(r.buckets, *b) }

func (r *recorder) Late(tenant string, offset uint64) {
	r.late = append(r.late, lateRec{tenant, offset})
}

type rig struct {
	t   *testing.T
	clk *clock.Manual
	seq *ingest.Sequencer
	agg *Aggregator
	rec *recorder
}

func newRig(t *testing.T, shards int, graceMs int64, ext ExtFactory) *rig {
	t.Helper()
	return newRigConfig(t, Config{BucketMs: ingest.DefaultBucketMs, Shards: shards, NewExt: ext}, graceMs)
}

func newRigConfig(t *testing.T, acfg Config, graceMs int64) *rig {
	t.Helper()
	l, _, err := wal.Open(vfs.NewMem(), "data/log", rng.New(1), wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(acfg)
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	a.Subscribe(rec)
	cfg := ingest.DefaultConfig()
	cfg.IdleGraceMs = graceMs
	for _, id := range []string{"a", "b"} {
		cfg.Tenants = append(cfg.Tenants, ingest.TenantConfig{ID: id, QuotaEPS: 1_000_000_000})
	}
	clk := clock.NewManual(base)
	seq, err := ingest.New(cfg, clk, l, a)
	if err != nil {
		t.Fatal(err)
	}
	return &rig{t: t, clk: clk, seq: seq, agg: a, rec: rec}
}

func (r *rig) poll() {
	r.t.Helper()
	if err := r.seq.Poll(); err != nil {
		r.t.Fatal(err)
	}
}

// send commits b, delivering it to the aggregator.
func (r *rig) send(b *event.Batch) {
	r.t.Helper()
	if c, ok := r.seq.Submit(b, nil); !ok {
		r.t.Fatalf("batch dropped: %v", c)
	}
	r.poll()
	r.clk.Advance(ingest.DefaultFsyncMaxDelay)
	r.poll()
}

type sample struct {
	key string
	ts  int64
	v   float64
}

func metrics(tenant string, samples ...sample) *event.Batch {
	b := &event.Batch{Tenant: tenant, Kind: event.KindMetrics}
	for _, s := range samples {
		b.Metrics = append(b.Metrics, event.Metric{Key: s.key, Value: s.v, TsMs: s.ts})
	}
	return b
}

func TestBucketsOpenOnTheLeft(t *testing.T) {
	r := newRig(t, 4, 0, nil)
	r.send(metrics("a",
		sample{"m{}", base + 1000, 1},   // ts multiple of S: bucket ending at ts
		sample{"m{}", base + 1001, 2},   // next bucket
		sample{"m{}", base + 2000, 4},   // still that one
		sample{"m{}", base + 4000, 8},   // W = base+2000 closes both
		sample{"m{}", base + 4001, 16})) // bucket base+5000 stays open
	want := []Bucket{
		{Tenant: "a", Series: "m{}", Kind: event.KindMetrics, End: base + 1000, Count: 1, Sum: 1, Rate: 1, ClosedAt: 3},
		{Tenant: "a", Series: "m{}", Kind: event.KindMetrics, End: base + 2000, Count: 2, Sum: 6, Rate: 6, ClosedAt: 3},
	}
	if !reflect.DeepEqual(r.rec.buckets, want) {
		t.Fatalf("closed = %+v, want %+v", r.rec.buckets, want)
	}
}

func TestWatermarkClosesExactlyAtEnd(t *testing.T) {
	r := newRig(t, 2, 0, nil)
	r.send(metrics("a", sample{"m{}", base + 1000, 1}))
	r.send(metrics("a", sample{"m{}", base + 2999, 1})) // W = end − 1
	if len(r.rec.buckets) != 0 {
		t.Fatalf("closed before W reached end: %+v", r.rec.buckets)
	}
	r.send(metrics("a", sample{"m{}", base + 3000, 1})) // W = end
	if len(r.rec.buckets) != 1 || r.rec.buckets[0].End != base+1000 || r.rec.buckets[0].ClosedAt != 2 {
		t.Fatalf("closed = %+v", r.rec.buckets)
	}
}

func TestZeroLatenessClosesWithItsOwnEvent(t *testing.T) {
	// With L = 0 the event at a bucket end closes the bucket it just
	// entered, and it must be counted in it.
	l, _, err := wal.Open(vfs.NewMem(), "data/log", rng.New(1), wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{BucketMs: 1000})
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	a.Subscribe(rec)
	cfg := ingest.DefaultConfig()
	cfg.LatenessMs = 0
	cfg.IdleGraceMs = 0
	cfg.Tenants = []ingest.TenantConfig{{ID: "a", QuotaEPS: 1000}}
	clk := clock.NewManual(base)
	seq, err := ingest.New(cfg, clk, l, a)
	if err != nil {
		t.Fatal(err)
	}
	if c, ok := seq.Submit(metrics("a", sample{"m{}", base, 5}), nil); !ok {
		t.Fatal(c)
	}
	if err := seq.Poll(); err != nil {
		t.Fatal(err)
	}
	if err := seq.Barrier(); err != nil {
		t.Fatal(err)
	}
	if len(rec.buckets) != 1 || rec.buckets[0].Count != 1 || rec.buckets[0].Sum != 5 {
		t.Fatalf("closed = %+v", rec.buckets)
	}
}

func TestTickClosesWithLastEventBefore(t *testing.T) {
	r := newRig(t, 4, ingest.DefaultIdleGraceMs, nil)
	r.send(metrics("a", sample{"m{}", base, 1}, sample{"m{}", base, 2}, sample{"m{}", base, 4}))
	r.send(metrics("b", sample{"n{}", base, 1}))
	if len(r.rec.buckets) != 0 {
		t.Fatalf("closed too early: %+v", r.rec.buckets)
	}
	r.clk.Advance(10 * time.Second)
	r.poll()
	r.clk.Advance(ingest.DefaultFsyncMaxDelay)
	r.poll()
	want := []Bucket{
		{Tenant: "a", Series: "m{}", Kind: event.KindMetrics, End: base, Count: 3, Sum: 7, Rate: 7, ClosedAt: 3},
		{Tenant: "b", Series: "n{}", Kind: event.KindMetrics, End: base, Count: 1, Sum: 1, Rate: 1, ClosedAt: 3},
	}
	if !reflect.DeepEqual(r.rec.buckets, want) {
		t.Fatalf("closed = %+v, want %+v", r.rec.buckets, want)
	}
}

func TestLateEventsStayOutAndClosedAtIsGlobal(t *testing.T) {
	r := newRig(t, 4, 0, nil)
	r.send(metrics("a", sample{"m{}", base + 10_000, 1})) // offset 0, W = base+8000
	r.send(metrics("b", sample{"m{}", base, 1}))          // offset 1, other tenant
	r.send(metrics("a",
		sample{"m{}", base + 8000, 100}, // offset 2, end = W: late
		sample{"m{}", base + 8001, 2}))  // offset 3, end = base+9000: not late
	r.send(metrics("a", sample{"m{}", base + 20_000, 1})) // offset 4 closes
	want := []Bucket{
		{Tenant: "a", Series: "m{}", Kind: event.KindMetrics, End: base + 9000, Count: 1, Sum: 2, Rate: 2, ClosedAt: 4},
		{Tenant: "a", Series: "m{}", Kind: event.KindMetrics, End: base + 10_000, Count: 1, Sum: 1, Rate: 1, ClosedAt: 4},
	}
	if !reflect.DeepEqual(r.rec.buckets, want) {
		t.Fatalf("closed = %+v, want %+v", r.rec.buckets, want)
	}
	if want := []lateRec{{"a", 2}}; !reflect.DeepEqual(r.rec.late, want) {
		t.Fatalf("late = %+v, want %+v", r.rec.late, want)
	}
}

func TestLogBucketsCountAndRate(t *testing.T) {
	r := newRig(t, 4, 0, nil)
	logs := &event.Batch{Tenant: "a", Kind: event.KindLogs, Logs: []event.Log{
		{TsMs: base, Labels: `{app="x"}`, Msg: "one"},
		{TsMs: base, Labels: `{app="x"}`, Msg: "two"},
		{TsMs: base + 5000, Labels: `{app="y"}`, Msg: "three"},
	}}
	r.send(logs)
	want := []Bucket{{Tenant: "a", Series: `{app="x"}`, Kind: event.KindLogs, End: base, Count: 2, Sum: 0, Rate: 2, ClosedAt: 2}}
	if !reflect.DeepEqual(r.rec.buckets, want) {
		t.Fatalf("closed = %+v, want %+v", r.rec.buckets, want)
	}
}

// randomBatches is a seeded load whose sums depend on the order of addition.
func randomBatches(seed uint64, n int) []*event.Batch {
	r := rng.New(seed)
	values := []float64{1e16, 1, -1e16, 0.1, 0.2, math.Pi}
	var out []*event.Batch
	for i := range n {
		tenant := []string{"a", "b"}[r.IntN(2)]
		var samples []sample
		for range 1 + r.IntN(40) {
			key := fmt.Sprintf(`m{s="%d"}`, r.IntN(12))
			ts := base + int64(i)*200 + int64(r.IntN(6000))
			samples = append(samples, sample{key, ts, values[r.IntN(len(values))]})
		}
		out = append(out, metrics(tenant, samples...))
	}
	return out
}

func TestSameEmissionsForAnyShardCount(t *testing.T) {
	type variant struct {
		shards int
		start  bool
	}
	run := func(v variant, seed uint64) *rig {
		r := newRig(t, v.shards, 0, nil)
		if v.start {
			r.agg.Start()
			defer r.agg.Stop()
		}
		for _, b := range randomBatches(seed, 200) {
			r.send(b)
		}
		r.agg.Barrier()
		return r
	}
	for seed := uint64(1); seed <= 5; seed++ {
		ref := run(variant{1, false}, seed)
		if len(ref.rec.buckets) == 0 || len(ref.rec.late) == 0 {
			t.Fatalf("seed %d: load closes %d buckets with %d late events", seed, len(ref.rec.buckets), len(ref.rec.late))
		}
		for _, v := range []variant{{4, false}, {4, true}, {3, true}} {
			got := run(v, seed)
			if !reflect.DeepEqual(got.rec.buckets, ref.rec.buckets) {
				t.Errorf("seed %d, %+v: closed buckets differ", seed, v)
			}
			if !reflect.DeepEqual(got.rec.late, ref.rec.late) {
				t.Errorf("seed %d, %+v: late events differ", seed, v)
			}
			if !reflect.DeepEqual(got.agg.OpenBuckets(), ref.agg.OpenBuckets()) {
				t.Errorf("seed %d, %+v: open buckets differ", seed, v)
			}
		}
	}
}

type countExt struct{ n byte }

func (e *countExt) Add(float64, string)           { e.n++ }
func (e *countExt) Close()                        {}
func (e *countExt) AppendState(dst []byte) []byte { return append(dst, e.n) }
func (e *countExt) LoadState(state []byte) error {
	if len(state) != 1 {
		return fmt.Errorf("state of %d bytes", len(state))
	}
	e.n = state[0]
	return nil
}

func TestBucketExtension(t *testing.T) {
	onlyM := func(_, series string, _ int64, _ event.Kind) Ext {
		if strings.HasPrefix(series, "m") {
			return &countExt{}
		}
		return nil
	}
	r := newRig(t, 4, 0, onlyM)
	r.send(metrics("a", sample{"m{}", base, 1}, sample{"m{}", base, 2}, sample{"n{}", base, 3}))

	open := r.agg.OpenBuckets()
	if len(open) != 2 || string(open[0].Ext) != "\x02" || open[1].Ext != nil {
		t.Fatalf("open = %+v", open)
	}
	restored, err := New(Config{BucketMs: 1000, Shards: 1, NewExt: onlyM})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(open, nil); err != nil {
		t.Fatal(err)
	}
	if got := restored.OpenBuckets(); !reflect.DeepEqual(got, open) {
		t.Fatalf("restored = %+v, want %+v", got, open)
	}

	r.send(metrics("a", sample{"m{}", base + 5000, 1}))
	if len(r.rec.buckets) != 2 {
		t.Fatalf("closed = %+v", r.rec.buckets)
	}
	if e, ok := r.rec.buckets[0].Ext.(*countExt); !ok || e.n != 2 {
		t.Fatalf("extension of the closed bucket = %+v", r.rec.buckets[0].Ext)
	}
	if r.rec.buckets[1].Ext != nil {
		t.Fatalf("series without extension got %+v", r.rec.buckets[1].Ext)
	}
}

func TestRestoreRejectsStateWithoutExtension(t *testing.T) {
	a, err := New(Config{BucketMs: 1000})
	if err != nil {
		t.Fatal(err)
	}
	err = a.Restore([]OpenBucket{{Tenant: "a", Series: "m{}", Kind: event.KindMetrics, End: 1000, Ext: []byte{1}}}, nil)
	if err == nil {
		t.Fatal("restored extension state with no extension")
	}
}

func TestRestoreLoadsOpenBuckets(t *testing.T) {
	ref := newRig(t, 4, 0, nil)
	r := newRig(t, 4, 0, nil)
	first := metrics("a", sample{"m{}", base + 500, 1}, sample{"m{}", base + 3000, 2})
	ref.send(first)
	r.send(metrics("b", sample{"other{}", base, 1})) // shifts nothing in tenant a's state
	if err := r.agg.Restore(ref.agg.OpenBuckets(), []ingest.WatermarkAdvance{{Tenant: "a", Watermark: base + 1000}}); err != nil {
		t.Fatal(err)
	}
	var got []OpenBucket
	for _, o := range r.agg.OpenBuckets() {
		if o.Tenant == "a" {
			got = append(got, o)
		}
	}
	if !reflect.DeepEqual(got, ref.agg.OpenBuckets()) {
		t.Fatalf("open = %+v, want %+v", got, ref.agg.OpenBuckets())
	}
}

// advanceRec records buckets and watermark steps in one sequence.
type advanceRec struct {
	recorder
	seq []string
}

func (r *advanceRec) Closed(b *Bucket) {
	r.seq = append(r.seq, fmt.Sprintf("closed %d", b.End-base))
}

func (r *advanceRec) Advanced(tenant string, upTo int64, closedAt uint64) {
	r.seq = append(r.seq, fmt.Sprintf("advanced %s %d %d", tenant, upTo-base, closedAt))
}

func TestAdvancedFollowsTheBucketsOfItsStep(t *testing.T) {
	a, err := New(Config{BucketMs: 1000, Shards: 2})
	if err != nil {
		t.Fatal(err)
	}
	rec := &advanceRec{}
	a.Subscribe(rec)
	send := func(offset uint64, ts, w int64) {
		a.Batch(&ingest.CommittedBatch{
			Batch:       &event.Batch{Tenant: "a", Kind: event.KindMetrics, Metrics: []event.Metric{{Key: "m{}", TsMs: base + ts}}},
			FirstOffset: offset,
			Late:        []bool{false},
			Watermark:   []int64{base + w},
		})
	}
	send(0, 500, -1500)
	send(1, 600, -1400) // no multiple of S crossed
	send(2, 3500, 1500)
	a.Tick(&ingest.TickEffect{NextOffset: 3, Advances: []ingest.WatermarkAdvance{{Tenant: "a", Watermark: base + 4000}}})
	want := []string{
		"advanced a -2000 0",
		"closed 1000",
		"advanced a 1000 2",
		"closed 4000",
		"advanced a 4000 2",
	}
	if !reflect.DeepEqual(rec.seq, want) {
		t.Fatalf("sequence = %q, want %q", rec.seq, want)
	}
}
