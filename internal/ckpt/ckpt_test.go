package ckpt

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/agg"
	"github.com/CesarManzoCode/oteadero/internal/clock"
	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/ingest"
	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

const (
	dataDir = "data"
	logDir  = "data/log"
	ckptDir = "data/ckpt"
	base    = int64(1_700_000_000_000)
	never   = uint64(1) << 62
)

var errInjected = errors.New("test: injected sync failure")

// faultFS fails Sync on files whose name ends in fail, while fail is set.
type faultFS struct {
	vfs.FS
	fail string
}

func (f *faultFS) Create(name string) (vfs.File, error)     { return f.wrap(name, f.FS.Create) }
func (f *faultFS) OpenAppend(name string) (vfs.File, error) { return f.wrap(name, f.FS.OpenAppend) }

func (f *faultFS) wrap(name string, open func(string) (vfs.File, error)) (vfs.File, error) {
	file, err := open(name)
	if err != nil {
		return nil, err
	}
	return &faultFile{File: file, fs: f, name: name}, nil
}

type faultFile struct {
	vfs.File
	fs   *faultFS
	name string
}

func (f *faultFile) Sync() error {
	if f.fs.fail != "" && strings.HasSuffix(f.name, f.fs.fail) {
		return errInjected
	}
	return f.File.Sync()
}

type lateRec struct {
	tenant string
	offset uint64
}

type recorder struct {
	buckets []agg.Bucket
	late    []lateRec
}

func (r *recorder) Closed(b *agg.Bucket) { r.buckets = append(r.buckets, *b) }

func (r *recorder) Late(tenant string, offset uint64) {
	r.late = append(r.late, lateRec{tenant, offset})
}

type setup struct {
	fsys    vfs.FS
	clk     *clock.Manual
	shards  int
	graceMs int64
	sub     agg.Subscriber
	opts    Options
	segment int64
	// sketches may be nil.
	sketches map[string]agg.SketchConfig
}

// node is one life of the process: everything but the disk and the clock.
type node struct {
	t   *testing.T
	su  setup
	log *wal.Log
	agg *agg.Aggregator
	seq *ingest.Sequencer
	mgr *Manager
	rep Report
}

// wire builds a node without recovering.
func wire(t *testing.T, su setup) *node {
	t.Helper()
	l, _, err := wal.Open(su.fsys, logDir, rng.New(1), wal.Options{SegmentBytes: su.segment})
	if err != nil {
		t.Fatal(err)
	}
	a, err := agg.New(agg.Config{BucketMs: ingest.DefaultBucketMs, Shards: su.shards, Sketches: su.sketches})
	if err != nil {
		t.Fatal(err)
	}
	if su.sub != nil {
		a.Subscribe(su.sub)
	}
	cfg := ingest.DefaultConfig()
	cfg.IdleGraceMs = su.graceMs
	for _, id := range []string{"a", "b", "c"} {
		cfg.Tenants = append(cfg.Tenants, ingest.TenantConfig{ID: id, QuotaEPS: 1_000_000_000})
	}
	seq, err := ingest.New(cfg, su.clk, l, a)
	if err != nil {
		t.Fatal(err)
	}
	return &node{t: t, su: su, log: l, agg: a, seq: seq, mgr: New(su.fsys, dataDir, a, su.opts)}
}

// start builds a node, recovers it and arms the checkpointer so that only
// explicit barriers checkpoint.
func start(t *testing.T, su setup) *node {
	t.Helper()
	n := wire(t, su)
	rep, err := n.mgr.Recover(n.seq)
	if err != nil {
		t.Fatal(err)
	}
	n.rep = rep
	n.seq.SetCheckpointer(n.mgr, 24*time.Hour, never)
	return n
}

// commit submits b and tries to commit it.
func (n *node) commit(b *event.Batch, ack func(ingest.Ack)) error {
	n.t.Helper()
	if c, ok := n.seq.Submit(b, ack); !ok {
		n.t.Fatalf("batch dropped: %v", c)
	}
	if err := n.seq.Poll(); err != nil {
		return err
	}
	n.su.clk.Advance(ingest.DefaultFsyncMaxDelay)
	return n.seq.Poll()
}

func (n *node) send(b *event.Batch) {
	n.t.Helper()
	if err := n.commit(b, nil); err != nil {
		n.t.Fatal(err)
	}
}

func (n *node) barrier() {
	n.t.Helper()
	if err := n.seq.Barrier(); err != nil {
		n.t.Fatal(err)
	}
}

func readAll(t *testing.T, fsys vfs.FS, name string) []byte {
	t.Helper()
	f, err := fsys.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, size)
	if _, err := f.ReadAt(data, 0); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	return data
}

func sampleBatch(tenant string, ts ...int64) *event.Batch {
	b := &event.Batch{Tenant: tenant, Kind: event.KindMetrics}
	for i, x := range ts {
		b.Metrics = append(b.Metrics, event.Metric{Key: fmt.Sprintf(`m{s="%d"}`, i%3), Value: float64(i) + 0.1, TsMs: x})
	}
	return b
}

func sampleState() *State {
	st := &State{
		Snapshot: ingest.Snapshot{
			LogID:       0xdeadbeefcafe,
			Pos:         wal.Pos{Segment: 100, Byte: 4242, Next: 137},
			BucketMs:    1000,
			LatenessMs:  2000,
			IdleGraceMs: 5000,
			Marks:       []ingest.Mark{{Tenant: "b", MaxTs: 99}, {Tenant: "a", MaxTs: -1}},
			Lease:       wal.Lease{Epoch: 7, Holder: "e2", ExpiresMs: 1234},
			Tenants:     []ingest.Stats{{Tenant: "a", Received: 10, Accepted: 8, Late: 1}, {Tenant: "zz"}},
		},
		Buckets: []agg.OpenBucket{
			{Tenant: "a", Series: `m{x="y"}`, Kind: event.KindMetrics, End: 2000, Count: 3, Sum: math.Float64frombits(0x7ff8000000001234)},
			{Tenant: "a", Series: "n{}", Kind: event.KindMetrics, End: 3000, Count: 1, Sum: math.Copysign(0, -1), Ext: []byte{1, 2, 3}},
			{Tenant: "b", Series: `{app="x"}`, Kind: event.KindLogs, End: 3000, Count: 7},
		},
		SketchErrorMax: 0.0099,
		Extra:          []byte{9, 8, 7},
	}
	st.Snapshot.Tenants[0].Dropped[ingest.CauseQuota] = 2
	return st
}

func TestRoundTripIsExact(t *testing.T) {
	st := sampleState()
	data := Encode(st)
	got, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(Encode(got), data) {
		t.Fatal("re-encoding changed the bytes")
	}
	for i := range st.Buckets {
		if math.Float64bits(got.Buckets[i].Sum) != math.Float64bits(st.Buckets[i].Sum) {
			t.Errorf("bucket %d: sum bits %x, want %x", i, math.Float64bits(got.Buckets[i].Sum), math.Float64bits(st.Buckets[i].Sum))
		}
	}
	if got.Snapshot.Pos != st.Snapshot.Pos || !reflect.DeepEqual(got.Snapshot.Marks, st.Snapshot.Marks) || !reflect.DeepEqual(got.Snapshot.Tenants, st.Snapshot.Tenants) {
		t.Errorf("snapshot = %+v, want %+v", got.Snapshot, st.Snapshot)
	}
	if !bytes.Equal(got.Buckets[1].Ext, []byte{1, 2, 3}) || got.Buckets[0].Ext != nil {
		t.Errorf("extension state = %v, %v", got.Buckets[1].Ext, got.Buckets[0].Ext)
	}
	if got.SketchErrorMax != st.SketchErrorMax {
		t.Errorf("sketch error max = %v, want %v", got.SketchErrorMax, st.SketchErrorMax)
	}
	if got.Snapshot.Lease != st.Snapshot.Lease {
		t.Errorf("lease = %+v, want %+v", got.Snapshot.Lease, st.Snapshot.Lease)
	}
	if !bytes.Equal(got.Extra, st.Extra) {
		t.Errorf("extra = %v, want %v", got.Extra, st.Extra)
	}
	if string(data[:8]) != "OTCKPT01" {
		t.Errorf("magic = %q", data[:8])
	}
}

func TestDecodeRejectsDamage(t *testing.T) {
	data := Encode(sampleState())
	for i := range data {
		bad := slices.Clone(data)
		bad[i] ^= 0x40
		if _, err := Decode(bad); !errors.Is(err, ErrInvalid) {
			t.Fatalf("flip at byte %d: %v", i, err)
		}
	}
	for n := range len(data) {
		if _, err := Decode(data[:n]); !errors.Is(err, ErrInvalid) {
			t.Fatalf("truncated to %d bytes: %v", n, err)
		}
	}
}

func TestKeepsLastTwoAtomically(t *testing.T) {
	mem := vfs.NewMem()
	n := start(t, setup{fsys: mem, clk: clock.NewManual(base), shards: 2})
	if err := mem.MkdirAll(ckptDir); err != nil {
		t.Fatal(err)
	}
	stray, err := mem.Create(filepath.Join(ckptDir, fileName(1)+".tmp"))
	if err != nil {
		t.Fatal(err)
	}
	stray.Close()
	for i := range 4 {
		n.send(sampleBatch("a", base+int64(i)*1000, base+int64(i)*1000))
		n.barrier()
	}
	names, err := mem.ReadDir(ckptDir)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{fmt.Sprintf("%020d.ckpt", 6), fmt.Sprintf("%020d.ckpt", 8)}
	if !slices.Equal(names, want) {
		t.Fatalf("files = %v, want %v", names, want)
	}
	st, err := Decode(readAll(t, mem, filepath.Join(ckptDir, want[1])))
	if err != nil {
		t.Fatal(err)
	}
	if st.Snapshot.Pos != n.log.End() || st.Snapshot.Pos.Next != 8 {
		t.Fatalf("position = %+v, log end = %+v", st.Snapshot.Pos, n.log.End())
	}
}

func TestCheckpointBytesDoNotDependOnShards(t *testing.T) {
	image := func(shards int) []byte {
		mem := vfs.NewMem()
		n := start(t, setup{fsys: mem, clk: clock.NewManual(base), shards: shards})
		for i := range 30 {
			n.send(sampleBatch([]string{"a", "b"}[i%2], base+int64(i)*400, base+int64(i)*300))
		}
		n.barrier()
		return readAll(t, mem, filepath.Join(ckptDir, fileName(60)))
	}
	if !bytes.Equal(image(1), image(4)) {
		t.Fatal("checkpoints differ between 1 and 4 shards")
	}
}

func TestDueCheckpointsFromPoll(t *testing.T) {
	mem := vfs.NewMem()
	clk := clock.NewManual(base)
	n := wire(t, setup{fsys: mem, clk: clk, shards: 2})
	n.seq.SetCheckpointer(n.mgr, DefaultEvery, 5)
	n.send(sampleBatch("a", base, base, base))
	if off, err := n.mgr.offsets(); err != nil || len(off) != 0 {
		t.Fatalf("offsets = %v, %v", off, err)
	}
	n.send(sampleBatch("a", base, base, base))
	off, err := n.mgr.offsets()
	if err != nil || !slices.Equal(off, []uint64{6}) {
		t.Fatalf("offsets after the event threshold = %v, %v", off, err)
	}
	// The interval rewrites the file at the same offset with what changed.
	n.seq.CountDrop("a", 1, ingest.CauseInvalid)
	clk.Advance(DefaultEvery)
	if err := n.seq.Poll(); err != nil {
		t.Fatal(err)
	}
	st, err := n.mgr.load(6)
	if err != nil {
		t.Fatal(err)
	}
	if st.Snapshot.Tenants[0].Dropped[ingest.CauseInvalid] != 1 {
		t.Fatalf("counters = %+v", st.Snapshot.Tenants[0])
	}
}

func TestFlushRunsBeforeCheckpointIsWritten(t *testing.T) {
	mem := vfs.NewMem()
	var filesAtFlush []uint64
	var n *node
	opts := Options{Flush: func() error {
		var err error
		filesAtFlush, err = n.mgr.offsets()
		return err
	}}
	n = start(t, setup{fsys: mem, clk: clock.NewManual(base), shards: 2, opts: opts})
	n.send(sampleBatch("a", base))
	n.barrier()
	if len(filesAtFlush) != 0 {
		t.Fatalf("checkpoint existed at flush time: %v", filesAtFlush)
	}
	boom := errors.New("boom")
	n.mgr.flush = func() error { return boom }
	n.send(sampleBatch("a", base+1))
	if err := n.seq.Barrier(); !errors.Is(err, boom) {
		t.Fatalf("Barrier = %v", err)
	}
	off, err := n.mgr.offsets()
	if err != nil || !slices.Equal(off, []uint64{1}) {
		t.Fatalf("offsets = %v, %v", off, err)
	}
}

func TestBrokenNewestFallsBack(t *testing.T) {
	mem := vfs.NewMem()
	clk := clock.NewManual(base)
	rec := &recorder{}
	n := start(t, setup{fsys: mem, clk: clk, shards: 4, sub: rec})
	n.send(sampleBatch("a", base, base+500))
	n.barrier()
	n.send(sampleBatch("a", base+1000, base+4000))
	n.barrier()
	n.send(sampleBatch("b", base+7000))
	wantOpen := n.agg.OpenBuckets()
	wantStats := n.seq.Stats()
	wantClosed := slices.Clone(rec.buckets)

	f, err := mem.OpenAppend(n.mgr.path(4))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	mem.Crash(rng.New(1))

	again := &recorder{}
	m := start(t, setup{fsys: mem, clk: clk, shards: 1, sub: again})
	if !m.rep.FromCheckpoint || m.rep.Skipped != 1 || m.rep.Offset != 2 {
		t.Fatalf("report = %+v", m.rep)
	}
	if got := m.agg.OpenBuckets(); !reflect.DeepEqual(got, wantOpen) {
		t.Fatalf("open = %+v, want %+v", got, wantOpen)
	}
	if got := m.seq.Stats(); !slices.Equal(got, wantStats) {
		t.Fatalf("stats = %+v, want %+v", got, wantStats)
	}
	if !reflect.DeepEqual(again.buckets, wantClosed) {
		t.Fatalf("replay closed %+v, want %+v", again.buckets, wantClosed)
	}
}

func TestAllBrokenReplaysFromStart(t *testing.T) {
	mem := vfs.NewMem()
	clk := clock.NewManual(base)
	n := start(t, setup{fsys: mem, clk: clk, shards: 4})
	n.send(sampleBatch("a", base, base+500))
	n.barrier()
	n.send(sampleBatch("a", base+4000))
	wantOpen := n.agg.OpenBuckets()
	f, err := mem.OpenAppend(n.mgr.path(2))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(3); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	m := start(t, setup{fsys: mem, clk: clk, shards: 4})
	if m.rep.FromCheckpoint || m.rep.Skipped != 1 {
		t.Fatalf("report = %+v", m.rep)
	}
	if got := m.agg.OpenBuckets(); !reflect.DeepEqual(got, wantOpen) {
		t.Fatalf("open = %+v, want %+v", got, wantOpen)
	}
}

func TestCheckpointOfAnotherLogIsSkipped(t *testing.T) {
	mem := vfs.NewMem()
	clk := clock.NewManual(base)
	n := start(t, setup{fsys: mem, clk: clk, shards: 2})
	n.send(sampleBatch("a", base))
	n.barrier()
	st, err := Decode(readAll(t, mem, n.mgr.path(1)))
	if err != nil {
		t.Fatal(err)
	}
	st.Snapshot.LogID++
	st.Snapshot.Pos.Next = 0
	if err := n.mgr.write(st); err != nil {
		t.Fatal(err)
	}
	if err := mem.Remove(n.mgr.path(1)); err != nil {
		t.Fatal(err)
	}
	m := start(t, setup{fsys: mem, clk: clk, shards: 2})
	if m.rep.FromCheckpoint || m.rep.Skipped != 1 {
		t.Fatalf("report = %+v", m.rep)
	}
}

func TestSegmentRemovalPolicy(t *testing.T) {
	run := func(retain uint64) (vfs.FS, *node, *clock.Manual) {
		mem := vfs.NewMem()
		clk := clock.NewManual(base)
		n := start(t, setup{fsys: mem, clk: clk, shards: 2, segment: 512, opts: Options{RetainEvents: retain}})
		for round := range 4 {
			for i := range 20 {
				at := base + int64(round)*20_000 + int64(i)*1000
				n.send(sampleBatch("a", at, at))
			}
			n.barrier()
		}
		return mem, n, clk
	}

	mem, n, clk := run(1)
	bases, err := wal.Segments(mem, logDir)
	if err != nil {
		t.Fatal(err)
	}
	off, err := n.mgr.offsets()
	if err != nil {
		t.Fatal(err)
	}
	oldest, err := n.mgr.load(off[0])
	if err != nil {
		t.Fatal(err)
	}
	if bases[0] != oldest.Snapshot.Pos.Segment || bases[0] == 0 {
		t.Fatalf("first segment %d, oldest checkpoint at %+v", bases[0], oldest.Snapshot.Pos)
	}
	wantOpen := n.agg.OpenBuckets()
	m := start(t, setup{fsys: mem, clk: clk, shards: 2, segment: 512})
	if !m.rep.FromCheckpoint {
		t.Fatalf("report = %+v", m.rep)
	}
	if got := m.agg.OpenBuckets(); !reflect.DeepEqual(got, wantOpen) {
		t.Fatalf("open = %+v, want %+v", got, wantOpen)
	}

	mem, _, _ = run(1 << 40)
	if bases, err = wal.Segments(mem, logDir); err != nil || bases[0] != 0 {
		t.Fatalf("retained log starts at %v, %v", bases, err)
	}
}

// load is a seeded mix of metrics and logs from three tenants, one of which
// goes idle, with late events.
func load(seed uint64, count int) []*event.Batch {
	r := rng.New(seed)
	var out []*event.Batch
	for i := range count {
		tenant := []string{"a", "b"}[r.IntN(2)]
		if i < 2 {
			tenant = "c"
		}
		at := func() int64 { return base + int64(i)*1000 + int64(r.IntN(3000)) - 2500 }
		k := 1 + r.IntN(8)
		if i%5 == 4 {
			b := &event.Batch{Tenant: tenant, Kind: event.KindLogs}
			for j := range k {
				b.Logs = append(b.Logs, event.Log{TsMs: at(), Labels: fmt.Sprintf(`{app="%d"}`, j%2), Fields: `{f="v"}`, Msg: "line"})
			}
			out = append(out, b)
			continue
		}
		b := &event.Batch{Tenant: tenant, Kind: event.KindMetrics}
		for range k {
			b.Metrics = append(b.Metrics, event.Metric{Key: fmt.Sprintf(`m{s="%d"}`, r.IntN(4)), Value: r.Float64() * 1e9, TsMs: at()})
		}
		out = append(out, b)
	}
	return out
}

// result is a closed bucket with its floats as bits, to compare exactly.
type result struct {
	tenant, series string
	end            int64
	count          uint64
	sum, rate      uint64
	closedAt       uint64
}

// dedup keeps the first emission of each bucket key, in order, and fails if
// a key shows up with two values.
func dedup(t *testing.T, buckets []agg.Bucket) []result {
	t.Helper()
	type key struct {
		tenant, series string
		end            int64
	}
	seen := map[key]result{}
	var out []result
	for i := range buckets {
		b := &buckets[i]
		r := result{b.Tenant, b.Series, b.End, b.Count, math.Float64bits(b.Sum), math.Float64bits(b.Rate), b.ClosedAt}
		k := key{b.Tenant, b.Series, b.End}
		if prev, ok := seen[k]; ok {
			if prev != r {
				t.Fatalf("bucket %+v emitted with two values: %+v and %+v", k, prev, r)
			}
			continue
		}
		seen[k] = r
		out = append(out, r)
	}
	return out
}

func dedupLate(late []lateRec) []lateRec {
	seen := map[uint64]bool{}
	var out []lateRec
	for _, l := range late {
		if !seen[l.offset] {
			seen[l.offset] = true
			out = append(out, l)
		}
	}
	return out
}

const stepGap = 1100 * time.Millisecond

// noCrash runs the load in one life.
func noCrash(t *testing.T, batches []*event.Batch, shards int, graceMs int64) (*recorder, *node) {
	t.Helper()
	rec := &recorder{}
	n := start(t, setup{fsys: vfs.NewMem(), clk: clock.NewManual(base), shards: shards, graceMs: graceMs, sub: rec})
	for _, b := range batches {
		n.su.clk.Advance(stepGap)
		n.send(b)
	}
	n.barrier()
	return rec, n
}

// replayAll rebuilds everything from the log alone, as a run that never
// fell over this very log would have.
func replayAll(t *testing.T, fsys vfs.FS, clk *clock.Manual, shards int, graceMs int64) (*recorder, *node) {
	t.Helper()
	rec := &recorder{}
	n := wire(t, setup{fsys: fsys, clk: clk, shards: shards, graceMs: graceMs, sub: rec})
	r, err := wal.OpenReader(fsys, logDir, wal.Pos{Byte: wal.HeaderSize})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := n.seq.Replay(r); err != nil {
		t.Fatal(err)
	}
	return rec, n
}

type crashMode int

const (
	crashWithUnsyncedBuffer crashMode = iota
	crashTornLogWrite
	crashDuringCheckpoint
	numCrashModes
)

// crash ends the life of n at one of the seeded points.
func crash(t *testing.T, n *node, mem *vfs.Mem, fault *faultFS, mode crashMode, b *event.Batch, ack func(ingest.Ack), r rng.Source) {
	t.Helper()
	switch mode {
	case crashWithUnsyncedBuffer:
		if c, ok := n.seq.Submit(b, nil); !ok {
			t.Fatalf("batch dropped: %v", c)
		}
		if err := n.seq.Poll(); err != nil {
			t.Fatal(err)
		}
	case crashTornLogWrite:
		fault.fail = ".seg"
		if err := n.commit(b, ack); !errors.Is(err, errInjected) {
			t.Fatalf("commit with failing fsync = %v", err)
		}
	case crashDuringCheckpoint:
		n.send(b)
		fault.fail = ".tmp"
		if err := n.seq.Barrier(); !errors.Is(err, errInjected) {
			t.Fatalf("barrier with failing fsync = %v", err)
		}
	}
	fault.fail = ""
	mem.Crash(r)
}

func TestRecoveryAfterSeededCrashes(t *testing.T) {
	const count = 60
	for _, graceMs := range []int64{0, ingest.DefaultIdleGraceMs} {
		for _, shards := range []int{1, 4} {
			for seed := uint64(1); seed <= 15; seed++ {
				name := fmt.Sprintf("grace=%d/shards=%d/seed=%d", graceMs, shards, seed)
				t.Run(name, func(t *testing.T) { checkRecovery(t, seed, count, shards, graceMs) })
			}
		}
	}
}

func checkRecovery(t *testing.T, seed uint64, count, shards int, graceMs int64) {
	batches := load(seed, count)
	indexAt := map[uint64]int{}
	var next uint64
	for i, b := range batches {
		indexAt[next] = i
		next += uint64(b.Len())
	}
	indexAt[next] = len(batches)

	r := rng.New(seed ^ 0x5eed)
	type crashPoint struct {
		at   int
		mode crashMode
	}
	var plan []crashPoint
	at := 0
	for range 1 + r.IntN(4) {
		at += 1 + r.IntN(count/4)
		if at < count {
			plan = append(plan, crashPoint{at, crashMode(r.IntN(int(numCrashModes)))})
		}
	}
	every := 2 + r.IntN(6)

	mem := vfs.NewMem()
	fault := &faultFS{FS: mem}
	clk := clock.NewManual(base)
	rec := &recorder{}
	acks := make([]int, count)
	su := setup{fsys: fault, clk: clk, shards: shards, graceMs: graceMs, sub: rec}

	var final *node
	for final == nil {
		ackedBefore := slices.Clone(acks)
		n := start(t, su)
		if !slices.Equal(acks, ackedBefore) {
			t.Fatal("recovery acked a producer")
		}
		i, ok := indexAt[n.log.NextOffset()]
		if !ok {
			t.Fatalf("recovered log ends at offset %d, inside a batch", n.log.NextOffset())
		}
		crashed := false
		for ; i < count; i++ {
			clk.Advance(stepGap)
			idx := i
			ack := func(a ingest.Ack) {
				if a.Err == nil {
					acks[idx]++
				}
			}
			if len(plan) > 0 && plan[0].at <= i {
				crash(t, n, mem, fault, plan[0].mode, batches[i], ack, r)
				plan = plan[1:]
				crashed = true
				break
			}
			if err := n.commit(batches[i], ack); err != nil {
				t.Fatal(err)
			}
			if (i+1)%every == 0 {
				n.barrier()
			}
		}
		if !crashed {
			final = n
		}
	}
	final.barrier()

	for i, a := range acks {
		if a > 1 {
			t.Fatalf("batch %d acked %d times: an acked batch was lost", i, a)
		}
	}

	// Same log, read from the start with no crash in the way.
	refRec, ref := replayAll(t, mem, clk, shards, graceMs)
	compare(t, "replay from the start", rec, final, refRec, ref)

	// With no TICK the log does not depend on the clock, so a live run that
	// never crashed writes the same log.
	if graceMs == 0 {
		liveRec, live := noCrash(t, batches, shards, graceMs)
		compare(t, "run without crash", rec, final, liveRec, live)
	}
}

func compare(t *testing.T, what string, rec *recorder, n *node, refRec *recorder, ref *node) {
	t.Helper()
	got, want := dedup(t, rec.buckets), dedup(t, refRec.buckets)
	if !slices.Equal(got, want) {
		t.Fatalf("%s: closed buckets differ:\n got %+v\nwant %+v", what, got, want)
	}
	if !slices.Equal(dedupLate(rec.late), dedupLate(refRec.late)) {
		t.Fatalf("%s: late events differ", what)
	}
	if !reflect.DeepEqual(n.agg.OpenBuckets(), ref.agg.OpenBuckets()) {
		t.Fatalf("%s: open buckets differ", what)
	}
	if !slices.Equal(n.seq.Stats(), ref.seq.Stats()) {
		t.Fatalf("%s: counters differ:\n got %+v\nwant %+v", what, n.seq.Stats(), ref.seq.Stats())
	}
}

func TestExportSurvivesCrashWithCheckpoint(t *testing.T) {
	mem := vfs.NewMem()
	clk := clock.NewManual(base)
	exp, err := agg.NewExport(mem, "exp", "r1")
	if err != nil {
		t.Fatal(err)
	}
	n := start(t, setup{fsys: mem, clk: clk, shards: 2, sub: exp, opts: Options{Flush: exp.Flush}})
	n.send(sampleBatch("a", base, base+5000)) // closes the bucket ending at base
	n.barrier()
	n.send(sampleBatch("a", base+9000)) // closes more, not flushed
	mem.Crash(rng.New(7))

	lines := strings.Count(string(readAll(t, mem, "exp/serve-r1/buckets.jsonl")), "\n")
	if lines != 1 {
		t.Fatalf("%d lines survive, want the one before the checkpoint", lines)
	}

	exp2, err := agg.NewExport(mem, "exp", "r2")
	if err != nil {
		t.Fatal(err)
	}
	m := start(t, setup{fsys: mem, clk: clk, shards: 2, sub: exp2, opts: Options{Flush: exp2.Flush}})
	if m.rep.Offset != 2 {
		t.Fatalf("report = %+v", m.rep)
	}
	if err := exp2.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, mem, "exp/serve-r2/buckets.jsonl"); len(got) == 0 {
		t.Fatal("replay did not re-emit what closed after the checkpoint")
	}
}

func TestSketchesSurviveCrashWithCheckpoint(t *testing.T) {
	// So many buckets close that some are audited, whichever the hash picks.
	const closing = 20_000
	mem := vfs.NewMem()
	// The clock stands at the last event, which would otherwise be more than
	// 10 min ahead of it and make its batch invalid (§4.1).
	clk := clock.NewManual(base + closing*1000)
	su := setup{fsys: mem, clk: clk, shards: 2, sketches: map[string]agg.SketchConfig{
		"a": {QuantileMetrics: []string{"lat"}, CardinalityFields: []string{"user"}},
	}}
	n := start(t, su)
	b := &event.Batch{Tenant: "a", Kind: event.KindMetrics}
	for i := range closing {
		b.Metrics = append(b.Metrics, event.Metric{Key: "lat{}", Value: 1.234, TsMs: base + int64(i)*1000})
	}
	n.send(b)
	n.send(&event.Batch{Tenant: "a", Kind: event.KindLogs, Logs: []event.Log{
		{TsMs: base + closing*1000, Labels: `{app="x"}`, Fields: `{user="u1"}`, Msg: "m"},
	}})
	n.barrier()
	wantMax := n.agg.SketchErrorMax()
	if wantMax <= 0 || wantMax > 0.01 {
		t.Fatalf("maximum before the crash = %v", wantMax)
	}
	wantOpen := n.agg.OpenBuckets()
	withState := 0
	for i := range wantOpen {
		if len(wantOpen[i].Ext) > 0 {
			withState++
		}
	}
	if withState < 2 {
		t.Fatalf("open buckets = %+v", wantOpen)
	}
	mem.Crash(rng.New(7))

	m := start(t, su)
	if !m.rep.FromCheckpoint || m.rep.Offset != closing+1 {
		t.Fatalf("report = %+v", m.rep)
	}
	if got := m.agg.SketchErrorMax(); got != wantMax {
		t.Fatalf("maximum after the crash = %v, want %v", got, wantMax)
	}
	if got := m.agg.OpenBuckets(); !reflect.DeepEqual(got, wantOpen) {
		t.Fatalf("open buckets differ after the crash:\n got %+v\nwant %+v", got, wantOpen)
	}
}

func TestSketchConfigChangeFailsRecovery(t *testing.T) {
	mem := vfs.NewMem()
	clk := clock.NewManual(base)
	su := setup{fsys: mem, clk: clk, shards: 2, sketches: map[string]agg.SketchConfig{"a": {CardinalityFields: []string{"user"}}}}
	n := start(t, su)
	n.send(&event.Batch{Tenant: "a", Kind: event.KindLogs, Logs: []event.Log{
		{TsMs: base, Labels: `{app="x"}`, Fields: `{user="u1"}`, Msg: "m"},
	}})
	n.barrier()
	mem.Crash(rng.New(7))

	su.sketches = map[string]agg.SketchConfig{"a": {CardinalityFields: []string{"host"}}}
	m := wire(t, su)
	if _, err := m.mgr.Recover(m.seq); err == nil {
		t.Fatal("recovered open buckets under another cardinality_fields")
	}
}

func TestPinnedSegmentsAreKept(t *testing.T) {
	mem := vfs.NewMem()
	clk := clock.NewManual(base)
	pin := uint64(0)
	opts := Options{RetainEvents: 1, Pinned: func() (uint64, bool) { return pin, true }}
	n := start(t, setup{fsys: mem, clk: clk, shards: 2, segment: 512, opts: opts})
	send := func(round int) {
		for i := range 20 {
			at := base + int64(round)*20_000 + int64(i)*1000
			n.send(sampleBatch("a", at, at))
		}
		n.barrier()
	}
	for round := range 4 {
		send(round)
	}
	bases, err := wal.Segments(mem, logDir)
	if err != nil || bases[0] != 0 {
		t.Fatalf("pinned log starts at %v, %v", bases, err)
	}

	// Once the reader moves on, the segments before it may go, and only those.
	pin = bases[2]
	send(4)
	if bases, err = wal.Segments(mem, logDir); err != nil || bases[0] != pin {
		t.Fatalf("log pinned at %d starts at %v, %v", pin, bases, err)
	}
}

func TestExtraStateRoundTrip(t *testing.T) {
	mem := vfs.NewMem()
	clk := clock.NewManual(base)
	opts := Options{Extra: func() []byte { return []byte("live") }}
	n := start(t, setup{fsys: mem, clk: clk, shards: 2, opts: opts})
	n.send(sampleBatch("a", base, base))
	n.barrier()

	var got []byte
	restored := 0
	opts = Options{RestoreExtra: func(b []byte) error { restored++; got = b; return nil }}
	m := start(t, setup{fsys: mem, clk: clk, shards: 2, opts: opts})
	if !m.rep.FromCheckpoint || restored != 1 || string(got) != "live" {
		t.Fatalf("report %+v, restored %d times with %q", m.rep, restored, got)
	}
}
