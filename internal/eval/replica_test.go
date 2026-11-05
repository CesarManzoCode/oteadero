package eval

import (
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/CesarManzoCode/oteadero/internal/clock"
	"github.com/CesarManzoCode/oteadero/internal/config"
	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/rules"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

const exportDir = "export"

// world is a log on a disk in memory with what the replicas need around it.
type world struct {
	t   *testing.T
	mem *vfs.Mem
	clk *clock.Manual
	rnd rng.Source
	set *rules.Set
	cfg config.Config
	log *wal.Log
}

func newWorld(t *testing.T) *world {
	t.Helper()
	set, err := rules.Parse([]byte(`{"rules":[{"name":"high","tenant":"a","source":"metrics",
		"match":{"__name__":"m"},"agg":"count","range_ms":1000,"op":">=","threshold":3}]}`), 1000)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.DataDir = "data"
	cfg.IdleGraceMs = 0
	cfg.Tenants = []config.Tenant{{ID: "a", QuotaEPS: 1}}
	w := &world{t: t, mem: vfs.NewMem(), clk: clock.NewManual(t0), rnd: rng.New(11), set: set, cfg: cfg}
	w.openLog()
	return w
}

func (w *world) openLog() {
	w.t.Helper()
	l, _, err := wal.Open(w.mem, logPath, w.rnd, wal.Options{})
	if err != nil {
		w.t.Fatal(err)
	}
	w.log = l
}

// second writes a batch of n events in the bucket that ends at end, and syncs.
func (w *world) second(end int64, n int) {
	w.t.Helper()
	b := &event.Batch{Tenant: "a", Kind: event.KindMetrics, IngestMs: t0}
	for i := range n {
		b.Metrics = append(b.Metrics, event.Metric{Key: `m{}`, Value: float64(i), TsMs: end})
	}
	if _, err := w.log.AppendBatch(event.AppendBatch(nil, b), uint64(n)); err != nil {
		w.t.Fatal(err)
	}
	if err := w.log.Sync(); err != nil {
		w.t.Fatal(err)
	}
}

// load writes the seconds of counts starting at the bucket index first. Counts of
// 3 or more fire the rule; the watermark, 2 s behind, closes them as it goes.
func (w *world) load(first int, counts ...int) {
	w.t.Helper()
	for i, n := range counts {
		w.second(int64(first+i)*1000, n)
	}
}

func (w *world) replica(id string, n *Notifier) *Replica {
	w.t.Helper()
	r, err := NewReplica(w.cfg, id, w.set, Deps{FS: w.mem, Clock: w.clk, Rand: w.rnd, ExportDir: exportDir}, n)
	if err != nil {
		w.t.Fatal(err)
	}
	return r
}

// truncateLog closes the log and cuts its only segment at size, as a recovery of
// serve does with a broken tail.
func (w *world) truncateLog(size int64) {
	w.t.Helper()
	if err := w.log.Close(); err != nil {
		w.t.Fatal(err)
	}
	f, err := w.mem.OpenAppend(path.Join(logPath, fmt.Sprintf("%020d.seg", 0)))
	if err != nil {
		w.t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		w.t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		w.t.Fatal(err)
	}
}

// readAll reads a file of the disk.
func (w *world) readAll(name string) string {
	w.t.Helper()
	f, err := w.mem.Open(name)
	if err != nil {
		w.t.Fatal(err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		w.t.Fatal(err)
	}
	data := make([]byte, size)
	if _, err := f.ReadAt(data, 0); err != nil && err != io.EOF {
		w.t.Fatal(err)
	}
	return string(data)
}

// exported returns the sorted distinct lines of a file of the export of the runs
// of evaluator id, as the scripts of §4.8 would merge them.
func (w *world) exported(id, file string) []string {
	w.t.Helper()
	runs, err := w.mem.ReadDir(exportDir)
	if err != nil {
		w.t.Fatal(err)
	}
	var lines []string
	for _, run := range runs {
		if strings.HasPrefix(run, id+"-") {
			for _, line := range strings.Split(w.readAll(path.Join(exportDir, run, file)), "\n") {
				if line != "" {
					lines = append(lines, line)
				}
			}
		}
	}
	slices.Sort(lines)
	return slices.Compact(lines)
}

func drain(t *testing.T, r *Replica) {
	t.Helper()
	for {
		n, err := r.Step(stepRecords)
		if err != nil {
			t.Fatal(err)
		}
		if n < stepRecords {
			return
		}
	}
}

func outboxKeys(n *Notifier) []string {
	var out []string
	for _, e := range n.order {
		out = append(out, e.key)
	}
	return out
}

var alertCounts = []int{1, 5, 5, 1, 1, 4, 1, 3, 3, 3, 1, 1, 6, 1, 1, 1, 7, 1, 1, 1}

func TestReplicasComputeTheSame(t *testing.T) {
	w := newWorld(t)
	w.load(10, alertCounts...)
	ids := []string{"e1", "e2", "e3"}
	var keys [][]string
	for _, id := range ids {
		n := NewNotifier(id, w.clk, &fakeTransport{}, nil)
		r := w.replica(id, n)
		if _, err := r.Recover(); err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, outboxKeys(n))
	}
	if len(keys[0]) == 0 {
		t.Fatal("no notification from this load")
	}
	for i, id := range ids[1:] {
		if !slices.Equal(keys[i+1], keys[0]) {
			t.Errorf("keys of %s = %v, want %v", id, keys[i+1], keys[0])
		}
		for _, file := range []string{"evals.jsonl", "transitions.jsonl"} {
			if got, want := w.exported(id, file), w.exported("e1", file); !slices.Equal(got, want) {
				t.Errorf("%s of %s = %v, want %v", file, id, got, want)
			}
		}
	}
	lines := w.exported("e1", "transitions.jsonl")
	if !slices.Contains(lines, `{"rule":"high","group":"{}","t":11000,"state":"firing","value":5,"closed_at":`+closedAtOf(lines, 11000)+`}`) {
		t.Errorf("no firing line at 11000: %v", lines)
	}
}

// closedAtOf returns the closed_at of the first line at instant at.
func closedAtOf(lines []string, at int64) string {
	for _, line := range lines {
		if strings.Contains(line, fmt.Sprintf(`"t":%d,`, at)) {
			_, v, _ := strings.Cut(line, `"closed_at":`)
			return strings.TrimSuffix(v, "}")
		}
	}
	return ""
}

func TestExportLineWithAbsentGroup(t *testing.T) {
	var buf []byte
	buf = appendLineEnd(appendLineStart(buf, "r", `{a="1"}`, 5000), 0, false, 9)
	want := `{"rule":"r","group":"{a=\"1\"}","t":5000,"value":null,"closed_at":9}` + "\n"
	if string(buf) != want {
		t.Fatalf("line = %s, want %s", buf, want)
	}
}

func TestRecoveryMatchesARunWithoutACrash(t *testing.T) {
	// The run without a crash.
	ref := newWorld(t)
	ref.load(10, alertCounts...)
	refNotifier := NewNotifier("e1", ref.clk, &fakeTransport{}, nil)
	refReplica := ref.replica("e1", refNotifier)
	if _, err := refReplica.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := refReplica.Close(); err != nil {
		t.Fatal(err)
	}
	wantKeys := outboxKeys(refNotifier)
	wantTransitions := ref.exported("e1", "transitions.jsonl")
	wantEvals := ref.exported("e1", "evals.jsonl")
	if len(wantKeys) == 0 || len(wantTransitions) == 0 {
		t.Fatal("no alerts from this load")
	}

	// Every cut: a checkpoint at ckptAt, a crash at crashAt.
	for ckptAt := 1; ckptAt < len(alertCounts); ckptAt += 3 {
		crashAt := min(ckptAt+4, len(alertCounts))
		t.Run(fmt.Sprintf("checkpoint at %d, crash at %d", ckptAt, crashAt), func(t *testing.T) {
			w := newWorld(t)
			first := newTestNotifier("e1", w.clk)
			w.load(10, alertCounts[:ckptAt]...)
			r := w.replica("e1", first.Notifier)
			if _, err := r.Recover(); err != nil {
				t.Fatal(err)
			}
			// It leads and everything up to the checkpoint gets confirmed.
			first.LeaseGranted(w.clk.Monotonic(), Grant{Epoch: 1, Holder: "e1", TTLMs: ttlMs})
			first.Poll()
			confirmed := first.tr.keys()
			for _, key := range confirmed {
				first.Delivered(key, 1, ResultConfirmed)
			}
			if err := r.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			// What comes after the checkpoint is confirmed too, and the crash
			// forgets that.
			w.load(10+ckptAt, alertCounts[ckptAt:crashAt]...)
			drain(t, r)
			first.Poll()
			for _, key := range first.tr.keys() {
				first.Delivered(key, 1, ResultConfirmed)
			}
			w.mem.Crash(rng.New(5))
			w.openLog()

			second := newTestNotifier("e1", w.clk)
			r2 := w.replica("e1", second.Notifier)
			rep, err := r2.Recover()
			if err != nil {
				t.Fatal(err)
			}
			if !rep.FromCheckpoint {
				t.Fatalf("recovery did not use the checkpoint: %+v", rep)
			}
			w.load(10+crashAt, alertCounts[crashAt:]...)
			drain(t, r2)

			if got := outboxKeys(second.Notifier); !slices.Equal(got, wantKeys) {
				t.Errorf("keys = %v, want %v", got, wantKeys)
			}

			// Promoted, it resends everything but what the checkpoint saved as
			// confirmed: no key is lost and none of those goes out again.
			second.LeaseGranted(w.clk.Monotonic(), Grant{Epoch: 2, Holder: "e1", TTLMs: 60_000})
			for {
				before := len(second.tr.sent)
				second.Poll()
				for _, x := range second.tr.sent[before:] {
					second.Delivered(x.Key, 2, ResultConfirmed)
				}
				if len(second.tr.sent) == before {
					break
				}
			}
			resent := second.tr.keys()
			for _, key := range confirmed {
				if slices.Contains(resent, key) {
					t.Errorf("resent %s, confirmed before the checkpoint", key)
				}
			}
			all := slices.Concat(resent, confirmed)
			slices.Sort(all)
			want := slices.Sorted(slices.Values(wantKeys))
			if !slices.Equal(all, want) {
				t.Errorf("sent and confirmed = %v, want %v", all, want)
			}
			if err := r2.Close(); err != nil {
				t.Fatal(err)
			}
			if got := w.exported("e1", "transitions.jsonl"); !slices.Equal(got, wantTransitions) {
				t.Errorf("transitions = %v, want %v", got, wantTransitions)
			}
			if got := w.exported("e1", "evals.jsonl"); !slices.Equal(got, wantEvals) {
				t.Errorf("evals = %v, want %v", got, wantEvals)
			}
		})
	}
}

func TestReplicaWaitsForTheLog(t *testing.T) {
	w := newWorld(t)
	w.mem = vfs.NewMem()
	n := NewNotifier("e1", w.clk, &fakeTransport{}, nil)
	r := w.replica("e1", n)
	if rep, err := r.Recover(); err != nil || rep.FromCheckpoint {
		t.Fatalf("recover with no log: %+v, %v", rep, err)
	}
	if got, err := r.Step(stepRecords); err != nil || got != 0 {
		t.Fatalf("step with no log: %d, %v", got, err)
	}
	if err := r.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	w.openLog()
	w.load(10, alertCounts...)
	drain(t, r)
	if len(n.order) == 0 {
		t.Fatal("nothing read once the log appeared")
	}
}

func TestReplicaNoticesATruncatedLog(t *testing.T) {
	w := newWorld(t)
	w.load(10, alertCounts...)
	n := NewNotifier("e1", w.clk, &fakeTransport{}, nil)
	r := w.replica("e1", n)
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := r.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	w.truncateLog(r.tail.start)
	if _, err := r.Step(stepRecords); !errors.Is(err, wal.ErrRewound) {
		t.Fatalf("step after the truncation: %v", err)
	}
}

func TestCheckpointOverARewrittenRecordIsSkipped(t *testing.T) {
	w := newWorld(t)
	w.load(10, alertCounts...)
	n := NewNotifier("e1", w.clk, &fakeTransport{}, nil)
	r := w.replica("e1", n)
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := r.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	cut := r.tail.start
	if err := r.Release(); err != nil {
		t.Fatal(err)
	}
	w.truncateLog(cut)

	// Same size and place as the record that was cut, with other values: the
	// position of the checkpoint looks fine, but it read something else.
	w.openLog()
	last := alertCounts[len(alertCounts)-1]
	b := &event.Batch{Tenant: "a", Kind: event.KindMetrics, IngestMs: t0}
	for range last {
		b.Metrics = append(b.Metrics, event.Metric{Key: `m{}`, Value: 99, TsMs: int64(10+len(alertCounts)-1) * 1000})
	}
	if _, err := w.log.AppendBatch(event.AppendBatch(nil, b), uint64(last)); err != nil {
		t.Fatal(err)
	}
	if err := w.log.Sync(); err != nil {
		t.Fatal(err)
	}

	n2 := NewNotifier("e1", w.clk, &fakeTransport{}, nil)
	r2 := w.replica("e1", n2)
	rep, err := r2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if rep.FromCheckpoint || rep.Skipped != 1 {
		t.Fatalf("recovery = %+v, want the checkpoint skipped", rep)
	}
	if err := r2.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestBadEvaluatorID(t *testing.T) {
	w := newWorld(t)
	for _, id := range []string{"", "a/b"} {
		if _, err := NewReplica(w.cfg, id, w.set, Deps{FS: w.mem, Clock: w.clk, Rand: w.rnd}, NewNotifier(id, w.clk, &fakeTransport{}, nil)); err == nil {
			t.Errorf("id %q accepted", id)
		}
	}
}

func TestCheckpointOfOtherRulesIsAnError(t *testing.T) {
	w := newWorld(t)
	w.load(10, alertCounts...)
	r := w.replica("e1", NewNotifier("e1", w.clk, &fakeTransport{}, nil))
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	set, err := rules.Parse([]byte(`{"rules":[{"name":"other","tenant":"a","source":"metrics",
		"match":{"__name__":"m"},"agg":"count","range_ms":1000,"op":">=","threshold":3}]}`), 1000)
	if err != nil {
		t.Fatal(err)
	}
	w.set = set
	r = w.replica("e1", NewNotifier("e1", w.clk, &fakeTransport{}, nil))
	defer r.Release()
	if _, err := r.Recover(); err == nil {
		t.Fatal("checkpoint of other rules accepted")
	}
}
