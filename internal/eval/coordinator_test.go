package eval

import (
	"errors"
	"testing"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/agg"
	"github.com/CesarManzoCode/oteadero/internal/ckpt"
	"github.com/CesarManzoCode/oteadero/internal/clock"
	"github.com/CesarManzoCode/oteadero/internal/ingest"
	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

const (
	t0      = int64(1_700_000_000_000)
	logPath = "data/log"
	ttlMs   = 2000
)

// syncedLog is a LeaseLog whose fsync is instant.
type syncedLog struct {
	records []wal.Lease
}

func (l *syncedLog) SubmitLease(r wal.Lease, done func(error)) bool {
	l.records = append(l.records, r)
	done(nil)
	return true
}

// heldLog is a LeaseLog whose fsync happens when the test says.
type heldLog struct {
	closed bool
	done   []func(error)
}

func (l *heldLog) SubmitLease(_ wal.Lease, done func(error)) bool {
	if l.closed {
		return false
	}
	l.done = append(l.done, done)
	return true
}

// ask returns the decision of a coordinator whose log answers at once.
func ask(t *testing.T, c *Coordinator, holder string) Decision {
	t.Helper()
	var got *Decision
	c.Acquire(holder, func(d Decision) { got = &d })
	if got == nil {
		t.Fatalf("Acquire(%s) did not decide", holder)
	}
	return *got
}

func TestCoordinatorGrants(t *testing.T) {
	clk := clock.NewManual(t0)
	log := &syncedLog{}
	c := NewCoordinator(clk, log, ttlMs)

	check := func(holder string, granted bool, epoch uint64, want string) {
		t.Helper()
		d := ask(t, c, holder)
		if d.Err != nil || d.Granted != granted || d.Epoch != epoch || d.Holder != want {
			t.Fatalf("Acquire(%s) = %+v, want granted=%v epoch=%d holder=%s", holder, d, granted, epoch, want)
		}
	}
	check("e1", true, 1, "e1")
	clk.Advance(time.Second)
	check("e2", false, 0, "e1")
	check("e1", true, 1, "e1")

	// The renewal moved the expiry: at the first expiry e2 still has to wait.
	clk.Advance(1999 * time.Millisecond)
	check("e2", false, 0, "e1")
	clk.Advance(time.Millisecond)
	check("e2", true, 2, "e2")
	check("e1", false, 0, "e2")

	// The same holder renews an expired lease with no new epoch: nobody else
	// had it in between.
	clk.Advance(10 * time.Second)
	check("e2", true, 2, "e2")
	clk.Advance(10 * time.Second)
	check("e3", true, 3, "e3")

	var last uint64
	for _, r := range log.records {
		if r.Epoch < last {
			t.Fatalf("epochs went back in the log: %+v", log.records)
		}
		last = r.Epoch
	}
	if got := log.records[len(log.records)-1]; got.ExpiresMs != clk.NowMs()+ttlMs || got.Holder != "e3" {
		t.Fatalf("last record = %+v", got)
	}
}

func TestCoordinatorAnswersAfterTheFsync(t *testing.T) {
	log := &heldLog{}
	c := NewCoordinator(clock.NewManual(t0), log, ttlMs)
	var got []Decision
	c.Acquire("e1", func(d Decision) { got = append(got, d) })
	if len(got) != 0 {
		t.Fatalf("answered before the fsync: %+v", got)
	}
	// Meanwhile the lease is taken: another holder cannot get the same epoch.
	if d := ask(t, c, "e2"); d.Granted || d.Holder != "e1" {
		t.Fatalf("second holder: %+v", d)
	}
	log.done[0](nil)
	if len(got) != 1 || !got[0].Granted || got[0].Epoch != 1 || got[0].TTLMs != ttlMs {
		t.Fatalf("after the fsync: %+v", got)
	}

	failure := errors.New("disk gone")
	c.Acquire("e1", func(d Decision) { got = append(got, d) })
	log.done[1](failure)
	if d := got[1]; d.Granted || !errors.Is(d.Err, failure) {
		t.Fatalf("failed fsync: %+v", d)
	}

	log.closed = true
	if d := ask(t, c, "e1"); d.Granted || !errors.Is(d.Err, ErrClosing) {
		t.Fatalf("closing: %+v", d)
	}
}

// nopSink ignores what the sequencer commits.
type nopSink struct{}

func (nopSink) Batch(*ingest.CommittedBatch) {}
func (nopSink) Tick(*ingest.TickEffect)      {}

// leaseNode is a coordinator over a real log, recovered from whatever the log
// holds.
type leaseNode struct {
	log *wal.Log
	seq *ingest.Sequencer
	c   *Coordinator
}

func openLeaseNode(t *testing.T, mem *vfs.Mem, clk clock.Clock) *leaseNode {
	t.Helper()
	l, _, err := wal.Open(mem, logPath, rng.New(1), wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	seq, err := ingest.New(ingest.DefaultConfig(), clk, l, nopSink{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := wal.OpenReader(mem, logPath, wal.Pos{Byte: wal.HeaderSize})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := seq.Replay(r); err != nil {
		t.Fatal(err)
	}
	c := NewCoordinator(clk, seq, ttlMs)
	c.Restore(seq.LastLease())
	return &leaseNode{log: l, seq: seq, c: c}
}

// acquire asks for the lease and commits the record, which the group commit does
// once its delay has passed.
func (n *leaseNode) acquire(t *testing.T, clk *clock.Manual, holder string) Decision {
	t.Helper()
	var got *Decision
	n.c.Acquire(holder, func(d Decision) { got = &d })
	if got != nil {
		return *got
	}
	if err := n.seq.Poll(); err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("answered before the group commit: %+v", *got)
	}
	clk.Advance(ingest.DefaultFsyncMaxDelay)
	if err := n.seq.Poll(); err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("no answer after the group commit")
	}
	return *got
}

func TestEpochsAreDurable(t *testing.T) {
	mem := vfs.NewMem()
	clk := clock.NewManual(t0)
	n := openLeaseNode(t, mem, clk)
	if d := n.acquire(t, clk, "e1"); d.Epoch != 1 {
		t.Fatalf("first grant: %+v", d)
	}
	clk.Advance(3 * time.Second)
	if d := n.acquire(t, clk, "e2"); d.Epoch != 2 {
		t.Fatalf("second grant: %+v", d)
	}

	// A crash loses nothing that was answered, and the lease in force still
	// excludes others.
	mem.Crash(rng.New(3))
	n = openLeaseNode(t, mem, clk)
	if d := n.acquire(t, clk, "e3"); d.Granted || d.Holder != "e2" {
		t.Fatalf("after the crash, within the lease: %+v", d)
	}
	if d := n.acquire(t, clk, "e2"); d.Epoch != 2 {
		t.Fatalf("after the crash, same holder: %+v", d)
	}
	clk.Advance(3 * time.Second)
	if d := n.acquire(t, clk, "e3"); d.Epoch != 3 {
		t.Fatalf("after the crash, new holder: %+v", d)
	}

	// A closed log reopened by the book gives the same.
	if err := n.log.Close(); err != nil {
		t.Fatal(err)
	}
	n = openLeaseNode(t, mem, clk)
	clk.Advance(3 * time.Second)
	if d := n.acquire(t, clk, "e1"); d.Epoch != 4 {
		t.Fatalf("after the reopen: %+v", d)
	}
}

func TestEpochNotSyncedWasNeverGiven(t *testing.T) {
	mem := vfs.NewMem()
	clk := clock.NewManual(t0)
	n := openLeaseNode(t, mem, clk)
	if d := n.acquire(t, clk, "e1"); d.Epoch != 1 {
		t.Fatalf("first grant: %+v", d)
	}
	clk.Advance(3 * time.Second)
	answered := false
	n.c.Acquire("e2", func(Decision) { answered = true })
	if err := n.seq.Poll(); err != nil {
		t.Fatal(err)
	}
	mem.Crash(rng.New(3))
	if answered {
		t.Fatal("answered without an fsync")
	}
	// The record may or may not have survived; either way the next epoch is
	// not below what anyone was told.
	n = openLeaseNode(t, mem, clk)
	if d := n.acquire(t, clk, "e3"); d.Granted && d.Epoch < 2 {
		t.Fatalf("after the crash: %+v", d)
	}
}

func TestEpochSurvivesInTheCheckpoint(t *testing.T) {
	mem := vfs.NewMem()
	clk := clock.NewManual(t0)
	open := func() (*wal.Log, *ingest.Sequencer, *ckpt.Manager) {
		t.Helper()
		l, _, err := wal.Open(mem, logPath, rng.New(1), wal.Options{})
		if err != nil {
			t.Fatal(err)
		}
		a, err := agg.New(agg.Config{BucketMs: 1000, Shards: 1})
		if err != nil {
			t.Fatal(err)
		}
		seq, err := ingest.New(ingest.DefaultConfig(), clk, l, a)
		if err != nil {
			t.Fatal(err)
		}
		mgr := ckpt.New(mem, "data", a, ckpt.Options{})
		if _, err := mgr.Recover(seq); err != nil {
			t.Fatal(err)
		}
		return l, seq, mgr
	}
	l, seq, mgr := open()
	seq.SetCheckpointer(mgr, time.Hour, 1<<40)
	c := NewCoordinator(clk, seq, ttlMs)
	c.Restore(seq.LastLease())
	c.Acquire("e1", func(Decision) {})
	if err := seq.Poll(); err != nil {
		t.Fatal(err)
	}
	if err := seq.Barrier(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// The checkpoint position is past the record, so that a replay does not see
	// it: only the checkpoint can bring the epoch back.
	offsets, err := mgr.Offsets()
	if err != nil || len(offsets) != 1 {
		t.Fatalf("checkpoints: %v, %v", offsets, err)
	}
	st, err := mgr.Load(offsets[0])
	if err != nil {
		t.Fatal(err)
	}
	if st.Snapshot.Lease.Epoch != 1 || st.Snapshot.Lease.Holder != "e1" {
		t.Fatalf("lease in the checkpoint: %+v", st.Snapshot.Lease)
	}
	l, seq, _ = open()
	defer l.Close()
	if got := seq.LastLease(); got != st.Snapshot.Lease {
		t.Fatalf("recovered lease %+v, want %+v", got, st.Snapshot.Lease)
	}
}
