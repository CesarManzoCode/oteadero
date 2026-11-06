package ingest

import (
	"errors"
	"testing"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

type stubCheckpointer struct {
	snaps []Snapshot
	err   error
}

func (c *stubCheckpointer) Checkpoint(snap *Snapshot) error {
	c.snaps = append(c.snaps, *snap)
	return c.err
}

func TestBarrierSnapshotExcludesQueues(t *testing.T) {
	f := newFixture(t, config(open("a")))
	ck := &stubCheckpointer{}
	f.seq.SetCheckpointer(ck, time.Hour, 1<<40)

	f.submit(metrics("a", t0, t0+1000))
	f.flush()
	queued := f.submit(metrics("a", t0+2000))
	if err := f.seq.Barrier(); err != nil {
		t.Fatal(err)
	}
	snap := ck.snaps[0]
	if snap.Pos != f.seq.log.End() || snap.Pos.Next != 2 || snap.LogID != f.seq.log.ID() {
		t.Fatalf("position = %+v", snap.Pos)
	}
	if len(snap.Marks) != 1 || snap.Marks[0] != (Mark{"a", t0 + 1000}) {
		t.Fatalf("marks = %+v", snap.Marks)
	}
	st := snap.Tenants[0]
	if st.Received != 2 || st.Accepted != 2 || st.QueuedEvents != 0 {
		t.Fatalf("counters = %+v", st)
	}
	if got := f.stats("a"); got.QueuedEvents != 1 || got.Accepted != 3 {
		t.Fatalf("the barrier changed the live counters: %+v", got)
	}

	// Written but not yet committed: the barrier commits it first.
	f.poll()
	if queued.calls != 0 {
		t.Fatal("acked before the sync")
	}
	if err := f.seq.Barrier(); err != nil {
		t.Fatal(err)
	}
	if queued.calls != 1 || ck.snaps[1].Pos.Next != 3 || ck.snaps[1].Tenants[0].Accepted != 3 {
		t.Fatalf("calls %d, snapshot %+v", queued.calls, ck.snaps[1])
	}
}

func TestCheckpointDue(t *testing.T) {
	f := newFixture(t, config(open("a")))
	ck := &stubCheckpointer{}
	f.seq.SetCheckpointer(ck, 10*time.Second, 3)

	f.submit(metrics("a", t0, t0))
	f.flush()
	if len(ck.snaps) != 0 {
		t.Fatalf("%d checkpoints below the thresholds", len(ck.snaps))
	}
	f.submit(metrics("a", t0))
	f.flush()
	if len(ck.snaps) != 1 {
		t.Fatalf("%d checkpoints after the event threshold", len(ck.snaps))
	}
	if d, ok := f.seq.NextWake(); !ok || d > 10*time.Second {
		t.Fatalf("NextWake = %v, %v", d, ok)
	}
	f.clk.Advance(10 * time.Second)
	f.poll()
	if len(ck.snaps) != 2 {
		t.Fatalf("%d checkpoints after the interval", len(ck.snaps))
	}
}

func TestBarrierFailureStopsSequencer(t *testing.T) {
	f := newFixture(t, config(open("a")))
	boom := errors.New("boom")
	f.seq.SetCheckpointer(&stubCheckpointer{err: boom}, time.Hour, 1<<40)
	if err := f.seq.Barrier(); !errors.Is(err, boom) {
		t.Fatalf("Barrier = %v", err)
	}
	if err := f.seq.Poll(); !errors.Is(err, boom) {
		t.Fatalf("Poll = %v", err)
	}
}

func TestRestoreAndReplayUseLivePathWithoutAcks(t *testing.T) {
	f := newFixture(t, config(open("a"), open("gone")))
	ck := &stubCheckpointer{}
	f.seq.SetCheckpointer(ck, time.Hour, 1<<40)
	f.submit(metrics("gone", t0))
	f.submit(metrics("a", t0+10_000))
	f.flush()
	if err := f.seq.Barrier(); err != nil {
		t.Fatal(err)
	}
	box := f.submit(metrics("a", t0, t0+11_000))
	f.submit(metrics("gone", t0))
	f.flush()
	if box.calls != 1 {
		t.Fatalf("%d acks", box.calls)
	}
	snap := ck.snaps[0]

	l, _, err := wal.Open(f.fsys, logDir, rng.New(1), wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	sink := &recSink{}
	seq, err := New(config(open("a")), f.clk, l, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := seq.Restore(&snap); err != nil {
		t.Fatal(err)
	}
	r, err := wal.OpenReader(f.fsys, logDir, snap.Pos)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := seq.Replay(r); err != nil {
		t.Fatal(err)
	}
	if box.calls != 1 {
		t.Fatalf("replay acked again: %d acks", box.calls)
	}
	got := sink.batches()
	if len(got) != 2 || got[0].FirstOffset != 2 || !got[0].Late[0] || got[0].Late[1] || got[1].FirstOffset != 4 {
		t.Fatalf("replayed = %+v", got)
	}
	stats := seq.Stats()
	if len(stats) != 2 || stats[0].Tenant != "a" || stats[0].Accepted != 3 || stats[0].Late != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if stats[1].Tenant != "gone" || stats[1].Received != 2 || stats[1].Accepted != 2 {
		t.Fatalf("counters of a tenant no longer configured = %+v", stats[1])
	}
}

func TestReplayReproducesTick(t *testing.T) {
	f := newFixture(t, config(open("a")))
	f.submit(metrics("a", t0))
	f.flush()
	f.advance(10 * time.Second)
	f.advance(f.cfg.FsyncMaxDelay)

	l, _, err := wal.Open(f.fsys, logDir, rng.New(1), wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	sink := &recSink{}
	seq, err := New(config(open("a")), f.clk, l, sink)
	if err != nil {
		t.Fatal(err)
	}
	r, err := wal.OpenReader(f.fsys, logDir, wal.Pos{Byte: wal.HeaderSize})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := seq.Replay(r); err != nil {
		t.Fatal(err)
	}
	if len(sink.items) != len(f.sink.items) {
		t.Fatalf("replayed %d records, live delivered %d", len(sink.items), len(f.sink.items))
	}
	got, ok := sink.items[1].(*TickEffect)
	want := f.sink.items[1].(*TickEffect)
	if !ok || got.TsMs != want.TsMs || got.NextOffset != want.NextOffset || len(got.Advances) != 1 || got.Advances[0] != want.Advances[0] {
		t.Fatalf("tick = %+v, want %+v", got, want)
	}
}

func TestRestoreRejectsWhatDoesNotFit(t *testing.T) {
	f := newFixture(t, config(open("a")))
	ck := &stubCheckpointer{}
	f.seq.SetCheckpointer(ck, time.Hour, 1<<40)
	f.submit(metrics("a", t0))
	f.flush()
	if err := f.seq.Barrier(); err != nil {
		t.Fatal(err)
	}
	good := ck.snaps[0]

	foreign := good
	foreign.LogID++
	past := good
	past.Pos.Byte += 8
	ahead := good
	ahead.Pos.Next++
	for _, s := range []Snapshot{foreign, past, ahead} {
		if err := f.seq.Restore(&s); !errors.Is(err, ErrForeignSnapshot) {
			t.Errorf("Restore(%+v) = %v", s, err)
		}
	}
	other := good
	other.LatenessMs++
	if err := f.seq.Restore(&other); err == nil || errors.Is(err, ErrForeignSnapshot) {
		t.Errorf("Restore with another L = %v", err)
	}
	if err := f.seq.Restore(&good); err != nil {
		t.Errorf("Restore = %v", err)
	}
}

func TestReplayOfEmptyLog(t *testing.T) {
	fsys := vfs.NewMem()
	f := newFixtureFS(t, config(open("a")), fsys)
	r, err := wal.OpenReader(fsys, logDir, wal.Pos{Byte: wal.HeaderSize})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := f.seq.Replay(r); err != nil {
		t.Fatal(err)
	}
	if len(f.sink.items) != 0 {
		t.Fatalf("delivered %d items", len(f.sink.items))
	}
}
