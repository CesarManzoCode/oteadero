package sim

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

// pinger is the toy process: it appends a line per tick to its journal,
// syncs about half of the time, pings its peer and answers pings with pongs.
// It rebuilds its tick count from the journal at every start.
type pinger struct {
	env   *Env
	peer  string
	f     vfs.File
	ticks int
}

func startPinger(peer string) Factory {
	return func(env *Env) Process {
		p := &pinger{env: env, peer: peer}
		fsys := env.FS()
		f, err := fsys.OpenAppend("journal")
		if errors.Is(err, fs.ErrNotExist) {
			f, err = fsys.Create("journal")
			if err == nil {
				err = fsys.SyncDir(".")
			}
		}
		if err != nil {
			panic(err)
		}
		size, _ := f.Size()
		buf := make([]byte, size)
		f.ReadAt(buf, 0)
		p.ticks = bytes.Count(buf, []byte("\n"))
		p.f = f
		p.schedule()
		return p
	}
}

func (p *pinger) schedule() {
	d := time.Duration(50+p.env.Rand().IntN(50)) * time.Millisecond
	p.env.After(d, "tick", p.tick)
}

func (p *pinger) tick() {
	p.ticks++
	line := fmt.Sprintf("%s %d %d\n", p.env.Node(), p.ticks, p.env.Clock().NowMs())
	p.f.Write([]byte(line))
	if p.env.Rand().IntN(2) == 0 {
		p.f.Sync()
	}
	p.env.Send(p.peer, "ping", []byte(line))
	p.schedule()
}

func (p *pinger) Receive(m Message) {
	if m.Kind == "ping" {
		p.env.Send(m.From, "pong", m.Payload)
	}
}

var toyNet = NetConfig{
	MinDelay:     time.Millisecond,
	MaxDelay:     20 * time.Millisecond,
	Loss:         0.05,
	Reorder:      0.1,
	ReorderDelay: 30 * time.Millisecond,
}

// runToy runs two pingers for 10 s of virtual time with seeded crashes, a
// partition, a pause and a skewed clock.
func runToy(seed uint64) *Sim {
	s := New(Config{Seed: seed, EpochMs: 1_700_000_000_000, Net: toyNet})
	s.AddNode(NodeConfig{Name: "a", Start: startPinger("b")})
	s.AddNode(NodeConfig{Name: "b", Start: startPinger("a"), ClockOffset: 40 * time.Millisecond, DriftPPM: 200})
	s.Start("a", 0)
	s.Start("b", 0)
	r := s.Rand()
	ca := time.Second + time.Duration(r.IntN(2000))*time.Millisecond
	s.Crash("a", ca)
	s.Start("a", ca+time.Duration(100+r.IntN(400))*time.Millisecond)
	s.Partition("a", "b", 4*time.Second, 5*time.Second)
	s.Pause("b", 6*time.Second, 6500*time.Millisecond)
	cb := 7*time.Second + time.Duration(r.IntN(1000))*time.Millisecond
	s.Crash("b", cb)
	s.Start("b", cb+300*time.Millisecond)
	s.RunUntil(10 * time.Second)
	return s
}

func TestSameSeedSameRecord(t *testing.T) {
	x, y := runToy(42), runToy(42)
	if !bytes.Equal(x.Record(), y.Record()) {
		t.Fatal("two runs with the same seed wrote different records")
	}
	if x.Hash() != y.Hash() {
		t.Fatalf("same seed, hashes %s and %s", x.Hash(), y.Hash())
	}
	if x.Stats() != y.Stats() {
		t.Fatalf("same seed, stats %+v and %+v", x.Stats(), y.Stats())
	}
	rec := string(x.Record())
	for _, kind := range []string{"tick", "ping", "pong", KindCrash, KindPause, KindResume} {
		if !strings.Contains(rec, " kind="+kind+" ") {
			t.Errorf("record has no %s line", kind)
		}
	}
	st := x.Stats()
	if st.Lost == 0 || st.Partitioned == 0 || st.Down == 0 || st.Reordered == 0 {
		t.Errorf("toy run did not exercise every fault: %+v", st)
	}
}

func TestDifferentSeedsDifferentRecords(t *testing.T) {
	x, y := runToy(1), runToy(2)
	if bytes.Equal(x.Record(), y.Record()) {
		t.Fatal("seeds 1 and 2 wrote the same record")
	}
	if x.Hash() == y.Hash() {
		t.Fatalf("seeds 1 and 2 have the same hash %s", x.Hash())
	}
}

var lineRE = regexp.MustCompile(`^t=(\d+) node=([a-z]+) kind=(\S+) h=([0-9a-f]{16})$`)

func TestRecordFormatAndHash(t *testing.T) {
	s := runToy(7)
	rec := s.Record()
	sum := sha256.Sum256(rec)
	if got := hex.EncodeToString(sum[:]); got != s.Hash() {
		t.Fatalf("Hash = %s, SHA-256 of the record = %s", s.Hash(), got)
	}
	lines := strings.Split(strings.TrimSuffix(string(rec), "\n"), "\n")
	if uint64(len(lines)) != s.Stats().Delivered {
		t.Fatalf("%d lines, Delivered = %d", len(lines), s.Stats().Delivered)
	}
	last := int64(-1)
	for i, l := range lines {
		m := lineRE.FindStringSubmatch(l)
		if m == nil {
			t.Fatalf("line %d %q does not match the record format", i, l)
		}
		ms, _ := strconv.ParseInt(m[1], 10, 64)
		if ms < last {
			t.Fatalf("line %d goes back in time: %d after %d", i, ms, last)
		}
		last = ms
	}
	empty := sha256.Sum256(nil)
	if want := "t=0 node=a kind=sim.start h=" + hex.EncodeToString(empty[:8]); lines[0] != want {
		t.Fatalf("first line %q, want %q", lines[0], want)
	}
}

// probe is a process that only records what it receives.
type probe struct{ got *[]Message }

func (p probe) Receive(m Message) { *p.got = append(*p.got, m) }

func TestCrashLosesExactlyTheUnsynced(t *testing.T) {
	s := New(Config{Seed: 3})
	var starts int
	var afterRestart, sharedBefore string
	var lateFired bool
	s.AddNode(NodeConfig{Name: "w", Start: func(env *Env) Process {
		starts++
		fsys := env.FS()
		if starts == 1 {
			f, _ := fsys.Create("data")
			f.Write([]byte("synced"))
			f.Sync()
			fsys.SyncDir(".")
			f.Write([]byte("-lost1"))
			f.Write([]byte("-lost2"))
			env.After(15*time.Millisecond, "late", func() { lateFired = true })
		} else {
			afterRestart = readFile(t, fsys, "data")
		}
		return probe{got: new([]Message)}
	}})
	s.AddNode(NodeConfig{Name: "e", Shared: "w", Start: func(env *Env) Process {
		env.After(5*time.Millisecond, "read", func() {
			sharedBefore = readFile(t, env.Shared(), "data")
			if _, err := env.Shared().Create("x"); !errors.Is(err, fs.ErrPermission) {
				t.Errorf("create on the shared disk: %v", err)
			}
		})
		return probe{got: new([]Message)}
	}})
	s.Start("w", 0)
	s.Start("e", 0)
	s.Crash("w", 10*time.Millisecond)
	s.Start("w", 20*time.Millisecond)
	s.RunUntil(time.Second)

	if sharedBefore != "synced" {
		t.Errorf("shared disk before the crash = %q, want only the synced bytes", sharedBefore)
	}
	if afterRestart != "synced" {
		t.Errorf("disk after the crash = %q, want exactly the synced bytes", afterRestart)
	}
	if starts != 2 {
		t.Errorf("factory ran %d times, want 2", starts)
	}
	if lateFired || s.Stats().Stale != 1 {
		t.Errorf("timer of the crashed process: fired %v, stale %d", lateFired, s.Stats().Stale)
	}
}

func TestCrashMayTearOnlyTheLastWrite(t *testing.T) {
	torn := false
	for seed := range uint64(32) {
		s := New(Config{Seed: seed})
		var got string
		s.AddNode(NodeConfig{Name: "w", Start: func(env *Env) Process {
			fsys := env.FS()
			if f, err := fsys.OpenAppend("data"); err == nil {
				got = readFile(t, fsys, "data")
				f.Close()
			} else {
				f, _ := fsys.Create("data")
				f.Write([]byte("synced"))
				f.Sync()
				fsys.SyncDir(".")
				f.Write([]byte("-tail"))
			}
			return probe{got: new([]Message)}
		}})
		s.Start("w", 0)
		s.Crash("w", time.Millisecond)
		s.Start("w", 2*time.Millisecond)
		s.RunUntil(3 * time.Millisecond)
		full := "synced-tail"
		if len(got) < len("synced") || got != full[:len(got)] {
			t.Fatalf("seed %d: after crash %q is not the synced bytes plus part of the last write", seed, got)
		}
		torn = torn || len(got) != len("synced") && len(got) != len(full)
	}
	if !torn {
		t.Error("no seed tore the last write")
	}
}

func readFile(t *testing.T, fsys vfs.FS, name string) string {
	t.Helper()
	f, err := fsys.Open(name)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	defer f.Close()
	size, _ := f.Size()
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	return string(buf)
}

// greeter sends hello to every peer at 10 ms and at 1500 ms.
func greeter(peers []string, got *[]Message) Factory {
	return func(env *Env) Process {
		hello := func() {
			for _, p := range peers {
				env.Send(p, "hello", []byte(env.Node()))
			}
		}
		env.After(10*time.Millisecond, "send", hello)
		env.After(1500*time.Millisecond, "send", hello)
		return probe{got: got}
	}
}

func TestPartitionBlocksOnlyDeclaredPairs(t *testing.T) {
	s := New(Config{Seed: 9, Net: NetConfig{MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}})
	var got []Message
	names := []string{"a", "b", "c"}
	for _, n := range names {
		peers := slices.DeleteFunc(slices.Clone(names), func(p string) bool { return p == n })
		s.AddNode(NodeConfig{Name: n, Start: greeter(peers, &got)})
		s.Start(n, 0)
	}
	s.Partition("a", "b", 0, time.Second)
	s.RunUntil(100 * time.Millisecond)

	pairs := func() []string {
		var ps []string
		for _, m := range got {
			ps = append(ps, m.From+">"+m.To)
		}
		slices.Sort(ps)
		return ps
	}
	if want := []string{"a>c", "b>c", "c>a", "c>b"}; !slices.Equal(pairs(), want) {
		t.Fatalf("during the partition delivered %v, want %v", pairs(), want)
	}
	if s.Stats().Partitioned != 2 {
		t.Fatalf("Partitioned = %d, want 2", s.Stats().Partitioned)
	}

	got = nil
	s.RunUntil(2 * time.Second)
	if want := []string{"a>b", "a>c", "b>a", "b>c", "c>a", "c>b"}; !slices.Equal(pairs(), want) {
		t.Fatalf("after the partition delivered %v, want %v", pairs(), want)
	}
}

func TestPauseHoldsEventsAndClockRuns(t *testing.T) {
	s := New(Config{Seed: 5, Net: NetConfig{MinDelay: time.Millisecond, MaxDelay: time.Millisecond}})
	var got []Message
	var tickAt time.Duration
	s.AddNode(NodeConfig{Name: "p", Start: func(env *Env) Process {
		env.After(200*time.Millisecond, "tick", func() { tickAt = env.Clock().Monotonic() })
		return probe{got: &got}
	}})
	s.AddNode(NodeConfig{Name: "q", Start: func(env *Env) Process {
		env.After(150*time.Millisecond, "send", func() { env.Send("p", "m", []byte("x")) })
		return probe{got: new([]Message)}
	}})
	s.Start("p", 0)
	s.Start("q", 0)
	s.Pause("p", 100*time.Millisecond, 300*time.Millisecond)
	s.RunUntil(299 * time.Millisecond)
	if len(got) != 0 || tickAt != 0 {
		t.Fatalf("paused node processed events: %d messages, tick at %v", len(got), tickAt)
	}
	s.RunUntil(time.Second)
	if len(got) != 1 || tickAt != 300*time.Millisecond {
		t.Fatalf("after resume: %d messages, tick read monotonic %v", len(got), tickAt)
	}
	rec := string(s.Record())
	for _, want := range []string{
		"t=300 node=p kind=sim.resume ",
		"t=300 node=p kind=m ",
		"t=300 node=p kind=tick ",
	} {
		if !strings.Contains(rec, want) {
			t.Errorf("record lacks %q:\n%s", want, rec)
		}
	}
	if strings.Index(rec, "kind=m ") > strings.Index(rec, "kind=tick ") {
		t.Error("held events did not run in arrival order")
	}
}

func TestPauseThenCrashDropsHeldEvents(t *testing.T) {
	s := New(Config{Seed: 5, Net: NetConfig{MinDelay: time.Millisecond, MaxDelay: time.Millisecond}})
	var got []Message
	s.AddNode(NodeConfig{Name: "p", Start: func(env *Env) Process {
		env.After(200*time.Millisecond, "tick", func() { t.Error("held timer ran after a crash") })
		return probe{got: &got}
	}})
	s.AddNode(NodeConfig{Name: "q", Start: func(env *Env) Process {
		env.After(150*time.Millisecond, "send", func() { env.Send("p", "m", nil) })
		return probe{got: new([]Message)}
	}})
	s.Start("p", 0)
	s.Start("q", 0)
	s.Pause("p", 100*time.Millisecond, 300*time.Millisecond)
	s.Crash("p", 250*time.Millisecond)
	s.RunUntil(time.Second)
	if len(got) != 0 || s.Stats().Crashed != 2 {
		t.Fatalf("got %d messages, Crashed = %d, want 0 and 2", len(got), s.Stats().Crashed)
	}
}

func TestNodeClockOffsetAndDrift(t *testing.T) {
	const epoch = 1_700_000_000_000
	s := New(Config{Seed: 1, EpochMs: epoch})
	var mono time.Duration
	var wall int64
	s.AddNode(NodeConfig{Name: "c", ClockOffset: 250 * time.Millisecond, DriftPPM: 1000, Start: func(env *Env) Process {
		env.After(10*time.Second, "read", func() {
			mono, wall = env.Clock().Monotonic(), env.Clock().NowMs()
		})
		return probe{got: new([]Message)}
	}})
	s.Start("c", 0)
	s.RunUntil(20 * time.Second)
	if mono != 10010*time.Millisecond {
		t.Errorf("Monotonic = %v, want 10.01s", mono)
	}
	if wall != epoch+250+10010 {
		t.Errorf("NowMs = %d, want %d", wall, epoch+250+10010)
	}
}

func TestLinkIsFIFOWithoutReorder(t *testing.T) {
	s := New(Config{Seed: 11, Net: NetConfig{MinDelay: time.Millisecond, MaxDelay: 50 * time.Millisecond}})
	var got []Message
	s.AddNode(NodeConfig{Name: "r", Start: func(env *Env) Process { return probe{got: &got} }})
	s.AddNode(NodeConfig{Name: "s", Start: func(env *Env) Process {
		for i := range 100 {
			env.Send("r", "n", []byte(strconv.Itoa(i)))
		}
		return probe{got: new([]Message)}
	}})
	s.Start("r", 0)
	s.Start("s", time.Millisecond)
	s.RunUntil(time.Second)
	if len(got) != 100 {
		t.Fatalf("delivered %d of 100", len(got))
	}
	for i, m := range got {
		if string(m.Payload) != strconv.Itoa(i) {
			t.Fatalf("message %d arrived as %s", i, m.Payload)
		}
	}
}
