package sim

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/config"
	evt "github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/rules"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

// Nodes of the engine scenario.
const (
	serveNode = "serve"
	recvNode  = "recv"
	tailNode  = "tail"
)

var (
	evaluators = []string{"e1", "e2", "e3"}
	producers  = []struct{ node, tenant string }{{"p1", "t1"}, {"p2", "t2"}}
)

const (
	// scenarioEpochMs is the wall clock of the scenario at virtual time zero.
	scenarioEpochMs = 1_800_000_000_000
	// DefaultLoad is how long the producers send by default.
	DefaultLoad = 40 * time.Second
	// faultsFrom is when the first fault may happen, and faultsAfterLoad how
	// long after the load the last one may still last.
	faultsFrom      = 2 * time.Second
	faultsAfterLoad = 5 * time.Second
	// settle is how long the run goes on with every node up after the last
	// fault: the idle ticks close the last windows, the incidents resolve and
	// the notifications get through.
	settle = 20 * time.Second

	dataDir   = "data"
	exportDir = "export"
)

var logDir = path.Join(dataDir, "log")

// Options fixes a run of the engine scenario.
type Options struct {
	Seed   uint64
	Faults bool
	// Rules is the JSON rule file the evaluators load, rules/alerts.json in
	// otsim.
	Rules []byte
	// Load is how long the producers send; zero means DefaultLoad.
	Load time.Duration
}

// Result is what a run leaves: the record and its hash, the files otsim
// writes and the verdict of each invariant.
type Result struct {
	Hash   string
	Record []byte
	Stats  Stats
	// LogFiles are the synced segments of the log of serve, by name, in the
	// format of §4.3.
	LogFiles []File
	// Receiver is receiver.jsonl, one line per request (§4.7).
	Receiver []byte
	// Transitions is transitions.jsonl: the distinct lines that the
	// evaluators exported in all their runs (§4.8), by t and then by bytes.
	Transitions []byte
	Checks      []Check
}

// File is a file of a node's disk.
type File struct {
	Name string
	Data []byte
}

// OK reports whether every invariant holds.
func (r *Result) OK() bool {
	for _, c := range r.Checks {
		if c.Err != nil {
			return false
		}
	}
	return true
}

// scenario is the harness state of a run: what outlives the crashes of the
// processes, and the processes that run now.
type scenario struct {
	sim   *Sim
	opts  Options
	cfg   config.Config
	rules *rules.Set
	// burstSec is the second of the load whose first slice brings the burst
	// of logs of service api.
	burstSec int64

	serve *serveProc
	evals map[string]*evalProc
	roles map[string]role
	// crashedServe is set once serve crashes: from then on its counters miss
	// the drops since its last checkpoint (LIMITATIONS.md).
	crashedServe bool
	// arrived counts, by tenant, the events of the batches that reached a
	// running serve.
	arrived  map[string]uint64
	receiver []ReceiverLine
	tail     tailLog
	errs     []error
}

// role is what onRole last said of an evaluator.
type role struct {
	leader bool
	epoch  uint64
}

// tailLog is what the tail subscriber got over all its connections.
type tailLog struct {
	offsets []uint64
	refused []string
}

// Run runs the engine scenario: serve with its disk, three evaluators, the
// receiver, two producers and a tail subscriber, with the fault schedule the
// seed draws unless faults are off. An error means that a component of the
// engine failed or the scenario could not be built; the invariants are in the
// result.
func Run(opts Options) (*Result, error) {
	if opts.Load == 0 {
		opts.Load = DefaultLoad
	}
	if opts.Load < time.Second {
		return nil, errors.New("sim: the load lasts less than a second")
	}
	cfg := scenarioConfig()
	set, err := rules.Parse(opts.Rules, cfg.BucketMs)
	if err != nil {
		return nil, err
	}
	net := NetConfig{MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
	if opts.Faults {
		net = NetConfig{
			MinDelay:     time.Millisecond,
			MaxDelay:     25 * time.Millisecond,
			Loss:         0.01,
			Reorder:      0.02,
			ReorderDelay: 100 * time.Millisecond,
		}
	}
	s := New(Config{Seed: opts.Seed, EpochMs: scenarioEpochMs, Net: net})
	sc := &scenario{
		sim:     s,
		opts:    opts,
		cfg:     cfg,
		rules:   set,
		evals:   map[string]*evalProc{},
		roles:   map[string]role{},
		arrived: map[string]uint64{},
	}
	r := s.Rand()
	loadSecs := int64(opts.Load / time.Second)
	sc.burstSec = loadSecs/4 + int64(r.IntN(int(loadSecs/4)+1))

	s.AddNode(NodeConfig{Name: serveNode, Start: sc.startServe})
	s.AddNode(NodeConfig{Name: recvNode, Start: sc.startReceiver})
	for _, e := range evaluators {
		s.AddNode(NodeConfig{
			Name:        e,
			Start:       sc.startEvaluator,
			ClockOffset: time.Duration(r.IntN(101)-50) * time.Millisecond,
			DriftPPM:    int64(r.IntN(401) - 200),
			Shared:      serveNode,
		})
	}
	for _, p := range producers {
		s.AddNode(NodeConfig{
			Name:        p.node,
			Start:       sc.startProducer(p.tenant),
			ClockOffset: time.Duration(r.IntN(41)-20) * time.Millisecond,
		})
	}
	s.AddNode(NodeConfig{Name: tailNode, Start: sc.startTail})

	s.Start(serveNode, 0)
	s.Start(recvNode, 0)
	for i, e := range evaluators {
		s.Start(e, time.Duration(5*(i+1))*time.Millisecond)
	}
	for _, p := range producers {
		s.Start(p.node, 100*time.Millisecond)
	}
	s.Start(tailNode, 150*time.Millisecond)

	faultsTo := opts.Load + faultsAfterLoad
	if opts.Faults {
		sc.scheduleFaults(faultsFrom, faultsTo)
	}
	s.RunUntil(faultsTo + settle)
	return sc.finish()
}

// scenarioConfig is the configuration of serve and of the evaluators: small
// segments and frequent checkpoints, so that a short run rotates and recovers
// from checkpoints, t1 with the sketches of rules/alerts.json and t2 with a
// quota below what it sends.
func scenarioConfig() config.Config {
	cfg := config.Default()
	cfg.DataDir = dataDir
	cfg.CheckpointIntervalMs = 5000
	cfg.CheckpointEvents = 4000
	cfg.SegmentBytes = 64 << 10
	cfg.Tenants = []config.Tenant{
		{ID: "t1", QuotaEPS: 5000, QuantileMetrics: []string{"latency_ms"}, CardinalityFields: []string{"user"}},
		{ID: "t2", QuotaEPS: 40},
	}
	return cfg
}

// scheduleFaults draws the fault schedule in [from, to): crashes and restarts
// of serve and of the evaluators, pauses of the leader longer than its lease,
// and partitions of the leader from the coordinator and from the receiver.
// Crashes of one node never overlap, so each restart finds it down. Who leads
// is only known when the fault starts, so those faults are drawn now and aimed
// then.
func (sc *scenario) scheduleFaults(from, to time.Duration) {
	s := sc.sim
	r := s.rnd
	between := func(lo, hi time.Duration) time.Duration { return lo + s.span(hi-lo) }

	crashes := func(node string, n int, minDown, maxDown time.Duration) {
		t := from + between(0, 4*time.Second)
		for range n {
			down := between(minDown, maxDown)
			if t+down > to {
				return
			}
			s.Crash(node, t)
			s.Start(node, t+down)
			t += down + between(3*time.Second, 12*time.Second)
		}
	}
	crashes(serveNode, 1+r.IntN(2), 200*time.Millisecond, 1500*time.Millisecond)
	for _, e := range evaluators {
		crashes(e, r.IntN(3), 300*time.Millisecond, 3*time.Second)
	}

	lease := time.Duration(sc.cfg.LeaseTTLMs) * time.Millisecond
	for range 1 + r.IntN(2) {
		at := between(from, to-2*lease-time.Second)
		d := between(lease+500*time.Millisecond, 2*lease+time.Second)
		s.schedule(at, func() { s.Pause(sc.leader(), s.now, s.now+d) })
	}
	for _, peer := range []string{serveNode, recvNode} {
		for range 1 + r.IntN(2) {
			at := between(from, to-4*time.Second)
			d := between(time.Second, 4*time.Second)
			s.schedule(at, func() { s.Partition(sc.leader(), peer, s.now, s.now+d) })
		}
	}
}

// leader returns the evaluator that leads the highest epoch as far as its
// onRole said, which may have crashed since, or else one the PCG picks.
func (sc *scenario) leader() string {
	best, epoch := "", uint64(0)
	for _, e := range evaluators {
		if r := sc.roles[e]; r.leader && r.epoch > epoch {
			best, epoch = e, r.epoch
		}
	}
	if best == "" {
		best = evaluators[sc.sim.rnd.IntN(len(evaluators))]
	}
	return best
}

func (sc *scenario) fail(node string, err error) {
	sc.errs = append(sc.errs, fmt.Errorf("%s at %v: %w", node, sc.sim.now, err))
}

// finish closes the evaluators, gathers the evidence and checks the
// invariants.
func (sc *scenario) finish() (*Result, error) {
	for _, e := range evaluators {
		p := sc.evals[e]
		if !sc.sim.node(e).up || p == nil || p.core == nil {
			sc.fail(e, errors.New("not running at the end of the run"))
			continue
		}
		if err := p.core.Close(); err != nil {
			sc.fail(e, err)
		}
		p.core = nil
	}
	if !sc.sim.node(serveNode).up || sc.serve == nil || sc.serve.srv == nil {
		sc.fail(serveNode, errors.New("not running at the end of the run"))
	}
	if len(sc.errs) > 0 {
		return nil, errors.Join(sc.errs...)
	}

	res := &Result{Hash: sc.sim.Hash(), Record: sc.sim.Record(), Stats: sc.sim.Stats()}
	synced := sc.sim.Disk(serveNode).Synced()
	names, err := synced.ReadDir(logDir)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		data, err := readAll(synced, path.Join(logDir, name))
		if err != nil {
			return nil, err
		}
		res.LogFiles = append(res.LogFiles, File{Name: name, Data: data})
	}
	logged, tailOffsets, err := scanLog(synced)
	if err != nil {
		return nil, err
	}
	for _, l := range sc.receiver {
		line, err := json.Marshal(l)
		if err != nil {
			return nil, err
		}
		res.Receiver = append(append(res.Receiver, line...), '\n')
	}
	if res.Transitions, err = sc.transitions(); err != nil {
		return nil, err
	}

	ev := &Evidence{
		Receiver:      sc.receiver,
		TailOffsets:   sc.tail.offsets,
		TailRefused:   sc.tail.refused,
		LogOffsets:    tailOffsets,
		ArrivalsExact: !sc.crashedServe,
	}
	engine := map[string]Account{}
	for _, st := range sc.serve.srv.Sequencer().Stats() {
		a := Account{Tenant: st.Tenant, Received: st.Received, Accepted: st.Accepted}
		for _, n := range st.Dropped {
			a.Dropped += n
		}
		engine[st.Tenant] = a
	}
	var tenants []string
	for t := range engine {
		tenants = append(tenants, t)
	}
	for t := range sc.arrived {
		tenants = append(tenants, t)
	}
	slices.Sort(tenants)
	for _, t := range slices.Compact(tenants) {
		a := engine[t]
		a.Tenant = t
		a.Logged = logged[t]
		a.Arrived = sc.arrived[t]
		ev.Accounts = append(ev.Accounts, a)
	}
	res.Checks = CheckInvariants(ev)
	return res, nil
}

// scanLog reads the synced log of serve and returns the events of each tenant
// and the offsets the tail subscriber follows.
func scanLog(fsys vfs.FS) (map[string]uint64, []uint64, error) {
	r, err := wal.OpenReader(fsys, logDir, wal.Pos{Byte: wal.HeaderSize})
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()
	logged := map[string]uint64{}
	var offsets []uint64
	for {
		rec, err := r.Next()
		if err == io.EOF {
			return logged, offsets, nil
		}
		if err != nil {
			return nil, nil, err
		}
		if rec.Type != wal.TypeBatch {
			continue
		}
		b, err := evt.DecodeBatch(rec.Payload)
		if err != nil {
			return nil, nil, err
		}
		logged[b.Tenant] += rec.Events
		if b.Tenant == tailTenant && b.Kind == evt.KindLogs {
			for i := range rec.Events {
				offsets = append(offsets, rec.Offset+i)
			}
		}
	}
}

// transitions merges the transitions.jsonl of every export run of every
// evaluator into its distinct whole lines, by t and then by bytes.
func (sc *scenario) transitions() ([]byte, error) {
	type line struct {
		t    int64
		text []byte
	}
	seen := map[string]bool{}
	var lines []line
	for _, e := range evaluators {
		disk := sc.sim.Disk(e)
		runs, err := disk.ReadDir(exportDir)
		if err != nil {
			return nil, err
		}
		for _, run := range runs {
			data, err := readAll(disk, path.Join(exportDir, run, "transitions.jsonl"))
			if err != nil {
				return nil, err
			}
			for text := range bytes.Lines(data) {
				// A line without its newline is a write a crash cut.
				if !bytes.HasSuffix(text, []byte("\n")) || seen[string(text)] {
					continue
				}
				seen[string(text)] = true
				var x struct {
					T int64 `json:"t"`
				}
				if err := json.Unmarshal(text, &x); err != nil {
					return nil, fmt.Errorf("%s: %s: %w", e, run, err)
				}
				lines = append(lines, line{t: x.T, text: text})
			}
		}
	}
	slices.SortFunc(lines, func(a, b line) int {
		if c := cmp.Compare(a.t, b.t); c != 0 {
			return c
		}
		return bytes.Compare(a.text, b.text)
	})
	var out []byte
	for _, l := range lines {
		out = append(out, l.text...)
	}
	return out, nil
}

func readAll(fsys vfs.FS, name string) ([]byte, error) {
	f, err := fsys.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if _, err := f.ReadAt(data, 0); err != nil && err != io.EOF {
		return nil, err
	}
	return data, nil
}

// splitFS is the disk of an evaluator: the log directory is the synced disk of
// serve, read-only, and everything else is the evaluator's own disk.
type splitFS struct {
	own, log vfs.FS
}

var _ vfs.FS = splitFS{}

func (f splitFS) pick(name string) vfs.FS {
	name = path.Clean(name)
	if name == logDir || strings.HasPrefix(name, logDir+"/") {
		return f.log
	}
	return f.own
}

func (f splitFS) Create(name string) (vfs.File, error)     { return f.pick(name).Create(name) }
func (f splitFS) OpenAppend(name string) (vfs.File, error) { return f.pick(name).OpenAppend(name) }
func (f splitFS) Open(name string) (vfs.File, error)       { return f.pick(name).Open(name) }
func (f splitFS) Remove(name string) error                 { return f.pick(name).Remove(name) }
func (f splitFS) ReadDir(dir string) ([]string, error)     { return f.pick(dir).ReadDir(dir) }
func (f splitFS) MkdirAll(dir string) error                { return f.pick(dir).MkdirAll(dir) }
func (f splitFS) SyncDir(dir string) error                 { return f.pick(dir).SyncDir(dir) }

func (f splitFS) Rename(oldname, newname string) error {
	if f.pick(oldname) != f.pick(newname) {
		return fmt.Errorf("sim: rename %s to %s crosses disks", oldname, newname)
	}
	return f.pick(oldname).Rename(oldname, newname)
}
