// Package agg is the output of the sequencer: it splits non-late events
// among shards by series hash (D03), keeps the open buckets of each series
// and closes them as the tenant watermark reaches their end (§4.4). What it
// emits depends only on the log, not on the number of shards.
package agg

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/ingest"
)

const (
	// DefaultShards is the shard count of D03.
	DefaultShards = 4

	// queueJobs bounds the batches waiting at each shard and at the merger;
	// a full queue blocks the sequencer, nothing is dropped.
	queueJobs = 64
	// addOp is the step of an op that adds an event to a bucket.
	addOp = int32(-1)
)

// Ext is the per-bucket extension point for what K2 computes besides count
// and sum. It sees the events of its bucket in log order.
type Ext interface {
	Add(value float64, fields string)
	// AppendState appends the exact state for a checkpoint.
	AppendState(dst []byte) []byte
	// LoadState loads what AppendState wrote.
	LoadState(state []byte) error
}

// ExtFactory returns the extension of a new bucket of the series, or nil
// when the series has none.
type ExtFactory func(tenant, series string, kind event.Kind) Ext

// Config configures an Aggregator.
type Config struct {
	// BucketMs is S of §4.4.
	BucketMs int64
	// Shards is the number of shards; zero means DefaultShards.
	Shards int
	// NewExt may be nil.
	NewExt ExtFactory
}

// Bucket is a closed bucket. Subscribers share it and must not change it.
type Bucket struct {
	Tenant   string
	Series   string
	Kind     event.Kind
	End      int64
	Count    uint64
	Sum      float64
	Rate     float64
	ClosedAt uint64
	Ext      Ext
}

// Subscriber receives closed buckets and late events in an order that
// depends only on the log: by log position of what closed them and, for
// buckets closed together, by end and series key. All subscribers are
// called from one goroutine and must not block for long.
type Subscriber interface {
	Closed(b *Bucket)
	Late(tenant string, offset uint64)
}

// OpenBucket is the exact state of an open bucket in a checkpoint.
type OpenBucket struct {
	Tenant string
	Series string
	Kind   event.Kind
	End    int64
	Count  uint64
	Sum    float64
	Ext    []byte
}

// op is one step of a shard: add an event to a bucket, or close the buckets
// of steps[step].
type op struct {
	key    string
	fields string
	value  float64
	end    int64
	step   int32
	kind   event.Kind
}

type closeStep struct {
	tenant   string
	upTo     int64
	closedAt uint64
}

type lateEvent struct {
	tenant string
	offset uint64
	// step is how many close steps of the job come before the event.
	step int
}

type stepBucket struct {
	step int32
	b    *Bucket
}

// job is the work of one record. Each shard reads its ops and fills its
// closed list; the merger then emits the results in log order.
type job struct {
	tenant string
	ops    [][]op
	steps  []closeStep
	late   []lateEvent
	closed [][]stepBucket
	wg     sync.WaitGroup
	done   chan struct{}
}

// watermark is what the sink side knows of a tenant: the largest multiple
// of S that the watermark has reached, up to which everything is closed.
type watermark struct {
	defined bool
	closed  int64
}

// Aggregator is the ingest.Sink that feeds the shards. Like every core
// component it is a synchronous state machine: until Start the shards and
// the merger run inside each call, as the simulation needs, and between
// Start and Stop each runs in its own goroutine joined by bounded channels.
type Aggregator struct {
	bucketMs int64
	newExt   ExtFactory
	subs     []Subscriber
	shards   []*shard
	marks    map[string]*watermark
	scratch  []*Bucket
	merge    chan *job
	stopped  chan struct{}
}

var _ ingest.Sink = (*Aggregator)(nil)

// New returns an Aggregator with no subscribers.
func New(cfg Config) (*Aggregator, error) {
	if cfg.BucketMs <= 0 {
		return nil, errors.New("agg: bucket_ms must be positive")
	}
	if cfg.Shards < 0 {
		return nil, errors.New("agg: shards must not be negative")
	}
	if cfg.Shards == 0 {
		cfg.Shards = DefaultShards
	}
	a := &Aggregator{
		bucketMs: cfg.BucketMs,
		newExt:   cfg.NewExt,
		shards:   make([]*shard, cfg.Shards),
		marks:    map[string]*watermark{},
	}
	for i := range a.shards {
		a.shards[i] = &shard{
			index:     i,
			bucketSec: float64(cfg.BucketMs) / 1000,
			newExt:    cfg.NewExt,
			tenants:   map[string]*openTenant{},
		}
	}
	return a, nil
}

// Subscribe adds s to the receivers of what is emitted. Call it before the
// first event.
func (a *Aggregator) Subscribe(s Subscriber) {
	a.subs = append(a.subs, s)
}

// Start runs the shards and the merger in their own goroutines.
func (a *Aggregator) Start() {
	a.merge = make(chan *job, queueJobs)
	a.stopped = make(chan struct{})
	for _, s := range a.shards {
		in := make(chan *job, queueJobs)
		s.in = in
		go func() {
			for j := range in {
				s.run(j)
				j.wg.Done()
			}
		}()
	}
	go func() {
		for j := range a.merge {
			j.wg.Wait()
			a.emit(j)
		}
		close(a.stopped)
	}()
}

// Stop finishes the work in flight and ends the goroutines of Start. Later
// calls run inline again.
func (a *Aggregator) Stop() {
	if a.merge == nil {
		return
	}
	for _, s := range a.shards {
		close(s.in)
		s.in = nil
	}
	close(a.merge)
	<-a.stopped
	a.merge = nil
}

func (a *Aggregator) newJob(tenant string) *job {
	return &job{
		tenant: tenant,
		ops:    make([][]op, len(a.shards)),
		closed: make([][]stepBucket, len(a.shards)),
	}
}

func (a *Aggregator) submit(j *job) {
	if a.merge == nil {
		for _, s := range a.shards {
			s.run(j)
		}
		a.emit(j)
		return
	}
	j.wg.Add(len(a.shards))
	for _, s := range a.shards {
		s.in <- j
	}
	a.merge <- j
}

func (a *Aggregator) mark(tenant string) *watermark {
	m := a.marks[tenant]
	if m == nil {
		m = &watermark{}
		a.marks[tenant] = m
	}
	return m
}

// floorTo returns the largest multiple of s that is at most v.
func floorTo(v, s int64) int64 {
	q := v / s
	if v%s < 0 {
		q--
	}
	return q * s
}

// advance adds a close step when the watermark w reaches a new multiple of
// S; only then can a bucket end have been reached.
func (a *Aggregator) advance(j *job, tenant string, m *watermark, w int64, closedAt uint64) {
	if m.defined && w < m.closed+a.bucketMs {
		return
	}
	m.defined = true
	m.closed = floorTo(w, a.bucketMs)
	step := int32(len(j.steps))
	j.steps = append(j.steps, closeStep{tenant: tenant, upTo: m.closed, closedAt: closedAt})
	for i := range j.ops {
		j.ops[i] = append(j.ops[i], op{step: step})
	}
}

func (j *job) add(o op) {
	s := event.SeriesHash(j.tenant, o.key) % uint64(len(j.ops))
	j.ops[s] = append(j.ops[s], o)
}

// Batch implements ingest.Sink. An event enters its bucket before the
// watermark it raises can close that bucket.
func (a *Aggregator) Batch(c *ingest.CommittedBatch) {
	b := c.Batch
	j := a.newJob(b.Tenant)
	m := a.mark(b.Tenant)
	for i := range b.Len() {
		o := c.FirstOffset + uint64(i)
		switch {
		case c.Late[i]:
			j.late = append(j.late, lateEvent{tenant: b.Tenant, offset: o, step: len(j.steps)})
		case b.Kind == event.KindLogs:
			e := &b.Logs[i]
			j.add(op{key: e.Labels, fields: e.Fields, end: ingest.BucketEnd(e.TsMs, a.bucketMs), step: addOp, kind: b.Kind})
		default:
			e := &b.Metrics[i]
			j.add(op{key: e.Key, value: e.Value, end: ingest.BucketEnd(e.TsMs, a.bucketMs), step: addOp, kind: b.Kind})
		}
		a.advance(j, b.Tenant, m, c.Watermark[i], o)
	}
	a.submit(j)
}

// Tick implements ingest.Sink. The buckets a TICK closes get as closed_at
// the last event before it.
func (a *Aggregator) Tick(t *ingest.TickEffect) {
	if len(t.Advances) == 0 {
		return
	}
	j := a.newJob("")
	for _, adv := range t.Advances {
		a.advance(j, adv.Tenant, a.mark(adv.Tenant), adv.Watermark, t.NextOffset-1)
	}
	a.submit(j)
}

// emit sends the results of j to the subscribers in log order.
func (a *Aggregator) emit(j *job) {
	cursor := make([]int, len(j.closed))
	late := 0
	for k := range j.steps {
		for late < len(j.late) && j.late[late].step <= k {
			a.emitLate(j.late[late])
			late++
		}
		out := a.scratch[:0]
		for s, list := range j.closed {
			for cursor[s] < len(list) && list[cursor[s]].step == int32(k) {
				out = append(out, list[cursor[s]].b)
				cursor[s]++
			}
		}
		slices.SortFunc(out, func(x, y *Bucket) int {
			return cmp.Or(cmp.Compare(x.End, y.End), cmp.Compare(x.Series, y.Series))
		})
		for _, b := range out {
			for _, sub := range a.subs {
				sub.Closed(b)
			}
		}
		a.scratch = out
	}
	for _, l := range j.late[late:] {
		a.emitLate(l)
	}
	if j.done != nil {
		close(j.done)
	}
}

func (a *Aggregator) emitLate(l lateEvent) {
	for _, sub := range a.subs {
		sub.Late(l.tenant, l.offset)
	}
}

// Barrier returns once a marker has walked through every shard and the
// merger: all that was delivered before it is applied and emitted, and the
// shards stay idle until the caller, the goroutine that feeds the sink,
// delivers more.
func (a *Aggregator) Barrier() {
	j := a.newJob("")
	j.done = make(chan struct{})
	a.submit(j)
	<-j.done
}

// OpenBuckets returns the open buckets sorted by tenant, series and end, so
// the result does not depend on the number of shards. Call it after Barrier
// and from the goroutine that feeds the sink.
func (a *Aggregator) OpenBuckets() []OpenBucket {
	var out []OpenBucket
	for _, s := range a.shards {
		out = s.appendOpen(out)
	}
	slices.SortFunc(out, func(x, y OpenBucket) int {
		return cmp.Or(cmp.Compare(x.Tenant, y.Tenant), cmp.Compare(x.Series, y.Series), cmp.Compare(x.End, y.End))
	})
	return out
}

// Restore loads the state of a checkpoint into an Aggregator that has
// processed nothing: the open buckets, on the shards the current count
// assigns them to, and the tenant watermarks.
func (a *Aggregator) Restore(open []OpenBucket, watermarks []ingest.WatermarkAdvance) error {
	for _, w := range watermarks {
		a.marks[w.Tenant] = &watermark{defined: true, closed: floorTo(w.Watermark, a.bucketMs)}
	}
	for i := range open {
		ob := &open[i]
		s := a.shards[event.SeriesHash(ob.Tenant, ob.Series)%uint64(len(a.shards))]
		o := s.tenant(ob.Tenant).bucket(s, ob.Tenant, ob.Series, ob.End, ob.Kind)
		o.count = ob.Count
		o.sum = ob.Sum
		switch {
		case o.ext != nil:
			if err := o.ext.LoadState(ob.Ext); err != nil {
				return fmt.Errorf("agg: state of bucket %s %s %d: %w", ob.Tenant, ob.Series, ob.End, err)
			}
		case len(ob.Ext) > 0:
			return fmt.Errorf("agg: bucket %s %s %d has extension state but no extension", ob.Tenant, ob.Series, ob.End)
		}
	}
	return nil
}

// shard owns the open buckets of its series.
type shard struct {
	index     int
	bucketSec float64
	newExt    ExtFactory
	tenants   map[string]*openTenant
	in        chan *job
}

type openTenant struct {
	// ends lists the keys of byEnd in ascending order.
	ends  []int64
	byEnd map[int64]map[string]*openState
}

type openState struct {
	kind  event.Kind
	count uint64
	sum   float64
	ext   Ext
}

func (s *shard) tenant(id string) *openTenant {
	t := s.tenants[id]
	if t == nil {
		t = &openTenant{byEnd: map[int64]map[string]*openState{}}
		s.tenants[id] = t
	}
	return t
}

func (t *openTenant) bucket(s *shard, tenant, key string, end int64, kind event.Kind) *openState {
	series := t.byEnd[end]
	if series == nil {
		series = map[string]*openState{}
		t.byEnd[end] = series
		i, _ := slices.BinarySearch(t.ends, end)
		t.ends = slices.Insert(t.ends, i, end)
	}
	o := series[key]
	if o == nil {
		o = &openState{kind: kind}
		if s.newExt != nil {
			o.ext = s.newExt(tenant, key, kind)
		}
		series[key] = o
	}
	return o
}

// run applies the ops of j for this shard, in log order.
func (s *shard) run(j *job) {
	ops := j.ops[s.index]
	var t *openTenant
	for i := range ops {
		x := &ops[i]
		if x.step != addOp {
			j.closed[s.index] = s.closeUpTo(&j.steps[x.step], x.step, j.closed[s.index])
			continue
		}
		if t == nil {
			t = s.tenant(j.tenant)
		}
		o := t.bucket(s, j.tenant, x.key, x.end, x.kind)
		o.count++
		o.sum += x.value
		if o.ext != nil {
			o.ext.Add(x.value, x.fields)
		}
	}
}

// closeUpTo removes the buckets of st.tenant with end ≤ st.upTo and appends
// them to out.
func (s *shard) closeUpTo(st *closeStep, step int32, out []stepBucket) []stepBucket {
	t := s.tenants[st.tenant]
	if t == nil {
		return out
	}
	n := 0
	for n < len(t.ends) && t.ends[n] <= st.upTo {
		end := t.ends[n]
		for key, o := range t.byEnd[end] {
			out = append(out, stepBucket{step: step, b: s.closed(st, key, end, o)})
		}
		delete(t.byEnd, end)
		n++
	}
	t.ends = slices.Delete(t.ends, 0, n)
	return out
}

func (s *shard) closed(st *closeStep, key string, end int64, o *openState) *Bucket {
	rate := o.sum / s.bucketSec
	if o.kind == event.KindLogs {
		rate = float64(o.count) / s.bucketSec
	}
	return &Bucket{
		Tenant:   st.tenant,
		Series:   key,
		Kind:     o.kind,
		End:      end,
		Count:    o.count,
		Sum:      o.sum,
		Rate:     rate,
		ClosedAt: st.closedAt,
		Ext:      o.ext,
	}
}

func (s *shard) appendOpen(out []OpenBucket) []OpenBucket {
	for id, t := range s.tenants {
		for end, series := range t.byEnd {
			for key, o := range series {
				ob := OpenBucket{Tenant: id, Series: key, Kind: o.kind, End: end, Count: o.count, Sum: o.sum}
				if o.ext != nil {
					ob.Ext = o.ext.AppendState(nil)
				}
				out = append(out, ob)
			}
		}
	}
	return out
}
