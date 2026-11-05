package rules

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/CesarManzoCode/oteadero/internal/agg"
	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/ingest"
	"github.com/CesarManzoCode/oteadero/internal/sketch"
)

const tenantA = "a"

type capture struct {
	evals       []Eval
	transitions []Transition
}

func (c *capture) Eval(e *Eval)             { c.evals = append(c.evals, *e) }
func (c *capture) Transition(t *Transition) { c.transitions = append(c.transitions, *t) }

// states returns the transitions as "group T state".
func (c *capture) states() []string {
	var out []string
	for _, tr := range c.transitions {
		out = append(out, fmt.Sprintf("%s %d %s", tr.Group, tr.T, tr.State))
	}
	return out
}

// values returns the value per T of group, skipping absent evaluations.
func (c *capture) values(group string) map[int64]float64 {
	out := map[int64]float64{}
	for _, e := range c.evals {
		if e.Group == group && e.Present {
			out[e.T] = e.Value
		}
	}
	return out
}

func newEvaluator(t *testing.T, sketches map[string]agg.SketchConfig, rules ...string) (*Evaluator, *capture) {
	t.Helper()
	set, err := Parse(file(rules...), 1000)
	if err != nil {
		t.Fatal(err)
	}
	c := &capture{}
	e, err := NewEvaluator(set, sketches, c)
	if err != nil {
		t.Fatal(err)
	}
	return e, c
}

func metricBucket(series string, end int64, count uint64, sum float64) *agg.Bucket {
	return &agg.Bucket{Tenant: tenantA, Series: series, Kind: event.KindMetrics, End: end, Count: count, Sum: sum}
}

// step delivers buckets and then the watermark step that closed them.
func step(e *Evaluator, upTo int64, buckets ...*agg.Bucket) {
	for _, b := range buckets {
		e.Closed(b)
	}
	e.Advanced(tenantA, upTo, uint64(upTo))
}

// noAlert is a condition that never holds for the values of these tests.
const noAlert = `"op":">","threshold":1e12`

func TestWindowEdges(t *testing.T) {
	e, c := newEvaluator(t, nil, metricRule(`"agg":"count","range_ms":3000,`+noAlert))
	step(e, 0)
	for i, end := range []int64{1000, 2000, 3000, 4000} {
		step(e, end, metricBucket(`m{}`, end, 1<<i, 0))
	}
	step(e, 7000)
	// (T−R, T]: the bucket with end T is in, the one with end T−R is out.
	want := map[int64]float64{1000: 1, 2000: 3, 3000: 7, 4000: 14, 5000: 12, 6000: 8}
	if got := c.values(`{}`); !reflect.DeepEqual(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
	for _, ev := range c.evals {
		if ev.T == 7000 {
			t.Fatal("an absent inactive group was evaluated at 7000")
		}
	}
}

func TestOneStepCrossingMoreThanTheRange(t *testing.T) {
	e, c := newEvaluator(t, nil, metricRule(`"agg":"count","range_ms":2000,`+noAlert))
	step(e, 0)
	step(e, 5000,
		metricBucket(`m{}`, 1000, 1, 0),
		metricBucket(`m{}`, 2000, 2, 0),
		metricBucket(`m{}`, 3000, 4, 0),
		metricBucket(`m{}`, 4000, 8, 0),
	)
	want := map[int64]float64{1000: 1, 2000: 3, 3000: 6, 4000: 12, 5000: 8}
	if got := c.values(`{}`); !reflect.DeepEqual(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
}

func TestEvaluationInstants(t *testing.T) {
	e, c := newEvaluator(t, nil, metricRule(`"agg":"count","range_ms":4000,"interval_ms":2000,`+noAlert))
	// The step that defines the watermark evaluates nothing, even with data.
	step(e, 5000, metricBucket(`m{}`, 6000, 1, 0))
	if len(c.evals) != 0 {
		t.Fatalf("the first step evaluated: %+v", c.evals)
	}
	step(e, 5000+4000)
	var ts []int64
	var closedAt []uint64
	for _, ev := range c.evals {
		ts = append(ts, ev.T)
		closedAt = append(closedAt, ev.ClosedAt)
	}
	if !reflect.DeepEqual(ts, []int64{6000, 8000}) {
		t.Fatalf("instants = %v", ts)
	}
	if !reflect.DeepEqual(closedAt, []uint64{9000, 9000}) {
		t.Fatalf("closed_at = %v", closedAt)
	}
}

func TestSumOrderIsTheWholeWindow(t *testing.T) {
	e, c := newEvaluator(t, nil, metricRule(`"agg":"sum","range_ms":2000,"by":[],`+noAlert))
	step(e, 0)
	step(e, 2000,
		metricBucket(`m{s="a"}`, 1000, 1, 1e16),
		metricBucket(`m{s="b"}`, 1000, 1, 1),
		metricBucket(`m{s="a"}`, 2000, 1, 1),
		metricBucket(`m{s="b"}`, 2000, 1, 1),
	)
	// One by one, each 1 is lost against 1e16. Adding per end first would give
	// 1e16 + 2.
	if got := c.values(`{}`)[2000]; got != 1e16 {
		t.Fatalf("sum = %v, want 1e16", got)
	}
}

func TestGrouping(t *testing.T) {
	buckets := func() []*agg.Bucket {
		return []*agg.Bucket{
			metricBucket(`m{dc="x",pod="1"}`, 1000, 1, 0),
			metricBucket(`m{dc="x",pod="2"}`, 1000, 2, 0),
			metricBucket(`m{dc="y",pod="1"}`, 1000, 4, 0),
			metricBucket(`m{pod="9"}`, 1000, 8, 0),
			metricBucket(`other{dc="x",pod="1"}`, 1000, 16, 0),
		}
	}
	cases := []struct {
		name string
		rule string
		want map[string]float64
	}{
		{
			"series without name",
			metricRule(`"agg":"count","range_ms":1000,` + noAlert),
			map[string]float64{`{dc="x",pod="1"}`: 1, `{dc="x",pod="2"}`: 2, `{dc="y",pod="1"}`: 4, `{pod="9"}`: 8},
		},
		{
			"by label, missing label left out",
			metricRule(`"agg":"count","range_ms":1000,"by":["dc"],` + noAlert),
			map[string]float64{`{dc="x"}`: 3, `{dc="y"}`: 4, `{}`: 8},
		},
		{
			"by in any order is canonical",
			metricRule(`"agg":"count","range_ms":1000,"by":["pod","dc","pod"],` + noAlert),
			map[string]float64{`{dc="x",pod="1"}`: 1, `{dc="x",pod="2"}`: 2, `{dc="y",pod="1"}`: 4, `{pod="9"}`: 8},
		},
		{
			"empty by is one group",
			metricRule(`"agg":"count","range_ms":1000,"by":[],` + noAlert),
			map[string]float64{`{}`: 15},
		},
		{
			"without name in match, names merge",
			`{"name":"r","tenant":"a","source":"metrics","match":{"dc":"x","pod":"1"},"agg":"count","range_ms":1000,` + noAlert + `}`,
			map[string]float64{`{dc="x",pod="1"}`: 17},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, c := newEvaluator(t, nil, tc.rule)
			step(e, 0)
			step(e, 1000, buckets()...)
			got := map[string]float64{}
			for _, ev := range c.evals {
				got[ev.Group] = ev.Value
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("groups = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMatchIsExactOverEscapedValuesAndKind(t *testing.T) {
	e, c := newEvaluator(t, nil,
		`{"name":"r","tenant":"a","source":"metrics","match":{"__name__":"m","path":"a\"b"},"agg":"count","range_ms":1000,`+noAlert+`}`)
	step(e, 0)
	logs := &agg.Bucket{Tenant: tenantA, Series: `{path="a\"b"}`, Kind: event.KindLogs, End: 1000, Count: 8}
	otherTenant := metricBucket(`m{path="a\"b"}`, 1000, 16, 0)
	otherTenant.Tenant = "b"
	step(e, 1000,
		metricBucket(`m{path="a\"b"}`, 1000, 1, 0),
		metricBucket(`m{path="ab"}`, 1000, 2, 0),
		metricBucket(`m{}`, 1000, 4, 0),
		logs,
		otherTenant,
	)
	if got := c.values(`{path="a\"b"}`); !reflect.DeepEqual(got, map[int64]float64{1000: 1}) {
		t.Fatalf("values = %v, evals = %+v", got, c.evals)
	}
	if len(c.evals) != 1 {
		t.Fatalf("evals = %+v", c.evals)
	}
}

func TestAbsentGroupHasNullValueAndFalseCondition(t *testing.T) {
	e, c := newEvaluator(t, nil, metricRule(`"agg":"count","range_ms":1000,"op":">=","threshold":0`))
	step(e, 0)
	step(e, 1000, metricBucket(`m{}`, 1000, 1, 0))
	step(e, 2000)
	want := []string{`{} 1000 firing`, `{} 2000 resolved`}
	if got := c.states(); !reflect.DeepEqual(got, want) {
		t.Fatalf("transitions = %q, want %q", got, want)
	}
	tr := c.transitions[1]
	if tr.Present {
		t.Fatalf("resolved with a value: %+v", tr)
	}
	last := c.evals[len(c.evals)-1]
	if last.T != 2000 || last.Present {
		t.Fatalf("last eval = %+v", last)
	}
}

func TestForNeedsItsDurationAndAbsenceResetsIt(t *testing.T) {
	e, c := newEvaluator(t, nil, metricRule(`"agg":"count","range_ms":1000,"for_ms":2000,"op":">","threshold":0`))
	step(e, 0)
	present := func(end int64) *agg.Bucket { return metricBucket(`m{}`, end, 1, 0) }
	step(e, 1000, present(1000))
	step(e, 2000, present(2000))
	step(e, 3000) // absent, one instant before for_ms is met
	step(e, 4000, present(4000))
	step(e, 5000, present(5000))
	step(e, 6000, present(6000))
	step(e, 7000, present(7000))
	want := []string{
		`{} 1000 pending`,
		`{} 3000 inactive`,
		`{} 4000 pending`,
		`{} 6000 firing`,
	}
	if got := c.states(); !reflect.DeepEqual(got, want) {
		t.Fatalf("transitions = %q, want %q", got, want)
	}
	if c.transitions[1].Present {
		t.Fatalf("inactive from an absent group has a value: %+v", c.transitions[1])
	}
	if got := c.transitions[3].Incident; got != (Incident{Rule: "r", Group: `{}`, FiredAt: 6000}) {
		t.Fatalf("incident = %+v", got)
	}
}

func TestPendingFallsBackWhenTheConditionFails(t *testing.T) {
	e, c := newEvaluator(t, nil, metricRule(`"agg":"sum","range_ms":1000,"for_ms":5000,"op":">","threshold":5`))
	step(e, 0)
	step(e, 1000, metricBucket(`m{}`, 1000, 1, 6))
	step(e, 2000, metricBucket(`m{}`, 2000, 1, 1))
	want := []string{`{} 1000 pending`, `{} 2000 inactive`}
	if got := c.states(); !reflect.DeepEqual(got, want) {
		t.Fatalf("transitions = %q, want %q", got, want)
	}
	if v := c.transitions[1]; !v.Present || v.Value != 1 {
		t.Fatalf("inactive = %+v", v)
	}
}

// pipeline feeds a real aggregator as the sequencer does, with one event per
// batch and L = 2 s.
type pipeline struct {
	t       *testing.T
	a       *agg.Aggregator
	next    uint64
	maxTs   int64
	defined bool
}

func newPipeline(t *testing.T, sketches map[string]agg.SketchConfig, subs ...agg.Subscriber) *pipeline {
	t.Helper()
	a, err := agg.New(agg.Config{BucketMs: 1000, Shards: 2, Sketches: sketches})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range subs {
		a.Subscribe(s)
	}
	return &pipeline{t: t, a: a}
}

func (p *pipeline) watermark(ts int64) int64 {
	if !p.defined || ts > p.maxTs {
		p.maxTs = ts
	}
	p.defined = true
	return p.maxTs - ingest.DefaultLatenessMs
}

func (p *pipeline) send(b *event.Batch, ts int64) {
	p.a.Batch(&ingest.CommittedBatch{Batch: b, FirstOffset: p.next, Late: []bool{false}, Watermark: []int64{p.watermark(ts)}})
	p.next++
}

func (p *pipeline) metric(key string, v float64, ts int64) {
	p.send(&event.Batch{Tenant: tenantA, Kind: event.KindMetrics, Metrics: []event.Metric{{Key: key, Value: v, TsMs: ts}}}, ts)
}

func (p *pipeline) log(labels, fields string, ts int64) {
	p.send(&event.Batch{Tenant: tenantA, Kind: event.KindLogs, Logs: []event.Log{{TsMs: ts, Labels: labels, Fields: fields}}}, ts)
}

// TestKeepFiringForMeasuredCase reproduces the case measured with promtool and
// vmalert-tool in vault/RESEARCH.md: fires from 60 to 90 s and not at 100 s.
func TestKeepFiringForMeasuredCase(t *testing.T) {
	e, c := newEvaluator(t, nil, metricRule(
		`"agg":"sum","range_ms":30000,"interval_ms":10000,"for_ms":20000,"keep_firing_for_ms":30000,"op":">","threshold":10`))
	p := newPipeline(t, nil, e)
	values := []float64{0, 0, 5, 5, 5, 5, 5, 0, 0, 0, 0, 0, 0}
	for i, v := range values {
		p.metric(`m{}`, v, int64(i)*10000)
	}
	type row struct {
		T        int64
		State    string
		Value    float64
		ClosedAt uint64
	}
	var got []row
	for _, tr := range c.transitions {
		got = append(got, row{tr.T, tr.State, tr.Value, tr.ClosedAt})
	}
	// Each T is reached by the event at T+10 s, whose offset is T/10 s + 1.
	want := []row{
		{40000, StatePending, 15, 5},
		{60000, StateFiring, 15, 7},
		{100000, StateResolved, 0, 11},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("transitions = %+v, want %+v", got, want)
	}
	if got := c.transitions[2].Incident; got != (Incident{Rule: "r", Group: `{}`, FiredAt: 60000}) {
		t.Fatalf("resolved incident = %+v", got)
	}
}

func TestKeepFiringForRestartsWhenTheConditionReturns(t *testing.T) {
	e, c := newEvaluator(t, nil, metricRule(`"agg":"sum","range_ms":1000,"keep_firing_for_ms":2000,"op":">","threshold":5`))
	step(e, 0)
	high := func(end int64) *agg.Bucket { return metricBucket(`m{}`, end, 1, 9) }
	low := func(end int64) *agg.Bucket { return metricBucket(`m{}`, end, 1, 1) }
	step(e, 1000, high(1000))
	step(e, 2000, low(2000))
	step(e, 3000, high(3000)) // clears keepSince
	step(e, 4000, low(4000))
	step(e, 5000, low(5000))
	step(e, 6000, low(6000))
	want := []string{`{} 1000 firing`, `{} 6000 resolved`}
	if got := c.states(); !reflect.DeepEqual(got, want) {
		t.Fatalf("transitions = %q, want %q", got, want)
	}
}

func TestHysteresisHoldsBetweenTheThresholds(t *testing.T) {
	cases := []struct {
		name   string
		cond   string
		values []float64
		want   []string
	}{
		{
			"without resolve_threshold",
			`"op":">","threshold":10`,
			[]float64{12, 7, 4},
			[]string{`{} 1000 firing`, `{} 2000 resolved`},
		},
		{
			"above",
			`"op":">","threshold":10,"resolve_threshold":5`,
			[]float64{12, 7, 9, 5},
			[]string{`{} 1000 firing`, `{} 4000 resolved`},
		},
		{
			"above, at the resolve threshold with >=",
			`"op":">=","threshold":10,"resolve_threshold":5`,
			[]float64{10, 5, 4},
			[]string{`{} 1000 firing`, `{} 3000 resolved`},
		},
		{
			"below",
			`"op":"<","threshold":5,"resolve_threshold":10`,
			[]float64{3, 7, 9, 10},
			[]string{`{} 1000 firing`, `{} 4000 resolved`},
		},
		{
			"between the thresholds does not fire",
			`"op":">","threshold":10,"resolve_threshold":5`,
			[]float64{7, 9},
			nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, c := newEvaluator(t, nil, metricRule(`"agg":"sum","range_ms":1000,`+tc.cond))
			step(e, 0)
			for i, v := range tc.values {
				end := int64(i+1) * 1000
				step(e, end, metricBucket(`m{}`, end, 1, v))
			}
			if got := c.states(); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("transitions = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIdleGroupsLeaveAndAJumpEndsAtOnce(t *testing.T) {
	e, c := newEvaluator(t, nil, metricRule(`"agg":"count","range_ms":1000,"op":">","threshold":0`))
	step(e, 0)
	step(e, 1000, metricBucket(`m{}`, 1000, 1, 0))
	// A step this long would never end if it visited every instant.
	step(e, 1<<61)
	if len(e.rules[0].groups) != 0 {
		t.Fatalf("groups left: %v", e.rules[0].groups)
	}
	want := []string{`{} 1000 firing`, `{} 2000 resolved`}
	if got := c.states(); !reflect.DeepEqual(got, want) {
		t.Fatalf("transitions = %q, want %q", got, want)
	}
}

func TestAggregationsOverTheWindow(t *testing.T) {
	sketches := map[string]agg.SketchConfig{tenantA: {QuantileMetrics: []string{"m"}, CardinalityFields: []string{"user"}}}
	e, c := newEvaluator(t, sketches,
		`{"name":"sum","tenant":"a","source":"metrics","match":{"__name__":"m"},"agg":"sum","range_ms":2000,"by":[],`+noAlert+`}`,
		`{"name":"rate","tenant":"a","source":"metrics","match":{"__name__":"m"},"agg":"rate","range_ms":2000,"by":[],`+noAlert+`}`,
		`{"name":"q","tenant":"a","source":"metrics","match":{"__name__":"m"},"agg":"quantile","quantile":0.9,"range_ms":2000,"by":[],`+noAlert+`}`,
		`{"name":"lograte","tenant":"a","source":"logs","match":{"app":"x"},"agg":"rate","range_ms":2000,"by":[],`+noAlert+`}`,
		`{"name":"card","tenant":"a","source":"logs","match":{"app":"x"},"agg":"cardinality","field":"user","range_ms":2000,"by":[],`+noAlert+`}`,
	)
	p := newPipeline(t, sketches, e)
	dd := sketch.NewDDSketch()
	var hll sketch.HLL
	sum := 0.0
	for i := range 40 {
		ts := int64(1000 + 1000*(i%2)) // the buckets of 1000 and 2000
		v := float64(i*i + 1)
		p.metric(fmt.Sprintf(`m{pod="%d"}`, i%3), v, ts)
		dd.Add(v)
		sum += v
		user := fmt.Sprintf("u%d", i%7)
		p.log(`{app="x"}`, `{user="`+user+`"}`, ts)
		hll.Add(user)
	}
	p.log(`{app="x"}`, `{}`, 2000) // a sample with no field counts for the rate only
	p.metric(`unmatched{}`, 1, 4000)

	got := map[string]float64{}
	for _, ev := range c.evals {
		if ev.T == 2000 {
			got[ev.Rule] = ev.Value
		}
	}
	want := map[string]float64{
		"sum":     sum,
		"rate":    sum / 2,
		"q":       dd.Quantile(0.9),
		"lograte": 41.0 / 2,
		"card":    float64(hll.Estimate()),
	}
	// The events of a bucket reach it in another order than the sum above, but
	// these values are integers small enough to add exactly.
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("values at 2000 = %v, want %v", got, want)
	}
}

func TestQuantileOfOnlyNaNsIsPresentAndFalse(t *testing.T) {
	sketches := map[string]agg.SketchConfig{tenantA: {QuantileMetrics: []string{"m"}}}
	e, c := newEvaluator(t, sketches, metricRule(`"agg":"quantile","quantile":0.5,"range_ms":1000,"op":"<","threshold":1e12`))
	p := newPipeline(t, sketches, e)
	p.metric(`m{}`, math.NaN(), 1000)
	p.metric(`unmatched{}`, 1, 3000)
	if len(c.evals) == 0 {
		t.Fatal("nothing evaluated")
	}
	last := c.evals[len(c.evals)-1]
	if last.T != 1000 || !last.Present || !math.IsNaN(last.Value) {
		t.Fatalf("eval = %+v", last)
	}
	if len(c.transitions) != 0 {
		t.Fatalf("transitions = %+v", c.transitions)
	}
}

func TestNewEvaluatorRejectsSketchesTheAggregatorLacks(t *testing.T) {
	quantile := metricRule(`"agg":"quantile","quantile":0.5,"range_ms":1000,` + noAlert)
	unnamed := `{"name":"r","tenant":"a","source":"metrics","match":{},"agg":"quantile","quantile":0.5,"range_ms":1000,` + noAlert + `}`
	card := `{"name":"r","tenant":"a","source":"logs","match":{},"agg":"cardinality","field":"user","range_ms":1000,` + noAlert + `}`
	cfg := map[string]agg.SketchConfig{tenantA: {QuantileMetrics: []string{"m"}, CardinalityFields: []string{"user"}}}
	cases := []struct {
		name     string
		rule     string
		sketches map[string]agg.SketchConfig
		ok       bool
	}{
		{"quantile configured", quantile, cfg, true},
		{"quantile not configured", quantile, nil, false},
		{"quantile with no metric name", unnamed, cfg, false},
		{"cardinality configured", card, cfg, true},
		{"cardinality not configured", card, map[string]agg.SketchConfig{tenantA: {CardinalityFields: []string{"other"}}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			set, err := Parse(file(tc.rule), 1000)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := NewEvaluator(set, tc.sketches, &capture{}); (err == nil) != tc.ok {
				t.Fatalf("err = %v, want ok = %v", err, tc.ok)
			}
		})
	}
}

// fullRules exercises every aggregation with alerts that change state.
func fullRules() []string {
	return []string{
		`{"name":"count","tenant":"a","source":"metrics","match":{"__name__":"m"},"agg":"count","range_ms":3000,"by":["pod"],"for_ms":1000,"keep_firing_for_ms":2000,"op":">","threshold":4}`,
		`{"name":"sum","tenant":"a","source":"metrics","match":{"__name__":"m"},"agg":"sum","range_ms":2000,"interval_ms":2000,"op":">","threshold":10,"resolve_threshold":2}`,
		`{"name":"rate","tenant":"a","source":"logs","match":{},"agg":"rate","range_ms":2000,"op":">=","threshold":1}`,
		`{"name":"q","tenant":"a","source":"metrics","match":{"__name__":"m"},"agg":"quantile","quantile":0.5,"range_ms":4000,"op":">","threshold":3,"for_ms":2000}`,
		`{"name":"card","tenant":"a","source":"logs","match":{},"agg":"cardinality","field":"user","range_ms":3000,"by":[],"op":">","threshold":2}`,
	}
}

func driveFull(p *pipeline, from, to int) {
	for i := from; i < to; i++ {
		ts := int64(i) * 300
		switch {
		case i%40 < 25:
			p.metric(fmt.Sprintf(`m{pod="%d"}`, i%3), float64(i%11), ts)
		default:
			p.log(fmt.Sprintf(`{app="%d"}`, i%2), fmt.Sprintf(`{user="u%d"}`, i%5), ts)
		}
	}
}

func TestStateRoundTrip(t *testing.T) {
	sketches := map[string]agg.SketchConfig{tenantA: {QuantileMetrics: []string{"m"}, CardinalityFields: []string{"user"}}}
	e1, c1 := newEvaluator(t, sketches, fullRules()...)
	p1 := newPipeline(t, sketches, e1)
	const mid, end = 500, 1000
	driveFull(p1, 0, mid)
	if len(c1.transitions) == 0 {
		t.Fatal("the first half made no transitions")
	}

	state := e1.AppendState(nil)
	e2, c2 := newEvaluator(t, sketches, fullRules()...)
	if err := e2.LoadState(state); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(e2.AppendState(nil), state) {
		t.Fatal("the state changed in the round trip")
	}

	p2 := newPipeline(t, sketches, e2)
	w := p1.maxTs - ingest.DefaultLatenessMs
	if err := p2.a.Restore(p1.a.OpenBuckets(), []ingest.WatermarkAdvance{{Tenant: tenantA, Watermark: w}}); err != nil {
		t.Fatal(err)
	}
	p2.next, p2.maxTs, p2.defined = p1.next, p1.maxTs, p1.defined

	c1.evals, c1.transitions = nil, nil
	driveFull(p1, mid, end)
	driveFull(p2, mid, end)
	if len(c1.evals) == 0 || len(c1.transitions) == 0 {
		t.Fatalf("the second half made %d evals and %d transitions", len(c1.evals), len(c1.transitions))
	}
	if !evalsEqual(c1.evals, c2.evals) {
		t.Fatalf("evals differ after the round trip:\n%+v\n%+v", c1.evals, c2.evals)
	}
	if !reflect.DeepEqual(c1.transitions, c2.transitions) {
		t.Fatalf("transitions differ after the round trip:\n%+v\n%+v", c1.transitions, c2.transitions)
	}
	if !bytes.Equal(e1.AppendState(nil), e2.AppendState(nil)) {
		t.Fatal("the final states differ")
	}
}

// evalsEqual compares by bits, since a quantile value may be NaN.
func evalsEqual(a, b []Eval) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if math.Float64bits(x.Value) != math.Float64bits(y.Value) {
			return false
		}
		x.Value, y.Value = 0, 0
		if x != y {
			return false
		}
	}
	return true
}

func TestStateKeepsFiringGroupAndItsIncident(t *testing.T) {
	rule := metricRule(`"agg":"count","range_ms":1000,"keep_firing_for_ms":3000,"op":">","threshold":0`)
	e1, _ := newEvaluator(t, nil, rule)
	step(e1, 0)
	step(e1, 1000, metricBucket(`m{}`, 1000, 1, 0))
	step(e1, 2000) // keepSince = 2000

	e2, c2 := newEvaluator(t, nil, rule)
	if err := e2.LoadState(e1.AppendState(nil)); err != nil {
		t.Fatal(err)
	}
	step(e2, 4000)
	if len(c2.transitions) != 0 {
		t.Fatalf("resolved early: %+v", c2.transitions)
	}
	step(e2, 5000)
	if got := c2.states(); !reflect.DeepEqual(got, []string{`{} 5000 resolved`}) {
		t.Fatalf("transitions = %q", got)
	}
	if got := c2.transitions[0].Incident.FiredAt; got != 1000 {
		t.Fatalf("T_f = %d", got)
	}
}

func TestLoadStateRejects(t *testing.T) {
	rule := metricRule(`"agg":"sum","range_ms":2000,"op":">","threshold":0`)
	e, _ := newEvaluator(t, nil, rule)
	step(e, 0)
	step(e, 1000, metricBucket(`m{a="1"}`, 1000, 1, 2), metricBucket(`m{a="2"}`, 1000, 1, 3))
	state := e.AppendState(nil)

	for n := range len(state) {
		e2, _ := newEvaluator(t, nil, rule)
		if err := e2.LoadState(state[:n]); err == nil {
			t.Fatalf("accepted the state cut at %d of %d", n, len(state))
		}
		if len(e2.rules[0].groups) != 0 {
			t.Fatalf("a rejected state left groups at %d", n)
		}
	}
	e2, _ := newEvaluator(t, nil, rule)
	if err := e2.LoadState(append(bytes.Clone(state), 0)); err == nil {
		t.Fatal("accepted trailing bytes")
	}
	other, _ := newEvaluator(t, nil, metricRule(`"agg":"sum","range_ms":3000,"op":">","threshold":0`))
	if err := other.LoadState(state); err == nil {
		t.Fatal("accepted the state of other rules")
	}
}
