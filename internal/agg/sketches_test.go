package agg

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/sketch"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

var testSketches = map[string]SketchConfig{
	"a": {QuantileMetrics: []string{"lat"}, CardinalityFields: []string{"user", "ip", "user"}},
}

// exportedLine is a line of buckets.jsonl; Q and Card stay nil when absent.
type exportedLine struct {
	Tenant string
	Series string
	End    int64
	Q      map[string]float64
	Card   map[string]uint64
}

func exportedLines(t *testing.T, text string) []exportedLine {
	t.Helper()
	var out []exportedLine
	for line := range strings.Lines(text) {
		var l exportedLine
		if err := json.Unmarshal([]byte(line), &l); err != nil {
			t.Fatalf("line %q: %v", line, err)
		}
		out = append(out, l)
	}
	return out
}

func TestExportHasSketchesOnlyInConfiguredSeries(t *testing.T) {
	r := newRigConfig(t, Config{BucketMs: 1000, Shards: 4, Sketches: testSketches}, 0)
	mem := vfs.NewMem()
	e, err := NewExport(mem, "exp", "r1")
	if err != nil {
		t.Fatal(err)
	}
	r.agg.Subscribe(e)

	var values []float64
	var lat []sample
	for i := 1; i <= 1000; i++ {
		values = append(values, float64(i))
		lat = append(lat, sample{`lat{svc="x"}`, base, float64(i)})
	}
	r.send(metrics("a", append(lat, sample{"other{}", base, 1})...))
	r.send(metrics("b", sample{"lat{}", base, 1}))
	r.send(&event.Batch{Tenant: "a", Kind: event.KindLogs, Logs: []event.Log{
		{TsMs: base, Labels: `{app="x"}`, Fields: `{ip="1",other="9",user="u1"}`, Msg: "m"},
		{TsMs: base, Labels: `{app="x"}`, Fields: `{ip="1",user="u2"}`, Msg: "m"},
		{TsMs: base, Labels: `{app="x"}`, Fields: `{user="u3"}`, Msg: "m"},
		{TsMs: base, Labels: `{app="y"}`, Fields: `{}`, Msg: "m"},
	}})
	r.send(&event.Batch{Tenant: "b", Kind: event.KindLogs, Logs: []event.Log{
		{TsMs: base, Labels: `{app="x"}`, Fields: `{user="u1"}`, Msg: "m"},
	}})
	r.send(metrics("a", sample{"other{}", base + 5000, 1}))
	r.send(metrics("b", sample{"other{}", base + 5000, 1}))
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}

	lines := exportedLines(t, readFile(t, mem, bucketsPath))
	if len(lines) != 6 {
		t.Fatalf("%d lines: %+v", len(lines), lines)
	}
	seen := 0
	for _, l := range lines {
		switch {
		case l.Tenant == "a" && l.Series == `lat{svc="x"}`:
			seen++
			if l.Card != nil || len(l.Q) != len(quantiles) {
				t.Errorf("%+v", l)
			}
			for _, q := range quantiles {
				exact := values[sketch.Rank(q.q, uint64(len(values)))]
				if got := l.Q[q.label]; math.Abs(got-exact) > sketch.Alpha*exact {
					t.Errorf("q=%s: got %v, exact %v", q.label, got, exact)
				}
			}
		case l.Tenant == "a" && l.Series == `{app="x"}`:
			seen++
			if l.Q != nil || len(l.Card) != 2 || l.Card["user"] != 3 || l.Card["ip"] != 1 {
				t.Errorf("%+v", l)
			}
		case l.Tenant == "a" && l.Series == `{app="y"}`:
			seen++
			if l.Q != nil || len(l.Card) != 2 || l.Card["user"] != 0 || l.Card["ip"] != 0 {
				t.Errorf("%+v", l)
			}
		default:
			if l.Q != nil || l.Card != nil {
				t.Errorf("series without configuration has sketches: %+v", l)
			}
		}
	}
	if seen != 3 {
		t.Errorf("%d configured series exported: %+v", seen, lines)
	}
	text := readFile(t, mem, bucketsPath)
	if !strings.Contains(text, `,"closed_at":`) || !strings.Contains(text, `{"0.5":`) || !strings.Contains(text, `,"card":{"ip":1,"user":3}}`) {
		t.Errorf("order of the members: %s", text)
	}
	if i := strings.Index(text, `"0.5"`); i < 0 || !strings.Contains(text[i:], `"0.9"`) || strings.Index(text[i:], `"0.999"`) < strings.Index(text[i:], `"0.99"`) {
		t.Errorf("order of the quantiles: %s", text)
	}
}

func TestNaNStaysOutOfTheQuantiles(t *testing.T) {
	r := newRigConfig(t, Config{BucketMs: 1000, Sketches: testSketches}, 0)
	r.send(metrics("a", sample{"lat{}", base, math.NaN()}, sample{"lat{}", base + 5000, 1}))
	if len(r.rec.buckets) != 1 || r.rec.buckets[0].Count != 1 {
		t.Fatalf("closed = %+v", r.rec.buckets)
	}
	line := r.rec.buckets[0].Ext.(exportedExt).appendExport(nil)
	want := `,"q":{"0.5":NaN,"0.9":NaN,"0.99":NaN,"0.999":NaN}`
	if string(line) != want {
		t.Fatalf("export = %s, want %s", line, want)
	}
}

func TestNextField(t *testing.T) {
	e := newCardinalityExt([]string{"a", "m", "z"})
	e.Add(0, `{a="x\"y",b="1\\",m="say \"z=\\\"2\\\"\"",n="\n"}`)
	want := newCardinalityExt([]string{"a", "m", "z"})
	want.hlls[0].Add(`x\"y`)
	want.hlls[1].Add(`say \"z=\\\"2\\\"\"`)
	if !bytes.Equal(e.AppendState(nil), want.AppendState(nil)) {
		t.Fatal("wrong values reached the HLLs")
	}
	for _, fields := range []string{"", "{}", `{a=`, `{a="1`, `{a="1\`, `{x`} {
		e.Add(0, fields) // malformed input must not panic
	}
}

func TestAuditSelection(t *testing.T) {
	n := 0
	for k := range int64(1_000_000) {
		if audited("a", `lat{svc="x"}`, base+k*1000) {
			n++
		}
	}
	if n < 800 || n > 1200 {
		t.Fatalf("%d of 1000000 buckets audited", n)
	}
}

// findEnd returns the first bucket end from base whose audit choice is want.
func findEnd(series string, want bool) int64 {
	end := base
	for audited("a", series, end) != want {
		end += 1000
	}
	return end
}

func TestAuditKeepsTheObservedMaximum(t *testing.T) {
	r := newRigConfig(t, Config{BucketMs: 1000, Sketches: testSketches}, 0)
	plain := findEnd("lat{}", false)
	r.send(metrics("a", sample{"lat{}", plain, 3.7}, sample{"lat{}", plain + 5000, 1}))
	if got := r.agg.SketchErrorMax(); got != 0 {
		t.Fatalf("a bucket outside the audit raised the maximum to %v", got)
	}

	end := findEnd("lat{}", true)
	r2 := newRigConfig(t, Config{BucketMs: 1000, Sketches: testSketches}, 0)
	// The audited bucket may lie more than 10 min ahead of base, which would
	// make its batch invalid (§4.1): the clock goes to it first.
	r2.clk.Advance(time.Duration(end-base) * time.Millisecond)
	dd := sketch.NewDDSketch()
	var batch []sample
	values := []float64{0.1, 3.7, 12.5, 1000.25, -8, 0, math.Inf(1), 77}
	for _, v := range values {
		batch = append(batch, sample{"lat{}", end, v})
		dd.Add(v)
	}
	batch = append(batch, sample{"lat{}", end, math.NaN()})
	r2.send(metrics("a", batch...))
	r2.send(metrics("a", sample{"lat{}", end + 5000, 1}))

	sorted := []float64{-8, 0, 0.1, 3.7, 12.5, 77, 1000.25, math.Inf(1)}
	want := 0.0
	for _, q := range quantiles {
		est := dd.Quantile(q.q)
		if exact := sorted[sketch.Rank(q.q, 8)]; est != exact {
			want = max(want, math.Abs(est-exact)/math.Abs(exact))
		}
	}
	got := r2.agg.SketchErrorMax()
	if got != want || got <= 0 || got > sketch.Alpha {
		t.Fatalf("maximum = %v, want %v, within %v", got, want, sketch.Alpha)
	}

	r2.agg.RestoreSketchErrorMax(got / 2)
	if r2.agg.SketchErrorMax() != got {
		t.Fatal("restoring a smaller value lowered the maximum")
	}
	r2.agg.RestoreSketchErrorMax(got * 2)
	if r2.agg.SketchErrorMax() != got*2 {
		t.Fatal("restoring did not raise the maximum")
	}
}

func TestSketchesDoNotDependOnShards(t *testing.T) {
	cfg := map[string]SketchConfig{"a": {QuantileMetrics: []string{"m"}}, "b": {QuantileMetrics: []string{"m"}}}
	run := func(shards int, start bool) (string, float64) {
		r := newRigConfig(t, Config{BucketMs: 1000, Shards: shards, Sketches: cfg}, 0)
		mem := vfs.NewMem()
		e, err := NewExport(mem, "exp", "r1")
		if err != nil {
			t.Fatal(err)
		}
		r.agg.Subscribe(e)
		if start {
			r.agg.Start()
			defer r.agg.Stop()
		}
		for _, b := range randomBatches(11, 300) {
			r.send(b)
		}
		r.agg.Barrier()
		if err := e.Flush(); err != nil {
			t.Fatal(err)
		}
		return readFile(t, mem, bucketsPath), r.agg.SketchErrorMax()
	}
	ref, refMax := run(1, false)
	if !strings.Contains(ref, `"q":{`) {
		t.Fatalf("no sketches in the export: %s", ref)
	}
	for _, v := range []struct {
		shards int
		start  bool
	}{{4, false}, {4, true}, {3, true}} {
		got, gotMax := run(v.shards, v.start)
		if got != ref || gotMax != refMax {
			t.Errorf("%+v: the export differs", v)
		}
	}
}

// extFor returns the extension the factory of testSketches gives a new bucket.
func extFor(t *testing.T, a *Aggregator, series string, end int64, kind event.Kind) Ext {
	t.Helper()
	e := a.newExt("a", series, end, kind)
	if e == nil {
		t.Fatalf("%s has no extension", series)
	}
	return e
}

func TestSketchStateRoundTrip(t *testing.T) {
	a, err := New(Config{BucketMs: 1000, Sketches: testSketches})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		series string
		end    int64
		kind   event.Kind
	}{
		{"not audited", "lat{}", findEnd("lat{}", false), event.KindMetrics},
		{"audited", "lat{}", findEnd("lat{}", true), event.KindMetrics},
		{"logs", `{app="x"}`, base, event.KindLogs},
	}
	feed := func(e Ext, from, to int) {
		for i := from; i < to; i++ {
			e.Add(float64(i)*1.5, `{ip="`+string(rune('a'+i%26))+`",user="u"}`)
		}
	}
	for _, c := range cases {
		ref := extFor(t, a, c.series, c.end, c.kind)
		cut := extFor(t, a, c.series, c.end, c.kind)
		feed(ref, 0, 100)
		feed(cut, 0, 40)
		loaded := extFor(t, a, c.series, c.end, c.kind)
		if err := loaded.LoadState(cut.AppendState(nil)); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		feed(loaded, 40, 100)
		if !bytes.Equal(loaded.AppendState(nil), ref.AppendState(nil)) {
			t.Errorf("%s: the state differs after the load", c.name)
		}
		loaded.Close()
		ref.Close()
		x, y := loaded.(exportedExt).appendExport(nil), ref.(exportedExt).appendExport(nil)
		if !bytes.Equal(x, y) {
			t.Errorf("%s: export %s, want %s", c.name, x, y)
		}
		if err := extFor(t, a, c.series, c.end, c.kind).LoadState(nil); err == nil {
			t.Errorf("%s: loaded an empty state", c.name)
		}
	}
}

func TestSketchStateRejectsOtherConfiguration(t *testing.T) {
	x := newCardinalityExt([]string{"ip", "user"})
	state := x.AppendState(nil)
	for _, names := range [][]string{{"ip"}, {"ip", "user", "zone"}, {"host", "user"}} {
		if err := newCardinalityExt(names).LoadState(state); err == nil {
			t.Errorf("state of [ip user] loaded into %v", names)
		}
	}
	dd := &quantileExt{dd: sketch.NewDDSketch(), audit: true, values: []float64{1}, max: &errorMax{}}
	dd.dd.Add(1)
	other := &quantileExt{dd: sketch.NewDDSketch(), max: &errorMax{}}
	if err := other.LoadState(dd.AppendState(nil)); err == nil {
		t.Error("audit values loaded into a bucket that is not audited")
	}
}

func TestSketchesAndExtensionExcludeEachOther(t *testing.T) {
	_, err := New(Config{BucketMs: 1000, Sketches: testSketches, NewExt: func(string, string, int64, event.Kind) Ext { return nil }})
	if err == nil {
		t.Fatal("accepted both Sketches and NewExt")
	}
}

func TestUnconfiguredTenantGetsNoExtension(t *testing.T) {
	a, err := New(Config{BucketMs: 1000, Sketches: map[string]SketchConfig{"a": {}, "c": {QuantileMetrics: []string{"lat"}}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		tenant, series string
		kind           event.Kind
	}{
		{"a", "lat{}", event.KindMetrics},
		{"a", `{app="x"}`, event.KindLogs},
		{"b", "lat{}", event.KindMetrics},
		{"c", "latency{}", event.KindMetrics},
		{"c", `{app="x"}`, event.KindLogs},
	} {
		// An untyped nil: a typed one would pass the checks of the shard.
		if e := a.newExt(c.tenant, c.series, base, c.kind); e != nil {
			t.Errorf("%+v got %T", c, e)
		}
	}
}

func TestBucketSketchAccessors(t *testing.T) {
	metric := &Bucket{Ext: &quantileExt{dd: sketch.NewDDSketch()}}
	if metric.DDSketch() == nil || metric.HLL("user") != nil {
		t.Fatal("a metric bucket has a DDSketch and no HLL")
	}
	log := &Bucket{Ext: newCardinalityExt([]string{"user"})}
	if log.DDSketch() != nil || log.HLL("user") == nil || log.HLL("other") != nil {
		t.Fatal("a log bucket has an HLL per configured field and no DDSketch")
	}
	plain := &Bucket{}
	if plain.DDSketch() != nil || plain.HLL("user") != nil {
		t.Fatal("a bucket without extension has no sketches")
	}
}
