package rules

import (
	"strings"
	"testing"

	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

// metricRule returns a valid rule over metric m, with extra as more members.
func metricRule(extra string) string {
	return `{"name":"r","tenant":"a","source":"metrics","match":{"__name__":"m"},` + extra + `}`
}

func file(rules ...string) []byte {
	return []byte(`{"rules":[` + strings.Join(rules, ",") + `]}`)
}

func TestParseValid(t *testing.T) {
	set, err := Parse(file(
		metricRule(`"agg":"count","range_ms":3000,"op":">","threshold":0`),
		`{"name":"q","tenant":"a","source":"metrics","match":{"__name__":"m"},"agg":"quantile","quantile":1,"range_ms":3000,"interval_ms":2000,
		  "by":[],"op":"<=","threshold":5,"resolve_threshold":5,"for_ms":1000,"keep_firing_for_ms":2000}`,
		`{"name":"c","tenant":"a","source":"logs","match":{},"agg":"cardinality","field":"user","range_ms":1000,"op":">=","threshold":5,"resolve_threshold":1}`,
	), 1000)
	if err != nil {
		t.Fatal(err)
	}
	r := set.Rules[0]
	if r.IntervalMs != 1000 || r.ForMs != 0 || r.KeepFiringForMs != 0 || r.By != nil || r.ResolveThreshold != nil {
		t.Fatalf("defaults = %+v", r)
	}
	q := set.Rules[1]
	if q.IntervalMs != 2000 || q.By == nil || len(q.By) != 0 {
		t.Fatalf("quantile rule = %+v", q)
	}
}

func TestParseRejects(t *testing.T) {
	cond := `"op":">","threshold":1`
	count := `"agg":"count","range_ms":1000,`
	cases := []struct {
		name, rule, want string
	}{
		{"no name", `{"tenant":"a","source":"metrics","match":{},` + count + cond + `}`, "name"},
		{"no tenant", `{"name":"r","source":"metrics","match":{},` + count + cond + `}`, "tenant"},
		{"bad source", `{"name":"r","tenant":"a","source":"x","match":{},` + count + cond + `}`, "source"},
		{"no match", `{"name":"r","tenant":"a","source":"metrics",` + count + cond + `}`, "match"},
		{"unknown field", metricRule(`"bogus":1,` + count + cond), "bogus"},
		{"range missing", metricRule(`"agg":"count",` + cond), "range_ms"},
		{"range not a multiple", metricRule(`"agg":"count","range_ms":1500,` + cond), "range_ms"},
		{"interval not a multiple", metricRule(count + `"interval_ms":500,` + cond), "interval_ms"},
		{"interval negative", metricRule(count + `"interval_ms":-1000,` + cond), "interval_ms"},
		{"negative for", metricRule(count + `"for_ms":-1,` + cond), "for_ms"},
		{"negative keep", metricRule(count + `"keep_firing_for_ms":-1,` + cond), "keep_firing_for_ms"},
		{"empty by label", metricRule(count + `"by":[""],` + cond), "by"},
		{"unknown agg", metricRule(`"agg":"max","range_ms":1000,` + cond), "agg"},
		{"quantile on logs", `{"name":"r","tenant":"a","source":"logs","match":{},"agg":"quantile","quantile":0.5,"range_ms":1000,` + cond + `}`, "metrics"},
		{"cardinality on metrics", metricRule(`"agg":"cardinality","field":"u","range_ms":1000,` + cond), "logs"},
		{"quantile missing", metricRule(`"agg":"quantile","range_ms":1000,` + cond), "quantile"},
		{"quantile zero", metricRule(`"agg":"quantile","quantile":0,"range_ms":1000,` + cond), "quantile"},
		{"quantile above one", metricRule(`"agg":"quantile","quantile":1.5,"range_ms":1000,` + cond), "quantile"},
		{"quantile on count", metricRule(`"quantile":0.5,` + count + cond), "quantile"},
		{"field missing", `{"name":"r","tenant":"a","source":"logs","match":{},"agg":"cardinality","range_ms":1000,` + cond + `}`, "field"},
		{"field on count", metricRule(`"field":"u",` + count + cond), "field"},
		{"no threshold", metricRule(count + `"op":">"`), "threshold"},
		{"bad op", metricRule(count + `"op":"==","threshold":1`), "op"},
		{"no op", metricRule(count + `"threshold":1`), "op"},
		{"resolve above with >", metricRule(count + `"op":">","threshold":5,"resolve_threshold":6`), "resolve_threshold"},
		{"resolve above with >=", metricRule(count + `"op":">=","threshold":5,"resolve_threshold":6`), "resolve_threshold"},
		{"resolve below with <", metricRule(count + `"op":"<","threshold":5,"resolve_threshold":4`), "resolve_threshold"},
		{"resolve below with <=", metricRule(count + `"op":"<=","threshold":5,"resolve_threshold":4`), "resolve_threshold"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(file(tc.rule), 1000)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestParseRejectsTheFile(t *testing.T) {
	ok := metricRule(`"agg":"count","range_ms":1000,"op":">","threshold":1`)
	if _, err := Parse(file(ok, ok), 1000); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate names: err = %v", err)
	}
	if _, err := Parse(append(file(ok), []byte(` {}`)...), 1000); err == nil {
		t.Fatal("accepted data after the object")
	}
	if _, err := Parse([]byte(`{"rules":[null]}`), 1000); err == nil {
		t.Fatal("accepted a null rule")
	}
	if _, err := Parse(file(ok), 0); err == nil {
		t.Fatal("accepted a zero bucket size")
	}
}

func TestLoad(t *testing.T) {
	mem := vfs.NewMem()
	if err := mem.MkdirAll("rules"); err != nil {
		t.Fatal(err)
	}
	f, err := mem.Create("rules/r.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(file(metricRule(`"agg":"count","range_ms":1000,"op":">","threshold":1`))); err != nil {
		t.Fatal(err)
	}
	f.Close()
	set, err := Load(mem, "rules/r.json", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Rules) != 1 || set.Rules[0].Name != "r" {
		t.Fatalf("set = %+v", set)
	}
	if _, err := Load(mem, "rules/missing.json", 1000); err == nil {
		t.Fatal("missing file loaded")
	}
}

func TestIncidentIDIsFixed(t *testing.T) {
	// printf 'cpu-high\0{svc="api"}\0123000' | sha256sum, first 32 characters.
	const want = "ae11e8c08ee4dbe41e9a7e0ad5a8c248"
	i := Incident{Rule: "cpu-high", Group: `{svc="api"}`, FiredAt: 123000}
	if got := i.ID(); got != want {
		t.Fatalf("ID = %s, want %s", got, want)
	}
	if got := i.IdempotencyKey(KindFiring); got != want+"/firing" {
		t.Fatalf("firing key = %s", got)
	}
	if got := i.IdempotencyKey(KindResolved); got != want+"/resolved" {
		t.Fatalf("resolved key = %s", got)
	}
	other := i
	other.FiredAt = 124000
	if other.ID() == want {
		t.Fatal("another T_f gave the same incident")
	}
}
