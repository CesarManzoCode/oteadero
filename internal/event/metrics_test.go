package event

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestParseMetricsValid(t *testing.T) {
	body := "# HELP up whether the target is up\n" +
		"# TYPE up gauge\n" +
		"\n" +
		"up 1 1700000000000\n" +
		"http_requests{svc=\"api\",code=\"200\"} 42 5\n" +
		"  spaced  { b = \"2\" , a=\"1\", }\t-3.5\t0  \r\n" +
		"ns:rule_total{__name__=\"ns:rule_total\",z=\"q\\\"x\\\\y\\nz\"} 1.5e3 7\n" +
		"empty{} +6.02E-2 8"
	got, err := ParseMetrics(nil, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	want := []Metric{
		{`up{}`, 1, 1700000000000},
		{`http_requests{code="200",svc="api"}`, 42, 5},
		{`spaced{a="1",b="2"}`, -3.5, 0},
		{`ns:rule_total{z="q\"x\\y\nz"}`, 1500, 7},
		{`empty{}`, 0.0602, 8},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d samples, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sample %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestParseMetricsKeyMatchesMetricKey(t *testing.T) {
	got, err := ParseMetrics(nil, []byte(`m{b="x\\y",a="q\"r\ns"} 1 1`))
	if err != nil {
		t.Fatal(err)
	}
	want := MetricKey("m", []Label{{"b", `x\y`}, {"a", "q\"r\ns"}})
	if got[0].Key != want {
		t.Errorf("got %s, want %s", got[0].Key, want)
	}
}

func TestParseMetricsSpecialValues(t *testing.T) {
	cases := []struct {
		in   string
		want float64
	}{
		{"+Inf", math.Inf(1)},
		{"-Inf", math.Inf(-1)},
		{"Inf", math.Inf(1)},
		{"1e3", 1000},
		{"-2.5E-1", -0.25},
		{"123456789012345", 123456789012345},
		{"1234567890123456789", 1234567890123456789},
		{"-0", math.Copysign(0, -1)},
	}
	for _, c := range cases {
		got, err := ParseMetrics(nil, []byte("m "+c.in+" 1"))
		if err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if v := got[0].Value; v != c.want || math.Signbit(v) != math.Signbit(c.want) {
			t.Errorf("%s: got %v, want %v", c.in, v, c.want)
		}
	}
	got, err := ParseMetrics(nil, []byte("m NaN 1"))
	if err != nil || !math.IsNaN(got[0].Value) {
		t.Errorf("NaN: got %v, %v", got, err)
	}
}

func TestParseMetricsInvalid(t *testing.T) {
	cases := map[string]string{
		"no timestamp":           "up 1",
		"no value":               "up",
		"unclosed label set":     `up{a="1" 1 2`,
		"unclosed label value":   `up{a="1} 1 2`,
		"missing comma":          `up{a="1" b="2"} 1 2`,
		"unquoted value":         `up{a=1} 1 2`,
		"bad label name":         `up{1a="1"} 1 2`,
		"leading comma":          `up{,a="1"} 1 2`,
		"duplicate label":        `up{a="1",a="2"} 1 2`,
		"bad escape":             `up{a="\t"} 1 2`,
		"bad metric name":        `1up 1 2`,
		"glued value":            `up{a="1"}1 2`,
		"bad value":              "up one 2",
		"float timestamp":        "up 1 2.5",
		"negative timestamp":     "up 1 -5",
		"timestamp overflow":     "up 1 9223372036854775808",
		"extra field":            "up 1 2 3",
		"invalid after comments": "# ok\nup 1 2\nbroken\n",
	}
	for name, body := range cases {
		dst := []Metric{{Key: "keep"}}
		got, err := ParseMetrics(dst, []byte(body))
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got err %v, want ErrInvalid", name, err)
		}
		if len(got) != 1 || got[0].Key != "keep" {
			t.Errorf("%s: dst changed to %v", name, got)
		}
	}
}

func TestParseMetricsTooLarge(t *testing.T) {
	var b strings.Builder
	for i := range MaxEvents {
		fmt.Fprintf(&b, "m{i=\"%d\"} 1 %d\n", i, i)
	}
	got, err := ParseMetrics(nil, []byte(b.String()))
	if err != nil || len(got) != MaxEvents {
		t.Fatalf("at the limit: %d samples, %v", len(got), err)
	}
	b.WriteString("m 1 1\n")
	if _, err := ParseMetrics(nil, []byte(b.String())); !errors.Is(err, ErrTooLarge) {
		t.Errorf("over the limit: got %v, want ErrTooLarge", err)
	}
}

func BenchmarkParseMetrics(b *testing.B) {
	const lines = 10_000
	var sb strings.Builder
	for i := range lines {
		fmt.Fprintf(&sb, "http_requests_total{svc=\"api-%d\",method=\"GET\",code=\"200\"} %d 17000000%05d\n", i%100, i, i)
	}
	body := []byte(sb.String())
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		dst := make([]Metric, 0, lines)
		for pb.Next() {
			var err error
			if dst, err = ParseMetrics(dst[:0], body); err != nil || len(dst) != lines {
				b.Error(len(dst), err)
				return
			}
		}
	})
	b.ReportMetric(float64(b.N)*lines/b.Elapsed().Seconds(), "lines/s")
}

func TestCountEvents(t *testing.T) {
	for _, tc := range []struct {
		kind Kind
		body string
		want int
	}{
		{KindMetrics, "", 0},
		{KindMetrics, "# c\n\n  \r\n", 0},
		{KindMetrics, "a 1 1\n# c\nbad\nlast", 3},
		{KindLogs, "{}\n\n# not a comment\n", 2},
	} {
		if got := CountEvents(tc.kind, []byte(tc.body)); got != tc.want {
			t.Errorf("CountEvents(%d, %q) = %d, want %d", tc.kind, tc.body, got, tc.want)
		}
	}
}
