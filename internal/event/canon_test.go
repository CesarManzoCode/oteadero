package event

import (
	"hash/fnv"
	"testing"
)

func TestCanonicalLabels(t *testing.T) {
	cases := []struct {
		name string
		in   []Label
		want string
	}{
		{"empty", nil, `{}`},
		{"one", []Label{{"a", "1"}}, `{a="1"}`},
		{"sorted by name", []Label{{"b", "2"}, {"a", "1"}, {"c", "3"}}, `{a="1",b="2",c="3"}`},
		{"byte order", []Label{{"a", "1"}, {"B", "2"}, {"_", "3"}}, `{B="2",_="3",a="1"}`},
		{"escapes", []Label{{"a", "x\\y\"z\nw"}}, `{a="x\\y\"z\nw"}`},
		{"empty value", []Label{{"a", ""}}, `{a=""}`},
		{"utf8 untouched", []Label{{"a", "ñ"}}, `{a="ñ"}`},
	}
	for _, c := range cases {
		if got := CanonicalLabels(c.in); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}

func TestMetricKey(t *testing.T) {
	got := MetricKey("http_requests", []Label{{"svc", "api"}, {"__name__", "http_requests"}, {"code", "200"}})
	if want := `http_requests{code="200",svc="api"}`; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	if got := MetricKey("up", nil); got != `up{}` {
		t.Errorf("no labels: got %s, want up{}", got)
	}
}

func TestSeriesHash(t *testing.T) {
	h := fnv.New64a()
	h.Write([]byte("t1\x00up{a=\"1\"}"))
	if got, want := SeriesHash("t1", `up{a="1"}`), h.Sum64(); got != want {
		t.Errorf("got %#x, want %#x", got, want)
	}
	if SeriesHash("ab", "c") == SeriesHash("a", "bc") {
		t.Error("tenant/key boundary does not change the hash")
	}
}
