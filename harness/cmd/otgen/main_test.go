package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/event"
)

// cycles returns the batches of up to n cycles of the first tenant.
func cycles(t *testing.T, name string, seed uint64, n int) []batch {
	t.Helper()
	p, err := lookupProfile(name, 0)
	if err != nil {
		t.Fatal(err)
	}
	g := newGenerator(p, 0, seed, false)
	var out []batch
	for i := 0; i < n && !g.done(); i++ {
		out = append(out, g.cycle()...)
	}
	return append(out, g.flush())
}

func sameBatches(a, b []batch) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].logs != b[i].logs || a[i].n != b[i].n || !bytes.Equal(a[i].body, b[i].body) {
			return false
		}
	}
	return true
}

var filledProfiles = []string{"basic", "late", "saturation", "fairness", "bench", "alerts"}

func TestSameSeedSameBytes(t *testing.T) {
	for _, name := range filledProfiles {
		if !sameBatches(cycles(t, name, 7, 5), cycles(t, name, 7, 5)) {
			t.Errorf("%s: same seed gave different batches", name)
		}
	}
	// The whole load of a fixed profile, including its flush.
	if !sameBatches(cycles(t, "late", 7, 1000), cycles(t, "late", 7, 1000)) {
		t.Error("late: same seed gave different batches over the whole load")
	}
}

func TestDifferentSeedDifferentBytes(t *testing.T) {
	for _, name := range filledProfiles {
		if sameBatches(cycles(t, name, 1, 2), cycles(t, name, 2, 2)) {
			t.Errorf("%s: different seeds gave the same batches", name)
		}
	}
}

func TestTenantsGetDifferentStreams(t *testing.T) {
	p, err := lookupProfile("basic", 0)
	if err != nil {
		t.Fatal(err)
	}
	if sameBatches(newGenerator(p, 0, 1, false).cycle(), newGenerator(p, 1, 1, false).cycle()) {
		t.Error("two tenants generated the same stream")
	}
}

func TestDelayDistribution(t *testing.T) {
	const samples = 1000000
	r := rand.New(rand.NewPCG(11, 13))
	var none, under, over int
	underValues, overValues := map[int64]bool{}, map[int64]bool{}
	for range samples {
		d := drawDelay(r)
		if d%100 != 0 {
			t.Fatalf("delay %d is off the grid", d)
		}
		switch {
		case d == 0:
			none++
		case d > 0 && d < latenessMs:
			under++
			underValues[d] = true
		case d > latenessMs && d < 3*latenessMs:
			over++
			overValues[d] = true
		default:
			t.Fatalf("delay %d is outside the declared ranges", d)
		}
	}
	for _, c := range []struct {
		name      string
		got       int
		want, tol float64
	}{
		{"none", none, 0.90, 0.003},
		{"under L", under, 0.09, 0.003},
		{"over L", over, 0.01, 0.001},
	} {
		f := float64(c.got) / samples
		if f < c.want-c.tol || f > c.want+c.tol {
			t.Errorf("%s: fraction %f, want %f ± %f", c.name, f, c.want, c.tol)
		}
	}
	if len(underValues) != 19 || len(overValues) != 39 {
		t.Errorf("distinct delays: %d under L, %d over L; want 19 and 39", len(underValues), len(overValues))
	}
}

// timestamps parses a body with the engine parsers, so it also proves that
// the engine accepts what the generator writes.
func timestamps(t *testing.T, b batch) []int64 {
	t.Helper()
	var out []int64
	if b.logs {
		logs, err := event.ParseLogs(nil, b.body)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range logs {
			out = append(out, l.TsMs)
		}
	} else {
		metrics, err := event.ParseMetrics(nil, b.body)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range metrics {
			out = append(out, m.TsMs)
		}
	}
	if int64(len(out)) != b.n {
		t.Fatalf("batch declares %d events and parses as %d", b.n, len(out))
	}
	return out
}

func TestEngineParsersAcceptEveryProfile(t *testing.T) {
	for _, name := range filledProfiles {
		for _, b := range cycles(t, name, 3, 3) {
			timestamps(t, b)
		}
	}
}

func TestLateHasEventsOnTheWatermarkEdge(t *testing.T) {
	var maxTs int64
	edge := 0
	for _, b := range cycles(t, "late", 2, 1000) {
		for _, ts := range timestamps(t, b) {
			if ts%100 != 0 {
				t.Fatalf("timestamp %d is not a multiple of 100 ms", ts)
			}
			end := (ts + 999) / 1000 * 1000
			if maxTs != 0 && end == maxTs-latenessMs {
				edge++
			}
			maxTs = max(maxTs, ts)
		}
	}
	if edge == 0 {
		t.Error("no event fell exactly on end(ts) = W")
	}
}

func TestBasicHasNoDelayAndCountsEvents(t *testing.T) {
	p, err := lookupProfile("basic", 0)
	if err != nil {
		t.Fatal(err)
	}
	g := newGenerator(p, 0, 1, false)
	var maxTs, events int64
	for !g.done() {
		for _, b := range g.cycle() {
			events += b.n
			for _, ts := range timestamps(t, b) {
				// A cycle spans under one second of event time.
				if ts < maxTs-1000 {
					t.Fatalf("timestamp %d is far behind %d", ts, maxTs)
				}
				maxTs = max(maxTs, ts)
			}
		}
	}
	if events != 200000 || g.overLateness != 0 {
		t.Errorf("events = %d, over L = %d", events, g.overLateness)
	}
}

func TestProfileParameters(t *testing.T) {
	for _, c := range []struct {
		name  string
		rates []float64
	}{
		{"saturation", []float64{400000, 400000, 400000, 400000}},
		{"fairness", []float64{200000, 200000, 200000, 1000000}},
	} {
		p, err := lookupProfile(c.name, 0)
		if err != nil || len(p.tenants) != len(c.rates) {
			t.Fatalf("%s: %+v, %v", c.name, p, err)
		}
		for i, plan := range p.tenants {
			if plan.rate != c.rates[i] || plan.events != int64(c.rates[i])*30 {
				t.Errorf("%s: tenant %d: %+v", c.name, i, plan)
			}
		}
	}
	p, err := lookupProfile("bench", 0)
	if err != nil || len(p.tenants) != 1 || p.metricSeries != 100000 || p.logSeries != 0 ||
		p.batchEvents != 10000 || p.conns != 8 || p.duration.Seconds() != 60 {
		t.Errorf("bench: %+v, %v", p, err)
	}
	if b := cycles(t, "bench", 1, 1); b[0].n != 10000 || b[0].logs {
		t.Errorf("bench batch: n = %d, logs = %v", b[0].n, b[0].logs)
	}
}

func TestAlertsPhases(t *testing.T) {
	seen := map[alertsPhase]int{}
	for sec := range int64(alertsCycleS) {
		total, gaps := 0, 0
		for i := range alertsServices {
			ph := alertsPhaseAt(sec, i)
			seen[ph]++
			total += alertsPhases[ph].weight
			if ph == phaseGap {
				gaps++
			}
		}
		if total == 0 || gaps > 1 {
			t.Errorf("second %d: total weight %d, %d services in a gap", sec, total, gaps)
		}
	}
	// Every service goes through a gap of 15 s and every other phase.
	if seen[phaseGap] != 15*len(alertsServices) || len(seen) != len(alertsPhases) {
		t.Errorf("phases seen: %v", seen)
	}
}

func TestAlertsLoad(t *testing.T) {
	p, err := lookupProfile("alerts", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.tenants) != 1 || p.conns != 1 || !p.delayed || p.tenants[0].events != alertsEventsPerS*alertsSeconds {
		t.Fatalf("alerts: %+v", p)
	}
	g := newGenerator(p, 0, 5, false)
	names := map[string]int64{}
	var logs, maxTs int64
	for !g.done() {
		for _, b := range g.cycle() {
			if b.logs {
				parsed, err := event.ParseLogs(nil, b.body)
				if err != nil {
					t.Fatal(err)
				}
				for _, l := range parsed {
					if !strings.HasPrefix(l.Fields, `{user="u`) {
						t.Fatalf("log fields %q", l.Fields)
					}
				}
				logs += int64(len(parsed))
				continue
			}
			parsed, err := event.ParseMetrics(nil, b.body)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range parsed {
				name, _, _ := strings.Cut(m.Key, "{")
				names[name]++
				if name == "errors" && m.Value != math.Trunc(m.Value) {
					t.Fatalf("errors value %v is not an integer", m.Value)
				}
				maxTs = max(maxTs, m.TsMs)
			}
		}
	}
	if logs == 0 || names["latency_ms"] == 0 || names["errors"] == 0 || len(names) != 2 {
		t.Errorf("logs %d, metrics %v", logs, names)
	}
	if g.overLateness == 0 {
		t.Error("no delay over L")
	}
	// Events of a step are spread over it, not stamped at its start.
	if maxTs < syntheticBaseMs+(alertsSeconds-1)*1000+500 {
		t.Errorf("max ts %d", maxTs)
	}
}

// TestPacedProfilesDoNotWaitForAcks holds every answer until 8 batches of one
// tenant arrived: with fewer connections the first ones never get their ack.
func TestPacedProfilesDoNotWaitForAcks(t *testing.T) {
	const inFlight = 8
	for _, name := range []string{"saturation", "fairness"} {
		var arrived atomic.Int64
		var stalled atomic.Bool
		release := make(chan struct{})
		var once sync.Once
		open := func() { once.Do(func() { close(release) }) }
		timer := time.AfterFunc(30*time.Second, func() {
			stalled.Store(true)
			open()
		})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.Copy(io.Discard, r.Body)
			if arrived.Add(1) >= inFlight {
				open()
			}
			<-release
		}))

		p, err := lookupProfile(name, 0)
		if err != nil {
			t.Fatal(err)
		}
		p.tenants = p.tenants[:1]
		p.tenants[0].events = inFlight * p.batchEvents
		rep := runProfile(p, 1, false, newSender(srv.URL, false, false, p.conns))
		timer.Stop()
		srv.Close()
		if stalled.Load() {
			t.Errorf("%s: fewer than %d batches in flight", name, inFlight)
		}
		if r := rep.Tenants["t1"]; r.Acked != r.Sent || r.Errors != 0 {
			t.Errorf("%s: report = %+v", name, r)
		}
	}
}

// shortBasic is `basic` with only events per tenant.
func shortBasic(t *testing.T, events int64) profile {
	t.Helper()
	p, err := lookupProfile("basic", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := range p.tenants {
		p.tenants[i].events = events
	}
	return p
}

func TestReportAddsUp(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		switch calls.Add(1) % 4 {
		case 0:
			fmt.Fprint(w, `{"accepted":1,"first_offset":0}`)
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"dropped":1,"cause":"quota"}`)
		case 2:
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"dropped":1,"cause":"invalid"}`)
		default:
			panic(http.ErrAbortHandler)
		}
	}))
	defer srv.Close()

	p := shortBasic(t, 4000)
	rep := runProfile(p, 1, false, newSender(srv.URL, false, false, 2))
	var acked, quota, invalid, errs int64
	for name, r := range rep.Tenants {
		var dropped int64
		for _, n := range r.Dropped {
			dropped += n
		}
		if r.Acked+dropped+r.Errors != r.Sent {
			t.Errorf("%s: %d + %d + %d != %d", name, r.Acked, dropped, r.Errors, r.Sent)
		}
		if r.Sent != 4001 { // the events and the flush sample
			t.Errorf("%s: sent = %d", name, r.Sent)
		}
		acked += r.Acked
		quota += r.Dropped["quota"]
		invalid += r.Dropped["invalid"]
		errs += r.Errors
	}
	if acked == 0 || quota == 0 || invalid == 0 || errs == 0 {
		t.Errorf("acked=%d quota=%d invalid=%d errors=%d, want all of them", acked, quota, invalid, errs)
	}
	if want := float64(acked) / rep.DurationS; rep.RateEPS != want {
		t.Errorf("rate_eps = %f, want %f", rep.RateEPS, want)
	}
}

type recorded struct {
	path, tenant string
	body         []byte
}

// recorder is a server that keeps every request and answers with status.
func recorder(status int) (*httptest.Server, func() []recorded) {
	var mu sync.Mutex
	var got []recorded
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = append(got, recorded{r.URL.Path, r.Header.Get(tenantHeader), body})
		mu.Unlock()
		w.WriteHeader(status)
	}))
	return srv, func() []recorded {
		mu.Lock()
		defer mu.Unlock()
		return got
	}
}

func TestSyntheticFlushComesLastPerTenant(t *testing.T) {
	srv, got := recorder(http.StatusOK)
	defer srv.Close()
	rep := runProfile(shortBasic(t, 2000), 1, false, newSender(srv.URL, false, false, 2))
	if rep.Tenants["t1"].Acked != 2001 || rep.Tenants["t2"].Acked != 2001 {
		t.Errorf("tenants: %+v", rep.Tenants)
	}

	maxTs, last := map[string]int64{}, map[string]recorded{}
	for _, r := range got() {
		last[r.tenant] = r
		if bytes.HasPrefix(r.body, []byte("otgen_flush")) {
			continue
		}
		for _, ts := range timestamps(t, batch{logs: r.path == "/v1/logs", n: int64(bytes.Count(r.body, []byte("\n"))), body: r.body}) {
			maxTs[r.tenant] = max(maxTs[r.tenant], ts)
		}
	}
	for _, tenant := range []string{"t1", "t2"} {
		want := fmt.Sprintf("otgen_flush{} 1 %d\n", maxTs[tenant]+flushAheadMs)
		if l := last[tenant]; l.path != "/v1/metrics" || string(l.body) != want {
			t.Errorf("%s: last request %s %q, want %q", tenant, l.path, l.body, want)
		}
	}
}

func TestRealtimeHasNoFlush(t *testing.T) {
	srv, got := recorder(http.StatusOK)
	defer srv.Close()
	runProfile(shortBasic(t, 1000), 1, true, newSender(srv.URL, false, false, 2))
	for _, r := range got() {
		if bytes.Contains(r.body, []byte("otgen_flush")) {
			t.Fatalf("flush in realtime mode: %q", r.body)
		}
		if bytes.Contains(r.body, []byte(fmt.Sprint(syntheticBaseMs))) {
			t.Fatalf("synthetic timestamp in realtime mode: %q", r.body)
		}
	}
}

func TestVMSendsOnlyMetricsWithoutTenant(t *testing.T) {
	srv, got := recorder(http.StatusNoContent)
	defer srv.Close()
	rep := runProfile(shortBasic(t, 2000), 1, false, newSender(srv.URL+"/", true, false, 2))
	requests := got()
	if len(requests) == 0 {
		t.Fatal("no requests")
	}
	for _, r := range requests {
		if r.path != "/api/v1/import/prometheus" || r.tenant != "" || bytes.Contains(r.body, []byte(`{"ts"`)) {
			t.Fatalf("request %s tenant %q body %.60q", r.path, r.tenant, r.body)
		}
	}
	for name, r := range rep.Tenants {
		if r.Sent >= 2001 || r.Acked != r.Sent {
			t.Errorf("%s: sent %d acked %d", name, r.Sent, r.Acked)
		}
	}
}

func TestRunWithNullSinkWritesTheReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	args := []string{"--profile", "late", "--seed", "2", "--sink", "null", "--report", path}
	if code := run(args, io.Discard); code != 0 {
		t.Fatalf("run = %d", code)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rep report
	if err := json.Unmarshal(data, &rep); err != nil {
		t.Fatal(err)
	}
	if len(rep.Tenants) != 2 || rep.RateEPS <= 0 || rep.DurationS <= 0 {
		t.Fatalf("report: %+v", rep)
	}
	for name, r := range rep.Tenants {
		if r.Sent != 200001 || r.Acked != r.Sent || r.Errors != 0 || r.LateGenerated == 0 {
			t.Errorf("%s: %+v", name, r)
		}
	}
}

func TestRunRejectsBadUsage(t *testing.T) {
	report := filepath.Join(t.TempDir(), "report.json")
	for _, args := range [][]string{
		nil,
		{"--nope"},
		{"--profile", "basic", "--target", "http://x", "--report", report},
		{"--profile", "basic", "--seed", "1", "--report", report},
		{"--profile", "basic", "--seed", "1", "--target", "http://x"},
		{"--profile", "basic", "--seed", "1", "--report", report, "--sink", "disk"},
		{"--profile", "nope", "--seed", "1", "--report", report, "--sink", "null"},
		{"--profile", "tail", "--seed", "1", "--report", report, "--sink", "null"},
		{"--profile", "alerts", "--seed", "1", "--report", report, "--sink", "null", "--realtime"},
		{"--profile", "alerts", "--seed", "1", "--report", report, "--sink", "null", "--duration-s", "5"},
		{"--profile", "basic", "--seed", "1", "--report", report, "--sink", "null", "--duration-s", "5"},
	} {
		if code := run(args, io.Discard); code != 2 {
			t.Errorf("run(%v) = %d, want 2", args, code)
		}
	}
}
