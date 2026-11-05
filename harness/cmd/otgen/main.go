// Command otgen is the seeded load generator and HTTP sender (D15). The same
// profile and seed produce the same batches, byte for byte, unless --realtime
// stamps them with the clock.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	usage = "usage: otgen --profile <p> --seed <n> --target <url> --report <file> [--duration-s <d>] [--realtime] [--vm] [--sink null]\n"

	tenantHeader = "X-Oteadero-Tenant"

	syntheticBaseMs = 1767225600000
	flushAheadMs    = 600000

	// latenessMs is the default L of §4.4, which the delays of D15 refer to.
	latenessMs = 2000
	// delayGridMs keeps delayed timestamps on the 100 ms grid of `late`.
	delayGridMs = 100

	logUsers = 10000

	// pacedConns is the connections per tenant of the profiles with a rate.
	// A connection waits for the fsync of its batch, so with too few of them
	// the acks, not the rate, would set what a tenant offers, and a load meant
	// to go over the quota could stay under it.
	pacedConns = 8

	// requestTimeout turns a stalled server into errors instead of a hang.
	requestTimeout = 60 * time.Second
	// errorBackoff stops a server that is down from burning the whole load
	// in a few milliseconds of refused connections.
	errorBackoff = 100 * time.Millisecond
)

// tenantPlan is the load of one tenant. Zero rate means maximum speed; zero
// events means until the duration of the profile ends. Synthetic time
// advances stepMs of the profile every stepEvents events.
type tenantPlan struct {
	name       string
	rate       float64
	events     int64
	stepEvents int64
}

type profile struct {
	name         string
	tenants      []tenantPlan
	metricSeries int
	logSeries    int
	// batchEvents is the number of events of one cycle. A cycle becomes one
	// metrics batch and one logs batch covering the same event time, which
	// is why it stays well under L in event time: the logs of a cycle sent
	// after its metrics must not turn late just for being split.
	batchEvents int64
	conns       int
	stepMs      int64
	delayed     bool
	duration    time.Duration
}

// lookupProfile returns the fixed parameters of a profile. durationS, when
// positive, overrides the duration of the profiles that have one.
func lookupProfile(name string, durationS float64) (profile, error) {
	mixed := profile{name: name, metricSeries: 1000, logSeries: 200, conns: 1, stepMs: 1}
	paced := func(rates ...float64) profile {
		p := mixed
		p.batchEvents = 5000
		p.conns = pacedConns
		d := 30.0
		if durationS > 0 {
			d = durationS
		}
		for i, rate := range rates {
			p.tenants = append(p.tenants, tenantPlan{
				name:       "t" + strconv.Itoa(i+1),
				rate:       rate,
				events:     int64(rate * d),
				stepEvents: int64(rate) / 1000,
			})
		}
		return p
	}
	switch name {
	case "basic", "late":
		if durationS > 0 {
			return profile{}, fmt.Errorf("profile %q has a fixed event count, not a duration", name)
		}
		p := mixed
		p.batchEvents = 1000
		step := int64(1)
		if name == "late" {
			p.delayed = true
			p.stepMs = 100
			step = 100
		}
		p.tenants = []tenantPlan{
			{name: "t1", events: 200000, stepEvents: step},
			{name: "t2", events: 200000, stepEvents: step},
		}
		return p, nil
	case "saturation":
		return paced(400000, 400000, 400000, 400000), nil
	case "fairness":
		return paced(200000, 200000, 200000, 1000000), nil
	case "bench":
		d := 60.0
		if durationS > 0 {
			d = durationS
		}
		return profile{
			name:         name,
			tenants:      []tenantPlan{{name: "t1", stepEvents: 1000}},
			metricSeries: 100000,
			batchEvents:  10000,
			conns:        8,
			stepMs:       1,
			duration:     time.Duration(d * float64(time.Second)),
		}, nil
	case "alerts", "tail", "reference":
		return profile{}, fmt.Errorf("profile %q is declared but not filled yet", name)
	}
	return profile{}, fmt.Errorf("unknown profile %q", name)
}

// batch is one request body: all metrics or all logs of one tenant.
type batch struct {
	logs bool
	n    int64
	body []byte
}

// generator produces the event stream of one tenant.
type generator struct {
	p            profile
	plan         tenantPlan
	r            *rand.Rand
	realtime     bool
	metricKeys   []string
	logHeads     []string
	generated    int64
	overLateness int64
	maxTs        int64
	free         chan []byte
}

func newGenerator(p profile, tenant int, seed uint64, realtime bool) *generator {
	g := &generator{
		p:          p,
		plan:       p.tenants[tenant],
		r:          rand.New(rand.NewPCG(seed, uint64(tenant)+1)),
		realtime:   realtime,
		metricKeys: make([]string, p.metricSeries),
		logHeads:   make([]string, p.logSeries),
		free:       make(chan []byte, 2*p.conns+2),
	}
	for i := range g.metricKeys {
		name := "otgen_requests"
		if i%2 == 1 {
			name = "otgen_latency"
		}
		g.metricKeys[i] = fmt.Sprintf("%s{series=\"m%06d\"} ", name, i)
	}
	for i := range g.logHeads {
		g.logHeads[i] = fmt.Sprintf(`,"labels":{"stream":"l%04d"},"fields":{"latency_ms":"`, i)
	}
	return g
}

// done reports whether a tenant with a fixed event count has generated them.
func (g *generator) done() bool {
	return g.plan.events > 0 && g.generated >= g.plan.events
}

func (g *generator) take() []byte {
	select {
	case b := <-g.free:
		return b[:0]
	default:
		return nil
	}
}

// release gives a body back once nothing reads it anymore.
func (g *generator) release(b []byte) {
	if cap(b) == 0 {
		return
	}
	select {
	case g.free <- b:
	default:
	}
}

// drawDelay draws a delay of D15 on the grid: 90% none, 9% uniform in (0, L),
// 1% uniform in (L, 3L).
func drawDelay(r *rand.Rand) int64 {
	const steps = latenessMs / delayGridMs
	switch p := r.IntN(100); {
	case p < 90:
		return 0
	case p < 99:
		return (1 + r.Int64N(steps-1)) * delayGridMs
	default:
		return (steps + 1 + r.Int64N(2*steps-1)) * delayGridMs
	}
}

// cycle generates the next batchEvents events (fewer at the end of the load)
// and returns their non-empty batches, metrics first.
func (g *generator) cycle() []batch {
	n := g.p.batchEvents
	if left := g.plan.events - g.generated; g.plan.events > 0 && left < n {
		n = left
	}
	var now int64
	if g.realtime {
		now = time.Now().UnixMilli()
		now -= now % g.p.stepMs
	}
	metrics, logs := g.take(), g.take()
	var nMetrics, nLogs int64
	for range n {
		ts := now
		if !g.realtime {
			ts = syntheticBaseMs + g.generated/g.plan.stepEvents*g.p.stepMs
		}
		if g.p.delayed {
			d := drawDelay(g.r)
			ts -= d
			if d > latenessMs {
				g.overLateness++
			}
		}
		g.maxTs = max(g.maxTs, ts)
		if s := g.r.IntN(len(g.metricKeys) + len(g.logHeads)); s < len(g.metricKeys) {
			metrics = g.appendMetric(metrics, s, ts)
			nMetrics++
		} else {
			logs = g.appendLog(logs, s-len(g.metricKeys), ts)
			nLogs++
		}
		g.generated++
	}
	var out []batch
	if nMetrics > 0 {
		out = append(out, batch{n: nMetrics, body: metrics})
	} else {
		g.release(metrics)
	}
	if nLogs > 0 {
		out = append(out, batch{logs: true, n: nLogs, body: logs})
	} else {
		g.release(logs)
	}
	return out
}

// appendMetric writes one sample: even series are counters with integer
// deltas, odd series are lognormal gauges (D15).
func (g *generator) appendMetric(b []byte, series int, ts int64) []byte {
	b = append(b, g.metricKeys[series]...)
	if series%2 == 0 {
		b = strconv.AppendInt(b, 1+g.r.Int64N(10), 10)
	} else {
		b = strconv.AppendFloat(b, g.lognormal(), 'g', -1, 64)
	}
	b = append(b, ' ')
	b = strconv.AppendInt(b, ts, 10)
	return append(b, '\n')
}

func (g *generator) appendLog(b []byte, series int, ts int64) []byte {
	b = append(b, `{"ts":`...)
	b = strconv.AppendInt(b, ts, 10)
	b = append(b, g.logHeads[series]...)
	b = strconv.AppendInt(b, int64(g.lognormal()), 10)
	b = append(b, `","user":"u`...)
	b = strconv.AppendInt(b, g.r.Int64N(logUsers), 10)
	return append(b, `"},"msg":"request"}`+"\n"...)
}

func (g *generator) lognormal() float64 {
	return math.Exp(3 + g.r.NormFloat64())
}

// flush returns the sample that lets the engine close every bucket without
// ticks in synthetic mode.
func (g *generator) flush() batch {
	b := append(g.take(), "otgen_flush{} 1 "...)
	b = strconv.AppendInt(b, g.maxTs+flushAheadMs, 10)
	return batch{n: 1, body: append(b, '\n')}
}

// sender delivers batches over persistent connections. A nil client is the
// null sink: nothing leaves the process and every batch counts as confirmed,
// so that the report gives the rate of the generator alone.
type sender struct {
	client *http.Client
	target string
	vm     bool
}

func newSender(target string, vm, null bool, conns int) *sender {
	if null {
		return &sender{vm: vm}
	}
	return &sender{
		client: &http.Client{
			Timeout: requestTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        conns,
				MaxIdleConnsPerHost: conns,
				DisableCompression:  true,
			},
		},
		target: strings.TrimRight(target, "/"),
		vm:     vm,
	}
}

// post sends one batch. A nil error and an empty cause mean confirmed; a
// cause means dropped by the server. An error means no usable answer, which
// includes a failure status that names no cause of §4.1 (`log_failure`).
func (s *sender) post(tenant string, b batch) (string, error) {
	if s.client == nil {
		return "", nil
	}
	path := "/v1/metrics"
	switch {
	case s.vm:
		path = "/api/v1/import/prometheus"
	case b.logs:
		path = "/v1/logs"
	}
	req, err := http.NewRequest(http.MethodPost, s.target+path, bytes.NewReader(b.body))
	if err != nil {
		return "", err
	}
	if !s.vm {
		req.Header.Set(tenantHeader, tenant)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	// Reading to the end is what lets the connection be reused.
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return "", nil
	}
	var r struct {
		Cause string `json:"cause"`
	}
	if json.Unmarshal(data, &r) != nil || r.Cause == "" {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	return r.Cause, nil
}

type tenantReport struct {
	Sent          int64            `json:"sent"`
	Acked         int64            `json:"acked"`
	Dropped       map[string]int64 `json:"dropped"`
	Errors        int64            `json:"errors"`
	LateGenerated int64            `json:"late_generated,omitempty"`
}

type report struct {
	Profile   string                  `json:"profile"`
	Seed      uint64                  `json:"seed"`
	DurationS float64                 `json:"duration_s"`
	RateEPS   float64                 `json:"rate_eps"`
	Tenants   map[string]tenantReport `json:"tenants"`
}

// runTenant generates and delivers the load of one tenant. A batch counts
// whole: it is atomic on the server, so its cause applies to all its events.
func runTenant(p profile, tenant int, seed uint64, realtime bool, s *sender, start time.Time) tenantReport {
	plan := p.tenants[tenant]
	g := newGenerator(p, tenant, seed, realtime)
	rep := tenantReport{Dropped: map[string]int64{}}
	var mu sync.Mutex
	deliver := func(b batch) {
		cause, err := s.post(plan.name, b)
		mu.Lock()
		rep.Sent += b.n
		switch {
		case err != nil:
			rep.Errors += b.n
		case cause != "":
			rep.Dropped[cause] += b.n
		default:
			rep.Acked += b.n
		}
		mu.Unlock()
		g.release(b.body)
		if err != nil {
			time.Sleep(errorBackoff)
		}
	}

	queue := make(chan batch, p.conns-1)
	var wg sync.WaitGroup
	for range p.conns {
		wg.Go(func() {
			for b := range queue {
				deliver(b)
			}
		})
	}
	for !g.done() && (plan.events > 0 || time.Since(start) < p.duration) {
		if plan.rate > 0 && s.client != nil {
			// time.Since reads the monotonic clock carried by start.
			due := time.Duration(float64(g.generated) / plan.rate * float64(time.Second))
			if wait := due - time.Since(start); wait > 0 {
				time.Sleep(wait)
			}
		}
		for _, b := range g.cycle() {
			if s.vm && b.logs {
				g.release(b.body)
				continue
			}
			queue <- b
		}
	}
	close(queue)
	wg.Wait()
	// Only now: a flush overtaking a batch in flight would make it late.
	if !realtime {
		deliver(g.flush())
	}
	rep.LateGenerated = g.overLateness
	return rep
}

func runProfile(p profile, seed uint64, realtime bool, s *sender) report {
	reports := make([]tenantReport, len(p.tenants))
	start := time.Now()
	var wg sync.WaitGroup
	for i := range p.tenants {
		wg.Go(func() { reports[i] = runTenant(p, i, seed, realtime, s, start) })
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()

	rep := report{Profile: p.name, Seed: seed, DurationS: elapsed, Tenants: map[string]tenantReport{}}
	var acked int64
	for i, r := range reports {
		rep.Tenants[p.tenants[i].name] = r
		acked += r.Acked
	}
	rep.RateEPS = float64(acked) / elapsed
	return rep
}

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

// run returns the exit code: 0, 1 on failure, 2 on bad usage. Batches that
// fail are not a failure of otgen: they go to the report.
func run(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("otgen", flag.ContinueOnError)
	flags.SetOutput(stderr)
	name := flags.String("profile", "", "load profile")
	seed := flags.Uint64("seed", 0, "generator seed")
	target := flags.String("target", "", "base URL of the server")
	reportPath := flags.String("report", "", "JSON report file")
	durationS := flags.Float64("duration-s", 0, "duration in seconds, for the profiles that have one")
	realtime := flags.Bool("realtime", false, "stamp events with the clock instead of synthetic time")
	vm := flags.Bool("vm", false, "send only metrics to the VictoriaMetrics import path")
	sink := flags.String("sink", "", "null: send nothing and measure the generator")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	seedSet := false
	flags.Visit(func(f *flag.Flag) { seedSet = seedSet || f.Name == "seed" })
	null := *sink == "null"
	if *name == "" || !seedSet || *reportPath == "" || (*target == "" && !null) ||
		(*sink != "" && !null) || flags.NArg() != 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	p, err := lookupProfile(*name, *durationS)
	if err != nil {
		fmt.Fprintf(stderr, "otgen: %v\n", err)
		return 2
	}

	rep := runProfile(p, *seed, *realtime, newSender(*target, *vm, null, p.conns*len(p.tenants)))
	data, err := json.MarshalIndent(rep, "", "  ")
	if err == nil {
		err = os.WriteFile(*reportPath, append(data, '\n'), 0o644)
	}
	if err != nil {
		fmt.Fprintf(stderr, "otgen: %v\n", err)
		return 1
	}
	return 0
}
