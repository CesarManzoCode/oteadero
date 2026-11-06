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

	// The alerts profile: alertsEventsPerS events per synthetic second during
	// alertsSeconds, paced so that an evaluator is still reading when a
	// criterion kills it.
	alertsEventsPerS = 2000
	alertsSeconds    = 600
	alertsRate       = 150000
	// alertsRealtimeSeconds is the length of the alerts load with --realtime,
	// which sends one step per second of the clock.
	alertsRealtimeSeconds = 90
	// alertsInstances is the number of latency_ms and of errors series per
	// service, and alertsStreams the number of log streams.
	alertsInstances = 4
	alertsStreams   = 2
	// alertsCycleS is the period of the phases of a service.
	alertsCycleS = 120

	// The reference profile: referenceEventsPerS integer samples per synthetic
	// second, one every 10 ms so that no series has two at the same timestamp,
	// over the services of referenceServices with referenceInstances each.
	referenceEventsPerS = 100
	referenceSeconds    = 300
	referenceInstances  = 2
	referenceCycleS     = 60
	// The reference-kff profile: one sample of each of kffSeries series every
	// kffIntervalMs from t = 0, the interval of rules/kff.json.
	kffSeries     = 2
	kffSeconds    = 240
	kffIntervalMs = 1000
	kffCycleS     = 40

	// The tail profile: one tenant of logs at tailEventsPerS events per second
	// of the clock, for 30 s unless --duration-s says otherwise.
	tailEventsPerS = 20000

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
	// scenario replaces the random series of the mixed profiles with the
	// services and phases of the alerts profile.
	scenario bool
	// shape replaces them with the series of a reference profile.
	shape refShape
	// wallClock stamps the events with the clock even without --realtime.
	wallClock bool
}

// refShape is the load of a reference profile (D12).
type refShape int

const (
	shapeNone refShape = iota
	// shapeCounts is the reference profile: integer samples spread over each
	// second, with service phases that move counts and sums across the
	// thresholds of rules/reference.json.
	shapeCounts
	// shapeRegular is the reference-kff profile: regular series from t = 0,
	// as promtool test rules writes them.
	shapeRegular
)

// baseMs returns the synthetic time of the first step.
func (p profile) baseMs() int64 {
	if p.shape == shapeRegular {
		return 0
	}
	return syntheticBaseMs
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
	case "alerts":
		if durationS > 0 {
			return profile{}, fmt.Errorf("profile %q has a fixed event count, not a duration", name)
		}
		// One connection: batches of a cycle span half a second of event
		// time and must reach the log in order, or the delays of D15 would
		// not be the only source of lateness.
		return profile{
			name: name,
			tenants: []tenantPlan{{
				name:       "t1",
				rate:       alertsRate,
				events:     alertsEventsPerS * alertsSeconds,
				stepEvents: alertsEventsPerS,
			}},
			batchEvents: 1000,
			conns:       1,
			stepMs:      1000,
			delayed:     true,
			scenario:    true,
		}, nil
	case "reference":
		if durationS > 0 {
			return profile{}, fmt.Errorf("profile %q has a fixed event count, not a duration", name)
		}
		// Metrics only and one connection: the same bytes go to
		// VictoriaMetrics, and batches in order leave no late event.
		return profile{
			name: name,
			tenants: []tenantPlan{{
				name:       "t1",
				events:     referenceEventsPerS * referenceSeconds,
				stepEvents: referenceEventsPerS,
			}},
			batchEvents: 1000,
			conns:       1,
			stepMs:      1000,
			shape:       shapeCounts,
		}, nil
	case "reference-kff":
		if durationS > 0 {
			return profile{}, fmt.Errorf("profile %q has a fixed event count, not a duration", name)
		}
		return profile{
			name: name,
			tenants: []tenantPlan{{
				name:       "t1",
				events:     kffSeries * kffSeconds,
				stepEvents: kffSeries,
			}},
			batchEvents: 1000,
			conns:       1,
			stepMs:      kffIntervalMs,
			shape:       shapeRegular,
		}, nil
	case "tail":
		d := 30.0
		if durationS > 0 {
			d = durationS
		}
		// Logs only, one connection: batches reach the log in the order of
		// their timestamps, so a subscriber sees every event of the load.
		p := mixed
		p.metricSeries = 0
		p.batchEvents = 1000
		p.wallClock = true
		p.tenants = []tenantPlan{{
			name:       "t1",
			rate:       tailEventsPerS,
			events:     int64(tailEventsPerS * d),
			stepEvents: tailEventsPerS / 1000,
		}}
		return p, nil
	}
	return profile{}, fmt.Errorf("unknown profile %q", name)
}

// realtimeProfile returns the parameters of a profile with --realtime. The
// phases of the alerts profile advance one step per second of the clock, so
// that its incidents come every few seconds of real time; the other profiles
// do not change.
func realtimeProfile(p profile) profile {
	if !p.scenario {
		return p
	}
	plan := p.tenants[0]
	plan.rate = alertsEventsPerS
	plan.events = alertsEventsPerS * alertsRealtimeSeconds
	p.tenants = []tenantPlan{plan}
	return p
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
	switch {
	case p.scenario:
		g.metricKeys, g.logHeads = alertsKeys()
		return g
	case p.shape != shapeNone:
		g.metricKeys, g.logHeads = referenceKeys(p.shape), nil
		return g
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
		// The latency of an alert is measured from the ts of its events
		// (D25), so the scenario keeps the clock as it is.
		if !g.p.scenario {
			now -= now % g.p.stepMs
		}
	}
	metrics, logs := g.take(), g.take()
	var nMetrics, nLogs int64
	for range n {
		ts := now
		sec := g.generated / g.plan.stepEvents
		if !g.realtime {
			ts = g.p.baseMs() + sec*g.p.stepMs
			if g.p.scenario || g.p.shape == shapeCounts {
				// Spread the events of a step over it.
				ts += g.generated % g.plan.stepEvents * g.p.stepMs / g.plan.stepEvents
			}
		}
		if g.p.delayed {
			d := drawDelay(g.r)
			ts -= d
			if d > latenessMs {
				g.overLateness++
			}
		}
		g.maxTs = max(g.maxTs, ts)
		if g.p.shape != shapeNone {
			metrics = g.appendReference(metrics, sec, ts)
			nMetrics++
		} else if g.p.scenario {
			var isLog bool
			metrics, logs, isLog = g.appendScenario(metrics, logs, sec, ts)
			if isLog {
				nLogs++
			} else {
				nMetrics++
			}
		} else if s := g.r.IntN(len(g.metricKeys) + len(g.logHeads)); s < len(g.metricKeys) {
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

// alertsServices are the services of the alerts profile. The shifts of their
// phases keep the gaps apart, so that some service always sends.
var alertsServices = [...]struct {
	name  string
	shift int64
}{{"api", 0}, {"db", 40}, {"web", 80}}

// alertsPhase is what a service does during one synthetic second.
type alertsPhase int

const (
	phaseNormal alertsPhase = iota
	phaseHigh
	phaseOscHigh
	phaseOscLow
	phaseGap
)

// alertsPhases sets, per phase, the share of the events of the second that the
// service gets, the location of its lognormal latency, its error deltas in
// [errorsMin, errorsMin+errorsSpan) and the number of users its logs draw from.
// The high phase crosses the thresholds of rules/alerts.json, the two
// oscillating ones alternate around them every 3 s, and a gap sends nothing.
var alertsPhases = [...]struct {
	weight     int
	latencyMu  float64
	errorsMin  int64
	errorsSpan int64
	users      int64
}{
	phaseNormal:  {2, 3.0, 0, 3, 50},
	phaseHigh:    {4, 5.0, 5, 10, 5000},
	phaseOscHigh: {3, 4.2, 3, 4, 1500},
	phaseOscLow:  {2, 3.6, 1, 3, 300},
	phaseGap:     {0, 0, 0, 1, 1},
}

// alertsPhaseAt returns the phase of a service at a synthetic second: within
// each cycle of 120 s, 30 s normal, 20 s high, 30 s oscillating, a gap of 15 s
// without data and 25 s normal.
func alertsPhaseAt(sec int64, service int) alertsPhase {
	p := (sec + alertsServices[service].shift) % alertsCycleS
	switch {
	case p >= 30 && p < 50:
		return phaseHigh
	case p >= 50 && p < 80:
		if (p-50)/3%2 == 0 {
			return phaseOscHigh
		}
		return phaseOscLow
	case p >= 80 && p < 95:
		return phaseGap
	}
	return phaseNormal
}

// alertsKeys returns the metric keys, latency_ms of every service and instance
// and then errors in the same order, and the heads of the log streams.
func alertsKeys() ([]string, []string) {
	var metrics, logs []string
	for _, name := range []string{"latency_ms", "errors"} {
		for _, s := range alertsServices {
			for i := range alertsInstances {
				metrics = append(metrics, fmt.Sprintf("%s{instance=\"i%d\",svc=\"%s\"} ", name, i, s.name))
			}
		}
	}
	for _, s := range alertsServices {
		for i := range alertsStreams {
			logs = append(logs, fmt.Sprintf(`,"labels":{"stream":"s%d","svc":"%s"},"fields":{"user":"u`, i, s.name))
		}
	}
	return metrics, logs
}

// appendScenario writes the next event of the alerts profile at second sec:
// a service drawn by the weights of its phase, then 45% latency_ms, 15%
// errors and 40% logs. It reports whether the event went to logs.
func (g *generator) appendScenario(metrics, logs []byte, sec, ts int64) ([]byte, []byte, bool) {
	var phases [len(alertsServices)]alertsPhase
	total := 0
	for i := range alertsServices {
		phases[i] = alertsPhaseAt(sec, i)
		total += alertsPhases[phases[i]].weight
	}
	service := 0
	for pick := g.r.IntN(total); pick >= alertsPhases[phases[service]].weight; service++ {
		pick -= alertsPhases[phases[service]].weight
	}
	ph := alertsPhases[phases[service]]
	switch k := g.r.IntN(100); {
	case k < 45:
		metrics = append(metrics, g.metricKeys[service*alertsInstances+g.r.IntN(alertsInstances)]...)
		metrics = strconv.AppendFloat(metrics, math.Exp(ph.latencyMu+g.r.NormFloat64()), 'g', -1, 64)
	case k < 60:
		first := (len(alertsServices) + service) * alertsInstances
		metrics = append(metrics, g.metricKeys[first+g.r.IntN(alertsInstances)]...)
		metrics = strconv.AppendInt(metrics, ph.errorsMin+g.r.Int64N(ph.errorsSpan), 10)
	default:
		logs = append(logs, `{"ts":`...)
		logs = strconv.AppendInt(logs, ts, 10)
		logs = append(logs, g.logHeads[service*alertsStreams+g.r.IntN(alertsStreams)]...)
		logs = strconv.AppendInt(logs, g.r.Int64N(ph.users), 10)
		logs = append(logs, `"},"msg":"request"}`+"\n"...)
		return metrics, logs, true
	}
	metrics = append(metrics, ' ')
	metrics = strconv.AppendInt(metrics, ts, 10)
	return append(metrics, '\n'), logs, false
}

// referenceServices are the services of the reference profile; the shifts
// keep their gaps apart.
var referenceServices = [...]struct {
	name  string
	shift int64
}{{"api", 0}, {"db", 20}, {"web", 40}}

// referenceKeys returns the metric keys of a reference profile: ref_events of
// every service and instance, or the kff_value series.
func referenceKeys(shape refShape) []string {
	var keys []string
	if shape == shapeRegular {
		for i := range kffSeries {
			keys = append(keys, fmt.Sprintf("kff_value{instance=\"i%d\"} ", i))
		}
		return keys
	}
	for _, s := range referenceServices {
		for i := range referenceInstances {
			keys = append(keys, fmt.Sprintf("ref_events{instance=\"i%d\",svc=\"%s\"} ", i, s.name))
		}
	}
	return keys
}

// referenceWeight returns the share of the events of a second that a service
// of the reference profile gets and the range [min, min+span) of its values:
// within each cycle of 60 s, 20 s normal, 12 s high, 12 s oscillating every
// 2 s, a gap of 8 s without data and 8 s normal.
func referenceWeight(sec int64, service int) (weight int, valueMin, valueSpan int64) {
	p := (sec + referenceServices[service].shift) % referenceCycleS
	switch {
	case p >= 20 && p < 32:
		return 6, 5, 10
	case p >= 32 && p < 44:
		if (p-32)/2%2 == 0 {
			return 5, 4, 8
		}
		return 2, 1, 5
	case p >= 44 && p < 52:
		return 0, 0, 1
	}
	return 2, 1, 9
}

// kffValue returns the next value of a kff_value series at second sec: within
// each cycle of 40 s, 15 s of 0 or 1, 10 s of 5 to 7 and 15 s alternating
// between both every 4 s.
func (g *generator) kffValue(sec int64, series int) int64 {
	p := (sec + int64(series)*17) % kffCycleS
	high := (p >= 15 && p < 25) || (p >= 25 && (p-25)/4%2 == 0)
	if high {
		return 5 + g.r.Int64N(3)
	}
	return g.r.Int64N(2)
}

// appendReference writes the next sample of a reference profile at second
// sec: in reference-kff the next series in turn, in reference a service drawn
// by the weights of its phase and one of its instances.
func (g *generator) appendReference(b []byte, sec, ts int64) []byte {
	var value int64
	if g.p.shape == shapeRegular {
		series := int(g.generated % g.plan.stepEvents)
		b = append(b, g.metricKeys[series]...)
		value = g.kffValue(sec, series)
	} else {
		var weights [len(referenceServices)]int
		total := 0
		for i := range referenceServices {
			weights[i], _, _ = referenceWeight(sec, i)
			total += weights[i]
		}
		service := 0
		for pick := g.r.IntN(total); pick >= weights[service]; service++ {
			pick -= weights[service]
		}
		_, valueMin, valueSpan := referenceWeight(sec, service)
		b = append(b, g.metricKeys[service*referenceInstances+g.r.IntN(referenceInstances)]...)
		value = valueMin + g.r.Int64N(valueSpan)
	}
	b = strconv.AppendInt(b, value, 10)
	b = append(b, ' ')
	b = strconv.AppendInt(b, ts, 10)
	return append(b, '\n')
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
	if err == nil && *realtime {
		p = realtimeProfile(p)
	}
	if err != nil {
		fmt.Fprintf(stderr, "otgen: %v\n", err)
		return 2
	}

	rep := runProfile(p, *seed, *realtime || p.wallClock, newSender(*target, *vm, null, p.conns*len(p.tenants)))
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
