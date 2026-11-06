package sim

import (
	"math"
	"strconv"
	"time"

	"github.com/CesarManzoCode/oteadero/internal/rng"
)

const (
	// slice is how often a producer sends the events of the time that passed.
	slice = 100 * time.Millisecond
	// latenessMs is L of §4.4, the scale of the delays of D15.
	latenessMs = 2000
	// burstLogs is the size of the burst of logs of service api: alone in its
	// window it crosses logs_count_by_svc, users_card_by_svc and
	// users_card_stream of rules/alerts.json.
	burstLogs  = 1600
	burstUsers = 5000
	// junkEvery is how many slices apart p2 sends a batch of an unknown tenant
	// and an invalid one.
	junkEvery = 20
)

// service is a service of t1; shift moves its phases so that the gaps of the
// three do not coincide.
type service struct {
	name  string
	shift int64
}

var services = []service{{"api", 0}, {"db", 7}, {"web", 14}}

// phase is what a service of t1 sends per second: latency_ms samples with a
// lognormal of location latencyMu, errors deltas in [errorsMin,
// errorsMin+errorsSpan) and logs whose user is drawn from users. The high
// phase crosses the thresholds of the errors and latency rules of
// rules/alerts.json, the oscillating ones go back and forth around some of
// them, and the gap sends nothing, so that the groups go absent.
type phase struct {
	latency, errors, logs int
	latencyMu             float64
	errorsMin, errorsSpan int
	users                 int
}

var (
	phaseNormal  = phase{8, 4, 10, 3.0, 0, 3, 50}
	phaseHigh    = phase{8, 35, 10, 6.5, 20, 20, 300}
	phaseOscHigh = phase{8, 30, 10, 5.5, 10, 10, 200}
	phaseOscLow  = phase{8, 10, 10, 4.0, 1, 3, 100}
	phaseGap     = phase{}
)

// phaseAt returns the phase of a service at a second of the load: within each
// cycle of 20 s, 6 s normal, 5 s high, 3 s oscillating, a gap of 3 s and 3 s
// normal.
func phaseAt(sec int64, s service) phase {
	switch p := (sec + s.shift) % 20; {
	case p >= 6 && p < 11:
		return phaseHigh
	case p >= 11 && p < 14:
		if p%2 == 1 {
			return phaseOscHigh
		}
		return phaseOscLow
	case p >= 14 && p < 17:
		return phaseGap
	}
	return phaseNormal
}

// producer sends the load of one tenant, a batch per kind every slice, and
// ignores the answers. It does not crash in the scenario.
type producer struct {
	sc      *scenario
	env     *Env
	tenant  string
	startMs int64
	n       int64
}

func (sc *scenario) startProducer(tenant string) Factory {
	return func(env *Env) Process {
		p := &producer{sc: sc, env: env, tenant: tenant, startMs: env.Clock().NowMs()}
		env.After(slice, kindProduce, p.produce)
		return p
	}
}

func (p *producer) Receive(Message) {}

// produce sends the events of slice n, whose times are in [base, base+slice)
// of the producer's clock before their delay.
func (p *producer) produce() {
	if time.Duration(p.n)*slice >= p.sc.opts.Load {
		return
	}
	r := p.env.Rand()
	base := p.startMs + p.n*slice.Milliseconds()
	var b batches
	if p.tenant == "t1" {
		b = p.sc.t1Slice(r, p.n, base)
	} else {
		b = t2Slice(r, base)
		if p.n%junkEvery == junkEvery-1 {
			p.send(kindMetrics, "t9", 2, "cpu{host=\"h0\"} 1 "+strconv.FormatInt(base, 10)+"\ncpu{host=\"h1\"} 2 "+strconv.FormatInt(base, 10)+"\n")
			p.send(kindMetrics, p.tenant, 2, "cpu{host=\"h0\"} 1 "+strconv.FormatInt(base, 10)+"\ncpu{host=\"h1\"} not_a_number "+strconv.FormatInt(base, 10)+"\n")
		}
	}
	if b.metricsN > 0 {
		p.send(kindMetrics, p.tenant, b.metricsN, string(b.metrics))
	}
	if b.logsN > 0 {
		p.send(kindLogs, p.tenant, b.logsN, string(b.logs))
	}
	p.n++
	p.env.After(slice, kindProduce, p.produce)
}

func (p *producer) send(kind, tenant string, events int, body string) {
	p.env.Send(serveNode, kind, encode(ingestMsg{Tenant: tenant, Events: events, Body: body}))
}

// batches holds the two bodies of a slice, one event per line.
type batches struct {
	metrics, logs   []byte
	metricsN, logsN int
}

func (b *batches) metric(key string, value []byte, ts int64) {
	b.metrics = append(b.metrics, key...)
	b.metrics = append(b.metrics, ' ')
	b.metrics = append(b.metrics, value...)
	b.metrics = append(b.metrics, ' ')
	b.metrics = strconv.AppendInt(b.metrics, ts, 10)
	b.metrics = append(b.metrics, '\n')
	b.metricsN++
}

func (b *batches) log(ts int64, labels, fields string) {
	b.logs = append(b.logs, `{"ts":`...)
	b.logs = strconv.AppendInt(b.logs, ts, 10)
	b.logs = append(b.logs, `,"labels":`...)
	b.logs = append(b.logs, labels...)
	b.logs = append(b.logs, `,"fields":`...)
	b.logs = append(b.logs, fields...)
	b.logs = append(b.logs, `,"msg":"request"}`+"\n"...)
	b.logsN++
}

// t1Slice draws the events of t1 in slice n: per service, the rates of its
// phase spread over the slices of the second, and the burst in the first
// slice of burstSec.
func (sc *scenario) t1Slice(r rng.Source, n, base int64) batches {
	sec := n * slice.Milliseconds() / 1000
	var b batches
	for _, s := range services {
		ph := phaseAt(sec, s)
		svc := `{svc="` + s.name + `"}`
		for range perSlice(r, ph.latency) {
			v := math.Exp(ph.latencyMu + normal(r))
			b.metric("latency_ms"+svc, strconv.AppendFloat(nil, v, 'g', -1, 64), eventTs(r, base))
		}
		for range perSlice(r, ph.errors) {
			v := ph.errorsMin + r.IntN(ph.errorsSpan)
			b.metric("errors"+svc, strconv.AppendInt(nil, int64(v), 10), eventTs(r, base))
		}
		labels := `{"svc":"` + s.name + `"}`
		for range perSlice(r, ph.logs) {
			b.log(eventTs(r, base), labels, `{"user":"u`+strconv.Itoa(r.IntN(ph.users))+`"}`)
		}
		if s.name == "api" && sec == sc.burstSec && n%10 == 0 {
			for range burstLogs {
				b.log(eventTs(r, base), labels, `{"user":"u`+strconv.Itoa(r.IntN(burstUsers))+`"}`)
			}
		}
	}
	return b
}

// t2Slice draws the events of t2, which has no rules: about 60 a second, half
// gauges and half logs, above its quota of 40.
func t2Slice(r rng.Source, base int64) batches {
	var b batches
	for range 3 {
		key := `cpu{host="h` + strconv.Itoa(r.IntN(4)) + `"}`
		b.metric(key, strconv.AppendFloat(nil, math.Exp(3+normal(r)), 'g', -1, 64), eventTs(r, base))
	}
	for range 3 {
		b.log(eventTs(r, base), `{"app":"shop"}`, `{"user":"u`+strconv.Itoa(r.IntN(100))+`"}`)
	}
	return b
}

// perSlice draws how many events of a rate per second fall in one slice: the
// whole part of rate/10 and one more with the probability of the rest.
func perSlice(r rng.Source, rate int) int {
	n := rate / 10
	if r.IntN(10) < rate%10 {
		n++
	}
	return n
}

// eventTs draws a time in the slice that starts at base and takes off a delay
// of D15: none for 90% of the events, uniform in (0, L) for 9% and uniform in
// (L, 3L) for 1%, which are late.
func eventTs(r rng.Source, base int64) int64 {
	ts := base + int64(r.IntN(int(slice.Milliseconds())))
	switch p := r.IntN(100); {
	case p < 90:
		return ts
	case p < 99:
		return ts - 1 - int64(r.IntN(latenessMs-1))
	default:
		return ts - latenessMs - 1 - int64(r.IntN(2*latenessMs-1))
	}
}

// normal draws a standard normal value by Box-Muller.
func normal(r rng.Source) float64 {
	u := 1 - r.Float64()
	return math.Sqrt(-2*math.Log(u)) * math.Cos(2*math.Pi*r.Float64())
}
