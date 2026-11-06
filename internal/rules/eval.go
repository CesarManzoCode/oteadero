package rules

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/CesarManzoCode/oteadero/internal/agg"
	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/sketch"
)

// States a Transition records (§4.6). StateInactive is only the way back from
// pending; a firing group ends in StateResolved.
const (
	StatePending  = "pending"
	StateInactive = "inactive"
	StateFiring   = "firing"
	StateResolved = "resolved"
)

// Eval is the value of a group at an instant. Value means nothing unless
// Present: an absent group has null as its value. ClosedAt is the closed_at of
// the watermark step that reached T.
type Eval struct {
	Rule     string
	Group    string
	T        int64
	Value    float64
	Present  bool
	ClosedAt uint64
}

// Transition is a change of state of a group (§4.6), with the value of the
// window at T as in Eval. Incident is set in firing and resolved.
type Transition struct {
	Rule     string
	Group    string
	T        int64
	State    string
	Value    float64
	Present  bool
	ClosedAt uint64
	Incident Incident
}

// Output receives what an Evaluator produces, for each rule by increasing T
// and within a T by group key. An instant yields an Eval for every group with
// samples in its window or in pending or firing; an absent inactive group
// yields nothing. The pointers are valid only during the call.
type Output interface {
	Eval(e *Eval)
	Transition(t *Transition)
}

// alertState is the state of (rule, group).
type alertState uint8

const (
	alertInactive alertState = iota
	alertPending
	alertFiring
)

// slot holds what a group has of one bucket end. The series sums stay apart
// because §4.5 fixes the order of the whole addition: adding them up per end
// and then per window rounds differently.
type slot struct {
	end   int64
	count uint64
	sums  []float64
	dd    *sketch.DDSketch
	hll   *sketch.HLL
}

// group is one (rule, group). slots acts as the ring of the window: by
// ascending end, only the ends that have data, with the front dropped as the
// window passes it. Keeping several ends beyond the window is what lets one
// watermark step close more than R and still evaluate each T it crossed.
type group struct {
	state     alertState
	activeAt  int64
	keeping   bool
	keepSince int64
	firedAt   int64
	slots     []slot
}

// label is a label of a series, its value as the canonical key escapes it.
type label struct {
	name, value string
}

type ruleState struct {
	*Rule
	kind     event.Kind
	matchers []label
	by       []string
	stay     float64
	groups   map[string]*group
}

type tenantState struct {
	rules   []*ruleState
	defined bool
	// prev is the upTo of the last step. For a T multiple of S, W_prev < T
	// and upTo_prev < T are the same condition.
	prev int64
}

// series is the parsed key of the bucket being delivered.
type series struct {
	metric bool
	name   string
	labels []label
}

// Evaluator feeds the windows from the closed buckets and evaluates the rules
// as the watermark advances. Subscribe it to the Aggregator: it relies on the
// order of a Subscriber, in which a tenant's buckets come by end and, within
// an end, by series key, all before the step that closed them.
type Evaluator struct {
	set     *Set
	out     Output
	rules   []*ruleState
	tenants map[string]*tenantState
	cur     series
	key     []byte
	hll     sketch.HLL
}

var _ agg.Advancer = (*Evaluator)(nil)

// NewEvaluator returns an Evaluator with no state. sketches is the sketch
// configuration of the aggregator: a rule over a sketch it does not compute
// would never have a value, so it is an error here.
func NewEvaluator(set *Set, sketches map[string]agg.SketchConfig, out Output) (*Evaluator, error) {
	if out == nil {
		return nil, errors.New("rules: evaluator needs an output")
	}
	e := &Evaluator{set: set, out: out, tenants: map[string]*tenantState{}}
	for i := range set.Rules {
		r := &set.Rules[i]
		if err := checkSketches(r, sketches[r.Tenant]); err != nil {
			return nil, fmt.Errorf("rules: rule %q: %w", r.Name, err)
		}
		rs := newRuleState(r)
		e.rules = append(e.rules, rs)
		t := e.tenants[r.Tenant]
		if t == nil {
			t = &tenantState{}
			e.tenants[r.Tenant] = t
		}
		t.rules = append(t.rules, rs)
	}
	return e, nil
}

func checkSketches(r *Rule, cfg agg.SketchConfig) error {
	switch r.Agg {
	case AggQuantile:
		name, ok := r.Match[nameLabel]
		if !ok || !slices.Contains(cfg.QuantileMetrics, name) {
			return fmt.Errorf("match must name a metric in quantile_metrics of tenant %s", r.Tenant)
		}
	case AggCardinality:
		if !slices.Contains(cfg.CardinalityFields, r.Field) {
			return fmt.Errorf("field %s is not in cardinality_fields of tenant %s", r.Field, r.Tenant)
		}
	}
	return nil
}

func newRuleState(r *Rule) *ruleState {
	rs := &ruleState{Rule: r, kind: event.KindMetrics, stay: *r.Threshold, groups: map[string]*group{}}
	if r.Source == SourceLogs {
		rs.kind = event.KindLogs
	}
	if r.ResolveThreshold != nil {
		rs.stay = *r.ResolveThreshold
	}
	for _, name := range slices.Sorted(maps.Keys(r.Match)) {
		rs.matchers = append(rs.matchers, label{name: name, value: event.EscapeValue(r.Match[name])})
	}
	if r.By != nil {
		// Not nil even when empty: it means one group for everything.
		rs.by = slices.Compact(slices.Sorted(slices.Values(r.By)))
		if rs.by == nil {
			rs.by = []string{}
		}
	}
	return rs
}

// Closed implements agg.Subscriber.
func (e *Evaluator) Closed(b *agg.Bucket) {
	t := e.tenants[b.Tenant]
	if t == nil {
		return
	}
	e.cur.parse(b.Series, b.Kind)
	for _, r := range t.rules {
		if r.kind == b.Kind && e.cur.matches(r.matchers) {
			r.add(e.groupKey(r), b)
		}
	}
}

// Late implements agg.Subscriber: a late event enters no window (D06).
func (e *Evaluator) Late(string, uint64) {}

// Advanced implements agg.Advancer. The step that defines the watermark
// evaluates nothing. A rule with no groups has nothing to do at any instant,
// so a jump of the watermark costs only the instants its groups last.
func (e *Evaluator) Advanced(tenant string, upTo int64, closedAt uint64) {
	t := e.tenants[tenant]
	if t == nil {
		return
	}
	if !t.defined {
		t.defined = true
		t.prev = upTo
		return
	}
	for _, r := range t.rules {
		for at := floorTo(t.prev, r.IntervalMs) + r.IntervalMs; at <= upTo && len(r.groups) > 0; at += r.IntervalMs {
			e.evaluate(r, at, closedAt)
		}
	}
	t.prev = upTo
}

// floorTo returns the largest multiple of s that is at most v.
func floorTo(v, s int64) int64 {
	q := v / s
	if v%s < 0 {
		q--
	}
	return q * s
}

func (s *series) parse(key string, kind event.Kind) {
	name, rest, _ := strings.Cut(key, "{")
	s.metric = kind == event.KindMetrics
	s.name = name
	s.labels = s.labels[:0]
	for i := 0; ; {
		n, v, next, ok := event.NextLabel(rest, i)
		if !ok {
			return
		}
		s.labels = append(s.labels, label{name: n, value: v})
		i = next
	}
}

func (s *series) get(name string) (string, bool) {
	if s.metric && name == nameLabel {
		return s.name, true
	}
	for i := range s.labels {
		if s.labels[i].name == name {
			return s.labels[i].value, true
		}
	}
	return "", false
}

func (s *series) matches(matchers []label) bool {
	for i := range matchers {
		if v, ok := s.get(matchers[i].name); !ok || v != matchers[i].value {
			return false
		}
	}
	return true
}

// groupKey returns the canonical group of the current series under r, valid
// until the next call. A by label the series lacks is left out, as in
// PromQL's `by`.
func (e *Evaluator) groupKey(r *ruleState) []byte {
	k := append(e.key[:0], '{')
	if r.by == nil {
		for i := range e.cur.labels {
			if e.cur.labels[i].name != nameLabel {
				k = appendLabel(k, e.cur.labels[i].name, e.cur.labels[i].value)
			}
		}
	} else {
		for _, name := range r.by {
			if v, ok := e.cur.get(name); ok {
				k = appendLabel(k, name, v)
			}
		}
	}
	k = append(k, '}')
	e.key = k
	return k
}

func appendLabel(k []byte, name, value string) []byte {
	if len(k) > 1 {
		k = append(k, ',')
	}
	k = append(k, name...)
	k = append(k, '=', '"')
	k = append(k, value...)
	return append(k, '"')
}

// keepsSums reports whether the value of the rule adds up the bucket sums.
func (r *ruleState) keepsSums() bool {
	return r.Agg == AggSum || (r.Agg == AggRate && r.kind == event.KindMetrics)
}

// add puts a closed bucket in the slot of its end. Ends only grow and series
// come in key order, which is the order §4.5 fixes for the sum.
func (r *ruleState) add(key []byte, b *agg.Bucket) {
	g := r.groups[string(key)]
	if g == nil {
		g = &group{}
		r.groups[string(key)] = g
	}
	if n := len(g.slots); n == 0 || g.slots[n-1].end != b.End {
		g.slots = append(g.slots, slot{end: b.End})
	}
	s := &g.slots[len(g.slots)-1]
	s.count += b.Count
	switch {
	case r.keepsSums():
		s.sums = append(s.sums, b.Sum)
	case r.Agg == AggQuantile:
		if dd := b.DDSketch(); dd != nil {
			if s.dd == nil {
				s.dd = sketch.NewDDSketch()
			}
			s.dd.Merge(dd)
		}
	case r.Agg == AggCardinality:
		if h := b.HLL(r.Field); h != nil {
			if s.hll == nil {
				s.hll = &sketch.HLL{}
			}
			s.hll.Merge(h)
		}
	}
}

// evaluate runs the instant T of rule r over its groups.
func (e *Evaluator) evaluate(r *ruleState, t int64, closedAt uint64) {
	for _, key := range slices.Sorted(maps.Keys(r.groups)) {
		g := r.groups[key]
		g.expire(t - r.RangeMs)
		v, present := e.value(r, g, t)
		if present || g.state != alertInactive {
			e.out.Eval(&Eval{Rule: r.Name, Group: key, T: t, Value: v, Present: present, ClosedAt: closedAt})
			if state, changed := r.step(g, t, v, present); changed {
				tr := &Transition{Rule: r.Name, Group: key, T: t, State: state, Value: v, Present: present, ClosedAt: closedAt}
				if state == StateFiring || state == StateResolved {
					tr.Incident = Incident{Rule: r.Name, Group: key, FiredAt: g.firedAt}
				}
				e.out.Transition(tr)
			}
		}
		if len(g.slots) == 0 && g.state == alertInactive {
			delete(r.groups, key)
		}
	}
}

// expire drops the slots with end ≤ edge: no later window can hold them.
func (g *group) expire(edge int64) {
	n := 0
	for n < len(g.slots) && g.slots[n].end <= edge {
		n++
	}
	clear(g.slots[:n])
	g.slots = g.slots[n:]
}

// value returns the value of g in (T−R, T], whose left edge expire has already
// applied, and whether it has samples (§4.5).
func (e *Evaluator) value(r *ruleState, g *group, t int64) (float64, bool) {
	present := false
	count := uint64(0)
	sum := 0.0
	var dd *sketch.DDSketch
	if r.Agg == AggCardinality {
		e.hll = sketch.HLL{}
	}
	for i := range g.slots {
		s := &g.slots[i]
		if s.end > t {
			break
		}
		present = true
		count += s.count
		for _, x := range s.sums {
			sum += x
		}
		if s.dd != nil {
			if dd == nil {
				dd = sketch.NewDDSketch()
			}
			dd.Merge(s.dd)
		}
		if s.hll != nil {
			e.hll.Merge(s.hll)
		}
	}
	if !present {
		return 0, false
	}
	seconds := float64(r.RangeMs) / 1000
	switch r.Agg {
	case AggCount:
		return float64(count), true
	case AggSum:
		return sum, true
	case AggRate:
		if r.kind == event.KindLogs {
			return float64(count) / seconds, true
		}
		return sum / seconds, true
	case AggQuantile:
		if dd == nil {
			dd = sketch.NewDDSketch()
		}
		return dd.Quantile(*r.Quantile), true
	default:
		return float64(e.hll.Estimate()), true
	}
}

// step applies the state machine of §4.6 to g at T and returns the transition
// to record, if any. An absent group has a false condition (D08).
func (r *ruleState) step(g *group, t int64, v float64, present bool) (string, bool) {
	fires := present && r.holds(v, *r.Threshold)
	switch g.state {
	case alertInactive:
		if !fires {
			return "", false
		}
		g.activeAt = t
		if r.ForMs == 0 {
			return g.fire(t), true
		}
		g.state = alertPending
		return StatePending, true
	case alertPending:
		if !fires {
			g.state = alertInactive
			return StateInactive, true
		}
		if t-g.activeAt >= r.ForMs {
			return g.fire(t), true
		}
		return "", false
	default:
		if present && r.holds(v, r.stay) {
			g.keeping = false
			return "", false
		}
		if r.KeepFiringForMs > 0 {
			if !g.keeping {
				g.keeping = true
				g.keepSince = t
			}
			if t-g.keepSince < r.KeepFiringForMs {
				return "", false
			}
		}
		g.state = alertInactive
		g.keeping = false
		return StateResolved, true
	}
}

func (g *group) fire(t int64) string {
	g.state = alertFiring
	g.firedAt = t
	g.keeping = false
	return StateFiring
}
