package agg

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/sketch"
)

// auditEvery is the share of the buckets with a DDSketch that also keep their
// exact values for the observed error of §4.10.
const auditEvery = 1000

// quantiles are the declared quantiles of D07, as §4.8 names them.
var quantiles = [...]struct {
	q     float64
	label string
}{{0.5, "0.5"}, {0.9, "0.9"}, {0.99, "0.99"}, {0.999, "0.999"}}

// SketchConfig says which series of a tenant carry sketches (D07).
type SketchConfig struct {
	// QuantileMetrics lists the metric names with a DDSketch per bucket.
	QuantileMetrics []string
	// CardinalityFields lists the log fields with an HLL per bucket.
	CardinalityFields []string
}

// DDSketch returns the DDSketch of the bucket, or nil if it has none. Like the
// bucket it must not be changed.
func (b *Bucket) DDSketch() *sketch.DDSketch {
	if e, ok := b.Ext.(*quantileExt); ok {
		return e.dd
	}
	return nil
}

// HLL returns the HLL of field in the bucket, or nil if it has none. Like the
// bucket it must not be changed.
func (b *Bucket) HLL(field string) *sketch.HLL {
	e, ok := b.Ext.(*cardinalityExt)
	if !ok {
		return nil
	}
	i, found := slices.BinarySearch(e.names, field)
	if !found {
		return nil
	}
	return &e.hlls[i]
}

// exportedExt is an extension that adds members to its line of buckets.jsonl.
type exportedExt interface {
	appendExport(dst []byte) []byte
}

// errorMax is the largest observed relative error. Shards raise it as they
// close buckets; being a maximum, the result does not depend on their order.
type errorMax struct {
	bits atomic.Uint64
}

func (m *errorMax) load() float64 { return math.Float64frombits(m.bits.Load()) }

func (m *errorMax) raise(v float64) {
	for {
		old := m.bits.Load()
		if !(v > math.Float64frombits(old)) || m.bits.CompareAndSwap(old, math.Float64bits(v)) {
			return
		}
	}
}

// sketchFactory returns the factory of the extensions cfg asks for. Metric
// buckets only ever get a DDSketch and log buckets only HLLs.
func sketchFactory(cfg map[string]SketchConfig, errMax *errorMax) ExtFactory {
	type tenantSketches struct {
		metrics map[string]bool
		fields  []string
	}
	tenants := make(map[string]*tenantSketches, len(cfg))
	for id, c := range cfg {
		t := &tenantSketches{metrics: make(map[string]bool, len(c.QuantileMetrics))}
		for _, name := range c.QuantileMetrics {
			t.metrics[name] = true
		}
		t.fields = slices.Compact(slices.Sorted(slices.Values(c.CardinalityFields)))
		tenants[id] = t
	}
	return func(tenant, series string, end int64, kind event.Kind) Ext {
		t := tenants[tenant]
		if t == nil {
			return nil
		}
		if kind == event.KindLogs {
			if len(t.fields) == 0 {
				return nil
			}
			return newCardinalityExt(t.fields)
		}
		name, _, _ := strings.Cut(series, "{")
		if !t.metrics[name] {
			return nil
		}
		return &quantileExt{dd: sketch.NewDDSketch(), audit: audited(tenant, series, end), max: errMax}
	}
}

// audited reports whether the bucket is one of the audited ones. The choice
// depends only on the key of the bucket, so a replay picks the same ones.
func audited(tenant, series string, end int64) bool {
	return sketch.Mix64(event.SeriesHash(tenant, series)^uint64(end))%auditEvery == 0
}

// quantileExt is the DDSketch of a metric bucket and, in an audited bucket,
// its exact values.
type quantileExt struct {
	dd     *sketch.DDSketch
	audit  bool
	values []float64
	max    *errorMax
	q      [len(quantiles)]float64
}

// Add implements Ext.
func (e *quantileExt) Add(value float64, _ string) {
	e.dd.Add(value)
	// NaN takes no rank in the sketch, so it has none here either.
	if e.audit && !math.IsNaN(value) {
		e.values = append(e.values, value)
	}
}

// Close implements Ext.
func (e *quantileExt) Close() {
	for i := range quantiles {
		e.q[i] = e.dd.Quantile(quantiles[i].q)
	}
	if e.audit {
		e.max.raise(e.observedError())
		e.values = nil
	}
}

// observedError is the largest relative error of the declared quantiles
// against the exact ones of the same rank. Zeros and infinities have exact
// estimates, which the comparison skips before the division.
func (e *quantileExt) observedError() float64 {
	if len(e.values) == 0 {
		return 0
	}
	slices.Sort(e.values)
	worst := 0.0
	for i := range quantiles {
		exact := e.values[sketch.Rank(quantiles[i].q, uint64(len(e.values)))]
		if e.q[i] != exact {
			worst = max(worst, math.Abs(e.q[i]-exact)/math.Abs(exact))
		}
	}
	return worst
}

// AppendState implements Ext: the state of the sketch and the exact values.
func (e *quantileExt) AppendState(dst []byte) []byte {
	dst = appendField(dst, e.dd.AppendState(nil))
	dst = binary.AppendUvarint(dst, 8*uint64(len(e.values)))
	for _, v := range e.values {
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(v))
	}
	return dst
}

// LoadState implements Ext.
func (e *quantileExt) LoadState(state []byte) error {
	dd, rest, ok := cutField(state)
	if !ok {
		return errors.New("agg: truncated sketch state")
	}
	raw, rest, ok := cutField(rest)
	if !ok || len(rest) != 0 || len(raw)%8 != 0 {
		return errors.New("agg: bad audit values in sketch state")
	}
	if err := e.dd.LoadState(dd); err != nil {
		return err
	}
	if n := uint64(len(raw) / 8); (e.audit && n != e.dd.Count()) || (!e.audit && n != 0) {
		return fmt.Errorf("agg: %d audit values for %d in the sketch, audited %v", n, e.dd.Count(), e.audit)
	}
	e.values = nil
	for ; len(raw) > 0; raw = raw[8:] {
		e.values = append(e.values, math.Float64frombits(binary.LittleEndian.Uint64(raw)))
	}
	return nil
}

func (e *quantileExt) appendExport(dst []byte) []byte {
	dst = append(dst, `,"q":{`...)
	for i := range quantiles {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONString(dst, quantiles[i].label)
		dst = append(dst, ':')
		dst = appendJSONFloat(dst, e.q[i])
	}
	return append(dst, '}')
}

// cardinalityExt is the HLL per configured field of a log bucket. names is
// sorted and shared among the buckets of the tenant.
type cardinalityExt struct {
	names []string
	hlls  []sketch.HLL
	card  []uint64
}

func newCardinalityExt(names []string) *cardinalityExt {
	return &cardinalityExt{names: names, hlls: make([]sketch.HLL, len(names))}
}

// Add implements Ext. fields is canonical (§4.2), so it is sorted like names
// and one pass over both is enough. What enters the HLL is the value as it is
// escaped there, which is one-to-one with the value itself.
func (e *cardinalityExt) Add(_ float64, fields string) {
	j := 0
	for i := 1; i < len(fields) && j < len(e.names); {
		name, value, next, ok := event.NextLabel(fields, i)
		if !ok {
			return
		}
		for j < len(e.names) && e.names[j] < name {
			j++
		}
		if j < len(e.names) && e.names[j] == name {
			e.hlls[j].Add(value)
			j++
		}
		i = next
	}
}

// Close implements Ext.
func (e *cardinalityExt) Close() {
	e.card = make([]uint64, len(e.hlls))
	for i := range e.hlls {
		e.card[i] = e.hlls[i].Estimate()
	}
}

// AppendState implements Ext: name and state of each HLL. The names make a
// change of cardinality_fields fail a recovery instead of mixing up fields.
func (e *cardinalityExt) AppendState(dst []byte) []byte {
	for i, name := range e.names {
		dst = appendField(dst, []byte(name))
		dst = appendField(dst, e.hlls[i].AppendState(nil))
	}
	return dst
}

// LoadState implements Ext.
func (e *cardinalityExt) LoadState(state []byte) error {
	for i, want := range e.names {
		name, rest, ok := cutField(state)
		if !ok || string(name) != want {
			return fmt.Errorf("agg: sketch state does not hold field %q", want)
		}
		hll, rest, ok := cutField(rest)
		if !ok {
			return errors.New("agg: truncated sketch state")
		}
		if err := e.hlls[i].LoadState(hll); err != nil {
			return err
		}
		state = rest
	}
	if len(state) != 0 {
		return errors.New("agg: sketch state holds more fields than configured")
	}
	return nil
}

func (e *cardinalityExt) appendExport(dst []byte) []byte {
	dst = append(dst, `,"card":{`...)
	for i, name := range e.names {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONString(dst, name)
		dst = append(dst, ':')
		dst = strconv.AppendUint(dst, e.card[i], 10)
	}
	return append(dst, '}')
}

func appendField(dst, v []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(v)))
	return append(dst, v...)
}

func cutField(p []byte) (v, rest []byte, ok bool) {
	n, k := binary.Uvarint(p)
	if k <= 0 || n > uint64(len(p)-k) {
		return nil, nil, false
	}
	end := k + int(n)
	return p[k:end], p[end:], true
}
