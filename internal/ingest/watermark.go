package ingest

import "github.com/CesarManzoCode/oteadero/internal/event"

// watermarks applies the rules of §4.4 in log order: maxTs per tenant,
// W = maxTs − L, late iff W is defined before the event and end(ts) ≤ W,
// and the effect of a TICK.
type watermarks struct {
	bucketMs    int64
	latenessMs  int64
	idleGraceMs int64
	byTenant    map[string]*mark
	// order lists tenants by their first event, so TICK effects come out
	// in an order that depends only on the log.
	order []*mark
}

// mark is the state of a tenant whose watermark is defined.
type mark struct {
	tenant string
	maxTs  int64
}

func newWatermarks(bucketMs, latenessMs, idleGraceMs int64) *watermarks {
	return &watermarks{
		bucketMs:    bucketMs,
		latenessMs:  latenessMs,
		idleGraceMs: idleGraceMs,
		byTenant:    map[string]*mark{},
	}
}

// BucketEnd returns ceil(ts / s) · s, the end of the bucket (end − s, end]
// that holds ts.
func BucketEnd(ts, s int64) int64 {
	q := ts / s
	if ts%s > 0 {
		q++
	}
	return q * s
}

func eventTs(b *event.Batch, i int) int64 {
	if b.Kind == event.KindLogs {
		return b.Logs[i].TsMs
	}
	return b.Metrics[i].TsMs
}

// apply classifies the events of b in order and advances the tenant's
// watermark. late[i] is the classification of event i and wm[i] the
// watermark right after it.
func (w *watermarks) apply(b *event.Batch) (late []bool, wm []int64, nLate int) {
	n := b.Len()
	late = make([]bool, n)
	wm = make([]int64, n)
	m := w.byTenant[b.Tenant]
	for i := 0; i < n; i++ {
		ts := eventTs(b, i)
		if m == nil {
			m = &mark{tenant: b.Tenant, maxTs: ts}
			w.byTenant[b.Tenant] = m
			w.order = append(w.order, m)
		} else {
			if BucketEnd(ts, w.bucketMs) <= m.maxTs-w.latenessMs {
				late[i] = true
				nLate++
			}
			m.maxTs = max(m.maxTs, ts)
		}
		wm[i] = m.maxTs - w.latenessMs
	}
	return late, wm, nLate
}

// tick applies a TICK at server time tsMs and returns the tenants whose
// watermark rose, with the new watermark.
func (w *watermarks) tick(tsMs int64) []WatermarkAdvance {
	floor := tsMs - w.idleGraceMs
	var adv []WatermarkAdvance
	for _, m := range w.order {
		if m.maxTs < floor {
			m.maxTs = floor
			adv = append(adv, WatermarkAdvance{Tenant: m.tenant, Watermark: floor - w.latenessMs})
		}
	}
	return adv
}

// marks returns the tenants with a defined watermark, in first-event order.
func (w *watermarks) marks() []Mark {
	out := make([]Mark, len(w.order))
	for i, m := range w.order {
		out[i] = Mark{Tenant: m.tenant, MaxTs: m.maxTs}
	}
	return out
}

// restore replaces the state with marks, given in first-event order.
func (w *watermarks) restore(marks []Mark) {
	w.byTenant = make(map[string]*mark, len(marks))
	w.order = w.order[:0]
	for _, x := range marks {
		m := &mark{tenant: x.Tenant, maxTs: x.MaxTs}
		w.byTenant[x.Tenant] = m
		w.order = append(w.order, m)
	}
}

// minMaxTs returns the smallest maxTs among tenants with a defined
// watermark; some tenant is idle at server time now iff it is below
// now − G.
func (w *watermarks) minMaxTs() (int64, bool) {
	if len(w.order) == 0 {
		return 0, false
	}
	lo := w.order[0].maxTs
	for _, m := range w.order[1:] {
		lo = min(lo, m.maxTs)
	}
	return lo, true
}
