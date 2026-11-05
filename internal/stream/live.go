package stream

import (
	"encoding/binary"
	"errors"
	"net/http"
	"slices"
	"sync"

	"github.com/CesarManzoCode/oteadero/internal/agg"
)

// RingSize is how many emissions of each tenant GET /v1/live can resume
// within (§4.9).
const RingSize = 100_000

// liveChunk bounds how many emissions a subscriber copies under the lock.
const liveChunk = 1024

// Live serves GET /v1/live. It subscribes to the aggregator: every closed
// bucket of a tenant gets the next seq of that tenant, so the numbering
// depends only on the log, and goes into the tenant's ring of the last
// emissions. Seqs are kept across restarts through the checkpoint; the ring
// starts empty and refills as the replay closes buckets again.
type Live struct {
	logID uint64
	size  uint64

	mu      sync.Mutex
	ch      chan struct{}
	closed  bool
	dirty   bool
	active  int
	tenants map[string]*ring
}

// ring holds the emissions with seq in [lo, next).
type ring struct {
	lo, next uint64
	items    [][]byte
}

var _ agg.Advancer = (*Live)(nil)

// NewLive returns the live stream of the log logID with a ring of size
// emissions per tenant.
func NewLive(logID uint64, size int) *Live {
	return &Live{logID: logID, size: uint64(size), ch: make(chan struct{}), tenants: map[string]*ring{}}
}

func (l *Live) ring(tenant string) *ring {
	r := l.tenants[tenant]
	if r == nil {
		r = &ring{}
		l.tenants[tenant] = r
	}
	return r
}

// Closed implements agg.Subscriber.
func (l *Live) Closed(b *agg.Bucket) {
	data := agg.AppendBucketJSON(nil, b)
	l.mu.Lock()
	r := l.ring(b.Tenant)
	if r.items == nil {
		r.items = make([][]byte, l.size)
	}
	r.items[r.next%l.size] = data
	r.next++
	if r.next-r.lo > l.size {
		r.lo = r.next - l.size
	}
	l.dirty = true
	l.mu.Unlock()
}

// Late implements agg.Subscriber.
func (l *Live) Late(string, uint64) {}

// Advanced implements agg.Advancer: it runs after the buckets of a step, and
// wakes the subscribers once for all of them.
func (l *Live) Advanced(string, int64, uint64) {
	l.mu.Lock()
	if l.dirty {
		l.dirty = false
		close(l.ch)
		l.ch = make(chan struct{})
	}
	l.mu.Unlock()
}

// Close ends every subscriber and refuses new ones.
func (l *Live) Close() {
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		close(l.ch)
		l.ch = make(chan struct{})
	}
	l.mu.Unlock()
}

// Active returns the number of connected subscribers.
func (l *Live) Active() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.active
}

// AppendState returns the next seq of each tenant, for the checkpoint. Call
// it while no bucket is being emitted, after the aggregator's barrier.
func (l *Live) AppendState() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	names := make([]string, 0, len(l.tenants))
	for t := range l.tenants {
		names = append(names, t)
	}
	slices.Sort(names)
	b := binary.AppendUvarint(nil, uint64(len(names)))
	for _, t := range names {
		b = binary.AppendUvarint(b, uint64(len(t)))
		b = append(b, t...)
		b = binary.AppendUvarint(b, l.tenants[t].next)
	}
	return b
}

var errBadState = errors.New("stream: bad live state")

// LoadState restores what AppendState wrote, with empty rings. Empty state,
// from a checkpoint written without it, means no emission yet.
func (l *Live) LoadState(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	uvarint := func() (uint64, bool) {
		v, k := binary.Uvarint(b)
		if k <= 0 {
			return 0, false
		}
		b = b[k:]
		return v, true
	}
	n, ok := uvarint()
	if !ok || n > uint64(len(b)) {
		return errBadState
	}
	tenants := make(map[string]*ring, n)
	for range n {
		tl, ok := uvarint()
		if !ok || tl > uint64(len(b)) {
			return errBadState
		}
		t := string(b[:tl])
		b = b[tl:]
		seq, ok := uvarint()
		if !ok {
			return errBadState
		}
		tenants[t] = &ring{lo: seq, next: seq}
	}
	if len(b) != 0 {
		return errBadState
	}
	l.mu.Lock()
	l.tenants = tenants
	l.mu.Unlock()
	return nil
}

// ServeHTTP implements GET /v1/live?tenant=<t>.
func (l *Live) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tenant := r.URL.Query().Get("tenant")
	tok, resume, err := requestToken(r)
	if tenant == "" || err != nil || (resume && !tok.live) {
		writeCode(w, http.StatusBadRequest, codeInvalid)
		return
	}
	if resume && tok.logID != l.logID {
		writeCode(w, http.StatusGone, codeUnknownLog)
		return
	}

	l.mu.Lock()
	var lo, head uint64
	if x := l.tenants[tenant]; x != nil {
		lo, head = x.lo, x.next
	}
	next := head
	if resume {
		next = tok.n + 1
	}
	status, code := 0, ""
	switch {
	case l.closed:
		status, code = http.StatusServiceUnavailable, codeShuttingDown
	case resume && (tok.n == ^uint64(0) || next < lo || next > head):
		status, code = http.StatusGone, codeExpired
	default:
		l.active++
	}
	l.mu.Unlock()
	if status != 0 {
		writeCode(w, status, code)
		return
	}
	defer func() {
		l.mu.Lock()
		l.active--
		l.mu.Unlock()
	}()

	rc := startEvents(w)
	var buf []byte
	chunk := make([][]byte, 0, liveChunk)
	for {
		l.mu.Lock()
		wake, closed := l.ch, l.closed
		x := l.tenants[tenant]
		fell := x != nil && next < x.lo
		chunk = chunk[:0]
		if x != nil && !fell {
			for s := next; s < x.next && len(chunk) < liveChunk; s++ {
				chunk = append(chunk, x.items[s%l.size])
			}
		}
		l.mu.Unlock()

		if fell {
			// The ring moved past what this subscriber has read.
			last := ""
			if next > 0 {
				last = formatLive(l.logID, next-1)
			}
			w.Write(appendLagged(buf[:0], last))
			rc.Flush()
			return
		}
		if len(chunk) > 0 {
			buf = buf[:0]
			for _, data := range chunk {
				buf = appendEvent(buf, formatLive(l.logID, next), data)
				next++
			}
			if _, err := w.Write(buf); err != nil {
				return
			}
			if err := rc.Flush(); err != nil {
				return
			}
			continue
		}
		if closed {
			return
		}
		select {
		case <-wake:
		case <-r.Context().Done():
			return
		}
	}
}
