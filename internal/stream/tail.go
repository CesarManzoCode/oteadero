package stream

import (
	"encoding/binary"
	"errors"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/CesarManzoCode/oteadero/internal/agg"
	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

// nameLabel is the label that matches a metric's name, as in rules.
const nameLabel = "__name__"

// Tail serves GET /v1/tail. Each subscriber reads the log files with its own
// reader, at its own pace, and only up to the durable end the sequencer
// reports after each fsync; between fsyncs it waits on a channel, never
// polling. A subscriber pins the segment it is reading, so that the log
// retention does not remove it until the subscriber is gone or cut off.
type Tail struct {
	fsys   vfs.FS
	dir    string
	logID  uint64
	maxLag uint64

	mu     sync.Mutex
	end    wal.Pos
	ch     chan struct{}
	closed bool
	subs   map[*tailSub]struct{}
}

type tailSub struct {
	// segment is the first segment the subscriber may still read; guarded
	// by Tail.mu.
	segment uint64
}

// NewTail returns the tail of the log in dir, whose durable end is end.
// maxLag is tail_max_lag_events.
func NewTail(fsys vfs.FS, dir string, logID uint64, end wal.Pos, maxLag uint64) *Tail {
	return &Tail{
		fsys:   fsys,
		dir:    dir,
		logID:  logID,
		maxLag: maxLag,
		end:    end,
		ch:     make(chan struct{}),
		subs:   map[*tailSub]struct{}{},
	}
}

// Synced records a new durable end of the log and wakes the subscribers. It
// is the sequencer's sync hook and does not block.
func (t *Tail) Synced(end wal.Pos) {
	t.mu.Lock()
	t.end = end
	close(t.ch)
	t.ch = make(chan struct{})
	t.mu.Unlock()
}

// Close ends every subscriber and refuses new ones.
func (t *Tail) Close() {
	t.mu.Lock()
	if !t.closed {
		t.closed = true
		close(t.ch)
		t.ch = make(chan struct{})
	}
	t.mu.Unlock()
}

// Active returns the number of connected subscribers.
func (t *Tail) Active() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.subs)
}

// Pinned returns the lowest segment a subscriber may still read; ok is false
// with no subscriber. It is the checkpoint manager's retention pin.
func (t *Tail) Pinned() (segment uint64, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for s := range t.subs {
		if !ok || s.segment < segment {
			segment, ok = s.segment, true
		}
	}
	return segment, ok
}

// matcher is one exact label match; value is escaped as in canonical keys.
type matcher struct {
	name, value string
}

// parseMatch parses `k=v,...`.
func parseMatch(s string) ([]matcher, bool) {
	if s == "" {
		return nil, true
	}
	var out []matcher
	for pair := range strings.SplitSeq(s, ",") {
		name, value, ok := strings.Cut(pair, "=")
		if !ok || name == "" {
			return nil, false
		}
		out = append(out, matcher{name: name, value: event.EscapeValue(value)})
	}
	return out, true
}

// labelValue returns the escaped value of name in a canonical label set.
func labelValue(set, name string) (string, bool) {
	for i := 1; i < len(set); {
		n, v, next, ok := event.NextLabel(set, i)
		if !ok {
			return "", false
		}
		if n == name {
			return v, true
		}
		i = next
	}
	return "", false
}

// matches applies ms to a series: a metric key `name{...}` or the canonical
// labels of a log.
func matches(ms []matcher, series string, kind event.Kind) bool {
	name, labels := "", series
	if kind == event.KindMetrics {
		if i := strings.IndexByte(series, '{'); i >= 0 {
			name, labels = series[:i], series[i:]
		}
	}
	for _, m := range ms {
		v, ok := "", false
		if kind == event.KindMetrics && m.name == nameLabel {
			v, ok = name, true
		} else {
			v, ok = labelValue(labels, m.name)
		}
		if !ok || v != m.value {
			return false
		}
	}
	return true
}

// batchIs reports whether a BATCH payload (§4.3) belongs to tenant and kind,
// reading only its header.
func batchIs(p []byte, tenant string, kind event.Kind) bool {
	tl, k := binary.Uvarint(p)
	if k <= 0 || tl >= uint64(len(p)-k) {
		return false
	}
	return string(p[k:k+int(tl)]) == tenant && event.Kind(p[k+int(tl)]) == kind
}

// unescape reverses the escaping of a canonical label value.
func unescape(v string) string {
	if !strings.Contains(v, `\`) {
		return v
	}
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\\' && i+1 < len(v) {
			i++
			if c = v[i]; c == 'n' {
				c = '\n'
			}
		}
		b.WriteByte(c)
	}
	return b.String()
}

// appendLabelsJSON appends a canonical label set as a JSON object.
func appendLabelsJSON(dst []byte, set string) []byte {
	dst = append(dst, '{')
	for i, first := 1, true; i < len(set); first = false {
		n, v, next, ok := event.NextLabel(set, i)
		if !ok {
			break
		}
		if !first {
			dst = append(dst, ',')
		}
		dst = agg.AppendJSONString(dst, n)
		dst = append(dst, ':')
		dst = agg.AppendJSONString(dst, unescape(v))
		i = next
	}
	return append(dst, '}')
}

func appendMetricJSON(dst []byte, o uint64, e *event.Metric) []byte {
	name, labels := e.Key, "{}"
	if i := strings.IndexByte(e.Key, '{'); i >= 0 {
		name, labels = e.Key[:i], e.Key[i:]
	}
	dst = append(dst, `{"o":`...)
	dst = strconv.AppendUint(dst, o, 10)
	dst = append(dst, `,"ts":`...)
	dst = strconv.AppendInt(dst, e.TsMs, 10)
	dst = append(dst, `,"name":`...)
	dst = agg.AppendJSONString(dst, name)
	dst = append(dst, `,"labels":`...)
	dst = appendLabelsJSON(dst, labels)
	dst = append(dst, `,"value":`...)
	dst = agg.AppendJSONFloat(dst, e.Value)
	return append(dst, '}')
}

func appendLogJSON(dst []byte, o uint64, e *event.Log) []byte {
	dst = append(dst, `{"o":`...)
	dst = strconv.AppendUint(dst, o, 10)
	dst = append(dst, `,"ts":`...)
	dst = strconv.AppendInt(dst, e.TsMs, 10)
	dst = append(dst, `,"labels":`...)
	dst = appendLabelsJSON(dst, e.Labels)
	dst = append(dst, `,"fields":`...)
	dst = appendLabelsJSON(dst, e.Fields)
	dst = append(dst, `,"msg":`...)
	dst = agg.AppendJSONString(dst, e.Msg)
	return append(dst, '}')
}

// before reports whether p comes before the durable end.
func before(p, end wal.Pos) bool {
	return p.Segment < end.Segment || (p.Segment == end.Segment && p.Byte < end.Byte)
}

// ServeHTTP implements GET /v1/tail?tenant=<t>&kind=logs|metrics[&match=k=v,...].
func (t *Tail) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	tenant := q.Get("tenant")
	var kind event.Kind
	switch q.Get("kind") {
	case "logs":
		kind = event.KindLogs
	case "metrics":
		kind = event.KindMetrics
	}
	ms, okMatch := parseMatch(q.Get("match"))
	tok, resume, err := requestToken(r)
	if tenant == "" || kind == 0 || !okMatch || err != nil || (resume && tok.live) {
		writeCode(w, http.StatusBadRequest, codeInvalid)
		return
	}
	if resume && tok.logID != t.logID {
		writeCode(w, http.StatusGone, codeUnknownLog)
		return
	}

	// Pin everything until the reader knows its segment, so that no
	// segment it is about to open can be removed under it.
	sub := &tailSub{}
	t.mu.Lock()
	end, closed := t.end, t.closed
	if !closed {
		t.subs[sub] = struct{}{}
	}
	t.mu.Unlock()
	if closed {
		writeCode(w, http.StatusServiceUnavailable, codeShuttingDown)
		return
	}
	defer func() {
		t.mu.Lock()
		delete(t.subs, sub)
		t.mu.Unlock()
	}()

	// Without a token, or with the token of the last durable event, the
	// reader starts at the durable end, which is a record boundary even when
	// the writer has just created a segment whose header is not written yet.
	var rd *wal.Reader
	next := end.Next
	switch {
	case resume && tok.n >= end.Next:
		// Past the durable end: no such offset is retained.
		writeCode(w, http.StatusGone, codeExpired)
		return
	case resume && tok.n+1 < end.Next:
		next = tok.n + 1
		rd, err = wal.OpenReaderAt(t.fsys, t.dir, next)
	default:
		rd, err = wal.OpenReader(t.fsys, t.dir, end)
	}
	switch {
	case errors.Is(err, wal.ErrOffsetGone) || errors.Is(err, fs.ErrNotExist):
		writeCode(w, http.StatusGone, codeExpired)
		return
	case err != nil:
		writeCode(w, http.StatusInternalServerError, "log_failure")
		return
	}
	defer rd.Close()
	t.mu.Lock()
	sub.segment = rd.Pos().Segment
	t.mu.Unlock()

	rc := startEvents(w)
	var buf, data []byte
	lagged := func() {
		last := ""
		if next > 0 {
			last = formatTail(t.logID, next-1)
		}
		w.Write(appendLagged(buf[:0], last))
		rc.Flush()
	}
	for {
		t.mu.Lock()
		end, wake, closed := t.end, t.ch, t.closed
		t.mu.Unlock()
		for before(rd.Pos(), end) {
			if end.Next > next && end.Next-next > t.maxLag {
				lagged()
				return
			}
			rec, err := rd.Next()
			if err != nil {
				return
			}
			if rec.Type != wal.TypeBatch || rec.Offset+rec.Events <= next {
				continue
			}
			if batchIs(rec.Payload, tenant, kind) {
				b, err := event.DecodeBatch(rec.Payload)
				if err != nil {
					return
				}
				buf = buf[:0]
				for i := range b.Len() {
					o := rec.Offset + uint64(i)
					if o < next {
						continue
					}
					if kind == event.KindMetrics {
						e := &b.Metrics[i]
						if !matches(ms, e.Key, kind) {
							continue
						}
						data = appendMetricJSON(data[:0], o, e)
					} else {
						e := &b.Logs[i]
						if !matches(ms, e.Labels, kind) {
							continue
						}
						data = appendLogJSON(data[:0], o, e)
					}
					buf = appendEvent(buf, formatTail(t.logID, o), data)
				}
				if len(buf) > 0 {
					// The write blocks while the client does not read: the
					// backpressure of TCP.
					if _, err := w.Write(buf); err != nil {
						return
					}
					if err := rc.Flush(); err != nil {
						return
					}
				}
			}
			next = rec.Offset + rec.Events
			if seg := rd.Pos().Segment; seg != sub.segment {
				t.mu.Lock()
				sub.segment = seg
				t.mu.Unlock()
			}
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
