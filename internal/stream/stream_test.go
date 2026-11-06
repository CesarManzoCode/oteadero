package stream

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/CesarManzoCode/oteadero/internal/agg"
	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
	"github.com/CesarManzoCode/oteadero/internal/wal"
)

const logDir = "data/log"

// sseEvent is one Server-Sent Event as read by the test client.
type sseEvent struct {
	id, event, data string
}

type client struct {
	t    *testing.T
	resp *http.Response
	br   *bufio.Reader
}

// get opens a stream; lastID, if set, goes in Last-Event-ID.
func get(t *testing.T, url, lastID string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	return http.DefaultClient.Do(req)
}

// open opens a stream that must start; it returns once the response header
// arrived, when the starting point of the stream is fixed.
func open(t *testing.T, url, lastID string) *client {
	t.Helper()
	resp, err := get(t, url, lastID)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("GET %s = %d %s", url, resp.StatusCode, body)
	}
	c := &client{t: t, resp: resp, br: bufio.NewReader(resp.Body)}
	t.Cleanup(c.close)
	return c
}

func (c *client) close() { c.resp.Body.Close() }

func (c *client) next() sseEvent {
	c.t.Helper()
	var e sseEvent
	for {
		line, err := c.br.ReadString('\n')
		if err != nil {
			c.t.Fatalf("stream ended: %v", err)
		}
		line = strings.TrimSuffix(line, "\n")
		switch {
		case line == "":
			if e != (sseEvent{}) {
				return e
			}
		case strings.HasPrefix(line, "id: "):
			e.id = line[len("id: "):]
		case strings.HasPrefix(line, "event: "):
			e.event = line[len("event: "):]
		case strings.HasPrefix(line, "data: "):
			e.data = line[len("data: "):]
		}
	}
}

// wantGone checks a 410 with the code of §4.9.
func wantGone(t *testing.T, url, lastID, code string) {
	t.Helper()
	resp, err := get(t, url, lastID)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusGone || strings.TrimSpace(string(body)) != fmt.Sprintf(`{"code":%q}`, code) {
		t.Fatalf("GET %s = %d %s, want 410 %s", url, resp.StatusCode, body, code)
	}
}

type tailFixture struct {
	t    *testing.T
	log  *wal.Log
	fsys vfs.FS
	tail *Tail
	srv  *httptest.Server
}

func newTail(t *testing.T, segBytes int64, maxLag uint64) *tailFixture {
	t.Helper()
	mem := vfs.NewMem()
	l, _, err := wal.Open(mem, logDir, rng.New(1), wal.Options{SegmentBytes: segBytes})
	if err != nil {
		t.Fatal(err)
	}
	f := &tailFixture{t: t, log: l, fsys: mem, tail: NewTail(mem, logDir, l.ID(), l.End(), maxLag)}
	f.srv = httptest.NewServer(f.tail)
	t.Cleanup(f.srv.Close)
	t.Cleanup(f.tail.Close)
	return f
}

// append writes b durably and reports the new end to the tail, as the
// sequencer does after its fsync.
func (f *tailFixture) append(b *event.Batch) uint64 {
	f.t.Helper()
	first, err := f.log.AppendBatch(event.AppendBatch(nil, b), uint64(b.Len()))
	if err != nil {
		f.t.Fatal(err)
	}
	if err := f.log.Sync(); err != nil {
		f.t.Fatal(err)
	}
	f.tail.Synced(f.log.End())
	return first
}

func (f *tailFixture) url(query string) string { return f.srv.URL + "/v1/tail?" + query }

func (f *tailFixture) token(o uint64) string { return formatTail(f.log.ID(), o) }

func logsOf(tenant string, apps ...string) *event.Batch {
	b := &event.Batch{Tenant: tenant, Kind: event.KindLogs}
	for i, app := range apps {
		b.Logs = append(b.Logs, event.Log{TsMs: int64(1000 + i), Labels: `{app="` + app + `"}`, Fields: `{f="a\"b"}`, Msg: "line"})
	}
	return b
}

func metricsOf(tenant string, n int) *event.Batch {
	b := &event.Batch{Tenant: tenant, Kind: event.KindMetrics}
	for i := range n {
		b.Metrics = append(b.Metrics, event.Metric{Key: `up{job="j"}`, Value: float64(i), TsMs: int64(1000 + i)})
	}
	return b
}

// wantTail checks that the next event is the one at offset o.
func (f *tailFixture) wantTail(c *client, o uint64) map[string]any {
	f.t.Helper()
	e := c.next()
	if e.event != "" || e.id != f.token(o) {
		f.t.Fatalf("got %+v, want id %s", e, f.token(o))
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(e.data), &data); err != nil {
		f.t.Fatalf("data %q: %v", e.data, err)
	}
	if data["o"] != float64(o) {
		f.t.Fatalf("data %s has no \"o\": %d", e.data, o)
	}
	return data
}

func TestTailWithoutTokenStartsAtHead(t *testing.T) {
	f := newTail(t, 0, 1000)
	f.append(logsOf("a", "x", "x", "x")) // 0..2, before the subscriber
	c := open(t, f.url("tenant=a&kind=logs"), "")
	if f.tail.Active() != 1 {
		t.Fatalf("active = %d", f.tail.Active())
	}
	f.append(logsOf("b", "x"))  // 3, other tenant
	f.append(metricsOf("a", 1)) // 4, other kind
	f.append(logsOf("a", "x"))  // 5
	data := f.wantTail(c, 5)
	labels, _ := data["labels"].(map[string]any)
	fields, _ := data["fields"].(map[string]any)
	if labels["app"] != "x" || fields["f"] != `a"b` || data["msg"] != "line" || data["ts"] != float64(1000) {
		t.Fatalf("event %v", data)
	}
}

func TestTailResumesAtNextFilteredOffset(t *testing.T) {
	f := newTail(t, 0, 1000)
	f.append(logsOf("a", "x", "y", "x", "y", "y", "x")) // x at 0, 2, 5
	for _, c := range []*client{
		open(t, f.url("tenant=a&kind=logs&match=app=x&after="+f.token(2)), ""),
		open(t, f.url("tenant=a&kind=logs&match=app=x"), f.token(2)),
		open(t, f.url("tenant=a&kind=logs&match=app=x"), f.token(3)),
	} {
		f.wantTail(c, 5)
	}
	c := open(t, f.url("tenant=a&kind=logs&match=app=x"), f.token(0))
	f.wantTail(c, 2)
	f.wantTail(c, 5)
	f.append(logsOf("a", "y", "x")) // x at 7
	f.wantTail(c, 7)

	m := open(t, f.url("tenant=a&kind=metrics&match=__name__=up,job=j"), f.token(0))
	f.append(metricsOf("a", 2)) // 8, 9
	if data := f.wantTail(m, 8); data["name"] != "up" || data["value"] != float64(0) {
		t.Fatalf("metric %v", data)
	}
	f.wantTail(m, 9)
}

func TestTailTokenOfAnotherLog(t *testing.T) {
	f := newTail(t, 0, 1000)
	f.append(logsOf("a", "x"))
	wantGone(t, f.url("tenant=a&kind=logs"), formatTail(f.log.ID()+1, 0), codeUnknownLog)
	wantGone(t, f.url("tenant=a&kind=logs&after="+formatTail(f.log.ID()+1, 0)), "", codeUnknownLog)
}

func TestTailRetiredOffsetExpired(t *testing.T) {
	f := newTail(t, 256, 1000)
	for range 20 {
		f.append(logsOf("a", "x", "x"))
	}
	bases, err := wal.Segments(f.fsys, logDir)
	if err != nil || len(bases) < 3 {
		t.Fatalf("segments %v, %v", bases, err)
	}
	if err := wal.RemoveSegment(f.fsys, logDir, bases[0]); err != nil {
		t.Fatal(err)
	}
	wantGone(t, f.url("tenant=a&kind=logs"), f.token(0), codeExpired)
	wantGone(t, f.url("tenant=a&kind=logs"), f.token(40), codeExpired) // past the end
	c := open(t, f.url("tenant=a&kind=logs"), f.token(bases[1]-1))
	f.wantTail(c, bases[1])
}

func TestSlowReaderPastMaxLagIsCut(t *testing.T) {
	f := newTail(t, 0, 5)
	f.append(logsOf("a", "x"))
	c := open(t, f.url("tenant=a&kind=logs"), "")
	f.append(logsOf("a", "x", "x", "x", "x", "x", "x")) // head 7, reader at 1
	e := c.next()
	want := fmt.Sprintf(`{"code":"lagged","last":%q}`, f.token(0))
	if e.event != "error" || e.data != want {
		t.Fatalf("got %+v, want event error %s", e, want)
	}
	if _, err := c.br.ReadString('\n'); err != io.EOF {
		t.Fatalf("stream still open after lagged: %v", err)
	}
}

func TestTailPinsItsSegment(t *testing.T) {
	f := newTail(t, 256, 1000)
	for range 20 {
		f.append(logsOf("a", "x", "x"))
	}
	if _, ok := f.tail.Pinned(); ok {
		t.Fatal("pinned without subscribers")
	}
	bases, err := wal.Segments(f.fsys, logDir)
	if err != nil {
		t.Fatal(err)
	}
	// Nothing is appended, so the reader stays in the last segment.
	last := bases[len(bases)-1]
	open(t, f.url("tenant=a&kind=logs"), "")
	if seg, ok := f.tail.Pinned(); !ok || seg != last {
		t.Fatalf("pinned = %d, %v; want %d", seg, ok, last)
	}
}

func TestTailRejectsBadRequests(t *testing.T) {
	f := newTail(t, 0, 1000)
	for _, q := range []string{"kind=logs", "tenant=a", "tenant=a&kind=x", "tenant=a&kind=logs&match=novalue", "tenant=a&kind=logs&after=zz-1", "tenant=a&kind=logs&after=" + formatLive(f.log.ID(), 0)} {
		resp, err := get(t, f.url(q), "")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s = %d, want 400", q, resp.StatusCode)
		}
	}
}

func TestParseToken(t *testing.T) {
	for s, want := range map[string]token{
		"00000000000000ff-12": {logID: 255, n: 12},
		"00000000000000ff-m0": {logID: 255, live: true},
	} {
		if got, err := parseToken(s); err != nil || got != want {
			t.Errorf("parseToken(%q) = %+v, %v", s, got, err)
		}
	}
	for _, s := range []string{"", "ff", "-1", "ff-", "ff-m", "ff--1", "ff-m-1", "ff-+1", "xx-1"} {
		if _, err := parseToken(s); err == nil {
			t.Errorf("parseToken(%q) accepted", s)
		}
	}
	if got := formatTail(255, 3); got != "00000000000000ff-3" {
		t.Errorf("formatTail = %s", got)
	}
}

type liveFixture struct {
	t    *testing.T
	live *Live
	srv  *httptest.Server
	ends map[string]int64
}

const liveLog = 0xabc

func newLive(t *testing.T, size int) *liveFixture {
	t.Helper()
	f := &liveFixture{t: t, live: NewLive(liveLog, size), ends: map[string]int64{}}
	f.srv = httptest.NewServer(f.live)
	t.Cleanup(f.srv.Close)
	t.Cleanup(f.live.Close)
	return f
}

// emit closes n buckets of tenant in one aggregator step; the bucket of seq
// s of a tenant ends at (s+1)·1000.
func (f *liveFixture) emit(tenant string, n int) {
	for range n {
		f.ends[tenant] += 1000
		f.live.Closed(&agg.Bucket{Tenant: tenant, Series: `m{}`, End: f.ends[tenant], Count: 1, Sum: 1, Rate: 1})
	}
	f.live.Advanced(tenant, f.ends[tenant], 0)
}

func (f *liveFixture) url() string { return f.srv.URL + "/v1/live?tenant=a" }

func (f *liveFixture) wantSeq(c *client, seq uint64) {
	f.t.Helper()
	e := c.next()
	if e.event != "" || e.id != formatLive(liveLog, seq) {
		f.t.Fatalf("got %+v, want seq %d", e, seq)
	}
	var b struct {
		Tenant string `json:"tenant"`
		End    int64  `json:"end"`
	}
	if err := json.Unmarshal([]byte(e.data), &b); err != nil || b.Tenant != "a" || b.End != int64(seq+1)*1000 {
		f.t.Fatalf("data %s: %v", e.data, err)
	}
}

func TestLiveResumesWithinRing(t *testing.T) {
	f := newLive(t, 4)
	f.emit("a", 6) // seq 0..5, the ring keeps 2..5
	f.emit("b", 1)
	c := open(t, f.url(), formatLive(liveLog, 2))
	f.wantSeq(c, 3)
	f.wantSeq(c, 4)
	f.wantSeq(c, 5)
	f.emit("a", 1)
	f.wantSeq(c, 6)

	c = open(t, f.url()+"&after="+formatLive(liveLog, 2), "") // ring is 3..6
	f.wantSeq(c, 3)
}

func TestLiveWithoutTokenStartsAtHead(t *testing.T) {
	f := newLive(t, 4)
	f.emit("a", 2)
	c := open(t, f.url(), "")
	if f.live.Active() != 1 {
		t.Fatalf("active = %d", f.live.Active())
	}
	f.emit("a", 1)
	f.wantSeq(c, 2)
}

func TestLiveOutsideRingExpired(t *testing.T) {
	f := newLive(t, 4)
	f.emit("a", 6) // ring keeps 2..5
	wantGone(t, f.url(), formatLive(liveLog, 0), codeExpired)
	wantGone(t, f.url(), formatLive(liveLog, 6), codeExpired)
	wantGone(t, f.url(), formatLive(liveLog+1, 3), codeUnknownLog)
	c := open(t, f.url(), formatLive(liveLog, 1)) // resumes at 2, the oldest kept
	f.wantSeq(c, 2)
}

func TestLiveStateKeepsSeq(t *testing.T) {
	f := newLive(t, 4)
	f.emit("a", 6)
	f.emit("b", 1)
	state := f.live.AppendState()

	g := newLive(t, 4)
	if err := g.live.LoadState(state); err != nil {
		t.Fatal(err)
	}
	if got := g.live.AppendState(); string(got) != string(state) {
		t.Fatalf("state %x, want %x", got, state)
	}
	// The ring is empty after a restore: only the next emission is there.
	wantGone(t, g.url(), formatLive(liveLog, 4), codeExpired)
	c := open(t, g.url(), formatLive(liveLog, 5))
	g.ends["a"] = 6000
	g.emit("a", 1)
	g.wantSeq(c, 6)

	if err := g.live.LoadState(state[:len(state)-1]); err == nil {
		t.Fatal("truncated state accepted")
	}
}
