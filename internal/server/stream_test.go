package server

import (
	"bufio"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

// openStream opens an SSE stream on ts and returns a reader of its lines.
func openStream(t *testing.T, ts *httptest.Server, path string) *bufio.Reader {
	t.Helper()
	resp, err := http.Get(ts.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("GET %s = %d %s", path, resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	return bufio.NewReader(resp.Body)
}

// nextID returns the id of the next event of the stream.
func nextID(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("stream ended: %v", err)
		}
		if id, ok := strings.CutPrefix(line, "id: "); ok {
			return strings.TrimSuffix(id, "\n")
		}
	}
}

func TestStreamRoutes(t *testing.T) {
	f := start(t, testConfig(), vfs.NewMem(), "")
	ts := httptest.NewServer(f.srv.Handler())
	t.Cleanup(ts.Close)
	id := f.srv.log.ID()

	f.wantAccepted("/v1/metrics", "a", sample(t0), 1, 0)
	tail := openStream(t, ts, "/v1/tail?tenant=a&kind=metrics")
	live := openStream(t, ts, "/v1/live?tenant=a")
	f.wantMetrics(`oteadero_queue_depth_events{queue="tail"} 2`)

	// W = t0 + 1000 closes the bucket that ends at t0.
	f.wantAccepted("/v1/metrics", "a", sample(t0+3000), 1, 1)
	if got, want := nextID(t, tail), fmt.Sprintf("%016x-1", id); got != want {
		t.Fatalf("tail id %s, want %s", got, want)
	}
	if got, want := nextID(t, live), fmt.Sprintf("%016x-m0", id); got != want {
		t.Fatalf("live id %s, want %s", got, want)
	}

	code, body := f.get(fmt.Sprintf("/v1/tail?tenant=a&kind=metrics&after=%016x-0", id+1))
	if code != http.StatusGone || !strings.Contains(body, `"unknown_log"`) {
		t.Fatalf("foreign token = %d %s", code, body)
	}
}

func TestLiveSeqSurvivesRestart(t *testing.T) {
	mem := vfs.NewMem()
	f := start(t, testConfig(), mem, "")
	// W = t0 + 2000 closes the buckets that end at t0 and t0 + 1000: seq 0, 1.
	f.wantAccepted("/v1/metrics", "a", sample(t0)+sample(t0+1000)+sample(t0+4000), 3, 0)
	f.close()
	state := f.srv.live.AppendState()

	g := start(t, testConfig(), mem, "")
	if got := g.srv.live.AppendState(); string(got) != string(state) {
		t.Fatalf("live state after restart %x, want %x", got, state)
	}
	code, body := g.get(fmt.Sprintf("/v1/live?tenant=a&after=%016x-m0", g.srv.log.ID()))
	if code != http.StatusGone || !strings.Contains(body, `"expired"`) {
		t.Fatalf("token before the restored ring = %d %s", code, body)
	}
}
