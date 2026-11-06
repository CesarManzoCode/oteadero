package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/CesarManzoCode/oteadero/internal/clock"
	"github.com/CesarManzoCode/oteadero/internal/config"
	"github.com/CesarManzoCode/oteadero/internal/event"
	"github.com/CesarManzoCode/oteadero/internal/ingest"
	"github.com/CesarManzoCode/oteadero/internal/rng"
	"github.com/CesarManzoCode/oteadero/internal/vfs"
)

const t0 = int64(1_700_000_000_000)

// response holds the union of the JSON answers of the ingestion endpoints.
type response struct {
	Accepted    *int   `json:"accepted"`
	FirstOffset *int   `json:"first_offset"`
	Dropped     *int   `json:"dropped"`
	Cause       string `json:"cause"`
}

type fixture struct {
	t      *testing.T
	srv    *Server
	rec    Recovery
	closed bool
}

// testConfig has one open tenant "a" and commits without waiting, since the
// manual clock would never reach a group commit deadline.
func testConfig() config.Config {
	cfg := config.Default()
	cfg.DataDir = "data"
	cfg.FsyncMaxDelayMs = 0
	cfg.Tenants = []config.Tenant{{ID: "a", QuotaEPS: 1_000_000}}
	return cfg
}

func start(t *testing.T, cfg config.Config, mem *vfs.Mem, exportDir string) *fixture {
	t.Helper()
	srv, err := New(cfg, Deps{FS: mem, Clock: clock.NewManual(t0), Rand: rng.New(1), ExportDir: exportDir})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := srv.Start()
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, srv: srv, rec: rec}
	t.Cleanup(f.close)
	return f
}

func (f *fixture) close() {
	f.t.Helper()
	if f.closed {
		return
	}
	f.closed = true
	if err := f.srv.Close(); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) serve(req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	f.srv.Handler().ServeHTTP(rec, req)
	return rec
}

func (f *fixture) post(path, tenant string, body io.Reader) (int, response) {
	f.t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, body)
	if tenant != "" {
		req.Header.Set(TenantHeader, tenant)
	}
	rec := f.serve(req)
	var out response
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		f.t.Fatalf("%s: bad response %q: %v", path, rec.Body, err)
	}
	return rec.Code, out
}

func (f *fixture) get(path string) (int, string) {
	f.t.Helper()
	rec := f.serve(httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

func (f *fixture) wantDrop(path, tenant, body string, status, n int, cause string) {
	f.t.Helper()
	code, out := f.post(path, tenant, strings.NewReader(body))
	if code != status || out.Dropped == nil || *out.Dropped != n || out.Cause != cause {
		f.t.Fatalf("got %d %+v, want %d dropped=%d cause=%s", code, out, status, n, cause)
	}
}

func (f *fixture) wantAccepted(path, tenant, body string, n, first int) {
	f.t.Helper()
	code, out := f.post(path, tenant, strings.NewReader(body))
	if code != http.StatusOK || out.Accepted == nil || *out.Accepted != n || out.FirstOffset == nil || *out.FirstOffset != first {
		f.t.Fatalf("got %d %+v, want 200 accepted=%d first_offset=%d", code, out, n, first)
	}
}

func (f *fixture) wantMetrics(lines ...string) {
	f.t.Helper()
	code, text := f.get("/metrics")
	if code != http.StatusOK {
		f.t.Fatalf("/metrics = %d", code)
	}
	for _, l := range lines {
		if !strings.Contains(text, l+"\n") {
			f.t.Errorf("/metrics lacks %q:\n%s", l, text)
		}
	}
}

func sample(ts int64) string {
	return fmt.Sprintf("m{k=\"v\"} 1 %d\n", ts)
}

func TestMetricsBatchAccepted(t *testing.T) {
	f := start(t, testConfig(), vfs.NewMem(), "")
	f.wantAccepted("/v1/metrics", "a", sample(t0)+"# comment\n\n"+sample(t0), 2, 0)
	f.wantAccepted("/v1/metrics", "a", sample(t0), 1, 2)
}

func TestLogsBatchAccepted(t *testing.T) {
	f := start(t, testConfig(), vfs.NewMem(), "")
	line := fmt.Sprintf(`{"ts": %d, "labels": {"app": "x"}, "fields": {"u": "1"}, "msg": "hi"}`, t0)
	f.wantAccepted("/v1/logs", "a", line+"\n"+line+"\n", 2, 0)
}

func TestEmptyBatchTakesNoOffset(t *testing.T) {
	f := start(t, testConfig(), vfs.NewMem(), "")
	f.wantAccepted("/v1/metrics", "a", "", 0, 0)
	f.wantAccepted("/v1/metrics", "a", sample(t0), 1, 0)
}

func TestInvalidBatch(t *testing.T) {
	f := start(t, testConfig(), vfs.NewMem(), "")
	f.wantDrop("/v1/metrics", "a", sample(t0)+"no timestamp 1\n", 400, 2, "invalid")
	f.wantDrop("/v1/logs", "a", "{\n", 400, 1, "invalid")
	f.wantDrop("/v1/metrics", "a", sample(t0)+sample(t0+ingest.MaxFutureMs+1), 400, 2, "invalid")
	f.wantMetrics(
		`oteadero_events_received_total{tenant="a"} 5`,
		`oteadero_events_accepted_total{tenant="a"} 0`,
		`oteadero_events_dropped_total{cause="invalid",tenant="a"} 5`,
	)
	// Nothing of a dropped batch reached the log.
	f.wantAccepted("/v1/metrics", "a", sample(t0), 1, 0)
}

func TestTruncatedBodyIsInvalid(t *testing.T) {
	f := start(t, testConfig(), vfs.NewMem(), "")
	body := io.MultiReader(strings.NewReader(sample(t0)), iotest.ErrReader(errors.New("connection lost")))
	code, out := f.post("/v1/metrics", "a", body)
	if code != 400 || out.Cause != "invalid" || *out.Dropped != 1 {
		t.Fatalf("got %d %+v", code, out)
	}
}

func TestTooLarge(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBodyBytes = 64
	f := start(t, cfg, vfs.NewMem(), "")
	body := strings.Repeat(sample(t0), 20)
	f.wantDrop("/v1/metrics", "a", body, 400, event.CountEvents(event.KindMetrics, []byte(body[:64])), "too_large")

	// A body of exactly the limit is not too large.
	line := sample(t0)
	exact := line + strings.Repeat("#", 63-len(line)) + "\n"
	f.wantAccepted("/v1/metrics", "a", exact, 1, 0)
}

func TestTooManyEvents(t *testing.T) {
	cfg := testConfig()
	f := start(t, cfg, vfs.NewMem(), "")
	n := event.MaxEvents + 1
	f.wantDrop("/v1/metrics", "a", strings.Repeat(sample(t0), n), 400, n, "too_large")
	f.wantMetrics(fmt.Sprintf(`oteadero_events_dropped_total{cause="too_large",tenant="a"} %d`, n))
}

func TestUnknownTenant(t *testing.T) {
	f := start(t, testConfig(), vfs.NewMem(), "")
	f.wantDrop("/v1/metrics", "zz", sample(t0), 400, 1, "unknown_tenant")
	f.wantDrop("/v1/metrics", "", sample(t0), 400, 1, "unknown_tenant")
	f.wantDrop("/v1/logs", "zz", "not json\n", 400, 1, "unknown_tenant")
	f.wantMetrics(
		`oteadero_events_dropped_total{cause="unknown_tenant",tenant="zz"} 2`,
		`oteadero_events_dropped_total{cause="unknown_tenant",tenant=""} 1`,
	)
}

func TestTenantLabelIsEscaped(t *testing.T) {
	f := start(t, testConfig(), vfs.NewMem(), "")
	f.wantDrop("/v1/metrics", `x"y\z`, sample(t0), 400, 1, "unknown_tenant")
	f.wantMetrics(`oteadero_events_received_total{tenant="x\"y\\z"} 1`)
}

func TestQuota(t *testing.T) {
	cfg := testConfig()
	cfg.Tenants = []config.Tenant{{ID: "a", QuotaEPS: 10, Burst: 3}}
	f := start(t, cfg, vfs.NewMem(), "")
	f.wantAccepted("/v1/metrics", "a", strings.Repeat(sample(t0), 2), 2, 0)
	f.wantDrop("/v1/metrics", "a", strings.Repeat(sample(t0), 2), 429, 2, "quota")
	f.wantAccepted("/v1/metrics", "a", sample(t0), 1, 2)
	f.wantMetrics(
		`oteadero_events_received_total{tenant="a"} 5`,
		`oteadero_events_accepted_total{tenant="a"} 3`,
		`oteadero_events_dropped_total{cause="quota",tenant="a"} 2`,
	)
}

func TestDropReplyStatus(t *testing.T) {
	for cause, want := range map[ingest.Cause]int{
		ingest.CauseQuota:         429,
		ingest.CauseQueueFull:     429,
		ingest.CauseInvalid:       400,
		ingest.CauseTooLarge:      400,
		ingest.CauseUnknownTenant: 400,
		ingest.CauseShuttingDown:  503,
	} {
		got := dropReply(7, cause)
		wantBody := fmt.Sprintf(`{"dropped":7,"cause":"%s"}`, cause)
		if got.Status != want || got.Body != wantBody {
			t.Errorf("%v: got %d %q", cause, got.Status, got.Body)
		}
	}
}

func TestMetricsEndpoint(t *testing.T) {
	f := start(t, testConfig(), vfs.NewMem(), "")
	f.wantMetrics(
		`oteadero_queue_depth_events{queue="a"} 0`,
		`oteadero_queue_depth_events{queue="sequencer"} 0`,
		`oteadero_sketch_observed_rel_error_max 0`,
	)
	if _, text := f.get("/metrics"); strings.Contains(text, "oteadero_watermark_lag_ms{") {
		t.Fatalf("watermark lag before the first event:\n%s", text)
	}

	// The second event's bucket ends at or below W = t0 − 2000: late.
	f.wantAccepted("/v1/metrics", "a", sample(t0)+sample(t0-10_000), 2, 0)
	f.wantDrop("/v1/metrics", "a", "bad\n", 400, 1, "invalid")
	f.wantMetrics(
		`oteadero_events_received_total{tenant="a"} 3`,
		`oteadero_events_accepted_total{tenant="a"} 2`,
		`oteadero_events_dropped_total{cause="invalid",tenant="a"} 1`,
		`oteadero_events_dropped_total{cause="quota",tenant="a"} 0`,
		`oteadero_events_late_total{tenant="a"} 1`,
		`oteadero_watermark_lag_ms{tenant="a"} 2000`,
		`oteadero_queue_depth_events{queue="a"} 0`,
	)
}

func TestHealthz(t *testing.T) {
	f := start(t, testConfig(), vfs.NewMem(), "")
	if code, _ := f.get("/healthz"); code != 200 {
		t.Fatalf("healthz = %d", code)
	}
	f.srv.Drain()
	if code, body := f.get("/healthz"); code != 503 || !strings.Contains(body, "shutting_down") {
		t.Fatalf("healthz while closing = %d %q", code, body)
	}
}

func TestMethodsAreRouted(t *testing.T) {
	f := start(t, testConfig(), vfs.NewMem(), "")
	if code, _ := f.get("/v1/metrics"); code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /v1/metrics = %d", code)
	}
}

func TestShutdownAnswers503(t *testing.T) {
	f := start(t, testConfig(), vfs.NewMem(), "")
	f.wantAccepted("/v1/metrics", "a", sample(t0), 1, 0)
	f.srv.Drain()
	f.wantDrop("/v1/metrics", "a", sample(t0)+sample(t0), 503, 2, "shutting_down")
	f.wantDrop("/v1/logs", "a", "bad\n", 503, 1, "shutting_down")
	f.wantDrop("/v1/metrics", "zz", sample(t0), 400, 1, "unknown_tenant")
	f.wantMetrics(`oteadero_events_dropped_total{cause="shutting_down",tenant="a"} 3`)
}

func TestCloseCheckpointsAndRestartRecovers(t *testing.T) {
	mem := vfs.NewMem()
	f := start(t, testConfig(), mem, "")
	if f.rec != (Recovery{}) {
		t.Fatalf("new data dir recovery = %+v", f.rec)
	}
	f.wantAccepted("/v1/metrics", "a", sample(t0)+sample(t0), 2, 0)
	f.wantDrop("/v1/metrics", "a", "bad\n", 400, 1, "invalid")
	f.srv.Drain()
	f.wantDrop("/v1/metrics", "a", sample(t0), 503, 1, "shutting_down")
	f.close()

	names, err := mem.ReadDir("data/ckpt")
	if err != nil || len(names) != 1 {
		t.Fatalf("checkpoints = %v, %v", names, err)
	}

	g := start(t, testConfig(), mem, "")
	if g.rec != (Recovery{NextOffset: 2}) {
		t.Fatalf("recovery = %+v", g.rec)
	}
	g.wantMetrics(
		`oteadero_events_received_total{tenant="a"} 4`,
		`oteadero_events_accepted_total{tenant="a"} 2`,
		`oteadero_events_dropped_total{cause="invalid",tenant="a"} 1`,
		`oteadero_events_dropped_total{cause="shutting_down",tenant="a"} 1`,
		`oteadero_watermark_lag_ms{tenant="a"} 2000`,
	)
	g.wantAccepted("/v1/metrics", "a", sample(t0), 1, 2)
}

func TestExport(t *testing.T) {
	mem := vfs.NewMem()
	f := start(t, testConfig(), mem, "exp")
	f.wantAccepted("/v1/metrics", "a", sample(t0)+sample(t0+5000), 2, 0) // closes the bucket ending at t0
	f.close()

	runs, err := mem.ReadDir("exp")
	if err != nil || len(runs) != 1 || !strings.HasPrefix(runs[0], "serve-") {
		t.Fatalf("runs = %v, %v", runs, err)
	}
	file, err := mem.Open("exp/" + runs[0] + "/buckets.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.NewSectionReader(file, 0, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf(`{"tenant":"a","series":"m{k=\"v\"}","end":%d,"count":1,"sum":1,"rate":1,"closed_at":1}`, t0) + "\n"
	if string(data) != want {
		t.Fatalf("buckets.jsonl = %q, want %q", data, want)
	}
}

func TestExportCarriesConfiguredSketches(t *testing.T) {
	mem := vfs.NewMem()
	cfg := testConfig()
	cfg.Tenants[0].QuantileMetrics = []string{"m"}
	cfg.Tenants[0].CardinalityFields = []string{"u"}
	f := start(t, cfg, mem, "exp")
	f.wantMetrics("oteadero_sketch_observed_rel_error_max 0")
	line := func(u string) string {
		return fmt.Sprintf(`{"ts": %d, "labels": {"app": "x"}, "fields": {"u": %q}, "msg": "hi"}`, t0, u) + "\n"
	}
	f.wantAccepted("/v1/logs", "a", line("1")+line("2")+line("1"), 3, 0)
	f.wantAccepted("/v1/metrics", "a", sample(t0)+"n{} 1 "+fmt.Sprint(t0)+"\n"+sample(t0+5000), 3, 3)
	f.close()

	runs, err := mem.ReadDir("exp")
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs = %v, %v", runs, err)
	}
	file, err := mem.Open("exp/" + runs[0] + "/buckets.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.NewSectionReader(file, 0, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	var withQ, withCard int
	for l := range strings.Lines(string(data)) {
		var got struct {
			Series string
			Q      map[string]float64
			Card   map[string]uint64
		}
		if err := json.Unmarshal([]byte(l), &got); err != nil {
			t.Fatalf("line %q: %v", l, err)
		}
		switch got.Series {
		case `m{k="v"}`:
			withQ++
			if len(got.Q) != 4 || got.Card != nil {
				t.Errorf("line %q", l)
			}
		case `{app="x"}`:
			withCard++
			if got.Q != nil || got.Card["u"] != 2 {
				t.Errorf("line %q", l)
			}
		default:
			if got.Q != nil || got.Card != nil {
				t.Errorf("line %q", l)
			}
		}
	}
	if withQ != 1 || withCard != 1 {
		t.Fatalf("buckets.jsonl = %s", data)
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	cfg := testConfig()
	cfg.Tenants = append(cfg.Tenants, cfg.Tenants[0]) // duplicate id
	if _, err := New(cfg, Deps{FS: vfs.NewMem(), Clock: clock.NewManual(t0), Rand: rng.New(1)}); err == nil {
		t.Fatal("duplicate tenant accepted")
	}
	cfg = testConfig()
	cfg.DataDir = ""
	if _, err := New(cfg, Deps{FS: vfs.NewMem(), Clock: clock.NewManual(t0), Rand: rng.New(1)}); err == nil {
		t.Fatal("empty data dir accepted")
	}
}

func (f *fixture) lease(holder string) (int, string) {
	f.t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/lease", strings.NewReader(`{"holder":"`+holder+`"}`))
	rec := f.serve(req)
	return rec.Code, strings.TrimSpace(rec.Body.String())
}

func TestLeaseEndpoint(t *testing.T) {
	mem := vfs.NewMem()
	f := start(t, testConfig(), mem, "")
	for _, c := range []struct {
		holder string
		code   int
		body   string
	}{
		{"e1", 200, `{"epoch":1,"holder":"e1","ttl_ms":2000}`},
		{"e2", 409, `{"holder":"e1"}`},
		{"e1", 200, `{"epoch":1,"holder":"e1","ttl_ms":2000}`},
	} {
		if code, body := f.lease(c.holder); code != c.code || body != c.body {
			t.Errorf("lease(%s) = %d %s, want %d %s", c.holder, code, body, c.code, c.body)
		}
	}
	for _, body := range []string{"", "{", `{"holder":""}`} {
		rec := f.serve(httptest.NewRequest(http.MethodPost, "/v1/lease", strings.NewReader(body)))
		if rec.Code != 400 {
			t.Errorf("body %q: %d", body, rec.Code)
		}
	}

	// A restart at the same clock keeps both the epoch and the exclusion.
	f.close()
	f = start(t, testConfig(), mem, "")
	if code, body := f.lease("e2"); code != 409 || body != `{"holder":"e1"}` {
		t.Errorf("after the restart: %d %s", code, body)
	}
	if code, body := f.lease("e1"); code != 200 || body != `{"epoch":1,"holder":"e1","ttl_ms":2000}` {
		t.Errorf("after the restart: %d %s", code, body)
	}

	f.srv.Drain()
	if code, _ := f.lease("e1"); code != 503 {
		t.Errorf("while draining: %d", code)
	}
}

// TestIngestInline drives the server on the caller's goroutine, as the
// simulation does: a drop is answered at once, and an admitted batch when Poll
// syncs it.
func TestIngestInline(t *testing.T) {
	srv, err := New(testConfig(), Deps{FS: vfs.NewMem(), Clock: clock.NewManual(t0), Rand: rng.New(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Recover(); err != nil {
		t.Fatal(err)
	}
	var got []Reply
	reply := func(r Reply) { got = append(got, r) }
	srv.Ingest("nobody", event.KindMetrics, []byte("m{} 1 1\n"), nil, reply)
	if len(got) != 1 || got[0].Status != http.StatusBadRequest {
		t.Fatalf("unknown tenant: %+v", got)
	}
	body := fmt.Appendf(nil, "m{} 1 %d\nm{} 2 %d\n", t0, t0)
	srv.Ingest("a", event.KindMetrics, body, nil, reply)
	if len(got) != 1 {
		t.Fatalf("answered before the fsync: %+v", got)
	}
	if err := srv.Sequencer().Poll(); err != nil {
		t.Fatal(err)
	}
	want := Reply{Status: http.StatusOK, Body: `{"accepted":2,"first_offset":0}`}
	if len(got) != 2 || got[1] != want {
		t.Fatalf("after Poll: %+v, want %+v last", got, want)
	}
	st := srv.Sequencer().Stats()
	if st[0].Tenant != "a" || st[0].Accepted != 2 || st[0].Received != 2 {
		t.Fatalf("stats: %+v", st[0])
	}
}
