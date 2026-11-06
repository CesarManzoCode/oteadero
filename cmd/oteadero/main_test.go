package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestVersionAndUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"version"}, &out, &errOut); code != 0 || out.String() != "oteadero "+version+"\n" {
		t.Fatalf("version: %d %q", code, out.String())
	}
	for _, args := range [][]string{
		nil,
		{"nope"},
		{"serve"},
		{"serve", "--nope"},
		{"serve", "--config", "x", "extra"},
		{"evaluate"},
		{"evaluate", "--nope"},
		{"evaluate", "--config", "x", "--id", "e1", "--rules", "r", "--coordinator", "c", "--receiver", "v"},
	} {
		if code := run(args, io.Discard, io.Discard); code != 2 {
			t.Errorf("run(%v) = %d, want 2", args, code)
		}
	}
}

func TestServeBadConfig(t *testing.T) {
	var errOut bytes.Buffer
	path := filepath.Join(t.TempDir(), "missing.json")
	if code := run([]string{"serve", "--config", path}, io.Discard, &errOut); code != 1 || !strings.Contains(errOut.String(), "missing.json") {
		t.Fatalf("code %d, stderr %q", code, errOut.String())
	}
}

// process is one run of serve whose stdout the test reads line by line.
type process struct {
	lines *bufio.Scanner
	stop  chan os.Signal
	done  chan error
}

func startServe(t *testing.T, configPath, exportDir string) *process {
	t.Helper()
	pr, pw := io.Pipe()
	t.Cleanup(func() { pr.Close(); pw.Close() })
	p := &process{lines: bufio.NewScanner(pr), stop: make(chan os.Signal, 1), done: make(chan error, 1)}
	go func() { p.done <- serve(configPath, exportDir, pw, p.stop) }()
	return p
}

func (p *process) line(t *testing.T) string {
	t.Helper()
	if !p.lines.Scan() {
		t.Fatalf("stdout ended: %v", p.lines.Err())
	}
	return p.lines.Text()
}

func TestServeLifecycle(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	export := filepath.Join(dir, "export")
	configPath := filepath.Join(dir, "serve.json")
	conf := fmt.Sprintf(`{"data_dir": %q, "listen": "127.0.0.1:0", "memory_limit_bytes": 4294967296,
		"tenants": [{"id": "a", "quota_eps": 1000000}]}`, data)
	if err := os.WriteFile(configPath, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}

	p := startServe(t, configPath, export)
	if got := p.line(t); got != "recovered next_offset=0 truncated_bytes=0" {
		t.Fatalf("first line = %q", got)
	}
	addr, ok := strings.CutPrefix(p.line(t), "listening ")
	if !ok {
		t.Fatal("no listening line")
	}

	body := fmt.Sprintf("m 1 %d\n", time.Now().UnixMilli())
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/metrics", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Oteadero-Tenant", "a")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || string(got) != `{"accepted":1,"first_offset":0}`+"\n" {
		t.Fatalf("POST: %d %q", resp.StatusCode, got)
	}

	p.stop <- syscall.SIGTERM
	if err := <-p.done; err != nil {
		t.Fatal(err)
	}
	if names, err := os.ReadDir(filepath.Join(data, "ckpt")); err != nil || len(names) != 1 {
		t.Fatalf("checkpoints: %v, %v", names, err)
	}

	p = startServe(t, configPath, export)
	if got := p.line(t); got != "recovered next_offset=1 truncated_bytes=0" {
		t.Fatalf("after restart = %q", got)
	}
	p.line(t)
	p.stop <- syscall.SIGINT
	if err := <-p.done; err != nil {
		t.Fatal(err)
	}
	if runs, err := os.ReadDir(export); err != nil || len(runs) != 2 {
		t.Fatalf("export runs: %v, %v", runs, err)
	}
}

func startEvaluate(t *testing.T, o evaluateOptions) *process {
	t.Helper()
	pr, pw := io.Pipe()
	t.Cleanup(func() { pr.Close(); pw.Close() })
	p := &process{lines: bufio.NewScanner(pr), stop: make(chan os.Signal, 1), done: make(chan error, 1)}
	go func() { p.done <- evaluate(o, pw, p.stop) }()
	return p
}

// discardRest keeps reading stdout, so that a change of role on a slow machine
// does not block the process.
func (p *process) discardRest() {
	go func() {
		for p.lines.Scan() {
		}
	}()
}

type notified struct {
	key, epoch, body string
}

func TestEvaluateLifecycle(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "serve.json")
	conf := fmt.Sprintf(`{"data_dir": %q, "listen": "127.0.0.1:0", "idle_grace_ms": 0,
		"tenants": [{"id": "a", "quota_eps": 1000000}]}`, filepath.Join(dir, "data"))
	if err := os.WriteFile(configPath, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	rulesPath := filepath.Join(dir, "rules.json")
	ruleFile := `{"rules": [{"name": "any", "tenant": "a", "source": "metrics", "match": {"__name__": "m"},
		"agg": "count", "range_ms": 1000, "op": ">=", "threshold": 1}]}`
	if err := os.WriteFile(rulesPath, []byte(ruleFile), 0o644); err != nil {
		t.Fatal(err)
	}

	received := make(chan notified, 16)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- notified{key: r.Header.Get("Idempotency-Key"), epoch: r.Header.Get("X-Oteadero-Epoch"), body: string(body)}
	}))
	defer receiver.Close()

	srv := startServe(t, configPath, "")
	srv.line(t)
	addr, ok := strings.CutPrefix(srv.line(t), "listening ")
	if !ok {
		t.Fatal("no listening line from serve")
	}

	export := filepath.Join(dir, "export")
	o := evaluateOptions{
		config:      configPath,
		id:          "e1",
		rules:       rulesPath,
		coordinator: "http://" + addr,
		receiver:    receiver.URL,
		listen:      "127.0.0.1:0",
		export:      export,
	}
	ev := startEvaluate(t, o)
	metricsAddr, ok := strings.CutPrefix(ev.line(t), "listening ")
	if !ok {
		t.Fatal("no listening line from evaluate")
	}
	if got := ev.line(t); got != "leader epoch=1" {
		t.Fatalf("role line = %q", got)
	}
	ev.discardRest()

	// The second event moves the watermark past the bucket of the first, which
	// fires at the end of its bucket and resolves one instant later.
	ts := time.Now().UnixMilli()
	body := fmt.Sprintf("m 1 %d\nm 1 %d\n", ts, ts+5000)
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/metrics", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Oteadero-Tenant", "a")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("POST: %d", resp.StatusCode)
	}

	seen := map[string]notified{}
	for len(seen) < 2 {
		select {
		case n := <-received:
			if n.epoch != "1" || !strings.Contains(n.body, `"evaluator":"e1"`) {
				t.Errorf("notification %+v", n)
			}
			_, kind, _ := strings.Cut(n.key, "/")
			seen[kind] = n
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out with %v", seen)
		}
	}
	firing, resolved := seen["firing"], seen["resolved"]
	if firing.key == "" || resolved.key == "" || strings.TrimSuffix(firing.key, "/firing") != strings.TrimSuffix(resolved.key, "/resolved") {
		t.Fatalf("keys: %+v", seen)
	}

	// The confirmations reach the metrics a moment after the receiver answers.
	want := `oteadero_notifications_total{status="confirmed"} 2`
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get("http://" + metricsAddr + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(got), want) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics:\n%s", got)
		}
		time.Sleep(10 * time.Millisecond)
	}

	ev.stop <- syscall.SIGTERM
	if err := <-ev.done; err != nil {
		t.Fatal(err)
	}
	if names, err := os.ReadDir(filepath.Join(dir, "data", "eval-e1", "ckpt")); err != nil || len(names) != 1 {
		t.Fatalf("checkpoints: %v, %v", names, err)
	}
	runs, err := os.ReadDir(export)
	if err != nil || len(runs) != 1 || !strings.HasPrefix(runs[0].Name(), "e1-") {
		t.Fatalf("export runs: %v, %v", runs, err)
	}
	lines, err := os.ReadFile(filepath.Join(export, runs[0].Name(), "transitions.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(lines), "\n"); n != 2 {
		t.Fatalf("transitions:\n%s", lines)
	}

	// A restart recovers from its checkpoint: what is confirmed goes out no more.
	ev = startEvaluate(t, o)
	ev.line(t)
	if got := ev.line(t); got != "leader epoch=1" {
		t.Fatalf("role line after the restart = %q", got)
	}
	ev.discardRest()
	ev.stop <- syscall.SIGTERM
	if err := <-ev.done; err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-received:
		t.Fatalf("sent again after the restart: %+v", n)
	default:
	}

	srv.stop <- syscall.SIGTERM
	if err := <-srv.done; err != nil {
		t.Fatal(err)
	}
}
