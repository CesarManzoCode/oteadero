package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
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
