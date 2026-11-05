package main

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestReceiver(t *testing.T) (*receiver, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "recv.jsonl")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	now := time.UnixMilli(1000)
	return newReceiver(f, func() time.Time { return now }), path
}

func post(t *testing.T, r *receiver, key, epoch, evaluator string) int {
	t.Helper()
	body := `{"incident":"abc","kind":"firing","rule":"r","group":"{}","t":1000,"value":1,"epoch":` +
		epoch + `,"evaluator":"` + evaluator + `"}`
	req := httptest.NewRequest(http.MethodPost, "/notify", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("X-Oteadero-Epoch", epoch)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec.Code
}

func readLines(t *testing.T, path string) []logLine {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var lines []logLine
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var line logLine
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			t.Fatalf("line %q: %v", sc.Text(), err)
		}
		lines = append(lines, line)
	}
	return lines
}

func TestStaleDuplicateAccepted(t *testing.T) {
	r, path := newTestReceiver(t)
	steps := []struct {
		key, epoch, evaluator string
		code                  int
		status                string
	}{
		{"abc/firing", "2", "e1", 200, statusAccepted},
		{"abc/firing", "2", "e1", 200, statusDuplicate},
		{"abc/firing", "3", "e2", 200, statusDuplicate},
		{"def/firing", "2", "e1", 409, statusStale},
		{"def/firing", "3", "e2", 200, statusAccepted},
		{"abc/resolved", "1", "e3", 409, statusStale},
		{"abc/resolved", "3", "e2", 200, statusAccepted},
	}
	for i, s := range steps {
		if code := post(t, r, s.key, s.epoch, s.evaluator); code != s.code {
			t.Fatalf("step %d: code %d, want %d", i, code, s.code)
		}
	}

	lines := readLines(t, path)
	if len(lines) != len(steps) {
		t.Fatalf("%d log lines, want %d", len(lines), len(steps))
	}
	for i, s := range steps {
		l := lines[i]
		if l.Key != s.key || l.Status != s.status || l.Evaluator != s.evaluator ||
			l.RecvMs != 1000 || strings.TrimSpace(s.epoch) != jsonUint(l.Epoch) {
			t.Errorf("line %d: %+v, want key %s status %s evaluator %s epoch %s",
				i, l, s.key, s.status, s.evaluator, s.epoch)
		}
		var body map[string]any
		if err := json.Unmarshal(l.Body, &body); err != nil || body["kind"] != "firing" {
			t.Errorf("line %d: body %s is not the request body", i, l.Body)
		}
	}

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	var counts map[string]int64
	if err := json.Unmarshal(rec.Body.Bytes(), &counts); err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{statusAccepted: 3, statusDuplicate: 2, statusStale: 2, statusInvalid: 0}
	for k, v := range want {
		if counts[k] != v {
			t.Errorf("stats %s = %d, want %d (all: %v)", k, counts[k], v, counts)
		}
	}
}

func TestStaleDoesNotRaiseTheEpoch(t *testing.T) {
	r, _ := newTestReceiver(t)
	post(t, r, "a/firing", "5", "e1")
	if code := post(t, r, "b/firing", "4", "e2"); code != 409 {
		t.Fatalf("epoch 4 after 5: code %d, want 409", code)
	}
	if code := post(t, r, "b/firing", "5", "e1"); code != 200 {
		t.Fatalf("epoch equal to the highest: code %d, want 200", code)
	}
}

func TestInvalidRequestIsLoggedAndCounted(t *testing.T) {
	r, path := newTestReceiver(t)
	req := httptest.NewRequest(http.MethodPost, "/notify", strings.NewReader("not json"))
	req.Header.Set("Idempotency-Key", "a/firing")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != 400 {
		t.Fatalf("code %d, want 400", rec.Code)
	}
	lines := readLines(t, path)
	if len(lines) != 1 || lines[0].Status != statusInvalid || string(lines[0].Body) != `"not json"` {
		t.Fatalf("lines %+v", lines)
	}
	if r.accepted["a/firing"] || r.counts[statusInvalid] != 1 {
		t.Fatalf("invalid request changed the state: %v %v", r.accepted, r.counts)
	}
}

func jsonUint(v uint64) string {
	data, _ := json.Marshal(v)
	return string(data)
}

func TestRunNeedsListenAndLog(t *testing.T) {
	var out, errOut strings.Builder
	if code := run([]string{"--listen", "127.0.0.1:0"}, &out, &errOut); code != 2 {
		t.Fatalf("code %d, want 2", code)
	}
	if !strings.Contains(errOut.String(), "usage") {
		t.Fatalf("stderr %q", errOut.String())
	}
}
