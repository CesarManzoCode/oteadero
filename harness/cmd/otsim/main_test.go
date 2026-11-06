package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/CesarManzoCode/oteadero/harness/sim"
)

func TestUsage(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"--seed", "1"},
		{"--out", t.TempDir()},
		{"--seed", "1", "--out", t.TempDir(), "extra"},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != 2 {
			t.Errorf("%q: exit %d, want 2", args, code)
		}
	}
}

func result(failing bool) *sim.Result {
	res := &sim.Result{
		Hash:        "abc",
		Record:      []byte("t=0 node=serve kind=sim.start h=00\n"),
		LogFiles:    []sim.File{{Name: "00000000000000000000.seg", Data: []byte("OTLOG001")}},
		Receiver:    []byte("{}\n"),
		Transitions: []byte("{}\n"),
		Checks:      []sim.Check{{Name: sim.InvEpochLeader}, {Name: sim.InvTail}},
	}
	if failing {
		res.Checks[1].Err = errors.New("a gap")
	}
	return res
}

func TestReportWritesTheFilesAndTheVerdict(t *testing.T) {
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	if code := report(result(false), dir, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	want := "hash: abc\ninvariant " + sim.InvEpochLeader + ": ok\ninvariant " + sim.InvTail + ": ok\n"
	if stdout.String() != want {
		t.Fatalf("stdout %q, want %q", stdout.String(), want)
	}
	for name, data := range map[string]string{
		"registry.log":                      "t=0 node=serve kind=sim.start h=00\n",
		"receiver.jsonl":                    "{}\n",
		"transitions.jsonl":                 "{}\n",
		"data/log/00000000000000000000.seg": "OTLOG001",
	} {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(got) != data {
			t.Errorf("%s: %q, %v", name, got, err)
		}
	}

	// The log of a run is never mixed with the one of another.
	if code := report(result(false), dir, &stdout, &stderr); code != 1 {
		t.Fatalf("second report in the same directory: exit %d", code)
	}
}

func TestReportFailsOnAFailedInvariant(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := report(result(true), t.TempDir(), &stdout, &stderr); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(stdout.String(), "invariant "+sim.InvTail+": fail\n") || !strings.Contains(stderr.String(), "a gap") {
		t.Fatalf("stdout %q, stderr %q", stdout.String(), stderr.String())
	}
}
