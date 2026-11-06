package sim

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

func alertRules(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("../../rules/alerts.json")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func runScenario(t *testing.T, opts Options) *Result {
	t.Helper()
	res, err := Run(opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range res.Checks {
		if c.Err != nil {
			t.Errorf("invariant %s: %v", c.Name, c.Err)
		}
	}
	return res
}

func TestShortScenarioSameSeedSameHash(t *testing.T) {
	opts := Options{Seed: 7, Faults: true, Rules: alertRules(t), Load: 6 * time.Second}
	a := runScenario(t, opts)
	b := runScenario(t, opts)
	if a.Hash != b.Hash || !bytes.Equal(a.Record, b.Record) {
		t.Fatalf("same seed, hashes %s and %s", a.Hash, b.Hash)
	}
	if !bytes.Equal(a.Receiver, b.Receiver) || !bytes.Equal(a.Transitions, b.Transitions) {
		t.Fatal("same seed, different receiver or transitions")
	}
	for _, kind := range []string{KindCrash, KindPause, kindLeaseRequest, kindNotify, kindTailData} {
		if !bytes.Contains(a.Record, []byte(" kind="+kind+" ")) {
			t.Errorf("the record has no %s", kind)
		}
	}
	if len(a.Receiver) == 0 || len(a.Transitions) == 0 || len(a.LogFiles) == 0 {
		t.Fatalf("receiver %d bytes, transitions %d bytes, %d log files", len(a.Receiver), len(a.Transitions), len(a.LogFiles))
	}
}

func TestShortScenarioWithoutFaults(t *testing.T) {
	res := runScenario(t, Options{Seed: 3, Rules: alertRules(t), Load: 4 * time.Second})
	if bytes.Contains(res.Record, []byte(" kind="+KindCrash+" ")) {
		t.Fatal("a crash without faults")
	}
}

// line fabricates a receiver line for a notification of incident and kind.
func line(evaluator string, epoch uint64, status, incident, kind string) ReceiverLine {
	return ReceiverLine{
		Key:       incident + "/" + kind,
		Epoch:     epoch,
		Evaluator: evaluator,
		Status:    status,
		Body:      fmt.Appendf(nil, `{"incident":%q,"kind":%q,"evaluator":%q,"epoch":%d}`, incident, kind, evaluator, epoch),
	}
}

// goodEvidence holds every invariant: a failover from e1 to e2 with a stale
// request and a duplicate, a tail that started at offset 2, and exact counts.
func goodEvidence() *Evidence {
	return &Evidence{
		Receiver: []ReceiverLine{
			line("e1", 1, statusAccepted, "a", "firing"),
			line("e2", 2, statusAccepted, "b", "firing"),
			line("e1", 1, statusStale, "a", "resolved"),
			line("e2", 2, statusDuplicate, "b", "firing"),
			line("e2", 2, statusAccepted, "a", "resolved"),
		},
		TailOffsets: []uint64{2, 3, 5, 8},
		LogOffsets:  []uint64{0, 2, 3, 5, 8, 9},
		Accounts: []Account{
			{Tenant: "t1", Received: 10, Accepted: 10, Logged: 10, Arrived: 10},
			{Tenant: "t2", Received: 10, Accepted: 6, Dropped: 4, Logged: 6, Arrived: 10},
		},
		ArrivalsExact: true,
	}
}

func checkOf(t *testing.T, ev *Evidence, name string) error {
	t.Helper()
	for _, c := range CheckInvariants(ev) {
		if c.Name == name {
			return c.Err
		}
	}
	t.Fatalf("no invariant %s", name)
	return nil
}

func TestInvariantsHoldOnGoodEvidence(t *testing.T) {
	for _, c := range CheckInvariants(goodEvidence()) {
		if c.Err != nil {
			t.Errorf("invariant %s: %v", c.Name, c.Err)
		}
	}
}

func TestInvariantsDetectViolations(t *testing.T) {
	for _, tc := range []struct {
		name, inv string
		spoil     func(ev *Evidence)
	}{
		{"two evaluators accepted in one epoch", InvEpochLeader, func(ev *Evidence) {
			ev.Receiver = append(ev.Receiver, line("e3", 2, statusAccepted, "c", "firing"))
		}},
		{"two evaluators accepted one incident in one epoch", InvIncidentLeader, func(ev *Evidence) {
			ev.Receiver = append(ev.Receiver, line("e1", 2, statusAccepted, "b", "resolved"))
		}},
		{"an incident accepted twice under two keys", InvDuplicates, func(ev *Evidence) {
			dup := line("e2", 2, statusAccepted, "a", "firing")
			dup.Key = "random-key"
			ev.Receiver = append(ev.Receiver, dup)
		}},
		{"a repeated tail offset", InvTail, func(ev *Evidence) {
			ev.TailOffsets = []uint64{2, 3, 3, 5}
		}},
		{"a gap in the tail", InvTail, func(ev *Evidence) {
			ev.TailOffsets = []uint64{2, 3, 8}
		}},
		{"a tail offset past the log", InvTail, func(ev *Evidence) {
			ev.TailOffsets = append(ev.TailOffsets, 9, 10)
		}},
		{"a refused resume", InvTail, func(ev *Evidence) {
			ev.TailRefused = []string{`410 {"code":"expired"}`}
		}},
		{"an empty tail", InvTail, func(ev *Evidence) {
			ev.TailOffsets = nil
		}},
		{"a drop not counted", InvAccounting, func(ev *Evidence) {
			ev.Accounts[1].Dropped--
		}},
		{"an accepted batch missing from the log", InvAccounting, func(ev *Evidence) {
			ev.Accounts[0].Logged--
		}},
		{"a batch that arrived and was not received", InvAccounting, func(ev *Evidence) {
			ev.Accounts[0].Arrived++
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev := goodEvidence()
			tc.spoil(ev)
			if checkOf(t, ev, tc.inv) == nil {
				t.Fatalf("%s holds", tc.inv)
			}
		})
	}
}

// TestArrivalsAfterACrashAreNotExact: after serve crashes, the drops since its
// last checkpoint are gone from its counters (LIMITATIONS.md), so only the
// equalities that survive a recovery are checked.
func TestArrivalsAfterACrashAreNotExact(t *testing.T) {
	ev := goodEvidence()
	ev.ArrivalsExact = false
	ev.Accounts[1].Arrived += 3
	if err := checkOf(t, ev, InvAccounting); err != nil {
		t.Fatal(err)
	}
}
