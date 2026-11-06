package sim

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// Names of the invariants, in the order they are checked and printed.
const (
	InvEpochLeader    = "one_evaluator_per_epoch"
	InvIncidentLeader = "one_evaluator_per_incident_epoch"
	InvDuplicates     = "no_effective_duplicates"
	InvTail           = "tail_offsets"
	InvAccounting     = "accounting"
)

// Check is the verdict of one invariant: Err is nil when it holds.
type Check struct {
	Name string
	Err  error
}

// Evidence is what the invariants are checked on, gathered at the end of a
// run.
type Evidence struct {
	Receiver []ReceiverLine
	// TailOffsets are the offsets the tail subscriber got, in the order it got
	// them, over all its connections; TailRefused are the answers or events
	// that broke its stream.
	TailOffsets []uint64
	TailRefused []string
	// LogOffsets are the offsets of the final log that the subscription
	// covers, in log order.
	LogOffsets []uint64
	// Accounts are by tenant, sorted.
	Accounts []Account
	// ArrivalsExact is true when serve never crashed, so that its counters
	// cover every batch that reached it.
	ArrivalsExact bool
}

// Account is the counting of one tenant.
type Account struct {
	Tenant string
	// Received, Accepted and Dropped (all causes) are the counters of serve
	// at the end of the run.
	Received, Accepted, Dropped uint64
	// Logged is the number of events of the tenant in the final log.
	Logged uint64
	// Arrived is the number of events in the batches that reached serve,
	// counted by the harness.
	Arrived uint64
}

// CheckInvariants checks every invariant of the scenario.
func CheckInvariants(ev *Evidence) []Check {
	return []Check{
		{InvEpochLeader, checkEpochLeader(ev.Receiver)},
		{InvIncidentLeader, checkIncidentLeader(ev.Receiver)},
		{InvDuplicates, checkDuplicates(ev.Receiver)},
		{InvTail, checkTail(ev.TailOffsets, ev.TailRefused, ev.LogOffsets)},
		{InvAccounting, checkAccounting(ev.Accounts, ev.ArrivalsExact)},
	}
}

// notification is what the invariants read of the body of a request.
type notification struct {
	Incident string `json:"incident"`
	Kind     string `json:"kind"`
}

func bodyOf(l *ReceiverLine) (notification, error) {
	var n notification
	if err := json.Unmarshal(l.Body, &n); err != nil {
		return n, fmt.Errorf("request %s: body: %w", l.Key, err)
	}
	return n, nil
}

// checkEpochLeader: never two evaluators with an accepted request in the
// same epoch.
func checkEpochLeader(lines []ReceiverLine) error {
	by := map[uint64]string{}
	for _, l := range lines {
		if l.Status != statusAccepted {
			continue
		}
		if e, ok := by[l.Epoch]; ok && e != l.Evaluator {
			return fmt.Errorf("epoch %d has accepted requests from %s and %s", l.Epoch, e, l.Evaluator)
		}
		by[l.Epoch] = l.Evaluator
	}
	return nil
}

// checkIncidentLeader: never two evaluators with an accepted request for the
// same incident in the same epoch.
func checkIncidentLeader(lines []ReceiverLine) error {
	type key struct {
		incident string
		epoch    uint64
	}
	by := map[key]string{}
	for i := range lines {
		l := &lines[i]
		if l.Status != statusAccepted {
			continue
		}
		n, err := bodyOf(l)
		if err != nil {
			return err
		}
		k := key{n.Incident, l.Epoch}
		if e, ok := by[k]; ok && e != l.Evaluator {
			return fmt.Errorf("incident %s in epoch %d accepted from %s and %s", n.Incident, l.Epoch, e, l.Evaluator)
		}
		by[k] = l.Evaluator
	}
	return nil
}

// checkDuplicates: no incident and kind with more than one accepted request
// (§4.7). The incident is read from the body, so that a request with a key
// that does not follow from it still counts as the same notification.
func checkDuplicates(lines []ReceiverLine) error {
	seen := map[notification]bool{}
	for i := range lines {
		l := &lines[i]
		if l.Status != statusAccepted {
			continue
		}
		n, err := bodyOf(l)
		if err != nil {
			return err
		}
		if seen[n] {
			return fmt.Errorf("incident %s %s accepted more than once", n.Incident, n.Kind)
		}
		seen[n] = true
	}
	return nil
}

// checkTail: the subscriber got the offsets of the log that match its
// subscription from where it started, each once, in order and without a gap,
// and nothing broke its stream. A run in which it got nothing does not count
// as a pass.
func checkTail(got []uint64, refused []string, log []uint64) error {
	if len(refused) > 0 {
		return fmt.Errorf("%d broken connections, the first: %s", len(refused), refused[0])
	}
	if len(got) == 0 {
		return errors.New("the subscriber got no event")
	}
	start, ok := slices.BinarySearch(log, got[0])
	if !ok {
		return fmt.Errorf("first offset %d is not in the log", got[0])
	}
	for i, o := range got {
		if start+i >= len(log) {
			return fmt.Errorf("offset %d at position %d is past the end of the log", o, i)
		}
		if want := log[start+i]; o != want {
			return fmt.Errorf("offset %d at position %d, want %d", o, i, want)
		}
	}
	return nil
}

// checkAccounting: for each tenant serve received what it accepted plus what
// it dropped, what it accepted is what the log holds and, while serve never
// crashed, what it received is what reached it.
func checkAccounting(accounts []Account, arrivalsExact bool) error {
	for _, a := range accounts {
		switch {
		case a.Received != a.Accepted+a.Dropped:
			return fmt.Errorf("tenant %s: received %d, accepted %d + dropped %d", a.Tenant, a.Received, a.Accepted, a.Dropped)
		case a.Accepted != a.Logged:
			return fmt.Errorf("tenant %s: accepted %d, %d in the log", a.Tenant, a.Accepted, a.Logged)
		case arrivalsExact && a.Received != a.Arrived:
			return fmt.Errorf("tenant %s: received %d, %d arrived", a.Tenant, a.Received, a.Arrived)
		}
	}
	return nil
}
