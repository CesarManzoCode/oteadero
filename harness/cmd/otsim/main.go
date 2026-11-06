// Command otsim runs the engine scenario of the deterministic simulation
// (D16): serve, three evaluators, the receiver, producers and a tail
// subscriber on virtual time, with the fault schedule its seed draws. It
// writes the record, the log in the format of §4.3, the receiver log, the
// transitions and the evals, prints the hash and one line per invariant, and exits with 0
// only if every invariant holds.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/CesarManzoCode/oteadero/harness/sim"
)

const usage = "usage: otsim --seed <n> --out <dir> [--no-faults] [--rules <file>]\n"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run returns 0 when every invariant holds, 1 when one fails or the run
// cannot be done, and 2 on bad usage.
func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("otsim", flag.ContinueOnError)
	flags.SetOutput(stderr)
	seed := flags.Uint64("seed", 0, "seed of the run")
	out := flags.String("out", "", "output directory")
	noFaults := flags.Bool("no-faults", false, "run without the fault schedule")
	rulesPath := flags.String("rules", "rules/alerts.json", "rule file of the evaluators")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	seedSet := false
	flags.Visit(func(f *flag.Flag) { seedSet = seedSet || f.Name == "seed" })
	if !seedSet || *out == "" || flags.NArg() != 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	rules, err := os.ReadFile(*rulesPath)
	if err != nil {
		fmt.Fprintf(stderr, "otsim: %v\n", err)
		return 1
	}
	res, err := sim.Run(sim.Options{Seed: *seed, Faults: !*noFaults, Rules: rules})
	if err != nil {
		fmt.Fprintf(stderr, "otsim: %v\n", err)
		return 1
	}
	return report(res, *out, stdout, stderr)
}

// report writes the files of a run to dir and prints its verdict.
func report(res *sim.Result, dir string, stdout, stderr io.Writer) int {
	if err := write(res, dir); err != nil {
		fmt.Fprintf(stderr, "otsim: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "hash: %s\n", res.Hash)
	code := 0
	for _, c := range res.Checks {
		verdict := "ok"
		if c.Err != nil {
			verdict, code = "fail", 1
			fmt.Fprintf(stderr, "otsim: invariant %s: %v\n", c.Name, c.Err)
		}
		fmt.Fprintf(stdout, "invariant %s: %s\n", c.Name, verdict)
	}
	return code
}

// write stores registry.log, data/log/, receiver.jsonl, transitions.jsonl and
// evals.jsonl in dir. A data/log/ that is not empty is refused, so that the oracle never
// reads the segments of two runs together.
func write(res *sim.Result, dir string) error {
	logDir := filepath.Join(dir, "data", "log")
	if entries, err := os.ReadDir(logDir); err == nil && len(entries) > 0 {
		return fmt.Errorf("%s is not empty", logDir)
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return err
	}
	for _, f := range res.LogFiles {
		if err := os.WriteFile(filepath.Join(logDir, f.Name), f.Data, 0o644); err != nil {
			return err
		}
	}
	for name, data := range map[string][]byte{
		"registry.log":      res.Record,
		"receiver.jsonl":    res.Receiver,
		"transitions.jsonl": res.Transitions,
		"evals.jsonl":       res.Evals,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			return err
		}
	}
	return nil
}
