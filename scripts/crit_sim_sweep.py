"""Criterion: 200 seeds of otsim with faults pass the invariants and agree with the oracle.

Each seed runs in .work/sim_sweep/<seed>/. A seed passes when otsim exits with
0, its transitions equal the ones the oracle replays from the log of the run,
and its receiver log has no lost notification, no effective duplicate and no
accepted request the oracle does not expect against the oracle incidents. The
seeds run in parallel; each is a single-threaded simulation.
"""

import concurrent.futures
import math
import os
import subprocess
import sys

import lib

SEEDS = range(1, 201)
WORK = "sim_sweep"
# The simulation runs serve and the evaluators with the default configuration.
IDLE_GRACE_MS = 5000
MAX_SHOWN = 5


def seed_dir(seed):
    return lib.REPO_ROOT / ".work" / WORK / str(seed)


def merge_rows(rows, field):
    """Join the rows of all evaluators by (rule, group, t); return them and the conflicts.

    otsim keeps the distinct lines of every export run, so the same row comes
    once per evaluator and run; two values of field for a key are a conflict.
    """
    merged, conflicts = {}, []
    for row in rows:
        key = (row["rule"], row["group"], row["t"])
        seen = merged.setdefault(key, row)
        if seen[field] != row[field]:
            conflicts.append(f"conflict {key}: {seen[field]} and {row[field]}")
    return list(merged.values()), conflicts


def check_seed(seed):
    """Run a seed and return the lines that explain why it failed; empty if it passed."""
    out = seed_dir(seed)
    run = lib.run_otsim(seed, out)
    if run.code != 0:
        return [f"otsim salió con {run.code}"] + run.output.splitlines()
    _, conflicts = merge_rows(lib.read_jsonl(out / "evals.jsonl"), "value")
    engine, found = merge_rows(lib.read_jsonl(out / "transitions.jsonl"), "state")
    conflicts += found
    oracle_dir = out / "oracle"
    try:
        oracle = lib.run_oracle_rules(
            out / "data", oracle_dir, lib.ALERTS_RULES, out / "evals.jsonl", IDLE_GRACE_MS
        )
        expected = lib.run_oracle_incidents(oracle_dir / "transitions.jsonl", oracle_dir)
    except subprocess.CalledProcessError as err:
        return [f"oráculo: {err}"]
    compared, diffs = lib.compare_transitions([engine], oracle, math.inf)
    outcome = lib.notify_outcome(expected, lib.read_jsonl(out / "receiver.jsonl"))
    problems = conflicts + diffs
    if compared == 0:
        problems.append("sin transiciones que comparar")
    problems += [f"perdido: {key}" for key in outcome.lost]
    problems += [f"duplicado efectivo: {key}" for key in outcome.duplicates]
    problems += [f"aceptado desconocido: {key}" for key in outcome.unknown]
    return problems


def main():
    lib.work_dir(WORK)
    lib.binary("otsim")
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as pool:
        results = dict(zip(SEEDS, pool.map(check_seed, SEEDS)))
    failed = [seed for seed in SEEDS if results[seed]]
    for seed in failed[:MAX_SHOWN]:
        for line in results[seed][:MAX_SHOWN]:
            print(f"semilla {seed}: {line}")
    print(f"semillas: {len(SEEDS)}")
    print(f"fallidas: {len(failed)}")
    if failed:
        print(f"primera fallida: {failed[0]}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
