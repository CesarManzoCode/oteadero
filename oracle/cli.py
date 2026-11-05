"""Command line of the oracle: recomputes results from the raw log alone."""

import argparse
import json
import os
import sys

from exact import collect_buckets, exact_row
from incidents import notifications
from logread import read_log
from rules import SKETCH_AGGS, evaluate, load_engine_values, load_rules, rule_windows, window_value
from windows import compute_windows


def _non_negative(text):
    value = int(text)
    if value < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return value


def _positive(text):
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return value


def _dump(value):
    return json.dumps(value, separators=(",", ":"))


def _write_jsonl(path, rows):
    with open(path, "w", encoding="ascii") as f:
        for row in rows:
            f.write(_dump(row) + "\n")


def _windows(args):
    result = compute_windows(
        read_log(args.data), args.bucket_ms, args.lateness_ms, args.idle_grace_ms
    )
    os.makedirs(args.out, exist_ok=True)
    _write_jsonl(os.path.join(args.out, "buckets.jsonl"), result.buckets)
    _write_jsonl(os.path.join(args.out, "late.jsonl"), ({"o": o} for o in result.late))
    with open(os.path.join(args.out, "accounting.json"), "w", encoding="ascii") as f:
        f.write(_dump(result.accounting) + "\n")


def _exact(args):
    entries = list(read_log(args.data))
    params = (args.bucket_ms, args.lateness_ms, args.idle_grace_ms)
    os.makedirs(args.out, exist_ok=True)
    buckets = collect_buckets(entries, *params)
    _write_jsonl(os.path.join(args.out, "exact_buckets.jsonl"), (exact_row(b) for b in buckets))
    if args.rules is None:
        return
    rules = [r for r in load_rules(args.rules, args.bucket_ms) if r.agg in SKETCH_AGGS]
    rows = (
        {"rule": rule.name, "group": key, "t": t, "value": window_value(rule, group)}
        for rule, t, groups in rule_windows(entries, rules, *params)
        for key, group in sorted(groups.items())
    )
    _write_jsonl(os.path.join(args.out, "exact_windows.jsonl"), rows)


def _rules(args):
    rules = load_rules(args.rules, args.bucket_ms)
    engine_values = load_engine_values(args.engine_evals)
    evals, transitions = evaluate(
        read_log(args.data),
        rules,
        args.bucket_ms,
        args.lateness_ms,
        args.idle_grace_ms,
        engine_values,
    )
    os.makedirs(args.out, exist_ok=True)
    _write_jsonl(os.path.join(args.out, "evals.jsonl"), evals)
    _write_jsonl(os.path.join(args.out, "transitions.jsonl"), transitions)


def _incidents(args):
    with open(args.transitions, encoding="utf-8") as f:
        transitions = [json.loads(line) for line in f if line.strip()]
    os.makedirs(args.out, exist_ok=True)
    _write_jsonl(os.path.join(args.out, "incidents.jsonl"), notifications(transitions))


def _log_params(command):
    command.add_argument("--data", required=True)
    command.add_argument("--bucket-ms", type=_positive, default=1000)
    command.add_argument("--lateness-ms", type=_non_negative, default=2000)
    command.add_argument("--idle-grace-ms", type=_non_negative, default=5000)
    command.add_argument("--out", required=True)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="oracle")
    commands = parser.add_subparsers(dest="command", required=True)
    windows = commands.add_parser("windows", help="recompute buckets and late events")
    _log_params(windows)
    windows.set_defaults(run=_windows)
    exact = commands.add_parser("exact", help="exact quantiles and distinct counts")
    _log_params(exact)
    exact.add_argument("--rules", help="also write the exact values of the sketch rule windows")
    exact.set_defaults(run=_exact)
    rules = commands.add_parser("rules", help="replay rule evaluations and transitions")
    _log_params(rules)
    rules.add_argument("--rules", required=True)
    rules.add_argument(
        "--engine-evals",
        action="append",
        default=[],
        help="evals.jsonl of the engine, read for the conditions of quantile and cardinality",
    )
    rules.set_defaults(run=_rules)
    incidents = commands.add_parser("incidents", help="expected incidents and keys")
    incidents.add_argument("--transitions", required=True)
    incidents.add_argument("--out", required=True)
    incidents.set_defaults(run=_incidents)
    args = parser.parse_args(argv)
    try:
        args.run(args)
    except (OSError, ValueError, KeyError) as err:
        print(f"oracle: {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
