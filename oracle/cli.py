"""Command line of the oracle: recomputes results from the raw log alone."""

import argparse
import json
import os
import sys

from logread import read_log
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


def main(argv=None):
    parser = argparse.ArgumentParser(prog="oracle")
    commands = parser.add_subparsers(dest="command", required=True)
    windows = commands.add_parser("windows", help="recompute buckets and late events")
    windows.add_argument("--data", required=True)
    windows.add_argument("--bucket-ms", type=_positive, default=1000)
    windows.add_argument("--lateness-ms", type=_non_negative, default=2000)
    windows.add_argument("--idle-grace-ms", type=_non_negative, default=5000)
    windows.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    try:
        _windows(args)
    except OSError as err:
        print(f"oracle: {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
