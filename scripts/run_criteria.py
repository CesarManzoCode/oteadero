"""Run every crit_*.py in name order; exit 0 only if none failed (77 means skipped)."""

import argparse
import subprocess
import sys
from pathlib import Path

SKIPPED = 77


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dir",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="directory of the criterion scripts",
    )
    args = parser.parse_args()

    scripts = sorted(args.dir.glob("crit_*.py"))
    if not scripts:
        print(f"no crit_*.py in {args.dir}")
        return 1
    counts = {"pass": 0, "fail": 0, "skipped": 0}
    for script in scripts:
        print(f"== {script.name}", flush=True)
        code = subprocess.run([sys.executable, str(script)]).returncode
        if code == 0:
            outcome = "pass"
        elif code == SKIPPED:
            outcome = "skipped"
        else:
            outcome = "fail"
        counts[outcome] += 1
        print(f"{script.name}: {outcome} (exit {code})", flush=True)
    print(f"pass={counts['pass']} fail={counts['fail']} skipped={counts['skipped']}")
    return 1 if counts["fail"] else 0


if __name__ == "__main__":
    sys.exit(main())
