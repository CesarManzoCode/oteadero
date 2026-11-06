"""Criterion: the peak resident memory of serve under the bench load is at most 4 GiB (D14).

It reads VmHWM of the engine run of crit_bench.py in .work/bench/engine.json,
taken at the end of the load before serve stops, and runs that engine phase
first if there is none.
"""

import json
import shutil
import sys

import crit_bench
import lib

MAX_MIB = 4096


def main():
    path = lib.REPO_ROOT / ".work" / crit_bench.WORK / crit_bench.RESULT
    if path.exists():
        result = json.loads(path.read_text())
    else:
        if shutil.which("taskset") is None:
            print("falta taskset")
            return lib.SKIPPED
        result = crit_bench.run_engine(lib.work_dir(crit_bench.WORK))
    mib = result["rss_kib"] / 1024
    print(f"rss máximo: {mib:.1f} MiB")
    return 0 if mib <= MAX_MIB else 1


if __name__ == "__main__":
    sys.exit(main())
