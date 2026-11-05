"""Criterion: closed buckets equal the oracle's, without faults."""

import sys

import lib

SEED = 1


def main():
    work = lib.work_dir("crit_windows")
    with lib.Serve(work, lib.tenants(2, lib.HIGH_QUOTA)) as engine:
        engine.start()
        lib.run_otgen("basic", SEED, engine.url, work / "report.json")
        engine.stop()
    oracle = lib.run_oracle(engine.data_dir, work / "oracle")
    return 0 if lib.check_windows(engine.export_dir, oracle) else 1


if __name__ == "__main__":
    sys.exit(main())
