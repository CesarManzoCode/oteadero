"""Criterion: buckets and late classification equal the oracle's, with disorder and late events."""

import sys

import lib

SEED = 2


def main():
    work = lib.work_dir("crit_windows_late")
    with lib.Serve(work, lib.tenants(2, lib.HIGH_QUOTA)) as engine:
        engine.start()
        lib.run_otgen("late", SEED, engine.url, work / "report.json")
        engine.stop()
    oracle = lib.run_oracle(engine.data_dir, work / "oracle")
    windows_ok = lib.check_windows(engine.export_dir, oracle)

    engine_late = {
        row["o"]
        for run in lib.export_runs(engine.export_dir)
        for row in lib.read_jsonl(run / "late.jsonl")
    }
    oracle_late = set(oracle.late)
    differences = len(engine_late ^ oracle_late)
    print(f"tardíos: {len(oracle_late)}")
    print(f"clasificación: {differences} diferencias")
    return 0 if windows_ok and oracle_late and differences == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
