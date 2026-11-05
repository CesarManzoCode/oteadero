"""Criterion: every bucket quantile is within alpha of the oracle's exact lower-rank quantile."""

import sys

import lib

SEED = 11


def main():
    work = lib.work_dir("crit_quantiles")
    with lib.Serve(work, lib.alerts_tenants()) as engine:
        engine.start()
        lib.accepted_events(lib.run_otgen("alerts", SEED, engine.url, work / "report.json"))
        engine.stop()

    runs = [lib.read_jsonl(run / "buckets.jsonl") for run in lib.export_runs(engine.export_dir)]
    merged, conflicts = lib.union_exports(runs, 0)
    exact_rows, _ = lib.run_exact(engine.data_dir, work / "exact")
    compared, worst, diffs = lib.compare_relative(
        lib.bucket_quantiles(merged.values()),
        lib.exact_bucket_quantiles(exact_rows),
        lib.QUANTILE_ALPHA,
    )
    return 0 if lib.print_relative("cuantiles", compared, worst, conflicts + diffs) else 1


if __name__ == "__main__":
    sys.exit(main())
