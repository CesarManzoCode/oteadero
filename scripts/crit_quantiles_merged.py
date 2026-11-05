"""Criterion: quantile rule values whose sketches merge buckets, series and shards are within alpha.

The rules are the quantile rules of rules/alerts.json with by and a range of
several buckets; their windows join the sketches of several series, which the
aggregation spreads over its shards.
"""

import sys

import lib

SEED = 12
BUCKET_MS = 1000


def merged_rules(rules):
    return {
        rule["name"]
        for rule in rules
        if rule["agg"] == "quantile" and "by" in rule and rule["range_ms"] > BUCKET_MS
    }


def main():
    work = lib.work_dir("crit_quantiles_merged")
    engine, evaluator, _ = lib.run_alerts(work, SEED)
    names = merged_rules(lib.load_rules())
    evals, conflicts = lib.evaluator_evals(engine.export_dir, evaluator.id)
    _, exact_windows = lib.run_exact(engine.data_dir, work / "exact", lib.ALERTS_RULES)
    compared, worst, diffs = lib.compare_relative(
        lib.window_values(evals.values(), names),
        lib.window_values(exact_windows, names),
        lib.QUANTILE_ALPHA,
    )
    return 0 if lib.print_relative("cuantiles comparados", compared, worst, conflicts + diffs) else 1


if __name__ == "__main__":
    sys.exit(main())
