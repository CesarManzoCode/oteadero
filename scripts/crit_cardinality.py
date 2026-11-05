"""Criterion: every bucket and rule cardinality is within 5% of the oracle's exact count."""

import sys

import lib

SEED = 13


def main():
    work = lib.work_dir("crit_cardinality")
    engine, evaluator, _ = lib.run_alerts(work, SEED)

    runs = [lib.read_jsonl(run / "buckets.jsonl") for run in lib.export_runs(engine.export_dir)]
    buckets, bucket_conflicts = lib.union_exports(runs, 0)
    evals, eval_conflicts = lib.evaluator_evals(engine.export_dir, evaluator.id)
    exact_rows, exact_windows = lib.run_exact(engine.data_dir, work / "exact", lib.ALERTS_RULES)
    names = {rule["name"] for rule in lib.load_rules() if rule["agg"] == "cardinality"}

    in_buckets, worst_buckets, bucket_diffs = lib.compare_relative(
        lib.bucket_cardinalities(buckets.values()),
        lib.exact_bucket_cardinalities(exact_rows),
        lib.CARDINALITY_BOUND,
    )
    in_rules, worst_rules, rule_diffs = lib.compare_relative(
        lib.window_values(evals.values(), names),
        lib.window_values(exact_windows, names),
        lib.CARDINALITY_BOUND,
    )
    print(f"cubetas: {in_buckets}, evaluaciones: {in_rules}")
    ok = lib.print_relative(
        "cardinalidades comparadas",
        in_buckets + in_rules,
        max(worst_buckets, worst_rules),
        bucket_conflicts + eval_conflicts + bucket_diffs + rule_diffs,
    )
    return 0 if ok and in_buckets > 0 and in_rules > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
