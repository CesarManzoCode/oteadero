import tempfile
import unittest
from pathlib import Path

import crit_quantiles_merged
import lib


def bucket(series, q=None, card=None, end=1000):
    row = {"tenant": "t1", "series": series, "end": end, "n": 1}
    if q is not None:
        row["q"] = q
    if card is not None:
        row["card"] = card
    return row


def quantiles(v):
    return {"0.5": v, "0.9": 2 * v, "0.99": 3 * v, "0.999": 4 * v}


def tr(t, state, rule="r", group="{}", closed_at=0):
    return {"rule": rule, "group": group, "t": t, "state": state, "value": 1.0, "closed_at": closed_at}


class QuantileComparisonTest(unittest.TestCase):
    def compare(self, engine_value, exact_value=100.0):
        engine = lib.bucket_quantiles([bucket('latency_ms{svc="api"}', quantiles(engine_value))])
        exact = lib.exact_bucket_quantiles([bucket('latency_ms{svc="api"}', quantiles(exact_value))])
        return lib.compare_relative(engine, exact, lib.QUANTILE_ALPHA)

    def test_within_alpha_passes(self):
        compared, worst, diffs = self.compare(100.9)
        self.assertEqual(compared, 4)
        self.assertAlmostEqual(worst, 0.009)
        self.assertEqual(diffs, [])

    def test_error_of_1_1_percent_is_detected(self):
        compared, worst, diffs = self.compare(101.1)
        self.assertEqual(compared, 4)
        self.assertAlmostEqual(worst, 0.011)
        self.assertEqual(len(diffs), 4)
        self.assertTrue(all(line.startswith("out of bound") for line in diffs))

    def test_error_below_the_exact_value_is_detected(self):
        _, worst, diffs = self.compare(98.9)
        self.assertAlmostEqual(worst, 0.011)
        self.assertEqual(len(diffs), 4)

    def test_missing_sketch_is_detected(self):
        exact = lib.exact_bucket_quantiles([bucket('latency_ms{svc="api"}', quantiles(1.0))])
        compared, _, diffs = lib.compare_relative({}, exact, lib.QUANTILE_ALPHA)
        self.assertEqual(compared, 4)
        self.assertTrue(all(line.startswith("missing") for line in diffs))

    def test_exact_keeps_only_sketched_metrics(self):
        rows = [bucket("otgen_flush{}", quantiles(1.0)), bucket('errors{svc="api"}', quantiles(1.0))]
        self.assertEqual(lib.exact_bucket_quantiles(rows), {})

    def test_print_relative_fails_on_differences_and_on_nothing_compared(self):
        self.assertTrue(lib.print_relative("cuantiles", 4, 0.009, []))
        self.assertFalse(lib.print_relative("cuantiles", 4, 0.011, ["out of bound"]))
        self.assertFalse(lib.print_relative("cuantiles", 0, 0.0, []))


class WindowComparisonTest(unittest.TestCase):
    def test_merged_quantile_error_of_1_1_percent_is_detected(self):
        names = {"p99"}
        engine = [{"rule": "p99", "group": '{svc="api"}', "t": 5000, "value": 505.5}]
        exact = [
            {"rule": "p99", "group": '{svc="api"}', "t": 5000, "value": 500.0},
            {"rule": "other", "group": "{}", "t": 5000, "value": 1.0},
        ]
        compared, worst, diffs = lib.compare_relative(
            lib.window_values(engine, names), lib.window_values(exact, names), lib.QUANTILE_ALPHA
        )
        self.assertEqual(compared, 1)
        self.assertAlmostEqual(worst, 0.011)
        self.assertEqual(len(diffs), 1)

    def test_merged_rules_need_by_and_several_buckets(self):
        rules = [
            {"name": "a", "agg": "quantile", "by": ["svc"], "range_ms": 5000},
            {"name": "b", "agg": "quantile", "by": [], "range_ms": 10000},
            {"name": "c", "agg": "quantile", "range_ms": 5000},
            {"name": "d", "agg": "quantile", "by": ["svc"], "range_ms": 1000},
            {"name": "e", "agg": "cardinality", "by": ["svc"], "range_ms": 5000},
        ]
        self.assertEqual(crit_quantiles_merged.merged_rules(rules), {"a", "b"})

    def test_alerts_rules_have_merged_quantile_rules(self):
        self.assertTrue(crit_quantiles_merged.merged_rules(lib.load_rules()))


class CardinalityComparisonTest(unittest.TestCase):
    def test_bound_of_5_percent(self):
        exact = lib.exact_bucket_cardinalities([bucket("{}", card={"user": 1000})])
        key = ("t1", "{}", 1000, "user")
        self.assertEqual(lib.compare_relative({key: 1049}, exact, lib.CARDINALITY_BOUND)[2], [])
        self.assertEqual(len(lib.compare_relative({key: 1051}, exact, lib.CARDINALITY_BOUND)[2]), 1)

    def test_field_without_values_counts_zero(self):
        exact = lib.exact_bucket_cardinalities([bucket("{}", card={})])
        engine = lib.bucket_cardinalities([bucket("{}", card={"user": 0})])
        self.assertEqual(lib.compare_relative(engine, exact, lib.CARDINALITY_BOUND), (1, 0.0, []))


class TransitionComparisonTest(unittest.TestCase):
    def setUp(self):
        self.oracle = [tr(1000, "pending"), tr(3000, "firing"), tr(9000, "resolved")]

    def test_equal(self):
        engine = [[tr(1000, "pending"), tr(3000, "firing"), tr(9000, "resolved")]]
        self.assertEqual(lib.compare_transitions(engine, self.oracle), (3, []))

    def test_changed_transition_is_detected(self):
        engine = [[tr(1000, "pending"), tr(3000, "inactive"), tr(9000, "resolved")]]
        compared, diffs = lib.compare_transitions(engine, self.oracle)
        self.assertEqual(compared, 3)
        self.assertEqual(len(diffs), 1)
        self.assertTrue(diffs[0].startswith("different"))

    def test_moved_transition_is_detected(self):
        engine = [[tr(1000, "pending"), tr(4000, "firing"), tr(9000, "resolved")]]
        _, diffs = lib.compare_transitions(engine, self.oracle)
        self.assertEqual(sorted(line.split()[0] for line in diffs), ["extra", "missing"])

    def test_repeated_transition_is_detected(self):
        engine = [[tr(1000, "pending"), tr(3000, "firing"), tr(3000, "firing"), tr(9000, "resolved")]]
        _, diffs = lib.compare_transitions(engine, self.oracle)
        self.assertEqual(len(diffs), 1)
        self.assertTrue(diffs[0].startswith("repeated"))

    def test_union_after_recovery(self):
        # The killed run counts below the checkpoint at 50; its line at 60 is
        # replayed by the new run.
        killed = [tr(1000, "pending", closed_at=10), tr(3000, "firing", closed_at=60)]
        recovered = [tr(3000, "firing", closed_at=60), tr(9000, "resolved", closed_at=90)]
        self.assertEqual(lib.compare_transitions([killed, recovered], self.oracle, 50), (3, []))

    def test_changed_transition_after_recovery_is_detected(self):
        killed = [tr(1000, "pending", closed_at=10)]
        recovered = [tr(1000, "inactive", closed_at=10), tr(3000, "firing"), tr(9000, "resolved")]
        _, diffs = lib.compare_transitions([killed, recovered], self.oracle, 50)
        self.assertEqual(len(diffs), 1)
        self.assertTrue(diffs[0].startswith("conflict"))

    def test_line_lost_with_the_kill_and_not_replayed_is_missing(self):
        killed = [tr(1000, "pending", closed_at=10), tr(3000, "firing", closed_at=60)]
        recovered = [tr(9000, "resolved", closed_at=90)]
        _, diffs = lib.compare_transitions([killed, recovered], self.oracle, 50)
        self.assertEqual(len(diffs), 1)
        self.assertTrue(diffs[0].startswith("missing"))


class ExportRunsTest(unittest.TestCase):
    def test_runs_of_an_evaluator(self):
        with tempfile.TemporaryDirectory() as tmp:
            for name in ("e1-1000-aa", "e1-999-ff", "serve-5-00", "e2-1-00"):
                (Path(tmp) / name).mkdir()
            names = [run.name for run in lib.export_runs(tmp, "e1")]
            self.assertEqual(names, ["e1-999-ff", "e1-1000-aa"])


if __name__ == "__main__":
    unittest.main()
