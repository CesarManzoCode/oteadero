import json
import tempfile
import unittest
from pathlib import Path

import rules2prom


def rule(**fields):
    base = {
        "name": "r",
        "tenant": "t1",
        "source": "metrics",
        "match": {"__name__": "m"},
        "agg": "count",
        "range_ms": 60000,
        "op": ">",
        "threshold": 3,
    }
    base.update(fields)
    return base


class ExprTest(unittest.TestCase):
    def test_count(self):
        self.assertEqual(rules2prom.expr(rule()), "count_over_time(m[60s]) > 3")

    def test_sum(self):
        self.assertEqual(
            rules2prom.expr(rule(agg="sum", range_ms=30000, threshold=10)),
            "sum_over_time(m[30s]) > 10",
        )

    def test_rate_divides_by_the_range_in_seconds(self):
        self.assertEqual(
            rules2prom.expr(rule(agg="rate", range_ms=5000, op=">=", threshold=2.5)),
            "sum_over_time(m[5s]) / 5 >= 2.5",
        )

    def test_count_by(self):
        self.assertEqual(
            rules2prom.expr(rule(by=["svc"], range_ms=2000, threshold=100)),
            "sum by (svc) (count_over_time(m[2s])) > 100",
        )

    def test_rate_by_divides_the_group_sum(self):
        self.assertEqual(
            rules2prom.expr(rule(agg="rate", by=["svc", "instance"], range_ms=10000, op="<")),
            "sum by (instance, svc) (sum_over_time(m[10s])) / 10 < 3",
        )

    def test_empty_by_is_one_group(self):
        self.assertEqual(rules2prom.expr(rule(by=[])), "sum by () (count_over_time(m[60s])) > 3")

    def test_match_labels_are_sorted_and_escaped(self):
        match = {"__name__": "m", "svc": 'a"b', "env": "x\\y"}
        self.assertEqual(
            rules2prom.expr(rule(match=match)),
            'count_over_time(m{env="x\\\\y",svc="a\\"b"}[60s]) > 3',
        )

    def test_range_under_a_second(self):
        self.assertEqual(rules2prom.expr(rule(range_ms=1500)), "count_over_time(m[1500ms]) > 3")


class TranslateTest(unittest.TestCase):
    def test_groups_by_interval_with_for_and_keep_firing_for(self):
        rules = [
            rule(name="a", for_ms=20000),
            rule(name="b", agg="sum", range_ms=30000, threshold=10, interval_ms=10000,
                 for_ms=20000, keep_firing_for_ms=30000),
            rule(name="c", by=["svc"]),
        ]
        self.assertEqual(
            rules2prom.translate(rules),
            "groups:\n"
            '  - name: "oteadero_1s"\n'
            "    interval: 1s\n"
            "    rules:\n"
            '      - alert: "a"\n'
            '        expr: "count_over_time(m[60s]) > 3"\n'
            "        for: 20s\n"
            '      - alert: "c"\n'
            '        expr: "sum by (svc) (count_over_time(m[60s])) > 3"\n'
            '  - name: "oteadero_10s"\n'
            "    interval: 10s\n"
            "    rules:\n"
            '      - alert: "b"\n'
            '        expr: "sum_over_time(m[30s]) > 10"\n'
            "        for: 20s\n"
            "        keep_firing_for: 30s\n",
        )

    def test_repository_rules_translate(self):
        root = Path(__file__).resolve().parent.parent
        for name in ("reference.json", "kff.json"):
            with tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp) / "out.yaml"
                rules2prom.translate_file(root / "rules" / name, out)
                self.assertTrue(out.read_text().startswith("groups:\n"))

    def test_main_writes_the_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "rules.json"
            src.write_text(json.dumps({"rules": [rule()]}))
            dst = Path(tmp) / "out.yaml"
            self.assertEqual(rules2prom.main([str(src), str(dst)]), 0)
            self.assertIn("count_over_time(m[60s]) > 3", dst.read_text())


class RejectTest(unittest.TestCase):
    def assert_rejected(self, *rules):
        with self.assertRaises(rules2prom.Unsupported):
            rules2prom.translate(list(rules))

    def test_logs_rule(self):
        self.assert_rejected(rule(source="logs", match={}))

    def test_quantile_and_cardinality(self):
        self.assert_rejected(rule(agg="quantile", quantile=0.99))
        self.assert_rejected(rule(agg="cardinality", field="user"))

    def test_hysteresis(self):
        self.assert_rejected(rule(resolve_threshold=1))

    def test_match_without_metric_name(self):
        self.assert_rejected(rule(match={"svc": "api"}))

    def test_two_tenants(self):
        self.assert_rejected(rule(name="a"), rule(name="b", tenant="t2"))

    def test_unknown_field(self):
        self.assert_rejected(rule(labels={"severity": "page"}))

    def test_main_fails_on_a_rule_outside_the_subset(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "rules.json"
            src.write_text(json.dumps({"rules": [rule(source="logs")]}))
            self.assertEqual(rules2prom.main([str(src), str(Path(tmp) / "out.yaml")]), 1)


if __name__ == "__main__":
    unittest.main()
