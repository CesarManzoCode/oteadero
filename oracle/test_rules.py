import json
import os
import subprocess
import sys
import tempfile
import unittest

from logread import KIND_LOGS, KIND_METRICS, Event, Tick
from rules import evaluate, instants, parse_rules, watermark_advances
from test_logread import batch, metric_event, write_segment

CLI = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cli.py")


def events(rows):
    """Build events from (ts_ms, series, value) rows, with offsets in row order."""
    return [
        Event(i, "a", KIND_METRICS, series, float(value), ts_ms)
        for i, (ts_ms, series, value) in enumerate(rows)
    ]


def every_10s(values, series="m{}"):
    return events([(10000 * i, series, v) for i, v in enumerate(values)])


def rule(**fields):
    base = {"name": "r", "tenant": "a", "source": "metrics", "agg": "sum", "op": ">"}
    base.update(fields)
    return base


def run(entries, *rules, engine=None, lateness_ms=0, idle_grace_ms=0):
    parsed = parse_rules({"rules": list(rules)}, 1000)
    return evaluate(entries, parsed, 1000, lateness_ms, idle_grace_ms, engine)


def states(transitions, name=None, group=None):
    return [
        (tr["state"], tr["t"], tr["value"])
        for tr in transitions
        if (name is None or tr["rule"] == name) and (group is None or tr["group"] == group)
    ]


class StateMachineTest(unittest.TestCase):
    """The cases of section 4.6, each driven from the log."""

    def test_keep_firing_for_measured_with_promtool_and_vmalert_tool(self):
        # vault/RESEARCH.md: sum_over_time(m[30s]) > 10, for 20s, keep_firing_for
        # 30s, interval 10s, values 0 0 5 5 5 5 5 0 ...: the condition holds at
        # 40, 50 and 60 s; it fires from 60 to 90 s and not at 100 s.
        entries = every_10s([0, 0, 5, 5, 5, 5, 5, 0, 0, 0, 0, 0])
        r = rule(
            range_ms=30000, interval_ms=10000, threshold=10, for_ms=20000,
            keep_firing_for_ms=30000,
        )
        evals, transitions = run(entries, r)
        self.assertEqual(
            states(transitions),
            [("pending", 40000, 15.0), ("firing", 60000, 15.0), ("resolved", 100000, 0.0)],
        )
        by_t = {e["t"]: e["value"] for e in evals}
        self.assertEqual([by_t[t] for t in range(30000, 80001, 10000)], [10.0, 15.0, 15.0, 15.0, 10.0, 5.0])

    def test_for_zero_fires_at_once_and_resolves_when_false(self):
        entries = every_10s([0, 20, 20, 0])
        _, transitions = run(entries, rule(range_ms=10000, interval_ms=10000, threshold=10))
        self.assertEqual(states(transitions), [("firing", 10000, 20.0), ("resolved", 30000, 0.0)])

    def test_pending_returns_to_inactive_and_fires_after_for(self):
        entries = every_10s([0, 20, 0, 20, 20, 20])
        r = rule(range_ms=10000, interval_ms=10000, threshold=10, for_ms=20000)
        _, transitions = run(entries, r)
        self.assertEqual(
            states(transitions),
            [
                ("pending", 10000, 20.0),
                ("inactive", 20000, 0.0),
                ("pending", 30000, 20.0),
                ("firing", 50000, 20.0),
            ],
        )

    def test_hysteresis_keeps_firing_above_the_resolve_threshold(self):
        entries = every_10s([0, 12, 7, 12, 4, 7])
        r = rule(range_ms=10000, interval_ms=10000, threshold=10, resolve_threshold=5)
        _, transitions = run(entries, r)
        self.assertEqual(states(transitions), [("firing", 10000, 12.0), ("resolved", 40000, 4.0)])

    def test_absent_group_resets_pending_and_counts_toward_keep_firing_for(self):
        rows = []
        for t in range(0, 60001, 10000):
            if t == 10000:
                rows.append((9500, 'm{x="2"}', 20))
            rows.append((t, 'm{x="1"}', 0))
        entries = events(rows)
        pending = rule(name="p", range_ms=10000, interval_ms=10000, threshold=10, for_ms=20000)
        keep = rule(name="k", range_ms=10000, interval_ms=10000, threshold=10, keep_firing_for_ms=20000)
        evals, transitions = run(entries, pending, keep)
        self.assertEqual(
            states(transitions, "p"), [("pending", 10000, 20.0), ("inactive", 20000, None)]
        )
        self.assertEqual(
            states(transitions, "k"), [("firing", 10000, 20.0), ("resolved", 40000, None)]
        )
        absent = [e for e in evals if e["group"] == '{x="2"}' and e["t"] > 10000]
        self.assertEqual(absent, [])


class WindowValueTest(unittest.TestCase):
    def test_sum_goes_by_end_then_by_series(self):
        # By end then series: (1e16 + 1) - 1e16 = 0. Series first would give 1.
        entries = events(
            [
                (500, 'm{s="a"}', 1e16),
                (600, 'm{s="b"}', 1.0),
                (1500, 'm{s="a"}', -1e16),
                (2000, 'm{s="c"}', 0.0),
            ]
        )
        evals, _ = run(entries, rule(range_ms=2000, by=[], threshold=0))
        self.assertEqual(evals[-1], {"rule": "r", "group": "{}", "t": 2000, "value": 0.0})

    def test_match_by_count_and_rate(self):
        entries = events(
            [
                (500, 'http{code="200",svc="api"}', 1),
                (500, 'http{code="500",svc="api"}', 2),
                (500, 'http{code="200",svc="web"}', 4),
                (500, 'other{svc="api"}', 8),
                (2000, 'other{svc="api"}', 0),
            ]
        )
        count = rule(name="count", agg="count", match={"__name__": "http"}, by=["svc"],
                     range_ms=2000, threshold=0)
        rate = rule(name="rate", agg="rate", match={"__name__": "http"}, by=["svc"],
                    range_ms=2000, threshold=0)
        api = rule(name="api", agg="count", match={"svc": "api"}, range_ms=2000, threshold=0)
        evals, _ = run(entries, count, rate, api)
        at = {(e["rule"], e["group"]): e["value"] for e in evals if e["t"] == 2000}
        self.assertEqual(
            at,
            {
                ("count", '{svc="api"}'): 2,
                ("count", '{svc="web"}'): 1,
                ("rate", '{svc="api"}'): 1.5,
                ("rate", '{svc="web"}'): 2.0,
                ("api", '{code="200",svc="api"}'): 1,
                ("api", '{code="500",svc="api"}'): 1,
                ("api", '{svc="api"}'): 2,
            },
        )

    def test_log_rate_is_count_over_range(self):
        entries = [
            Event(i, "a", KIND_LOGS, '{svc="api"}', 0.0, 500, "{}") for i in range(3)
        ] + [Event(3, "a", KIND_LOGS, '{svc="x"}', 0.0, 2000, "{}")]
        r = rule(source="logs", agg="rate", match={"svc": "api"}, range_ms=2000, threshold=0)
        evals, _ = run(entries, r)
        self.assertEqual(evals[-1], {"rule": "r", "group": '{svc="api"}', "t": 2000, "value": 1.5})


class SketchConditionTest(unittest.TestCase):
    def test_quantile_decides_with_the_engine_value_and_reports_the_exact_one(self):
        entries = events([(100 * v, "lat{}", v) for v in range(1, 10)] + [(5000, "lat{}", 100)])
        r = rule(name="p50", agg="quantile", quantile=0.5, range_ms=1000, threshold=50)
        engine = {("p50", "{}", 1000): 60.0}
        evals, transitions = run(entries, r, engine=engine)
        self.assertEqual(states(transitions), [("firing", 1000, 5.0), ("resolved", 2000, None)])
        # At 5000 the exact value is above the threshold, but without an engine
        # value the condition is false.
        self.assertEqual([(e["t"], e["value"]) for e in evals], [(1000, 5.0), (5000, 100.0)])

    def test_cardinality_decides_with_the_engine_value(self):
        entries = [
            Event(0, "a", KIND_LOGS, "{}", 0.0, 100, '{user="u1"}'),
            Event(1, "a", KIND_LOGS, "{}", 0.0, 200, '{user="u2"}'),
            Event(2, "a", KIND_LOGS, "{}", 0.0, 300, '{user="u1"}'),
            Event(3, "a", KIND_LOGS, '{z="1"}', 0.0, 2000, '{user="u9"}'),
        ]
        r = rule(name="users", source="logs", agg="cardinality", field="user", by=[],
                 range_ms=1000, threshold=2)
        engine = {("users", "{}", 1000): 3}
        _, transitions = run(entries, r, engine=engine)
        self.assertEqual(states(transitions)[0], ("firing", 1000, 2))


class WatermarkTest(unittest.TestCase):
    def test_first_watermark_evaluates_nothing(self):
        self.assertEqual(list(instants(None, 5000, 1000)), [])

    def test_instants_are_multiples_in_the_half_open_range(self):
        self.assertEqual(list(instants(-1000, 3000, 1000)), [0, 1000, 2000, 3000])
        self.assertEqual(list(instants(-1500, -1000, 1000)), [-1000])
        self.assertEqual(list(instants(1000, 1999, 1000)), [])
        self.assertEqual(list(instants(999, 30000, 10000)), [10000, 20000, 30000])

    def test_ticks_advance_only_defined_watermarks(self):
        entries = [
            Tick(50000),
            Event(0, "a", KIND_METRICS, "m{}", 1.0, 1000),
            Tick(10000),
            Tick(9000),
            Event(1, "b", KIND_METRICS, "m{}", 1.0, 500),
            Event(2, "a", KIND_METRICS, "m{}", 1.0, 900),
        ]
        self.assertEqual(
            list(watermark_advances(entries, 2000, 5000)),
            [("a", None, -1000), ("a", -1000, 3000), ("b", None, -1500)],
        )


class ParseRulesTest(unittest.TestCase):
    def test_defaults(self):
        (r,) = parse_rules({"rules": [rule(range_ms=5000, threshold=1)]}, 1000)
        self.assertEqual(
            (r.kind, r.match, r.interval_ms, r.by, r.resolve_threshold, r.for_ms,
             r.keep_firing_for_ms),
            (KIND_METRICS, {}, 1000, None, None, 0, 0),
        )

    def test_invalid(self):
        bad = {
            "duplicate": [rule(range_ms=1000, threshold=1), rule(range_ms=1000, threshold=1)],
            "range not a multiple": [rule(range_ms=1500, threshold=1)],
            "interval zero": [rule(range_ms=1000, interval_ms=0, threshold=1)],
            "quantile on logs": [rule(source="logs", agg="quantile", quantile=0.5,
                                      range_ms=1000, threshold=1)],
            "cardinality without field": [rule(source="logs", agg="cardinality",
                                               range_ms=1000, threshold=1)],
            "bad op": [rule(op="==", range_ms=1000, threshold=1)],
            "bool threshold": [rule(range_ms=1000, threshold=True)],
            "missing threshold": [rule(range_ms=1000)],
        }
        for name, rules in bad.items():
            with self.subTest(name):
                with self.assertRaises(ValueError):
                    parse_rules({"rules": rules}, 1000)
        with self.assertRaises(ValueError):
            parse_rules([], 1000)


class RulesCliTest(unittest.TestCase):
    def test_writes_evals_and_transitions(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = os.path.join(tmp, "data")
            out = os.path.join(tmp, "out")
            samples = [metric_event("m{}", v, 1000 * i) for i, v in enumerate([0, 20, 0])]
            write_segment(data, 0, [batch("a", KIND_METRICS, samples)])
            rules = os.path.join(tmp, "rules.json")
            with open(rules, "w") as f:
                json.dump({"rules": [rule(range_ms=1000, threshold=10)]}, f)
            proc = subprocess.run(
                [sys.executable, CLI, "rules", "--data", data, "--rules", rules,
                 "--lateness-ms", "0", "--idle-grace-ms", "0", "--out", out],
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            with open(os.path.join(out, "transitions.jsonl")) as f:
                rows = [json.loads(line) for line in f]
            self.assertEqual(
                rows,
                [
                    {"rule": "r", "group": "{}", "t": 1000, "state": "firing", "value": 20.0},
                    {"rule": "r", "group": "{}", "t": 2000, "state": "resolved", "value": 0.0},
                ],
            )
            with open(os.path.join(out, "evals.jsonl")) as f:
                self.assertEqual(len(f.readlines()), 2)

    def test_invalid_rules_exit_1(self):
        with tempfile.TemporaryDirectory() as tmp:
            rules = os.path.join(tmp, "rules.json")
            with open(rules, "w") as f:
                f.write("{")
            proc = subprocess.run(
                [sys.executable, CLI, "rules", "--data", tmp, "--rules", rules, "--out", tmp],
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 1)


if __name__ == "__main__":
    unittest.main()
