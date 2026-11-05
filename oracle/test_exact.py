import json
import os
import subprocess
import sys
import tempfile
import unittest

from exact import (
    collect_buckets,
    exact_row,
    lower_quantile,
    window_cardinality,
    window_quantile,
)
from logread import KIND_LOGS, KIND_METRICS, Event
from test_logread import batch, metric_event, write_segment

CLI = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cli.py")


def metric(offset, ts_ms, value, series="m{}"):
    return Event(offset, "a", KIND_METRICS, series, value, ts_ms)


def log(offset, ts_ms, fields, series="{}"):
    return Event(offset, "a", KIND_LOGS, series, 0.0, ts_ms, fields)


class LowerQuantileTest(unittest.TestCase):
    def test_index_is_floor_of_q_times_n_minus_one(self):
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        cases = {0: 10.0, 0.24: 10.0, 0.25: 20.0, 0.5: 30.0, 0.9: 40.0, 0.99: 40.0, 1: 50.0}
        for q, want in cases.items():
            with self.subTest(q):
                self.assertEqual(lower_quantile(values, q), want)

    def test_decimal_q_is_exact(self):
        # 0.29 * 100 is 28.999999999999996 in floating point; the rank is 29.
        values = [float(i) for i in range(101)]
        self.assertEqual(lower_quantile(values, 0.29), 29.0)
        self.assertEqual(lower_quantile(values, "0.999"), 99.0)

    def test_single_value(self):
        for q in (0.5, 0.9, 0.99, 0.999):
            self.assertEqual(lower_quantile([7.0], q), 7.0)


class CollectBucketsTest(unittest.TestCase):
    def setUp(self):
        entries = [
            metric(0, 500, 3.0),
            metric(1, 900, 1.0),
            log(2, 600, '{user="u1"}'),
            log(3, 700, '{user="u2",path="/"}'),
            log(4, 800, '{user="u1",path="/"}'),
            metric(5, 1500, 2.0),
            metric(6, 5000, 9.0),  # W = 3000 closes the buckets ending at 1000 and 2000
            metric(7, 800, 7.0),  # late: its bucket ends at 1000 <= W
        ]
        self.buckets = collect_buckets(entries, 1000, 2000, 0)

    def test_only_closed_buckets_without_late_events(self):
        keys = [(b.series, b.end) for b in self.buckets]
        # Within an end, series close in byte order, and "m" sorts before "{".
        self.assertEqual(keys, [("m{}", 1000), ("{}", 1000), ("m{}", 2000)])
        first = self.buckets[0]
        self.assertEqual(first.values, [3.0, 1.0])
        self.assertEqual((first.count, first.sum), (2, 4.0))

    def test_rows(self):
        self.assertEqual(
            exact_row(self.buckets[0]),
            {
                "tenant": "a",
                "series": "m{}",
                "end": 1000,
                "n": 2,
                "q": {"0.5": 1.0, "0.9": 1.0, "0.99": 1.0, "0.999": 1.0},
            },
        )
        self.assertEqual(
            exact_row(self.buckets[1]),
            {"tenant": "a", "series": "{}", "end": 1000, "n": 3, "card": {"path": 1, "user": 2}},
        )

    def test_windows_merge_buckets(self):
        metrics = [b for b in self.buckets if b.kind == KIND_METRICS]
        self.assertEqual(window_quantile(metrics, 0.5), 2.0)
        self.assertEqual(window_quantile(metrics, 1), 3.0)
        logs = [b for b in self.buckets if b.kind == KIND_LOGS]
        self.assertEqual(window_cardinality(logs, "user"), 2)
        self.assertEqual(window_cardinality(logs, "absent"), 0)


class ExactCliTest(unittest.TestCase):
    def test_writes_exact_buckets_and_sketch_windows(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = os.path.join(tmp, "data")
            out = os.path.join(tmp, "out")
            events = [metric_event("lat{}", float(v), 100 * v) for v in range(1, 10)]
            events.append(metric_event("lat{}", 0.5, 4000))
            write_segment(data, 0, [batch("a", KIND_METRICS, events)])
            rules = os.path.join(tmp, "rules.json")
            with open(rules, "w") as f:
                json.dump(
                    {
                        "rules": [
                            {
                                "name": "p90",
                                "tenant": "a",
                                "source": "metrics",
                                "match": {"__name__": "lat"},
                                "agg": "quantile",
                                "quantile": 0.9,
                                "range_ms": 2000,
                                "op": ">",
                                "threshold": 5,
                            },
                            {
                                "name": "n",
                                "tenant": "a",
                                "source": "metrics",
                                "agg": "count",
                                "range_ms": 1000,
                                "op": ">",
                                "threshold": 0,
                            },
                        ]
                    },
                    f,
                )
            proc = subprocess.run(
                [sys.executable, CLI, "exact", "--data", data, "--idle-grace-ms", "0",
                 "--out", out, "--rules", rules],
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            with open(os.path.join(out, "exact_buckets.jsonl")) as f:
                rows = [json.loads(line) for line in f]
            self.assertEqual([(r["end"], r["n"]) for r in rows], [(1000, 9)])
            self.assertEqual(rows[0]["q"]["0.5"], 5.0)
            with open(os.path.join(out, "exact_windows.jsonl")) as f:
                windows = [json.loads(line) for line in f]
            # W jumps from 900 - 2000 to 4000 - 2000; of the instants -1000 to
            # 2000, only 1000 and 2000 have the bucket ending at 1000 in range.
            self.assertEqual(
                windows,
                [
                    {"rule": "p90", "group": "{}", "t": 1000, "value": 8.0},
                    {"rule": "p90", "group": "{}", "t": 2000, "value": 8.0},
                ],
            )


if __name__ == "__main__":
    unittest.main()
