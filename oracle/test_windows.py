import json
import os
import subprocess
import sys
import tempfile
import unittest

from logread import KIND_LOGS, KIND_METRICS, Event, Tick
from test_logread import batch, metric_event, tick, write_segment
from windows import bucket_end, compute_windows

CLI = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cli.py")


def metric(offset, ts_ms, value=1.0, tenant="a", series="m{}"):
    return Event(offset, tenant, KIND_METRICS, series, value, ts_ms)


def log(offset, ts_ms, tenant="a", series="{}"):
    return Event(offset, tenant, KIND_LOGS, series, 0.0, ts_ms)


def run(entries, bucket_ms=1000, lateness_ms=2000, idle_grace_ms=5000):
    return compute_windows(entries, bucket_ms, lateness_ms, idle_grace_ms)


def closed(result):
    return [(b["tenant"], b["series"], b["end"], b["count"]) for b in result.buckets]


class BucketEndTest(unittest.TestCase):
    def test_buckets_are_open_on_the_left(self):
        vectors = {0: 0, 1: 1000, 999: 1000, 1000: 1000, 1001: 2000, 2000: 2000, -1: 0, -1000: -1000}
        for ts_ms, end in vectors.items():
            with self.subTest(ts_ms):
                self.assertEqual(bucket_end(ts_ms, 1000), end)


class ComputeWindowsTest(unittest.TestCase):
    def test_timestamp_on_the_boundary_belongs_to_the_bucket_ending_there(self):
        result = run([metric(0, 1000), metric(1, 1001), metric(2, 4001)])
        self.assertEqual(closed(result), [("a", "m{}", 1000, 1), ("a", "m{}", 2000, 1)])
        self.assertEqual(result.late, [])

    def test_late_boundary(self):
        # After the first event W = 5000 - 2000 = 3000.
        result = run(
            [
                metric(0, 5000),
                metric(1, 3000),  # end 3000 <= W: late
                metric(2, 3001),  # end 4000 > W: on time
                metric(3, 2001),  # end 3000 <= W: late
                metric(4, 10000),  # W = 8000
            ]
        )
        self.assertEqual(result.late, [1, 3])
        self.assertEqual(closed(result), [("a", "m{}", 4000, 1), ("a", "m{}", 5000, 1)])
        self.assertEqual(result.accounting, {"a": {"accepted": 5, "late": 2}})

    def test_late_event_uses_the_watermark_of_the_previous_events(self):
        result = run([metric(0, 5000), metric(1, 9000), metric(2, 5000)])
        self.assertEqual(result.late, [2])

    def test_first_event_of_a_tenant_is_never_late(self):
        result = run([metric(0, 100000), metric(1, 1, tenant="b")])
        self.assertEqual(result.late, [])

    def test_late_event_enters_no_bucket(self):
        result = run([metric(0, 5000, value=1.0), metric(1, 1000, value=50.0), metric(2, 9000)])
        self.assertEqual(result.late, [1])
        self.assertEqual(closed(result), [("a", "m{}", 5000, 1)])
        self.assertEqual(result.buckets[0]["sum"], 1.0)

    def test_bucket_closes_exactly_when_the_watermark_reaches_its_end(self):
        below = run([metric(0, 1000), metric(1, 2999)])  # W = 999
        self.assertEqual(closed(below), [])
        reached = run([metric(0, 1000), metric(1, 2999), metric(2, 3000)])  # W = 1000
        self.assertEqual(closed(reached), [("a", "m{}", 1000, 1)])

    def test_out_of_order_event_on_time_is_added(self):
        result = run([metric(0, 2500), metric(1, 1500), metric(2, 5000)])
        self.assertEqual(result.late, [])
        self.assertEqual(closed(result), [("a", "m{}", 2000, 1), ("a", "m{}", 3000, 1)])

    def test_zero_lateness(self):
        result = run([metric(0, 1000), metric(1, 1000)], lateness_ms=0)
        self.assertEqual(closed(result), [("a", "m{}", 1000, 1)])
        self.assertEqual(result.late, [1])

    def test_watermark_is_per_tenant(self):
        result = run([metric(0, 1000), metric(1, 1000, tenant="b"), metric(2, 100000)])
        self.assertEqual(closed(result), [("a", "m{}", 1000, 1)])
        self.assertEqual(result.late, [])
        result = run([metric(0, 1000, tenant="b"), metric(1, 100000), metric(2, 1000, tenant="b")])
        self.assertEqual(result.late, [])

    def test_open_buckets_are_not_emitted(self):
        result = run([metric(0, 1000), log(1, 1000, tenant="b")])
        self.assertEqual(result.buckets, [])
        self.assertEqual(
            result.accounting,
            {"a": {"accepted": 1, "late": 0}, "b": {"accepted": 1, "late": 0}},
        )

    def test_buckets_are_per_series(self):
        result = run([metric(0, 500, series="b{}"), metric(1, 600, series="a{}"), metric(2, 700, series="b{}"), metric(3, 3000)])
        self.assertEqual(closed(result), [("a", "a{}", 1000, 1), ("a", "b{}", 1000, 2)])

    def test_sum_follows_log_order(self):
        # 1e16 + 1.0 rounds back to 1e16; summing the ones first would give 1e16 + 2.
        entries = [metric(0, 900, 1e16), metric(1, 900, 1.0), metric(2, 900, 1.0), metric(3, 3000, 0.0)]
        self.assertEqual(run(entries).buckets[0]["sum"], 1e16)

    def test_metric_aggregates(self):
        entries = [metric(0, 100, 2.5), metric(1, 200, 3.5), metric(2, 10000, 0.0)]
        self.assertEqual(
            run(entries, bucket_ms=2000).buckets,
            [{"tenant": "a", "series": "m{}", "end": 2000, "count": 2, "sum": 6.0, "rate": 3.0}],
        )

    def test_log_aggregates(self):
        entries = [log(0, 100), log(1, 200), log(2, 300), log(3, 10000)]
        self.assertEqual(
            run(entries, bucket_ms=2000).buckets,
            [{"tenant": "a", "series": "{}", "end": 2000, "count": 3, "sum": 0.0, "rate": 1.5}],
        )

    def test_tick_closes_at_its_boundary(self):
        below = run([metric(0, 1000), Tick(7999)])  # maxTs = 2999, W = 999
        self.assertEqual(closed(below), [])
        reached = run([metric(0, 1000), Tick(8000)])  # maxTs = 3000, W = 1000
        self.assertEqual(closed(reached), [("a", "m{}", 1000, 1)])

    def test_tick_only_touches_tenants_with_a_watermark(self):
        result = run([Tick(1000000), metric(0, 1), metric(1, 1, tenant="b"), Tick(20000), metric(2, 1, tenant="c")])
        # a and b advance to W = 13000 and close; c starts afterwards and is on time.
        self.assertEqual(closed(result), [("a", "m{}", 1000, 1), ("b", "m{}", 1000, 1)])
        self.assertEqual(result.late, [])

    def test_tick_moves_the_late_boundary(self):
        result = run([metric(0, 1000), Tick(20000), metric(1, 13000), metric(2, 13001)])
        self.assertEqual(result.late, [1])

    def test_tick_never_moves_the_watermark_back(self):
        result = run([metric(0, 10000), Tick(6000), metric(1, 8000), metric(2, 8001)])
        self.assertEqual(result.late, [1])


class CliTest(unittest.TestCase):
    def test_windows_writes_the_three_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = os.path.join(tmp, "data")
            out = os.path.join(tmp, "nested", "out")
            events = [
                metric_event("m{}", 2.5, 1000),
                metric_event("m{}", 1.0, 3000),
                metric_event("m{}", 9.0, 999),
            ]
            write_segment(data, 0, [batch("a", KIND_METRICS, events), tick(10000)])
            done = subprocess.run(
                [
                    sys.executable,
                    CLI,
                    "windows",
                    "--data",
                    data,
                    "--bucket-ms",
                    "1000",
                    "--lateness-ms",
                    "2000",
                    "--idle-grace-ms",
                    "5000",
                    "--out",
                    out,
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(done.returncode, 0, done.stderr)
            with open(os.path.join(out, "buckets.jsonl")) as f:
                buckets = [json.loads(line) for line in f]
            with open(os.path.join(out, "late.jsonl")) as f:
                late = [json.loads(line) for line in f]
            with open(os.path.join(out, "accounting.json")) as f:
                accounting = json.load(f)
        self.assertEqual(
            buckets,
            [
                {"tenant": "a", "series": "m{}", "end": 1000, "count": 1, "sum": 2.5, "rate": 2.5},
                {"tenant": "a", "series": "m{}", "end": 3000, "count": 1, "sum": 1.0, "rate": 1.0},
            ],
        )
        self.assertEqual(late, [{"o": 2}])
        self.assertEqual(accounting, {"a": {"accepted": 3, "late": 1}})

    def test_missing_data_directory_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            done = subprocess.run(
                [sys.executable, CLI, "windows", "--data", os.path.join(tmp, "no"), "--out", os.path.join(tmp, "out")],
                capture_output=True,
                text=True,
            )
        self.assertEqual(done.returncode, 1)
        self.assertIn("oracle:", done.stderr)


if __name__ == "__main__":
    unittest.main()
