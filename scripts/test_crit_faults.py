import unittest

import crit_notify_once
import lib


def expected(incident, kind, t, rule="r", group='{svc="api"}'):
    return {
        "incident": incident,
        "kind": kind,
        "rule": rule,
        "group": group,
        "t": t,
        "value": 1.0,
        "key": f"{incident}/{kind}",
    }


def request(key, kind, t, status="accepted", recv_ms=None, rule="r", group='{svc="api"}'):
    incident = key.split("/")[0]
    return {
        "recv_ms": t + 1000 if recv_ms is None else recv_ms,
        "key": key,
        "epoch": 1,
        "evaluator": "e1",
        "status": status,
        "body": {
            "incident": incident,
            "kind": kind,
            "rule": rule,
            "group": group,
            "t": t,
            "value": 1.0,
            "epoch": 1,
            "evaluator": "e1",
        },
    }


class NotifyOutcomeTest(unittest.TestCase):
    def setUp(self):
        # Incident a fires at 5000 and resolves at 9000; b fires at 7000 on
        # another group and does not resolve.
        self.expected = [
            expected("a", "firing", 5000),
            expected("b", "firing", 7000, group='{svc="db"}'),
            expected("a", "resolved", 9000),
        ]

    def test_all_once_passes(self):
        requests = [
            request("a/firing", "firing", 5000),
            request("b/firing", "firing", 7000, group='{svc="db"}'),
            request("a/resolved", "resolved", 9000),
        ]
        outcome = lib.notify_outcome(self.expected, requests)
        self.assertEqual(outcome.incidents, 2)
        self.assertEqual((outcome.lost, outcome.duplicates, outcome.unknown), ([], [], []))

    def test_lost_effective_and_non_effective_duplicates(self):
        requests = [
            # a/firing is accepted under two keys of the same incident: an
            # effective duplicate.
            request("a/firing", "firing", 5000),
            request("x/firing", "firing", 5000),
            # b/firing is accepted once and then answered as a duplicate,
            # which is not effective.
            request("b/firing", "firing", 7000, group='{svc="db"}'),
            request("b/firing", "firing", 7000, status="duplicate", group='{svc="db"}'),
            # a/resolved only arrives stale: it is lost.
            request("a/resolved", "resolved", 9000, status="stale"),
        ]
        outcome = lib.notify_outcome(self.expected, requests)
        self.assertEqual(outcome.lost, ["a/resolved"])
        self.assertEqual(outcome.duplicates, ["a/firing"])
        self.assertEqual(outcome.unknown, [])

    def test_accepted_incident_the_oracle_does_not_know_is_reported(self):
        requests = [
            request("a/firing", "firing", 5000),
            request("b/firing", "firing", 7000, group='{svc="db"}'),
            request("a/resolved", "resolved", 9000),
            request("c/firing", "firing", 11000),
            request("d/firing", "firing", 12000, status="duplicate"),
        ]
        outcome = lib.notify_outcome(self.expected, requests)
        self.assertEqual(outcome.unknown, ["c/firing"])
        self.assertEqual((outcome.lost, outcome.duplicates), ([], []))


class LatencyTest(unittest.TestCase):
    def test_latencies_count_only_accepted_requests(self):
        requests = [
            request("a/firing", "firing", 5000, recv_ms=8000),
            request("a/firing", "firing", 5000, status="duplicate", recv_ms=60000),
            request("b/firing", "firing", 7000, recv_ms=7500),
        ]
        self.assertEqual(lib.notification_latencies(requests), [3000, 500])

    def test_nearest_rank_percentiles(self):
        values = list(range(100, 0, -1))  # 1..100, unsorted
        self.assertEqual(lib.percentile(values, 0.5), 50)
        self.assertEqual(lib.percentile(values, 0.99), 99)
        self.assertEqual(lib.percentile(values, 1.0), 100)
        self.assertEqual(lib.percentile([7], 0.99), 7)
        self.assertEqual(lib.percentile([1, 2, 3], 0.5), 2)
        self.assertEqual(lib.percentile([1, 2, 3], 0.0), 1)

    def test_percentile_of_nothing_fails(self):
        with self.assertRaises(ValueError):
            lib.percentile([], 0.99)


class SettleTest(unittest.TestCase):
    def test_settle_covers_the_slowest_resolution(self):
        rules = [
            {"range_ms": 10000, "interval_ms": 5000},
            {"range_ms": 5000, "keep_firing_for_ms": 4000},
        ]
        self.assertEqual(crit_notify_once.settle_ms(rules), 5000 + 2000 + 1000 + 15000)


if __name__ == "__main__":
    unittest.main()
