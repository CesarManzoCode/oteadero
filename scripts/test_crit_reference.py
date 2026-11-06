import unittest
from pathlib import Path

import crit_keep_firing_for
import crit_vmalert_replay

T0 = 1767225600000


def alerts(state, values, timestamps, alertname="r", **labels):
    metric = {"__name__": "ALERTS", "alertname": alertname, "alertstate": state,
              "alertgroup": "oteadero_1s", **labels}
    return {"metric": metric, "values": values, "timestamps": timestamps}


def at(*seconds):
    return [T0 + s * 1000 for s in seconds]


class ReconstructTest(unittest.TestCase):
    def reconstruct(self, rows, last_s=10):
        return crit_vmalert_replay.reconstruct(rows, T0, T0 + last_s * 1000, 1000)

    def test_pending_firing_resolved(self):
        rows = [
            alerts("pending", [1, 1], at(2, 3), svc="api"),
            alerts("firing", [1, 1, 1], at(4, 5, 6), svc="api"),
        ]
        transitions, diffs = self.reconstruct(rows)
        self.assertEqual(diffs, [])
        group = '{svc="api"}'
        self.assertEqual(
            transitions,
            {
                ("r", group, T0 + 2000): "pending",
                ("r", group, T0 + 4000): "firing",
                ("r", group, T0 + 7000): "resolved",
            },
        )

    def test_pending_that_returns_to_inactive(self):
        rows = [alerts("pending", [1], at(1), instance="i0", svc="db")]
        transitions, diffs = self.reconstruct(rows)
        self.assertEqual(diffs, [])
        group = '{instance="i0",svc="db"}'
        self.assertEqual(
            transitions,
            {("r", group, T0 + 1000): "pending", ("r", group, T0 + 2000): "inactive"},
        )

    def test_firing_without_for_and_at_the_first_instant(self):
        transitions, _ = self.reconstruct([alerts("firing", [1], at(0))])
        self.assertEqual(
            transitions,
            {("r", "{}", T0): "firing", ("r", "{}", T0 + 1000): "resolved"},
        )

    def test_firing_up_to_the_last_instant_stays_open(self):
        transitions, _ = self.reconstruct([alerts("firing", [1, 1], at(9, 10))])
        self.assertEqual(transitions, {("r", "{}", T0 + 9000): "firing"})

    def test_stale_markers_and_other_series_are_ignored(self):
        rows = [
            alerts("firing", [1, float("nan")], at(3, 4)),
            {"metric": {"__name__": "ALERTS_FOR_STATE", "alertname": "r"},
             "values": [1], "timestamps": at(5)},
        ]
        transitions, diffs = self.reconstruct(rows)
        self.assertEqual(diffs, [])
        self.assertEqual(
            transitions,
            {("r", "{}", T0 + 3000): "firing", ("r", "{}", T0 + 4000): "resolved"},
        )

    def test_rules_and_groups_apart(self):
        rows = [
            alerts("firing", [1], at(1), alertname="a", svc="api"),
            alerts("firing", [1], at(1), alertname="b", svc="api"),
            alerts("firing", [1], at(2), alertname="a", svc="web"),
        ]
        transitions, _ = self.reconstruct(rows)
        self.assertEqual(transitions[("a", '{svc="api"}', T0 + 1000)], "firing")
        self.assertEqual(transitions[("b", '{svc="api"}', T0 + 1000)], "firing")
        self.assertEqual(transitions[("a", '{svc="web"}', T0 + 2000)], "firing")
        self.assertEqual(transitions[("a", '{svc="api"}', T0 + 2000)], "resolved")

    def test_sample_off_the_instants_is_a_difference(self):
        _, diffs = self.reconstruct([alerts("firing", [1], [T0 + 1500])])
        self.assertEqual(len(diffs), 1)
        self.assertIn("off the instants", diffs[0])

    def test_pending_and_firing_at_once_is_a_difference(self):
        rows = [alerts("pending", [1], at(1)), alerts("firing", [1], at(1))]
        _, diffs = self.reconstruct(rows)
        self.assertEqual(len(diffs), 1)

    def test_label_values_are_escaped(self):
        transitions, _ = self.reconstruct([alerts("firing", [1], at(1), svc='a"b')])
        self.assertIn(("r", '{svc="a\\"b"}', T0 + 1000), transitions)

    def test_compare(self):
        engine = {("r", "{}", 1): "firing", ("r", "{}", 2): "resolved", ("r", "{}", 3): "pending"}
        reference = {("r", "{}", 1): "firing", ("r", "{}", 2): "firing", ("r", "{}", 4): "pending"}
        compared, diffs = crit_vmalert_replay.compare(engine, reference)
        self.assertEqual(compared, 4)
        self.assertEqual(len(diffs), 3)
        self.assertEqual(crit_vmalert_replay.compare(engine, dict(engine)), (3, []))

    def test_rfc3339(self):
        self.assertEqual(crit_vmalert_replay.rfc3339(T0), "2026-01-01T00:00:00Z")
        with self.assertRaises(ValueError):
            crit_vmalert_replay.rfc3339(T0 + 1)


class KeepFiringForTest(unittest.TestCase):
    def test_input_series_from_buckets(self):
        buckets = [
            {"series": 'm{instance="i0"}', "end": 0, "count": 1, "sum": 0.0},
            {"series": 'm{instance="i0"}', "end": 2000, "count": 1, "sum": 5.0},
            {"series": 'm{instance="i0"}', "end": 1000, "count": 1, "sum": 1.0},
            {"series": "otgen_flush{}", "end": 900000, "count": 1, "sum": 1.0},
        ]
        series = crit_keep_firing_for.input_series(buckets, {"m"}, 1000)
        self.assertEqual(series, {'m{instance="i0"}': "0 1 5"})
        gap = [buckets[0], buckets[1]]
        self.assertEqual(crit_keep_firing_for.input_series(gap, {"m"}, 1000),
                         {'m{instance="i0"}': "0 _ 5"})
        with self.assertRaises(ValueError):
            crit_keep_firing_for.input_series(
                [{"series": "m{}", "end": 1000, "count": 2, "sum": 2.0}], {"m"}, 1000
            )

    def test_firing_at(self):
        transitions = {
            ("r", '{instance="i0"}', 2000): "pending",
            ("r", '{instance="i0"}', 4000): "firing",
            ("r", '{instance="i0"}', 7000): "resolved",
            ("r", '{instance="i1"}', 5000): "firing",
        }
        out = crit_keep_firing_for.firing_at(transitions, ["r"], list(range(0, 9000, 1000)))
        self.assertEqual(out[3000], {"r": []})
        self.assertEqual(out[4000], {"r": ['{instance="i0"}']})
        self.assertEqual(out[6000], {"r": ['{instance="i0"}', '{instance="i1"}']})
        self.assertEqual(out[8000], {"r": ['{instance="i1"}']})

    def test_parse_labels(self):
        self.assertEqual(crit_keep_firing_for.parse_labels("{}"), {})
        self.assertEqual(
            crit_keep_firing_for.parse_labels('{a="x\\"y",b="1\\\\2"}'),
            {"a": 'x"y', "b": "1\\2"},
        )

    def test_promtool_test_file(self):
        text = crit_keep_firing_for.promtool_test_file(
            Path("/w/kff.yaml"),
            1000,
            {'m{instance="i0"}': "0 1 5"},
            {0: {"r": []}, 2000: {"r": ['{instance="i0"}']}},
        )
        self.assertEqual(
            text,
            "rule_files:\n"
            '  - "/w/kff.yaml"\n'
            "evaluation_interval: 1s\n"
            "tests:\n"
            "  - interval: 1s\n"
            "    input_series:\n"
            '      - series: "m{instance=\\"i0\\"}"\n'
            '        values: "0 1 5"\n'
            "    alert_rule_test:\n"
            "      - eval_time: 0s\n"
            '        alertname: "r"\n'
            "        exp_alerts: []\n"
            "      - eval_time: 2s\n"
            '        alertname: "r"\n'
            "        exp_alerts:\n"
            '          - exp_labels: {"instance": "i0"}\n',
        )


if __name__ == "__main__":
    unittest.main()
