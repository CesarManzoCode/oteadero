import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest

from incidents import incident_id, notifications

CLI = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cli.py")


def tr(state, t, group="{}", rule="r", value=1.0):
    return {"rule": rule, "group": group, "t": t, "state": state, "value": value}


class IncidentIdTest(unittest.TestCase):
    def test_sha256_prefix_of_rule_group_and_firing_instant(self):
        want = hashlib.sha256(b'high\x00{svc="api"}\x0060000').hexdigest()[:32]
        self.assertEqual(incident_id("high", '{svc="api"}', 60000), want)
        self.assertEqual(len(want), 32)

    def test_each_part_changes_the_id(self):
        base = incident_id("r", "{}", 1000)
        self.assertNotEqual(base, incident_id("r2", "{}", 1000))
        self.assertNotEqual(base, incident_id("r", '{a="1"}', 1000))
        self.assertNotEqual(base, incident_id("r", "{}", 2000))

    def test_negative_instant_is_decimal(self):
        want = hashlib.sha256(b"r\x00{}\x00-1000").hexdigest()[:32]
        self.assertEqual(incident_id("r", "{}", -1000), want)


class NotificationsTest(unittest.TestCase):
    def test_firing_and_resolved_share_the_incident(self):
        rows = notifications(
            [
                tr("pending", 1000),
                tr("firing", 3000, value=12.0),
                tr("resolved", 5000, value=None),
                tr("pending", 7000),
                tr("inactive", 8000),
                tr("firing", 9000, value=20.0),
            ]
        )
        first = incident_id("r", "{}", 3000)
        second = incident_id("r", "{}", 9000)
        self.assertEqual(
            rows,
            [
                {"incident": first, "kind": "firing", "rule": "r", "group": "{}", "t": 3000,
                 "value": 12.0, "key": first + "/firing"},
                {"incident": first, "kind": "resolved", "rule": "r", "group": "{}", "t": 5000,
                 "value": None, "key": first + "/resolved"},
                {"incident": second, "kind": "firing", "rule": "r", "group": "{}", "t": 9000,
                 "value": 20.0, "key": second + "/firing"},
            ],
        )

    def test_groups_and_rules_are_separate_incidents(self):
        rows = notifications(
            [
                tr("firing", 1000, group='{a="1"}'),
                tr("firing", 1000, group='{a="2"}'),
                tr("firing", 1000, rule="other", group='{a="1"}'),
                tr("resolved", 2000, group='{a="2"}'),
            ]
        )
        self.assertEqual(
            [(r["rule"], r["group"], r["kind"]) for r in rows],
            [
                ("r", '{a="1"}', "firing"),
                ("r", '{a="2"}', "firing"),
                ("other", '{a="1"}', "firing"),
                ("r", '{a="2"}', "resolved"),
            ],
        )
        self.assertEqual(rows[1]["incident"], rows[3]["incident"])
        self.assertEqual(len({r["key"] for r in rows}), 4)

    def test_resolved_without_firing_is_not_an_incident(self):
        self.assertEqual(notifications([tr("resolved", 1000)]), [])


class IncidentsCliTest(unittest.TestCase):
    def test_writes_incidents(self):
        with tempfile.TemporaryDirectory() as tmp:
            transitions = os.path.join(tmp, "transitions.jsonl")
            with open(transitions, "w") as f:
                for row in (tr("firing", 1000), tr("resolved", 2000)):
                    f.write(json.dumps(row) + "\n")
            proc = subprocess.run(
                [sys.executable, CLI, "incidents", "--transitions", transitions, "--out", tmp],
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            with open(os.path.join(tmp, "incidents.jsonl")) as f:
                rows = [json.loads(line) for line in f]
            incident = incident_id("r", "{}", 1000)
            self.assertEqual(
                [r["key"] for r in rows], [incident + "/firing", incident + "/resolved"]
            )


if __name__ == "__main__":
    unittest.main()
