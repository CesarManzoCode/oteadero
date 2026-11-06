import json
import tempfile
import unittest
from pathlib import Path

import crit_sim_sweep
import crit_split_brain
import lib


def request(epoch, evaluator, incident, kind="firing", status="accepted"):
    return {
        "recv_ms": 1,
        "key": f"{incident}/{kind}",
        "epoch": epoch,
        "evaluator": evaluator,
        "status": status,
        "body": {"incident": incident, "kind": kind, "epoch": epoch, "evaluator": evaluator},
    }


def fabricated(rows):
    """Write rows as a receiver.jsonl and read it back as the criterion does."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "receiver.jsonl"
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        return lib.read_jsonl(path)


class SplitBrainTest(unittest.TestCase):
    def test_clean_log_passes(self):
        rows = fabricated(
            [
                request(1, "e1", "a"),
                request(1, "e1", "a", kind="resolved"),
                request(1, "e1", "a", status="duplicate"),
                request(2, "e2", "b"),
                # A stale leader answered 409 does not count.
                request(1, "e1", "c", status="stale"),
            ]
        )
        self.assertEqual(crit_split_brain.violations(rows), (2, []))

    def test_two_evaluators_in_one_epoch_are_detected(self):
        rows = fabricated([request(3, "e1", "a"), request(3, "e2", "b")])
        epochs, lines = crit_split_brain.violations(rows)
        self.assertEqual(epochs, 1)
        self.assertEqual(lines, ["época 3: aceptados de e1, e2"])

    def test_two_evaluators_for_one_incident_are_detected(self):
        rows = fabricated([request(3, "e1", "a"), request(3, "e2", "a", kind="resolved")])
        _, lines = crit_split_brain.violations(rows)
        self.assertEqual(
            lines,
            ["época 3: aceptados de e1, e2", "época 3 incidente a: aceptados de e1, e2"],
        )


class MergeRowsTest(unittest.TestCase):
    def row(self, t, state, closed_at):
        return {"rule": "r", "group": "{}", "t": t, "state": state, "closed_at": closed_at}

    def test_same_row_from_two_evaluators_is_one(self):
        rows, conflicts = crit_sim_sweep.merge_rows(
            [self.row(1, "firing", 5), self.row(1, "firing", 7), self.row(2, "resolved", 9)],
            "state",
        )
        self.assertEqual([r["t"] for r in rows], [1, 2])
        self.assertEqual(conflicts, [])

    def test_two_states_for_a_key_are_a_conflict(self):
        _, conflicts = crit_sim_sweep.merge_rows(
            [self.row(1, "firing", 5), self.row(1, "pending", 7)], "state"
        )
        self.assertEqual(conflicts, ["conflict ('r', '{}', 1): firing and pending"])


if __name__ == "__main__":
    unittest.main()
