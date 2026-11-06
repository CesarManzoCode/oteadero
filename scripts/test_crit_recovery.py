import random
import struct
import tempfile
import unittest
from pathlib import Path

import crit_recovery


def record(payload):
    return struct.pack("<II", len(payload), 0) + payload


class CutTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)

    def test_last_record_span_ignores_a_torn_tail(self):
        path = self.dir / "s.seg"
        first, second = record(b"\x01abc"), record(b"\x01de")
        torn = record(b"\x01fghij")[:-2]
        path.write_bytes(b"\0" * 16 + first + second + torn)
        start = 16 + len(first)
        self.assertEqual(crit_recovery.last_record_span(path), (start, start + len(second)))

    def test_last_record_span_of_an_empty_segment(self):
        path = self.dir / "s.seg"
        path.write_bytes(b"\0" * 16)
        self.assertIsNone(crit_recovery.last_record_span(path))

    def test_pick_cut_stays_inside_the_record_and_above_the_floor(self):
        rng = random.Random(1)
        for _ in range(100):
            self.assertTrue(101 <= crit_recovery.pick_cut((100, 120), 0, rng) <= 119)
            self.assertTrue(110 <= crit_recovery.pick_cut((100, 120), 110, rng) <= 119)

    def test_pick_cut_refuses_a_record_before_the_checkpoint(self):
        self.assertIsNone(crit_recovery.pick_cut((100, 120), 120, random.Random(1)))

    def test_newest_checkpoint_pos(self):
        ckpt = self.dir / "ckpt"
        ckpt.mkdir()
        for offset, byte in ((5, 111), (7, 222)):
            body = b"OTCKPT01" + b"\0" * 8 + struct.pack("<QQqQ", 9, 3, byte, offset)
            (ckpt / f"{offset:020d}.ckpt").write_bytes(body)
        (ckpt / f"{9:020d}.ckpt.tmp").write_bytes(b"partial")
        self.assertEqual(crit_recovery.newest_checkpoint_pos(self.dir), (3, 222))

    def test_no_checkpoint(self):
        self.assertIsNone(crit_recovery.newest_checkpoint_pos(self.dir))


if __name__ == "__main__":
    unittest.main()
