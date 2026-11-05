import os
import struct
import tempfile
import unittest
import zlib

from logread import KIND_LOGS, KIND_METRICS, Event, Tick, _Invalid, read_log, read_uvarint

LOG_ID = bytes(range(1, 9))

# Written by hand from the record layout; the CRCs are the zlib ones of each body.
LITERAL_SEGMENT = bytes.fromhex(
    """
    4f544c4f47303031 0102030405060708
    21000000 9929e83a
      01 0161 01 0000000000000000 01
      036d7b7d 000000000000f83f e803000000000000
    09000000 e212f470
      02 401f000000000000
    1d000000 e24aaeec
      01 0162 02 0000000000000000 01
      d007000000000000 027b7d 027b7d 0168
    """
)


def uvarint(n):
    out = bytearray()
    while n >= 0x80:
        out.append(n & 0x7F | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


def text(s):
    raw = s.encode()
    return uvarint(len(raw)) + raw


def record(record_type, payload):
    body = bytes([record_type]) + payload
    return struct.pack("<II", len(body), zlib.crc32(body)) + body


def metric_event(series, value, ts_ms):
    return text(series) + struct.pack("<dq", value, ts_ms)


def log_event(labels, ts_ms, fields="{}", msg="m"):
    return struct.pack("<q", ts_ms) + text(labels) + text(fields) + text(msg)


def batch(tenant, kind, events, count=None):
    count = len(events) if count is None else count
    payload = text(tenant) + bytes([kind]) + struct.pack("<q", 0) + uvarint(count)
    return record(1, payload + b"".join(events))


def tick(ts_ms):
    return record(2, struct.pack("<q", ts_ms))


def lease(epoch, holder, expires_ms):
    return record(3, struct.pack("<Q", epoch) + text(holder) + struct.pack("<q", expires_ms))


def write_segment(data_dir, first_offset, records, log_id=LOG_ID, header=None):
    log_dir = os.path.join(data_dir, "log")
    os.makedirs(log_dir, exist_ok=True)
    head = b"OTLOG001" + log_id if header is None else header
    with open(os.path.join(log_dir, f"{first_offset:020d}.seg"), "wb") as f:
        f.write(head + b"".join(records))


class ReadLogTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.data = tmp.name

    def read(self):
        return list(read_log(self.data))

    def good_batch(self, ts_ms=1000):
        return batch("a", KIND_METRICS, [metric_event("m{}", 1.0, ts_ms)])

    def good_event(self, offset, ts_ms=1000):
        return Event(offset, "a", KIND_METRICS, "m{}", 1.0, ts_ms)

    def test_literal_segment(self):
        write_segment(self.data, 0, [LITERAL_SEGMENT], header=b"")
        self.assertEqual(
            self.read(),
            [
                Event(0, "a", KIND_METRICS, "m{}", 1.5, 1000),
                Tick(8000),
                Event(1, "b", KIND_LOGS, "{}", 0.0, 2000),
            ],
        )

    def test_tick_and_lease_consume_no_offset(self):
        write_segment(
            self.data,
            0,
            [
                batch(
                    "a",
                    KIND_METRICS,
                    [metric_event('m{x="1"}', 1.0, 10), metric_event('m{x="2"}', 2.0, 20)],
                ),
                tick(30),
                lease(7, "e1", 40),
                batch("b", KIND_LOGS, [log_event('{s="w"}', 50, '{f="v"}', "hello")]),
            ],
        )
        self.assertEqual(
            self.read(),
            [
                Event(0, "a", KIND_METRICS, 'm{x="1"}', 1.0, 10),
                Event(1, "a", KIND_METRICS, 'm{x="2"}', 2.0, 20),
                Tick(30),
                Event(2, "b", KIND_LOGS, '{s="w"}', 0.0, 50),
            ],
        )

    def test_offsets_continue_across_segments(self):
        write_segment(self.data, 0, [self.good_batch(), self.good_batch()])
        write_segment(self.data, 2, [self.good_batch()])
        self.assertEqual(self.read(), [self.good_event(0), self.good_event(1), self.good_event(2)])

    def test_offsets_start_at_the_oldest_segment(self):
        write_segment(self.data, 100, [self.good_batch()])
        self.assertEqual(self.read(), [self.good_event(100)])

    def test_files_that_are_not_segments_are_ignored(self):
        write_segment(self.data, 0, [self.good_batch()])
        with open(os.path.join(self.data, "log", "notes.tmp"), "wb") as f:
            f.write(b"junk")
        self.assertEqual(self.read(), [self.good_event(0)])

    def test_empty_log(self):
        os.makedirs(os.path.join(self.data, "log"))
        self.assertEqual(self.read(), [])

    def test_missing_log_directory_is_an_error(self):
        with self.assertRaises(FileNotFoundError):
            self.read()

    def test_torn_final_record_ends_the_log(self):
        final = self.good_batch(2000)
        good = [self.good_batch(), tick(1500)]
        expected = [self.good_event(0), Tick(1500)]
        huge_len = struct.pack("<I", 1 << 30) + final[4:]
        flipped = final[:-1] + bytes([final[-1] ^ 0x01])
        cases = {
            "cut payload": final[:-3],
            "cut header": final[:5],
            "bad crc": flipped,
            "len beyond the file": huge_len,
        }
        for name, broken in cases.items():
            with self.subTest(name):
                write_segment(self.data, 0, good + [broken])
                self.assertEqual(self.read(), expected)

    def test_nothing_is_read_after_a_broken_record(self):
        final = self.good_batch()
        flipped = final[:-1] + bytes([final[-1] ^ 0x01])
        write_segment(self.data, 0, [self.good_batch(), flipped, self.good_batch()])
        write_segment(self.data, 2, [self.good_batch()])
        self.assertEqual(self.read(), [self.good_event(0)])

    def test_valid_crc_with_invalid_content_ends_the_log(self):
        event = metric_event("m{}", 1.0, 1000)
        cases = {
            "empty record": struct.pack("<II", 0, 0),
            "unknown type": record(9, b""),
            "unknown kind": batch("a", 3, [event]),
            "short batch": batch("a", KIND_METRICS, [event], count=2),
            "trailing bytes": batch("a", KIND_METRICS, [event, b"x"], count=1),
            "string beyond the payload": record(1, uvarint(50) + b"a"),
            "short tick": record(2, b"\x00"),
        }
        for name, broken in cases.items():
            with self.subTest(name):
                write_segment(self.data, 0, [self.good_batch(), broken, self.good_batch()])
                self.assertEqual(self.read(), [self.good_event(0)])

    def test_bad_segment_header_ends_the_log(self):
        write_segment(self.data, 0, [self.good_batch()])
        write_segment(self.data, 1, [self.good_batch()], header=b"NOTALOG!" + LOG_ID)
        self.assertEqual(self.read(), [self.good_event(0)])

    def test_short_segment_header_ends_the_log(self):
        write_segment(self.data, 0, [self.good_batch()])
        write_segment(self.data, 1, [], header=b"OTLOG001")
        self.assertEqual(self.read(), [self.good_event(0)])

    def test_segment_of_another_log_ends_the_log(self):
        write_segment(self.data, 0, [self.good_batch()])
        write_segment(self.data, 1, [self.good_batch()], log_id=b"\xff" * 8)
        self.assertEqual(self.read(), [self.good_event(0)])

    def test_gap_in_segment_offsets_ends_the_log(self):
        write_segment(self.data, 0, [self.good_batch()])
        write_segment(self.data, 5, [self.good_batch()])
        self.assertEqual(self.read(), [self.good_event(0)])


class ReadUvarintTest(unittest.TestCase):
    def test_vectors(self):
        vectors = {
            "00": 0,
            "7f": 127,
            "8001": 128,
            "ac02": 300,
            "ffffffffffffffffff01": 2**64 - 1,
        }
        for encoded, value in vectors.items():
            with self.subTest(encoded):
                raw = bytes.fromhex(encoded)
                self.assertEqual(read_uvarint(raw, 0), (value, len(raw)))

    def test_reads_from_a_position(self):
        self.assertEqual(read_uvarint(bytes.fromhex("ffac02ff"), 1), (300, 3))

    def test_invalid(self):
        for encoded in ("", "80", "ffffffffffffffffff02", "ffffffffffffffffffff01"):
            with self.subTest(encoded):
                with self.assertRaises((_Invalid, IndexError)):
                    read_uvarint(bytes.fromhex(encoded), 0)


if __name__ == "__main__":
    unittest.main()
