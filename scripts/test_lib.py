import os
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import lib


def bucket(end, count=1, total=2.0, closed_at=0, series="s{}"):
    return {
        "tenant": "t1",
        "series": series,
        "end": end,
        "count": count,
        "sum": total,
        "rate": total,
        "closed_at": closed_at,
    }


class UnionExportsTest(unittest.TestCase):
    def test_earlier_run_counts_only_below_the_truncate_offset(self):
        earlier = [bucket(1000, closed_at=9), bucket(2000, closed_at=10)]
        merged, conflicts = lib.union_exports([earlier, []], 10)
        self.assertEqual(sorted(key[2] for key in merged), [1000])
        self.assertEqual(conflicts, [])

    def test_last_run_counts_whatever_its_closed_at(self):
        merged, conflicts = lib.union_exports([[], [bucket(1000, closed_at=99)]], 10)
        self.assertEqual(len(merged), 1)
        self.assertEqual(conflicts, [])

    def test_discarded_line_is_replaced_without_conflict(self):
        earlier = [bucket(1000, count=1, closed_at=10)]
        new = [bucket(1000, count=2, closed_at=10)]
        merged, conflicts = lib.union_exports([earlier, new], 10)
        self.assertEqual(merged[("t1", "s{}", 1000)]["count"], 2)
        self.assertEqual(conflicts, [])

    def test_same_value_twice_is_no_conflict(self):
        row = bucket(1000, closed_at=5)
        merged, conflicts = lib.union_exports([[row], [dict(row)]], 10)
        self.assertEqual(len(merged), 1)
        self.assertEqual(conflicts, [])

    def test_two_values_for_a_key_are_a_conflict(self):
        earlier = [bucket(1000, total=2.0, closed_at=5)]
        new = [bucket(1000, total=3.0, closed_at=5)]
        _, conflicts = lib.union_exports([earlier, new], 10)
        self.assertEqual(len(conflicts), 1)
        self.assertIn("conflict", conflicts[0])


class CompareBucketsTest(unittest.TestCase):
    def compare(self, engine_rows, oracle_rows):
        merged, _ = lib.union_exports([engine_rows], 0)
        return lib.compare_buckets(merged, oracle_rows)

    def test_equal(self):
        rows = [bucket(1000), bucket(2000)]
        self.assertEqual(self.compare(rows, [dict(r) for r in rows]), (2, []))

    def test_closed_at_is_not_compared_and_the_oracle_has_none(self):
        oracle = bucket(1000)
        del oracle["closed_at"]
        self.assertEqual(self.compare([bucket(1000, closed_at=7)], [oracle]), (1, []))

    def test_integer_and_float_forms_of_a_value_are_equal(self):
        self.assertEqual(self.compare([bucket(1000, total=2)], [bucket(1000, total=2.0)]), (1, []))

    def test_sum_difference(self):
        compared, diffs = self.compare([bucket(1000, total=2.0)], [bucket(1000, total=2.5)])
        self.assertEqual(compared, 1)
        self.assertEqual(len(diffs), 1)
        self.assertIn("different", diffs[0])

    def test_missing_bucket(self):
        compared, diffs = self.compare([], [bucket(1000)])
        self.assertEqual(compared, 1)
        self.assertEqual(len(diffs), 1)
        self.assertIn("missing", diffs[0])

    def test_extra_bucket(self):
        compared, diffs = self.compare([bucket(1000)], [])
        self.assertEqual(compared, 1)
        self.assertEqual(len(diffs), 1)
        self.assertIn("extra", diffs[0])

    def test_key_includes_series(self):
        compared, diffs = self.compare([bucket(1000, series="a{}")], [bucket(1000, series="b{}")])
        self.assertEqual(compared, 2)
        self.assertEqual(len(diffs), 2)


class RefBinaryTest(unittest.TestCase):
    def executable(self, path):
        path.write_text("#!/bin/sh\n")
        path.chmod(0o755)
        return path

    def test_refbin_comes_before_path(self):
        with tempfile.TemporaryDirectory() as refbin, tempfile.TemporaryDirectory() as bindir:
            local = self.executable(Path(refbin) / "promtool")
            self.executable(Path(bindir) / "promtool")
            with mock.patch.dict(os.environ, {"PATH": bindir}):
                self.assertEqual(lib.ref_binary("promtool", refbin), local)

    def test_path_when_refbin_lacks_it(self):
        with tempfile.TemporaryDirectory() as refbin, tempfile.TemporaryDirectory() as bindir:
            found = self.executable(Path(bindir) / "promtool")
            (Path(refbin) / "promtool").write_text("not executable")
            with mock.patch.dict(os.environ, {"PATH": bindir}):
                self.assertEqual(lib.ref_binary("promtool", refbin), found)

    def test_missing_everywhere(self):
        with tempfile.TemporaryDirectory() as refbin, tempfile.TemporaryDirectory() as bindir:
            with mock.patch.dict(os.environ, {"PATH": bindir}):
                self.assertIsNone(lib.ref_binary("promtool", refbin))


class BinaryTest(unittest.TestCase):
    def test_oteadero_bin_is_never_built_from_this_tree(self):
        with tempfile.TemporaryDirectory() as bindir:
            with mock.patch.dict(os.environ, {"OTEADERO_BIN": bindir}):
                with mock.patch("subprocess.run") as run:
                    with self.assertRaises(SystemExit) as raised:
                        lib.binary("oteadero")
            run.assert_not_called()
            self.assertEqual(raised.exception.code, 1)

    def test_oteadero_bin_binary_is_used(self):
        with tempfile.TemporaryDirectory() as bindir:
            path = Path(bindir) / "oteadero"
            path.write_text("")
            with mock.patch.dict(os.environ, {"OTEADERO_BIN": bindir}):
                self.assertEqual(lib.binary("oteadero"), path)


class ExportFilesTest(unittest.TestCase):
    def test_read_jsonl_ignores_a_torn_last_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "buckets.jsonl"
            path.write_text('{"o":1}\n{"o":2}\n{"o":')
            self.assertEqual(lib.read_jsonl(path), [{"o": 1}, {"o": 2}])

    def test_export_runs_are_in_start_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            for name in ("serve-1000-aa", "serve-999-ff", "other"):
                (Path(tmp) / name).mkdir()
            names = [run.name for run in lib.export_runs(tmp)]
            self.assertEqual(names, ["serve-999-ff", "serve-1000-aa"])


if __name__ == "__main__":
    unittest.main()
