import hashlib
import io
import os
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import fetch_refs


def make_tar(path, files):
    with tarfile.open(path, "w:gz") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return hashlib.sha256(path.read_bytes()).hexdigest()


class FetchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.src = root / "src"
        self.src.mkdir()
        self.refbin = root / "refbin"
        self.tar = self.src / "tool.tar.gz"
        self.sha = make_tar(self.tar, {"dir/tool": b"binary", "other": b"x"})
        self.archives = [("tool.tar.gz", self.tar.as_uri(), self.sha, {"dir/tool": "tool"})]

    def tearDown(self):
        self.tmp.cleanup()

    def test_downloads_verifies_and_extracts_only_the_named_member(self):
        fetch_refs.fetch(self.refbin, self.archives)
        tool = self.refbin / "tool"
        self.assertEqual(tool.read_bytes(), b"binary")
        self.assertTrue(os.access(tool, os.X_OK))
        self.assertEqual(sorted(p.name for p in self.refbin.iterdir()), ["tool", "tool.tar.gz"])

    def test_verified_tar_is_not_downloaded_again(self):
        fetch_refs.fetch(self.refbin, self.archives)
        with mock.patch.object(fetch_refs, "download") as download:
            fetch_refs.fetch(self.refbin, self.archives)
        download.assert_not_called()

    def test_tar_with_another_checksum_is_downloaded_again(self):
        self.refbin.mkdir()
        (self.refbin / "tool.tar.gz").write_bytes(b"truncated")
        fetch_refs.fetch(self.refbin, self.archives)
        self.assertEqual(fetch_refs.sha256(self.refbin / "tool.tar.gz"), self.sha)

    def test_wrong_checksum_keeps_nothing(self):
        archives = [("tool.tar.gz", self.tar.as_uri(), "0" * 64, {"dir/tool": "tool"})]
        with self.assertRaises(RuntimeError):
            fetch_refs.fetch(self.refbin, archives)
        self.assertEqual(list(self.refbin.iterdir()), [])

    def test_archives_match_the_research(self):
        names = {name for _, _, _, members in fetch_refs.ARCHIVES for name in members.values()}
        self.assertEqual(
            names, {"promtool", "victoria-metrics-prod", "vmalert-prod", "vmalert-tool-prod"}
        )
        for _, url, sha, _ in fetch_refs.ARCHIVES:
            self.assertTrue(url.startswith("https://github.com/"))
            self.assertEqual(len(sha), 64)


if __name__ == "__main__":
    unittest.main()
