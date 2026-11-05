import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "crit_oracle_imports.py"


def run_on(files):
    """Write files into a temporary oracle and run the criterion on it."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for name, source in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source)
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--root", tmp],
            capture_output=True,
            text=True,
        )


class CritOracleImportsTest(unittest.TestCase):
    def assert_fails(self, files, expected):
        result = run_on(files)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(expected, result.stdout)

    def test_clean_oracle_passes(self):
        result = run_on(
            {
                "__init__.py": "",
                "helper.py": "VALUE = 'log/ and cmd line'\n",
                "main.py": "import os.path\nimport zlib\nfrom helper import VALUE\nfrom . import helper\n",
            }
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(result.stdout.strip(), "oracle imports: ok")

    def test_clean_oracle_nested_in_a_tree_passes(self):
        result = run_on({"oracle/main.py": "import json\n", "other/bad.py": "import numpy\n"})
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_forbidden_import_fails(self):
        self.assert_fails({"main.py": "import numpy\n"}, "main.py:1: forbidden import 'numpy'")

    def test_forbidden_from_import_fails(self):
        self.assert_fails({"main.py": "\nfrom requests.api import get\n"}, "main.py:2: forbidden import")

    def test_forbidden_import_in_nested_tree_fails(self):
        self.assert_fails({"oracle/main.py": "import numpy\n"}, "forbidden import 'numpy'")

    def test_relative_import_out_of_the_oracle_fails(self):
        self.assert_fails({"main.py": "from .. import engine\n"}, "relative import leaves the oracle")

    def test_mention_of_engine_paths_fails(self):
        for mention in ("internal/", "cmd/", "harness/", "oteadero"):
            with self.subTest(mention=mention):
                source = f"PATH = 'x/{mention}y'\n"
                self.assert_fails({"main.py": source}, f"string mentions {mention!r}")

    def test_mention_in_docstring_and_fstring_fails(self):
        self.assert_fails({"main.py": '"""Reads internal/wal."""\n'}, "string mentions 'internal/'")
        self.assert_fails({"main.py": "x = 1\ny = f'{x}/cmd/z'\n"}, "main.py:2: string mentions 'cmd/'")

    def test_every_finding_is_printed(self):
        result = run_on({"a.py": "import numpy\n", "b.py": "PATH = 'internal/'\n"})
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(result.stdout.strip().splitlines()), 2)

    def test_unparsable_file_fails(self):
        self.assert_fails({"main.py": "def broken(:\n"}, "cannot parse")

    def test_tree_without_python_files_fails(self):
        self.assert_fails({"notes.txt": "nothing\n"}, "no Python files")

    def test_repository_oracle_passes(self):
        result = subprocess.run([sys.executable, str(SCRIPT)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout)


if __name__ == "__main__":
    unittest.main()
