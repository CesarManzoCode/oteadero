import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

RUNNER = Path(__file__).resolve().parent / "run_criteria.py"


def write_script(directory, name, code):
    (directory / name).write_text(f"import sys\nprint('output of {name}')\nsys.exit({code})\n")


def run_on(directory):
    return subprocess.run(
        [sys.executable, str(RUNNER), "--dir", str(directory)],
        capture_output=True,
        text=True,
    )


class RunCriteriaTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        write_script(self.dir, "crit_a_pass.py", 0)
        write_script(self.dir, "crit_b_skip.py", 77)

    def test_a_failure_fails_the_run(self):
        write_script(self.dir, "crit_c_fail.py", 1)
        result = run_on(self.dir)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("pass=1 fail=1 skipped=1", result.stdout)

    def test_skipped_is_reported_and_does_not_fail(self):
        result = run_on(self.dir)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("pass=1 fail=0 skipped=1", result.stdout)
        self.assertIn("crit_b_skip.py: skipped", result.stdout)

    def test_runs_in_name_order_and_shows_output(self):
        write_script(self.dir, "other.py", 1)
        result = run_on(self.dir)
        self.assertLess(
            result.stdout.index("output of crit_a_pass.py"),
            result.stdout.index("output of crit_b_skip.py"),
        )
        self.assertNotIn("other.py", result.stdout)

    def test_no_scripts_fails(self):
        with tempfile.TemporaryDirectory() as empty:
            self.assertEqual(run_on(empty).returncode, 1)


if __name__ == "__main__":
    unittest.main()
