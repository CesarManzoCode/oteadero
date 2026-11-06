import shutil
import subprocess
import unittest

import crit_mutants
import lib


class PatchesTest(unittest.TestCase):
    def test_one_patch_per_mutant_of_d17(self):
        found = sorted(path.stem for path in crit_mutants.MUTANTS_DIR.glob("*.patch"))
        self.assertEqual(found, [f"M{i}" for i in range(1, 9)])
        self.assertEqual(sorted(crit_mutants.CRITERIA), found)

    def test_every_patch_applies_to_the_current_tree(self):
        if shutil.which("git") is None:
            self.skipTest("no git")
        inside = subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            cwd=lib.REPO_ROOT,
            capture_output=True,
            text=True,
        )
        if inside.returncode != 0:
            self.skipTest("not a git work tree")
        for name in sorted(crit_mutants.CRITERIA):
            with self.subTest(mutant=name):
                proc = subprocess.run(
                    ["git", "apply", "--check", crit_mutants.MUTANTS_DIR / f"{name}.patch"],
                    cwd=lib.REPO_ROOT,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_criteria_are_scripts_of_this_repository(self):
        for name in crit_mutants.affected():
            self.assertTrue((lib.REPO_ROOT / "scripts" / name).is_file(), name)


class DecisionTest(unittest.TestCase):
    def test_a_failing_criterion_detects(self):
        self.assertTrue(crit_mutants.detected([1]))
        self.assertTrue(crit_mutants.detected([0, 2]))

    def test_passing_or_skipped_criteria_do_not_detect(self):
        self.assertFalse(crit_mutants.detected([0]))
        self.assertFalse(crit_mutants.detected([lib.SKIPPED]))
        self.assertFalse(crit_mutants.detected([0, lib.SKIPPED]))
        self.assertFalse(crit_mutants.detected([]))

    def test_all_detected_with_a_passing_base_passes(self):
        results = {name: True for name in crit_mutants.CRITERIA}
        self.assertEqual(crit_mutants.verdict([0] * 8, results), 0)

    def test_a_survivor_fails(self):
        results = {name: True for name in crit_mutants.CRITERIA}
        results["M5"] = crit_mutants.detected([0])
        self.assertEqual(crit_mutants.verdict([0] * 8, results), 1)

    def test_a_failing_base_fails_even_if_all_are_detected(self):
        results = {name: True for name in crit_mutants.CRITERIA}
        self.assertEqual(crit_mutants.verdict([0, 1], results), 1)

    def test_a_skipped_base_criterion_skips(self):
        self.assertEqual(crit_mutants.verdict([0, lib.SKIPPED], {}), lib.SKIPPED)

    def test_m7_runs_the_sweep_before_split_brain(self):
        scripts = crit_mutants.CRITERIA["M7"]
        self.assertLess(scripts.index("crit_sim_sweep.py"), scripts.index("crit_split_brain.py"))
        affected = crit_mutants.affected()
        self.assertLess(affected.index("crit_sim_sweep.py"), affected.index("crit_split_brain.py"))
        self.assertEqual(len(affected), len(set(affected)))


if __name__ == "__main__":
    unittest.main()
