"""Criterion: the 8 mutants of D17 are detected and the unmutated code passes.

The committed tree (git archive HEAD; changes without a commit do not enter) is
copied to .work/mutants/<name>/, base for the unmutated code and M1..M8 with the
patch of harness/mutants/ applied, and built there into <copy>/bin. The
criteria run from this repository with OTEADERO_BIN=<copy>/bin. Every criterion
must pass on base; a mutant is detected when at least one of its criteria exits
with neither 0 nor 77. M7 runs crit_sim_sweep.py with all its seeds and then
crit_split_brain.py over that sweep. The runs of the mutants that other
criteria read back, .work/notify_once/ and .work/sim_sweep/, are removed at
the end.
"""

import os
import shutil
import subprocess
import sys
import tarfile

import crit_notify_once
import crit_sim_sweep
import lib

WORK = "mutants"
MUTANTS_DIR = lib.REPO_ROOT / "harness" / "mutants"
# The criteria that must detect each mutant (D17), in the order they run.
CRITERIA = {
    "M1": ["crit_windows_late.py"],
    "M2": ["crit_notify_once.py"],
    "M3": ["crit_tail_resume.py"],
    "M4": ["crit_transitions.py"],
    "M5": ["crit_windows_late.py"],
    "M6": ["crit_recovery.py"],
    "M7": ["crit_sim_sweep.py", "crit_split_brain.py"],
    "M8": ["crit_accounting.py"],
}
SHOWN_LINES = 10


def affected():
    """Return every criterion of CRITERIA once, in first-use order."""
    return list(dict.fromkeys(name for names in CRITERIA.values() for name in names))


def detected(codes):
    """Return whether the exit codes of a mutant's criteria detect it; 77 is not a detection."""
    return any(code not in (0, lib.SKIPPED) for code in codes)


def verdict(base_codes, results):
    """Return the exit code of the criterion.

    base_codes are the exit codes of the criteria on the unmutated code and
    results maps each mutant to whether it was detected.
    """
    if any(code == lib.SKIPPED for code in base_codes):
        return lib.SKIPPED
    if any(code != 0 for code in base_codes):
        return 1
    return 0 if all(results.values()) else 1


def prepare(name, patch):
    """Copy HEAD to .work/mutants/<name>/, apply patch if given and build; return its bin.

    Returns None with what went wrong printed if a step fails.
    """
    tree = lib.REPO_ROOT / ".work" / WORK / name
    shutil.rmtree(tree, ignore_errors=True)
    tree.mkdir(parents=True)
    archive = tree.parent / f"{name}.tar"
    subprocess.run(
        ["git", "archive", "--format=tar", "-o", archive, "HEAD"], cwd=lib.REPO_ROOT, check=True
    )
    with tarfile.open(archive) as tar:
        tar.extractall(tree, filter="data")
    archive.unlink()
    steps = []
    if patch is not None:
        # The copy lies inside this repository; without the ceiling git apply
        # would take its paths as relative to the repository and skip them.
        env = dict(os.environ, GIT_CEILING_DIRECTORIES=str(tree.parent))
        steps.append((["git", "apply", patch], env))
    build = ["go", "build", "-trimpath", "-o", f"{tree / 'bin'}/", "./cmd/...", "./harness/cmd/..."]
    steps.append((build, None))
    for command, env in steps:
        proc = subprocess.run(command, cwd=tree, env=env, capture_output=True, text=True)
        if proc.returncode != 0:
            print(f"{name}: {' '.join(map(str, command[:2]))} salió con {proc.returncode}")
            for line in (proc.stdout + proc.stderr).splitlines()[:SHOWN_LINES]:
                print(f"  {line}")
            return None
    return tree / "bin"


def run_criterion(script, bindir):
    """Run a criterion of this repository against the binaries of bindir; return its exit code."""
    env = dict(os.environ, OTEADERO_BIN=str(bindir))
    proc = subprocess.run(
        [sys.executable, lib.REPO_ROOT / "scripts" / script],
        cwd=lib.REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stdout + proc.stderr


def run_all(label, scripts, bindir):
    """Run scripts in order, print one line per criterion and return the exit codes."""
    codes = []
    for script in scripts:
        code, output = run_criterion(script, bindir)
        print(f"{label} {script}: salida {code}", flush=True)
        if label == "base" and code != 0:
            for line in output.splitlines()[-SHOWN_LINES:]:
                print(f"  {line}")
        codes.append(code)
    return codes


def forget_mutant_runs():
    """Remove the runs other criteria read back, so none of them reads a mutant's.

    crit_alert_latency.py reuses .work/notify_once/ and crit_split_brain.py
    reuses .work/sim_sweep/; both run their source again when it is missing.
    """
    for work in (crit_notify_once.WORK, crit_sim_sweep.WORK):
        shutil.rmtree(lib.REPO_ROOT / ".work" / work, ignore_errors=True)


def main():
    for tool in ("git", "go"):
        if shutil.which(tool) is None:
            print(f"falta {tool}")
            return lib.SKIPPED
    lib.work_dir(WORK)

    base = prepare("base", None)
    if base is None:
        return 1
    base_codes = run_all("base", affected(), base)
    if verdict(base_codes, {}) != 0:
        print("el código sin mutar no pasa sus criterios")
        return verdict(base_codes, {})

    print(f"M7: crit_sim_sweep.py con todas sus semillas ({len(crit_sim_sweep.SEEDS)})")
    results = {}
    for name, scripts in CRITERIA.items():
        bindir = prepare(name, MUTANTS_DIR / f"{name}.patch")
        codes = [] if bindir is None else run_all(name, scripts, bindir)
        results[name] = detected(codes)
    forget_mutant_runs()
    for name, found in results.items():
        print(f"{name}: {'detected' if found else 'survived'}")
    return verdict(base_codes, results)


if __name__ == "__main__":
    sys.exit(main())
