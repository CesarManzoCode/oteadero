"""Criterion: promtool check rules accepts the translation of the reference rules (D12, D22)."""

import subprocess
import sys

import lib
import rules2prom

RULE_FILES = ("reference.json", "kff.json")


def main():
    (promtool,) = lib.ref_binaries("promtool")
    work = lib.work_dir("crit_promtool_check")
    yamls = []
    for name in RULE_FILES:
        out = work / name.replace(".json", ".yaml")
        rules2prom.translate_file(lib.REPO_ROOT / "rules" / name, out)
        yamls.append(out)
    result = subprocess.run([promtool, "check", "rules", *yamls], capture_output=True, text=True)
    print(result.stdout + result.stderr, end="")
    print(f"promtool check rules: {'ok' if result.returncode == 0 else 'fail'}")
    return 0 if result.returncode == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
