"""Criterion: the build needs neither root nor network."""

import os
import shutil
import subprocess
import sys

import lib


def main():
    if os.geteuid() == 0:
        print("build: running as root")
        return 1
    for tool in ("make", "go"):
        if shutil.which(tool) is None:
            print(f"build: missing {tool}")
            return lib.SKIPPED
    env = {**os.environ, "GOPROXY": "off", "GOFLAGS": "-mod=readonly"}
    if subprocess.run(["make", "build"], cwd=lib.REPO_ROOT, env=env).returncode != 0:
        print("build: make build failed")
        return 1
    missing = [n for n in ("oteadero", "otgen") if not (lib.REPO_ROOT / "bin" / n).exists()]
    if missing:
        print(f"build: missing {' '.join(missing)}")
        return 1
    print("build: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
