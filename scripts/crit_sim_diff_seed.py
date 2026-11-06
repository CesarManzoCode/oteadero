"""Criterion: runs of otsim with different seeds leave different hashes (D16)."""

import sys

import lib

SEEDS = (42, 43)
WORK = "sim_diff_seed"


def main():
    work = lib.work_dir(WORK)
    hashes = []
    for seed in SEEDS:
        run = lib.run_otsim(seed, work / str(seed))
        if run.hash is None:
            print(run.output, end="")
            print(f"otsim --seed {seed} no imprimió hash (salida {run.code})")
            return 1
        print(f"hash {seed}: {run.hash}")
        hashes.append(run.hash)
    return 0 if len(set(hashes)) == len(hashes) else 1


if __name__ == "__main__":
    sys.exit(main())
