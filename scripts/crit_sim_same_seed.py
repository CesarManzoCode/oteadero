"""Criterion: two runs of otsim with the same seed and faults leave the same hash (D16)."""

import sys

import lib

SEED = 42
WORK = "sim_same_seed"


def main():
    work = lib.work_dir(WORK)
    runs = [lib.run_otsim(SEED, work / name) for name in ("a", "b")]
    for run in runs:
        if run.hash is None:
            print(run.output, end="")
            print(f"otsim --seed {SEED} no imprimió hash (salida {run.code})")
            return 1
    a, b = runs
    print(f"hash: {a.hash}")
    if a.hash != b.hash:
        print(f"hash de la segunda corrida: {b.hash}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
