"""Criterion: never two evaluators accepted in the same epoch, nor for the same incident.

Reads receiver.jsonl of every seed of the sweep in .work/sim_sweep/<seed>/ and
runs otsim for the seeds that lack it. The check is the script's own, over the
receiver log alone, not the verdict of otsim. Epochs belong to one run, so they
are counted by seed.
"""

import sys

import crit_sim_sweep
import lib

MAX_SHOWN = 20


def violations(requests):
    """Return the epochs with accepted requests and one line per violation.

    A violation is an epoch with accepted requests from two evaluators, or an
    (epoch, incident) with accepted requests from two evaluators. The incident
    is read from the body.
    """
    by_epoch = {}
    by_incident = {}
    for request in requests:
        if request["status"] != "accepted":
            continue
        epoch = request["epoch"]
        evaluator = request["evaluator"]
        by_epoch.setdefault(epoch, set()).add(evaluator)
        body = request["body"]
        incident = body.get("incident") if isinstance(body, dict) else None
        by_incident.setdefault((epoch, incident), set()).add(evaluator)
    lines = [
        f"época {epoch}: aceptados de {', '.join(sorted(who))}"
        for epoch, who in sorted(by_epoch.items())
        if len(who) > 1
    ]
    lines += [
        f"época {epoch} incidente {incident}: aceptados de {', '.join(sorted(who))}"
        for (epoch, incident), who in sorted(
            by_incident.items(), key=lambda item: (item[0][0], str(item[0][1]))
        )
        if len(who) > 1
    ]
    return len(by_epoch), lines


def main():
    epochs = 0
    found = []
    for seed in crit_sim_sweep.SEEDS:
        log = crit_sim_sweep.seed_dir(seed) / "receiver.jsonl"
        if not log.exists():
            run = lib.run_otsim(seed, log.parent)
            if not log.exists():
                print(run.output, end="")
                print(f"otsim --seed {seed} no dejó receiver.jsonl (salida {run.code})")
                return 1
        n, lines = violations(lib.read_jsonl(log))
        epochs += n
        found += [f"semilla {seed}: {line}" for line in lines]
    for line in found[:MAX_SHOWN]:
        print(line)
    print(f"épocas: {epochs}")
    print(f"violaciones: {len(found)}")
    return 0 if epochs > 0 and not found else 1


if __name__ == "__main__":
    sys.exit(main())
