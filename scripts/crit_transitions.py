"""Criterion: alert transitions equal the oracle's, also after kill -9 of the evaluator.

The first run has no failure. The second checkpoints every 50 000 events and
kills the evaluator at a seeded point after its first checkpoint; the export of
both of its runs is joined with the closed_at rule of section 4.8, with the
offset of the checkpoint it recovers from as the truncate offset.
"""

import random
import sys
import time

import lib

SEED = 14
CHECKPOINT_EVENTS = 50_000
# Well under what is left of the load after the first checkpoint.
MAX_KILL_DELAY_S = 1.0
POLL_S = 0.005


def kill_after_checkpoint(rng):
    def during_load(engine, evaluator, load):
        while not any(o > 0 for o in evaluator.checkpoint_offsets()):
            if load.poll() is not None:
                raise RuntimeError("the load ended before the first checkpoint of the evaluator")
            time.sleep(POLL_S)
        time.sleep(rng.uniform(0, MAX_KILL_DELAY_S))
        if load.poll() is not None:
            raise RuntimeError("the load ended before the kill")
        evaluator.kill()
        # The restart recovers from the newest checkpoint: lines of the killed
        # run count only if they closed before it.
        truncate_offset = evaluator.checkpoint_offsets()[-1]
        print(f"evaluador muerto; checkpoint de recuperación: {truncate_offset}")
        evaluator.start()
        return truncate_offset

    return during_load


def compare(work, engine, evaluator, truncate_offset):
    evals, conflicts = lib.evaluator_evals(engine.export_dir, evaluator.id, truncate_offset)
    for line in conflicts:
        print(line)
    engine_evals = work / "engine-evals.jsonl"
    lib.write_jsonl(engine_evals, evals.values())
    oracle = lib.run_oracle_rules(engine.data_dir, work / "oracle", lib.ALERTS_RULES, engine_evals)
    ok = lib.check_transitions(engine.export_dir, evaluator.id, oracle, truncate_offset)
    return ok and not conflicts


def main():
    rng = random.Random(SEED)
    work = lib.work_dir("crit_transitions")

    plain = work / "plain"
    plain.mkdir()
    print("sin fallas")
    engine, evaluator, _ = lib.run_alerts(plain, SEED)
    plain_ok = compare(plain, engine, evaluator, 0)

    recovery = work / "recovery"
    recovery.mkdir()
    print("con kill -9 del evaluador")
    engine, evaluator, truncate_offset = lib.run_alerts(
        recovery,
        SEED,
        {"checkpoint_events": CHECKPOINT_EVENTS},
        kill_after_checkpoint(rng),
    )
    recovery_ok = compare(recovery, engine, evaluator, truncate_offset)
    return 0 if plain_ok and recovery_ok else 1


if __name__ == "__main__":
    sys.exit(main())
