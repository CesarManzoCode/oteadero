"""Criterion: every incident notified once in effect, under kill -9 of the evaluators.

serve, otrecv and evaluators e1, e2 and e3 run over 90 s of the alerts load in
real time. At seeded instants the script kills with SIGKILL the leader, or
another evaluator every other time, and starts it again 1 to 3 s later. Once the
load ends it waits for the watermark to close everything and for the receiver
to get no new request for 5 s, stops all and compares the receiver log with the
notifications the oracle derives from the final log. The run stays in
.work/notify_once/ for crit_alert_latency.py.
"""

import contextlib
import json
import math
import random
import sys
import time

import lib

SEED = 25
EVALUATORS = ("e1", "e2", "e3")
# Ticks close the buckets once the load ends (section 4.4).
IDLE_GRACE_MS = 5000
LATENESS_MS = 2000
BUCKET_MS = 1000
MIN_KILL_GAP_S = 4.0
MAX_KILL_GAP_S = 10.0
MIN_RESTART_S = 1.0
MAX_RESTART_S = 3.0
QUIET_S = 5.0
DRAIN_TIMEOUT_S = 180.0
POLL_S = 0.05
MAX_SHOWN = 20

WORK = "notify_once"
RESULT = "result.json"


def settle_ms(rules):
    """Return how long after the last event every incident has resolved.

    Ticks start G after it and close buckets L behind them; a group turns absent
    R after its last bucket and resolves keep_firing_for later, at the next
    evaluation instant.
    """
    return IDLE_GRACE_MS + LATENESS_MS + BUCKET_MS + max(
        r["range_ms"] + r.get("keep_firing_for_ms", 0) + r.get("interval_ms", BUCKET_MS)
        for r in rules
    )


def kill_during_load(rng, evaluators, load, start):
    """Kill and restart evaluators until the load ends; return (kills, leader kills)."""
    kills = leader_kills = 0
    while True:
        # Every draw happens each round, so the instants depend on the seed alone.
        gap = rng.uniform(MIN_KILL_GAP_S, MAX_KILL_GAP_S)
        restart = rng.uniform(MIN_RESTART_S, MAX_RESTART_S)
        pick = rng.randrange(len(evaluators))
        deadline = time.monotonic() + gap
        while time.monotonic() < deadline:
            if load.poll() is not None:
                return kills, leader_kills
            time.sleep(POLL_S)
        leaders = [e for e in evaluators if e.leading()]
        others = [e for e in evaluators if e not in leaders]
        if leaders and (kills % 2 == 0 or not others):
            victim, role = leaders[0], "líder"
            leader_kills += 1
        else:
            victim, role = others[pick % len(others)], "seguidor"
        victim.kill()
        kills += 1
        print(f"kill -9 {victim.id} ({role}) a los {time.monotonic() - start:.1f} s", flush=True)
        time.sleep(restart)
        victim.start()


def wait_drained(receiver, not_before):
    """Wait until not_before and then until the receiver log stays the same for QUIET_S."""
    deadline = time.monotonic() + DRAIN_TIMEOUT_S
    size, since = -1, time.monotonic()
    while True:
        now = time.monotonic()
        current = receiver.log.stat().st_size if receiver.log.exists() else 0
        if current != size:
            size, since = current, now
        if now >= not_before and now - since >= QUIET_S:
            return
        if now > deadline:
            raise RuntimeError("the receiver did not go quiet")
        time.sleep(POLL_S)


def engine_evals(export_dir):
    """Join the present evals of every run of every evaluator; return them and the conflicts."""
    merged, conflicts = {}, []
    for evaluator_id in EVALUATORS:
        # Every run counts: serve is never killed, so no log was truncated.
        evals, found = lib.evaluator_evals(export_dir, evaluator_id, math.inf)
        conflicts += found
        for key, row in evals.items():
            seen = merged.setdefault(key, row)
            if seen["value"] != row["value"]:
                conflicts.append(f"conflict {key}: {seen['value']} and {row['value']}")
    return list(merged.values()), conflicts


def main():
    rng = random.Random(SEED)
    work = lib.work_dir(WORK)
    rules = lib.load_rules()
    report = work / "report.json"
    with contextlib.ExitStack() as stack:
        engine = stack.enter_context(
            lib.Serve(work, lib.alerts_tenants(), idle_grace_ms=IDLE_GRACE_MS)
        )
        receiver = stack.enter_context(lib.Receiver(work))
        evaluators = [
            stack.enter_context(lib.Evaluator(work, engine, receiver, i)) for i in EVALUATORS
        ]
        engine.start()
        receiver.start()
        for evaluator in evaluators:
            evaluator.start()
        start = time.monotonic()
        load = lib.start_otgen("alerts", SEED, engine.url, report, realtime=True)
        try:
            kills, leader_kills = kill_during_load(rng, evaluators, load, start)
            lib.accepted_events(lib.wait_otgen(load, report))
        finally:
            if load.poll() is None:
                load.kill()
                load.wait()
        wait_drained(receiver, time.monotonic() + settle_ms(rules) / 1000)
        for evaluator in evaluators:
            evaluator.stop()
        receiver.stop()
        engine.stop()

    evals, conflicts = engine_evals(engine.export_dir)
    for line in conflicts[:MAX_SHOWN]:
        print(line)
    evals_path = work / "engine-evals.jsonl"
    lib.write_jsonl(evals_path, evals)
    oracle_dir = work / "oracle"
    lib.run_oracle_rules(
        engine.data_dir, oracle_dir, lib.ALERTS_RULES, evals_path, IDLE_GRACE_MS
    )
    expected = lib.run_oracle_incidents(oracle_dir / "transitions.jsonl", oracle_dir)
    outcome = lib.notify_outcome(expected, lib.read_jsonl(receiver.log))
    for label, keys in (
        ("perdido", outcome.lost),
        ("duplicado efectivo", outcome.duplicates),
        ("aceptado desconocido", outcome.unknown),
    ):
        for key in keys[:MAX_SHOWN]:
            print(f"{label}: {key}")
    print(f"kills: {kills}")
    print(f"kills del líder: {leader_kills}")
    print(f"incidentes: {outcome.incidents}")
    print(f"perdidos: {len(outcome.lost)}")
    print(f"duplicados efectivos: {len(outcome.duplicates)}")
    print(f"aceptados desconocidos: {len(outcome.unknown)}")
    ok = (
        leader_kills > 0
        and outcome.incidents > 0
        and not outcome.lost
        and not outcome.duplicates
        and not outcome.unknown
        and not conflicts
    )
    (work / RESULT).write_text(
        json.dumps(
            {
                "kills": kills,
                "leader_kills": leader_kills,
                "incidents": outcome.incidents,
                "lost": len(outcome.lost),
                "duplicates": len(outcome.duplicates),
                "unknown": len(outcome.unknown),
                "pass": ok,
            }
        )
        + "\n"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
