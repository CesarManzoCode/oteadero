"""Criterion: p99 of the notification latency under kill -9 is at most 8000 ms (D14).

It reads the run of crit_notify_once.py in .work/notify_once/ and launches it
first if there is none. The latency of each accepted notification is
recv_ms - t: the load stamps its events with the clock (D25).
"""

import subprocess
import sys

import crit_notify_once
import lib

P99_MAX_MS = 8000


def main():
    work = lib.REPO_ROOT / ".work" / crit_notify_once.WORK
    if not (work / crit_notify_once.RESULT).exists():
        # Its verdict is its own criterion; here only the run matters.
        subprocess.run([sys.executable, crit_notify_once.__file__])
    if not (work / crit_notify_once.RESULT).exists():
        print("crit_notify_once.py left no run")
        return 1
    latencies = lib.notification_latencies(lib.read_jsonl(work / "receiver.jsonl"))
    if not latencies:
        print("no accepted notification")
        return 1
    p99 = lib.percentile(latencies, 0.99)
    print(f"notificaciones aceptadas: {len(latencies)}")
    print(f"p50: {lib.percentile(latencies, 0.5)}")
    print(f"p99: {p99}")
    print(f"max: {max(latencies)}")
    return 0 if p99 <= P99_MAX_MS else 1


if __name__ == "__main__":
    sys.exit(main())
