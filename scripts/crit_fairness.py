"""Criterion: with a noisy t4, t1-t3 get at least 99% of what they send (D14)."""

import sys

import lib

SEED = 5
DURATION_S = 10
QUOTA_EPS = 200_000
QUIET_TENANTS = ("t1", "t2", "t3")
MIN_RATIO = 0.99


def main():
    work = lib.work_dir("crit_fairness")
    with lib.Serve(work, lib.tenants(4, QUOTA_EPS)) as engine:
        engine.start()
        report = lib.run_otgen("fairness", SEED, engine.url, work / "report.json", DURATION_S)
        engine.stop()

    passed = True
    for tenant, row in sorted(report["tenants"].items()):
        sent, accepted = row["sent"], row["acked"]
        ratio = accepted / sent if sent else 0.0
        print(f"{tenant}: enviados={sent} aceptados={accepted} cociente={ratio:.4f}")
        if tenant in QUIET_TENANTS and (sent == 0 or accepted < MIN_RATIO * sent):
            passed = False
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
