"""Criterion: accepted + dropped = sent per tenant under saturation, and accepted = in the log."""

import sys

import lib

SEED = 4
# Long past the 1 s burst, and short enough for the oracle to read the log.
DURATION_S = 10
QUOTA_EPS = 200_000


def main():
    work = lib.work_dir("crit_accounting")
    report_path = work / "report.json"
    with lib.Serve(work, lib.tenants(4, QUOTA_EPS)) as engine:
        engine.start()
        report = lib.run_otgen("saturation", SEED, engine.url, report_path, DURATION_S)
        metrics = lib.fetch_accounting(engine.url)
        engine.stop()
    oracle = lib.run_oracle(engine.data_dir, work / "oracle")

    differences = 0
    for tenant, sent_report in sorted(report["tenants"].items()):
        sent = sent_report["sent"]
        counters = metrics.get(tenant, {"accepted": 0, "dropped": 0})
        accepted, dropped = counters["accepted"], counters["dropped"]
        in_log = oracle.accounting.get(tenant, {"accepted": 0})["accepted"]
        print(f"{tenant}: enviados={sent} aceptados={accepted} descartados={dropped}")
        if sent_report["errors"] != 0:
            print(f"{tenant}: errors={sent_report['errors']}")
            differences += 1
        if accepted + dropped != sent:
            differences += 1
        if accepted != in_log:
            print(f"{tenant}: accepted in the log={in_log}")
            differences += 1
    print(f"contabilidad: {differences} diferencias")
    return 0 if differences == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
