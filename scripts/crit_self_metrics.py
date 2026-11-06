"""Criterion: the engine's own metrics (section 4.10) equal what the generator and the oracle
count after a `late` run with drops."""

import sys
import urllib.error
import urllib.request

import lib

SEED = 6
CAUSES = ("quota", "queue_full", "invalid", "too_large", "unknown_tenant", "shutting_down")
# t2 sends far over its quota, so that quota drops happen; t1 sketches a metric.
TENANT_CONFIG = [
    {"id": "t1", "quota_eps": lib.HIGH_QUOTA, "quantile_metrics": ["otgen_latency"]},
    {"id": "t2", "quota_eps": 20_000},
]
# Batches the script sends itself: tenant, path, body, cause and events.
EXTRA = [
    ("t1", "/v1/metrics", b'm{a="1"} 1 1\nnot a sample\n', "invalid", 2),
    ("zz", "/v1/metrics", b"m{} 1 1\nm{} 2 2\nm{} 3 3\n", "unknown_tenant", 3),
]


def post(url, tenant, path, body):
    """Send a batch and return its status; a drop is an answer, not an error."""
    request = urllib.request.Request(
        url + path, data=body, method="POST", headers={"X-Oteadero-Tenant": tenant}
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request) as resp:
            return resp.status
    except urllib.error.HTTPError as err:
        return err.code


def expected_counts(report, oracle_accounting):
    """Return tenant -> {"received", "accepted", "dropped": {cause: n}, "late"}.

    Received and dropped come from the generator and the script's own batches,
    accepted from the generator, late from the oracle.
    """
    counts = {}
    for name, tenant in report["tenants"].items():
        counts[name] = {
            "received": tenant["sent"],
            "accepted": tenant["acked"],
            "dropped": dict(tenant["dropped"]),
            "late": oracle_accounting.get(name, {}).get("late", 0),
        }
    for name, _, _, cause, n in EXTRA:
        c = counts.setdefault(name, {"received": 0, "accepted": 0, "dropped": {}, "late": 0})
        c["received"] += n
        c["dropped"][cause] = c["dropped"].get(cause, 0) + n
    return counts


def compare(samples, expected, oracle_accounting):
    """Print one line per comparison and return the number of differences."""
    differences = 0

    def check(label, got, want):
        nonlocal differences
        same = got == want
        differences += not same
        print(f"{label}: motor {got}, esperado {want}{'' if same else '  DIFERENTE'}")

    def value(name, **labels):
        v = samples.get((name, tuple(sorted(labels.items()))))
        return None if v is None else int(v)

    for tenant, c in sorted(expected.items()):
        check(f"recibidos {tenant}", value("oteadero_events_received_total", tenant=tenant), c["received"])
        check(f"aceptados {tenant}", value("oteadero_events_accepted_total", tenant=tenant), c["accepted"])
        oracle_accepted = oracle_accounting.get(tenant, {}).get("accepted", 0)
        check(f"aceptados {tenant} según el oráculo", c["accepted"], oracle_accepted)
        for cause in CAUSES:
            check(
                f"descartes {tenant} {cause}",
                value("oteadero_events_dropped_total", tenant=tenant, cause=cause),
                c["dropped"].get(cause, 0),
            )
        check(f"tardíos {tenant}", value("oteadero_events_late_total", tenant=tenant), c["late"])

    def present(label, key):
        nonlocal differences
        v = samples.get(key)
        differences += v is None
        print(f"{label}: {'ausente  DIFERENTE' if v is None else v}")
        return v

    for tenant in (t["id"] for t in TENANT_CONFIG):
        present(f"retraso de watermark {tenant}", ("oteadero_watermark_lag_ms", (("tenant", tenant),)))
    for queue in [t["id"] for t in TENANT_CONFIG] + ["sequencer"]:
        present(f"profundidad de cola {queue}", ("oteadero_queue_depth_events", (("queue", queue),)))
    error = present("error relativo observado máximo", ("oteadero_sketch_observed_rel_error_max", ()))
    if error is not None and error > lib.QUANTILE_ALPHA:
        differences += 1
        print(f"error relativo observado máximo {error} > {lib.QUANTILE_ALPHA}  DIFERENTE")
    return differences


def main():
    work = lib.work_dir("crit_self_metrics")
    report_path = work / "report.json"
    with lib.Serve(work, TENANT_CONFIG) as engine:
        engine.start()
        for tenant, path, body, _, _ in EXTRA:
            status = post(engine.url, tenant, path, body)
            if status != 400:
                raise RuntimeError(f"extra batch of {tenant} answered {status}")
        report = lib.run_otgen("late", SEED, engine.url, report_path)
        samples = lib.fetch_metrics(engine.url)
        engine.stop()
    oracle = lib.run_oracle(engine.data_dir, work / "oracle")

    errors = sum(t["errors"] for t in report["tenants"].values())
    if errors:
        print(f"eventos con error de conexión: {errors}")
    dropped = sum(sum(t["dropped"].values()) for t in report["tenants"].values())
    print(f"descartes del generador: {dropped}")
    differences = compare(samples, expected_counts(report, oracle.accounting), oracle.accounting)
    print(f"métricas propias: {differences} diferencias")
    return 0 if differences == 0 and errors == 0 and dropped > 0 and oracle.late else 1


if __name__ == "__main__":
    sys.exit(main())
