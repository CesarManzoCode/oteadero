"""Criterion: promtool test rules and vmalert-tool unittest agree with the engine on keep_firing_for (D12, D13).

The engine evaluator runs rules/kff.json over the reference-kff load, regular
series from t = 0. A promtool test file gets the load as input_series, read
from the raw log by the oracle, and as exp_alerts the alerts the engine had
firing at every instant; it passes if promtool and vmalert-tool accept it.
"""

import json
import re
import subprocess
import sys

import lib
import rules2prom

SEED = 23
RULES = lib.REPO_ROOT / "rules" / "kff.json"

_TEST_TIMEOUT_S = 600
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')
_UNESCAPE = re.compile(r"\\(.)")


def parse_labels(group):
    """Return the label map of canonical labels of section 4.2."""
    unescape = {"\\": "\\", '"': '"', "n": "\n"}
    return {
        name: _UNESCAPE.sub(lambda m: unescape.get(m.group(1), m.group(0)), value)
        for name, value in _LABEL.findall(group)
    }


def input_series(buckets, metrics, interval):
    """Return series -> promtool values of the buckets of the named metrics.

    The load has one sample per series at each multiple of the interval, which
    is the end of its bucket; an instant without one is `_`.
    """
    samples = {}
    for row in buckets:
        name = row["series"].split("{", 1)[0]
        if name not in metrics:
            continue
        if row["count"] != 1 or row["end"] % interval or row["end"] < 0:
            raise ValueError(f"bucket {row} is not one sample on the interval grid")
        samples.setdefault(row["series"], {})[row["end"] // interval] = row["sum"]
    return {
        series: " ".join(
            rules2prom.number(at[k]) if k in at else "_" for k in range(max(at) + 1)
        )
        for series, at in sorted(samples.items())
    }


def firing_at(transitions, rules, grid):
    """Return T -> rule -> sorted groups firing in the engine after its step at T."""
    by_key = {}
    for (rule, group, t), state in transitions.items():
        by_key.setdefault((rule, group), []).append((t, state))
    out = {t: {rule: [] for rule in rules} for t in grid}
    for (rule, group), changes in sorted(by_key.items()):
        changes.sort()
        state, i = "inactive", 0
        for t in grid:
            while i < len(changes) and changes[i][0] <= t:
                state = changes[i][1]
                i += 1
            if state == "firing":
                out[t][rule].append(group)
    return out


def promtool_test_file(rules_yaml, interval, series, alerts):
    """Return the promtool test file of the series and the expected firing alerts."""
    step = rules2prom.duration(interval)
    lines = [
        "rule_files:",
        f"  - {json.dumps(str(rules_yaml))}",
        f"evaluation_interval: {step}",
        "tests:",
        f"  - interval: {step}",
        "    input_series:",
    ]
    for name, values in series.items():
        lines.append(f"      - series: {json.dumps(name)}")
        lines.append(f"        values: {json.dumps(values)}")
    lines.append("    alert_rule_test:")
    for t in sorted(alerts):
        for rule, groups in alerts[t].items():
            lines.append(f"      - eval_time: {rules2prom.duration(t)}")
            lines.append(f"        alertname: {json.dumps(rule)}")
            if not groups:
                lines.append("        exp_alerts: []")
                continue
            lines.append("        exp_alerts:")
            for group in groups:
                lines.append(f"          - exp_labels: {json.dumps(parse_labels(group))}")
    return "\n".join(lines) + "\n"


def run_tool(label, command, work):
    out = work / f"{label}.out"
    with open(out, "wb") as f:
        code = subprocess.run(
            command, cwd=work, stdout=f, stderr=subprocess.STDOUT, timeout=_TEST_TIMEOUT_S
        ).returncode
    if code != 0:
        print(out.read_text(errors="replace"), end="")
    print(f"{label}: {'ok' if code == 0 else 'fail'}")
    return code == 0


def main():
    promtool, vmalert_tool = lib.ref_binaries("promtool", "vmalert-tool-prod")
    work = lib.work_dir("crit_keep_firing_for")
    rules = lib.load_rules(RULES)
    interval = rules2prom.common_interval(rules)
    rules_yaml = work / "kff.yaml"
    rules2prom.translate_file(RULES, rules_yaml)

    engine_work = work / "engine"
    engine_work.mkdir()
    engine, evaluator, _ = lib.run_evaluated(
        engine_work, "reference-kff", SEED, RULES, lib.tenants(1, lib.HIGH_QUOTA)
    )
    transitions, conflicts = lib.evaluator_transitions(engine.export_dir, evaluator.id)
    for line in conflicts:
        print(line)
    evals = [
        row
        for run in lib.export_runs(engine.export_dir, evaluator.id)
        for row in lib.read_jsonl(run / "evals.jsonl")
    ]
    if not evals:
        print("el motor no evaluó ningún instante")
        return 1
    # promtool evaluates from 0; the engine evaluates nothing earlier with data.
    grid = list(range(0, max(row["t"] for row in evals) + 1, interval))

    oracle = lib.run_oracle(engine.data_dir, work / "oracle")
    metrics = {rule["match"]["__name__"] for rule in rules}
    series = input_series(oracle.buckets, metrics, interval)
    alerts = firing_at(transitions, [rule["name"] for rule in rules], grid)
    firing = sum(1 for t in grid if any(alerts[t].values()))
    test = work / "kff_test.yaml"
    test.write_text(promtool_test_file(rules_yaml, interval, series, alerts))
    print(f"instantes: {len(grid)}, con alertas firing: {firing}")

    promtool_ok = run_tool("promtool", [promtool, "test", "rules", test], work)
    vmalert_ok = run_tool(
        "vmalert-tool",
        [vmalert_tool, "unittest", "--disableAlertgroupLabel", f"--files={test}"],
        work,
    )
    return 0 if promtool_ok and vmalert_ok and firing > 0 and not conflicts else 1


if __name__ == "__main__":
    sys.exit(main())
