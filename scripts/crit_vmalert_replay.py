"""Criterion: the alert transitions of the engine equal those of vmalert in replay (D12).

The reference load goes to the engine and, byte for byte, to a local
victoria-metrics-prod. The engine evaluator runs rules/reference.json; vmalert
replays their translation from the first to the last instant the engine
evaluated. Its transitions are rebuilt from the exported ALERTS{alertstate}
samples on the instants timeFrom + k·interval, and both sides are compared as
(rule, group, T, state).
"""

import datetime
import json
import signal
import subprocess
import sys
import time
import urllib.parse
import urllib.request

import lib
import rules2prom

SEED = 21
RULES = lib.REPO_ROOT / "rules" / "reference.json"

_LISTEN_TIMEOUT_S = 60
_REPLAY_TIMEOUT_S = 600
_STOP_TIMEOUT_S = 60
_POLL_S = 0.05
_MAX_SHOWN = 20

# Labels of an ALERTS series that are not the group of section 4.5.
_ALERT_LABELS = {"__name__", "alertname", "alertstate", "alertgroup"}
# What a change of the state of a (rule, group) between two instants records
# (section 4.6). firing -> pending has no single transition in section 4.6.
_TRANSITIONS = {
    ("inactive", "pending"): "pending",
    ("inactive", "firing"): "firing",
    ("pending", "firing"): "firing",
    ("pending", "inactive"): "inactive",
    ("firing", "inactive"): "resolved",
    ("firing", "pending"): "pending",
}

# Loopback must not go through a proxy of the environment.
_LOCAL = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _escape(value):
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def canonical(labels):
    """Return the canonical labels of section 4.2 of a label map."""
    names = sorted(labels, key=str.encode)
    return "{" + ",".join(f'{k}="{_escape(labels[k])}"' for k in names) + "}"


def instants(time_from, time_to, interval):
    """Return the evaluation instants of the replay: timeFrom + k·interval up to timeTo."""
    return list(range(time_from, time_to + 1, interval))


def alert_states(rows):
    """Return (rule, group) -> {T: state} from exported ALERTS rows, and the conflicts.

    Only samples with value 1 count: a resolved alert may leave a stale marker.
    """
    states = {}
    diffs = []
    for row in rows:
        metric = row["metric"]
        if metric.get("__name__") != "ALERTS":
            continue
        state = metric.get("alertstate")
        key = (
            metric.get("alertname"),
            canonical({k: v for k, v in metric.items() if k not in _ALERT_LABELS}),
        )
        if state not in ("pending", "firing"):
            diffs.append(f"alertstate {state!r} in {key}")
            continue
        at = states.setdefault(key, {})
        for value, t in zip(row["values"], row["timestamps"]):
            if value != 1:
                continue
            if at.get(t, state) != state:
                diffs.append(f"both pending and firing {key} at {t}")
            at[t] = state
    return states, diffs


def reconstruct(rows, time_from, time_to, interval):
    """Return (rule, group, T) -> transition of vmalert and one line per sample off the grid.

    The state of a (rule, group) at an instant without an ALERTS sample is inactive.
    """
    states, diffs = alert_states(rows)
    grid = instants(time_from, time_to, interval)
    on_grid = set(grid)
    transitions = {}
    for key in sorted(states):
        rule, group = key
        off = sorted(t for t in states[key] if t not in on_grid)
        if off:
            diffs.append(f"off the instants {key}: {off[:_MAX_SHOWN]}")
        prev = "inactive"
        for t in grid:
            cur = states[key].get(t, "inactive")
            name = _TRANSITIONS.get((prev, cur))
            if name is not None:
                transitions[(rule, group, t)] = name
            prev = cur
    return transitions, diffs


def compare(engine, reference):
    """Compare two maps (rule, group, T) -> state; return the keys compared and the differences."""
    diffs = []
    keys = sorted(engine.keys() | reference.keys())
    for key in keys:
        if key not in engine:
            diffs.append(f"missing {key}: vmalert {reference[key]}")
        elif key not in reference:
            diffs.append(f"extra {key}: engine {engine[key]}")
        elif engine[key] != reference[key]:
            diffs.append(f"different {key}: engine {engine[key]} vmalert {reference[key]}")
    return len(keys), diffs


def rfc3339(ms):
    if ms % 1000:
        raise ValueError(f"instant {ms} is not a whole second")
    return datetime.datetime.fromtimestamp(ms // 1000, datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class VictoriaMetrics:
    """victoria-metrics-prod on a free port with its storage under work.

    cpus, if given, pins it with taskset -c.
    """

    def __init__(self, binary, work, cpus=None):
        self.binary = binary
        self.work = work
        self.cpus = cpus
        self.port = lib.free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.proc = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()

    def start(self):
        with open(self.work / "victoria-metrics.out", "wb") as out:
            self.proc = subprocess.Popen(
                lib.pinned(
                    self.cpus,
                    [
                        self.binary,
                        f"-storageDataPath={self.work / 'vm-data'}",
                        f"-httpListenAddr=127.0.0.1:{self.port}",
                        "-retentionPeriod=100y",
                        "-search.disableCache",
                        "-search.latencyOffset=0s",
                    ],
                ),
                stdout=out,
                stderr=subprocess.STDOUT,
            )
        deadline = time.monotonic() + _LISTEN_TIMEOUT_S
        while True:
            try:
                with _LOCAL.open(f"{self.url}/health") as resp:
                    if resp.status == 200:
                        return
            except OSError:
                pass
            if self.proc.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError("victoria-metrics-prod did not start")
            time.sleep(_POLL_S)

    def force_flush(self):
        with _LOCAL.open(urllib.request.Request(f"{self.url}/internal/force_flush", data=b"")):
            pass

    def export(self, match, time_from, time_to):
        """Return the rows of /api/v1/export for match between two instants in ms."""
        query = urllib.parse.urlencode(
            {"match[]": match, "start": time_from / 1000, "end": time_to / 1000}
        )
        with _LOCAL.open(f"{self.url}/api/v1/export?{query}") as resp:
            return [json.loads(line) for line in resp.read().decode().splitlines() if line.strip()]

    def stop(self):
        self.proc.terminate()
        self.proc.wait(timeout=_STOP_TIMEOUT_S)


def replay(vmalert, rules_yaml, url, time_from, time_to, work):
    command = [
        vmalert,
        f"-rule={rules_yaml}",
        f"-datasource.url={url}",
        f"-remoteWrite.url={url}",
        f"-replay.timeFrom={rfc3339(time_from)}",
        f"-replay.timeTo={rfc3339(time_to)}",
        "-replay.disableProgressBar",
        "-rule.evalDelay=0s",
        "-notifier.blackhole",
    ]
    with open(work / "vmalert.out", "wb") as out:
        code = subprocess.run(
            command, stdout=out, stderr=subprocess.STDOUT, timeout=_REPLAY_TIMEOUT_S
        ).returncode
    if code != 0:
        raise RuntimeError(f"vmalert-prod exited with {code}: see {work / 'vmalert.out'}")


def main():
    victoria, vmalert = lib.ref_binaries("victoria-metrics-prod", "vmalert-prod")
    work = lib.work_dir("crit_vmalert_replay")
    interval = rules2prom.common_interval(lib.load_rules(RULES))
    rules_yaml = work / "reference.yaml"
    rules2prom.translate_file(RULES, rules_yaml)

    engine_work = work / "engine"
    engine_work.mkdir()
    engine, evaluator, _ = lib.run_evaluated(
        engine_work, "reference", SEED, RULES, lib.tenants(1, lib.HIGH_QUOTA)
    )
    accepted = lib.accepted_events(json.loads((engine_work / "report.json").read_text()))
    evals = [
        row
        for run in lib.export_runs(engine.export_dir, evaluator.id)
        for row in lib.read_jsonl(run / "evals.jsonl")
    ]
    if not evals:
        print("el motor no evaluó ningún instante")
        return 1
    time_from = min(row["t"] for row in evals)
    time_to = max(row["t"] for row in evals)
    engine_transitions, conflicts = lib.evaluator_transitions(engine.export_dir, evaluator.id)

    with VictoriaMetrics(victoria, work) as vm:
        vm.start()
        report = lib.run_otgen("reference", SEED, vm.url, work / "vm-report.json", vm=True)
        if lib.accepted_events(report) != accepted:
            print(f"VictoriaMetrics recibió {lib.accepted_events(report)} eventos, el motor {accepted}")
            return 1
        vm.force_flush()
        replay(vmalert, rules_yaml, vm.url, time_from, time_to, work)
        vm.force_flush()
        rows = vm.export("ALERTS", time_from, time_to)
        vm.stop()

    reference, off_grid = reconstruct(rows, time_from, time_to, interval)
    compared, diffs = compare(engine_transitions, reference)
    diffs = conflicts + off_grid + diffs
    for line in diffs[:_MAX_SHOWN]:
        print(line)
    print(f"instantes: {rfc3339(time_from)} a {rfc3339(time_to)} cada {interval} ms")
    print(f"transiciones comparadas: {compared}")
    print(f"vmalert: {len(diffs)} diferencias")
    return 0 if compared > 0 and not diffs else 1


if __name__ == "__main__":
    sys.exit(main())
