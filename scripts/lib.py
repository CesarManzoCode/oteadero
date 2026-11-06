"""What the criterion scripts share: processes, otgen, oracle and export comparisons."""

import http.client
import json
import math
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import NamedTuple

REPO_ROOT = Path(__file__).resolve().parent.parent
# Where scripts/fetch_refs.py leaves the reference binaries (D24).
REFBIN_DIR = REPO_ROOT / ".refbin"

# Exit code of a criterion that lacks an external binary.
SKIPPED = 77

# Quota of the runs that must not drop anything.
HIGH_QUOTA = 100_000_000

_LISTEN_TIMEOUT_S = 60
# How long an evaluator may take to read the rest of the log once the load
# ended; its checkpoint interval (10 s by default) is part of it.
_CATCH_UP_TIMEOUT_S = 120
_STOP_TIMEOUT_S = 60
_POLL_S = 0.01
_MAX_SHOWN = 20

_RECOVERED = re.compile(r"recovered next_offset=(\d+) truncated_bytes=(\d+)")
_LISTENING = re.compile(r"^listening ", re.MULTILINE)
_ROLE = re.compile(r"^(leader epoch=\d+|follower)$", re.MULTILINE)
_ACCOUNTING_LINE = re.compile(
    r"^oteadero_events_(accepted|dropped)_total\{(.*)\} (\d+)$", re.MULTILINE
)
_TENANT_LABEL = re.compile(r'tenant="([^"]*)"')
_METRIC_LINE = re.compile(r"^([A-Za-z_:][A-Za-z0-9_:]*)(\{.*\})? (\S+)$")
_METRIC_LABEL = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)="((?:[^"\\]|\\.)*)"')

ALERTS_RULES = REPO_ROOT / "rules" / "alerts.json"
# What the alerts profile sketches, and the published bounds of D07 and D14.
QUANTILE_METRICS = ("latency_ms",)
CARDINALITY_FIELDS = ("user",)
QUANTILE_ALPHA = 0.01
CARDINALITY_BOUND = 0.05


def bin_dir():
    return Path(os.environ.get("OTEADERO_BIN", REPO_ROOT / "bin"))


def binary(name):
    """Return the path of a binary, building all of them if it is missing."""
    path = bin_dir() / name
    if path.exists():
        return path
    if shutil.which("go") is None:
        print(f"missing {path} and no go to build it")
        sys.exit(SKIPPED)
    subprocess.run(
        ["go", "build", "-trimpath", "-o", f"{bin_dir()}/", "./cmd/...", "./harness/cmd/..."],
        cwd=REPO_ROOT,
        check=True,
    )
    return path


def ref_binary(name, refbin=None):
    """Return the path of a reference binary in .refbin/ or else in PATH, or None."""
    path = Path(refbin or REFBIN_DIR) / name
    if path.is_file() and os.access(path, os.X_OK):
        return path
    found = shutil.which(name)
    return Path(found) if found else None


def ref_binaries(*names):
    """Return the paths of the reference binaries; exit with 77 if one is missing."""
    paths = [ref_binary(name) for name in names]
    missing = [name for name, path in zip(names, paths) if path is None]
    if missing:
        print(f"faltan {', '.join(missing)} en .refbin/ y en PATH (make refs)")
        sys.exit(SKIPPED)
    return paths


def work_dir(script):
    """Return an empty .work/<script>/ for this run."""
    path = REPO_ROOT / ".work" / script
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True)
    return path


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def tenants(count, quota_eps):
    """Return the configuration of t1..t<count>; burst and weight keep their defaults (D14)."""
    return [{"id": f"t{i + 1}", "quota_eps": quota_eps} for i in range(count)]


def alerts_tenants():
    """Return the configuration of the one tenant of the alerts profile, with its sketches."""
    return [
        {
            "id": "t1",
            "quota_eps": HIGH_QUOTA,
            "quantile_metrics": list(QUANTILE_METRICS),
            "cardinality_fields": list(CARDINALITY_FIELDS),
        }
    ]


def _start_listening(command, output):
    """Start a process that prints `listening` when ready; return it and its output so far."""
    with open(output, "wb") as out:
        proc = subprocess.Popen(command, stdout=out, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + _LISTEN_TIMEOUT_S
    while True:
        text = output.read_text(errors="replace")
        if _LISTENING.search(text):
            return proc, text
        if proc.poll() is not None or time.monotonic() > deadline:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            raise RuntimeError(f"{Path(command[0]).name} did not listen: {text}")
        time.sleep(_POLL_S)


class Serve:
    """`oteadero serve` on a free port with its config, data and export under work.

    Synthetic timestamps are far behind the clock, so idle_grace_ms defaults to
    0: a tick would close every bucket. Restarting after kill keeps the port.
    """

    def __init__(self, work, tenant_config, **settings):
        self.work = work
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.data_dir = work / "data"
        self.export_dir = work / "export"
        self.config_path = work / "config.json"
        self.config_path.write_text(
            json.dumps(
                {
                    "data_dir": str(self.data_dir),
                    "listen": f"127.0.0.1:{self.port}",
                    "idle_grace_ms": 0,
                    "tenants": tenant_config,
                    **settings,
                }
            )
        )
        self.proc = None
        self.starts = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        if self.proc is not None and self.proc.poll() is None:
            self.kill()

    def start(self):
        """Start serve, wait for `listening` and return what it recovered."""
        self.starts += 1
        output = self.work / f"serve-{self.starts}.out"
        self.proc, text = _start_listening(
            [
                binary("oteadero"),
                "serve",
                "--config",
                self.config_path,
                "--export",
                self.export_dir,
            ],
            output,
        )
        next_offset, truncated = _RECOVERED.search(text).groups()
        return {"next_offset": int(next_offset), "truncated_bytes": int(truncated)}

    def kill(self):
        """Stop serve the way kill -9 does."""
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()

    def stop(self):
        """Stop serve cleanly; anything but exit code 0 is an error."""
        self.proc.terminate()
        code = self.proc.wait(timeout=_STOP_TIMEOUT_S)
        if code != 0:
            raise RuntimeError(f"serve exited with {code}")


class Receiver:
    """otrecv on a free port, logging to work/receiver.jsonl."""

    def __init__(self, work):
        self.work = work
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.log = work / "receiver.jsonl"
        self.proc = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        if self.proc is not None and self.proc.poll() is None:
            self.stop()

    def start(self):
        self.proc, _ = _start_listening(
            [binary("otrecv"), "--listen", f"127.0.0.1:{self.port}", "--log", self.log],
            self.work / "otrecv.out",
        )

    def stop(self):
        """Stop otrecv; it serves until a signal, so its exit code says nothing."""
        self.proc.terminate()
        self.proc.wait(timeout=_STOP_TIMEOUT_S)


class Evaluator:
    """`oteadero evaluate` over the log and config of a Serve, exporting next to it.

    Its checkpoints are <data_dir>/eval-<id>/ckpt/<offset>.ckpt, where offset is
    the next event to read. Restarting after kill keeps the port.
    """

    def __init__(self, work, engine, receiver, evaluator_id="e1", rules=ALERTS_RULES):
        self.work = work
        self.engine = engine
        self.receiver = receiver
        self.id = evaluator_id
        self.rules = rules
        self.port = free_port()
        self.ckpt_dir = engine.data_dir / f"eval-{evaluator_id}" / "ckpt"
        self.proc = None
        self.starts = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        if self.proc is not None and self.proc.poll() is None:
            self.kill()

    def start(self):
        """Start the evaluator and wait until it replayed the log and listens."""
        self.starts += 1
        self.proc, _ = _start_listening(
            [
                binary("oteadero"),
                "evaluate",
                "--config",
                self.engine.config_path,
                "--id",
                self.id,
                "--rules",
                self.rules,
                "--coordinator",
                self.engine.url,
                "--receiver",
                self.receiver.url,
                "--listen",
                f"127.0.0.1:{self.port}",
                "--export",
                self.engine.export_dir,
            ],
            self.work / f"{self.id}-{self.starts}.out",
        )

    def leading(self):
        """Return whether this evaluator runs and the last role it printed is leader."""
        if self.proc is None or self.proc.poll() is not None:
            return False
        output = self.work / f"{self.id}-{self.starts}.out"
        roles = _ROLE.findall(output.read_text(errors="replace"))
        return bool(roles) and roles[-1] != "follower"

    def checkpoint_offsets(self):
        """Return the offsets of the checkpoints on disk, oldest first."""
        return sorted(int(path.stem) for path in self.ckpt_dir.glob("*.ckpt"))

    def wait_offset(self, offset):
        """Wait for a checkpoint at offset or later: everything before it is exported."""
        deadline = time.monotonic() + _CATCH_UP_TIMEOUT_S
        while not any(o >= offset for o in self.checkpoint_offsets()):
            if self.proc.poll() is not None:
                raise RuntimeError(f"evaluator {self.id} exited with {self.proc.returncode}")
            if time.monotonic() > deadline:
                raise RuntimeError(f"evaluator {self.id} did not reach offset {offset}")
            time.sleep(_POLL_S)

    def kill(self):
        """Stop the evaluator the way kill -9 does."""
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()

    def stop(self):
        """Stop the evaluator cleanly; anything but exit code 0 is an error."""
        self.proc.terminate()
        code = self.proc.wait(timeout=_STOP_TIMEOUT_S)
        if code != 0:
            raise RuntimeError(f"evaluator {self.id} exited with {code}")


def start_otgen(profile, seed, url, report, duration_s=None, realtime=False, vm=False):
    command = [
        binary("otgen"),
        "--profile",
        profile,
        "--seed",
        str(seed),
        "--target",
        url,
        "--report",
        report,
    ]
    if duration_s is not None:
        command += ["--duration-s", str(duration_s)]
    if realtime:
        command.append("--realtime")
    if vm:
        command.append("--vm")
    return subprocess.Popen(command)


def wait_otgen(proc, report):
    """Wait for otgen and return its report."""
    code = proc.wait()
    if code != 0:
        raise RuntimeError(f"otgen exited with {code}")
    return json.loads(Path(report).read_text())


def run_otgen(profile, seed, url, report, duration_s=None, vm=False):
    return wait_otgen(start_otgen(profile, seed, url, report, duration_s, vm=vm), report)


def accepted_events(report):
    """Return the events the engine confirmed, which is its next offset.

    A run that must lose nothing fails on any dropped batch or connection error.
    """
    total = 0
    for name, tenant in report["tenants"].items():
        if tenant["errors"] or any(tenant["dropped"].values()):
            raise RuntimeError(f"tenant {name} lost events: {tenant}")
        total += tenant["acked"]
    return total


def run_alerts(work, seed, settings=None, during_load=None):
    """Run serve, otrecv and evaluator e1 over the alerts load until e1 read all of it.

    during_load(engine, evaluator, load), if given, runs while otgen sends and
    returns the truncate offset of section 4.8 for the evaluator export.
    Returns (engine, evaluator, truncate offset), all stopped.
    """
    return run_evaluated(
        work, "alerts", seed, ALERTS_RULES, alerts_tenants(), settings, during_load
    )


def run_evaluated(work, profile, seed, rules, tenant_config, settings=None, during_load=None):
    """Run serve, otrecv and evaluator e1 with rules over a load until e1 read all of it.

    Same arguments and result as run_alerts, for any profile and rules.
    """
    report = work / "report.json"
    truncate_offset = 0
    with (
        Serve(work, tenant_config, **(settings or {})) as engine,
        Receiver(work) as receiver,
        Evaluator(work, engine, receiver, rules=rules) as evaluator,
    ):
        engine.start()
        receiver.start()
        evaluator.start()
        load = start_otgen(profile, seed, engine.url, report)
        try:
            if during_load is not None:
                truncate_offset = during_load(engine, evaluator, load)
            accepted = accepted_events(wait_otgen(load, report))
        finally:
            if load.poll() is None:
                load.kill()
                load.wait()
        evaluator.wait_offset(accepted)
        evaluator.stop()
        receiver.stop()
        engine.stop()
    return engine, evaluator, truncate_offset


def fetch_accounting(url):
    """Return tenant -> {"accepted": n, "dropped": n} from /metrics of the engine."""
    # Loopback must not go through a proxy of the environment.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(f"{url}/metrics") as resp:
        text = resp.read().decode()
    result = {}
    for name, labels, value in _ACCOUNTING_LINE.findall(text):
        tenant = _TENANT_LABEL.search(labels).group(1)
        counters = result.setdefault(tenant, {"accepted": 0, "dropped": 0})
        counters[name] += int(value)
    return result


def parse_metrics(text):
    """Return (name, ((label, value), ...)) -> value of a Prometheus text exposition.

    Labels are sorted by name; comment and blank lines are skipped.
    """
    samples = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = _METRIC_LINE.match(line)
        if match is None:
            raise ValueError(f"bad metrics line {line!r}")
        name, labels, value = match.groups()
        pairs = tuple(sorted(_METRIC_LABEL.findall(labels or "")))
        samples[(name, pairs)] = float(value)
    return samples


def fetch_metrics(url):
    """Return the parsed /metrics of a process, as parse_metrics does."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(f"{url}/metrics") as resp:
        return parse_metrics(resp.read().decode())


def sse_events(lines):
    """Yield the events of a Server-Sent Events stream as dicts with id, event and data.

    lines are str, with or without their line end. A blank line ends an event,
    a line that starts with ':' is a comment and several data lines join with
    newlines. id is None when the event has none and event is "message" by
    default; an event without data is not dispatched, as in the SSE spec.
    """
    ident, kind, data = None, None, []
    for line in lines:
        line = line.rstrip("\r\n")
        if not line:
            if data:
                yield {"id": ident, "event": kind or "message", "data": "\n".join(data)}
            ident, kind, data = None, None, []
            continue
        if line.startswith(":"):
            continue
        name, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if name == "id":
            ident = value
        elif name == "event":
            kind = value
        elif name == "data":
            data.append(value)


def response_lines(resp):
    """Yield the lines of an HTTP response body as str, as they arrive."""
    for raw in iter(resp.readline, b""):
        yield raw.decode("utf-8", "replace")


def open_tail(url, tenant, kind, match="", token=None, header=True, timeout=10, connection=None):
    """Open GET /v1/tail and return (connection, response).

    token resumes with Last-Event-ID, or with ?after= when header is false.
    connection, if given, is an unconnected http.client.HTTPConnection to use.
    """
    query = {"tenant": tenant, "kind": kind}
    if match:
        query["match"] = match
    headers = {"Accept": "text/event-stream"}
    if token is not None:
        if header:
            headers["Last-Event-ID"] = token
        else:
            query["after"] = token
    parts = urllib.parse.urlsplit(url)
    conn = connection or http.client.HTTPConnection(parts.hostname, parts.port, timeout=timeout)
    conn.request("GET", "/v1/tail?" + urllib.parse.urlencode(query), headers=headers)
    return conn, conn.getresponse()


def offset_differences(received, expected):
    """Compare the offsets a subscriber received with the ones it must receive.

    received must be strictly increasing and hold exactly the offsets of
    expected. Returns one line per repeated offset, per offset that goes back,
    per gap (an expected offset never received) and per offset left over.
    """
    diffs = []
    for prev, cur in zip(received, received[1:]):
        if cur == prev:
            diffs.append(f"repeated {cur}")
        elif cur < prev:
            diffs.append(f"goes back: {cur} after {prev}")
    got, want = set(received), set(expected)
    diffs += [f"gap: missing {o}" for o in sorted(want - got)]
    diffs += [f"extra {o}" for o in sorted(got - want)]
    return diffs


def print_offsets(diffs):
    """Print the offset differences and the count line; return whether there are none."""
    for line in diffs[:_MAX_SHOWN]:
        print(line)
    print(f"offsets: {len(diffs)} diferencias")
    return not diffs


def run_oracle_offsets(data_dir, out, tenant, kind, match=""):
    """Return the offsets that the oracle says GET /v1/tail serves, in log order."""
    subprocess.run(
        [
            sys.executable,
            REPO_ROOT / "oracle" / "cli.py",
            "offsets",
            "--data",
            data_dir,
            "--tenant",
            tenant,
            "--kind",
            kind,
            "--match",
            match,
            "--out",
            out,
        ],
        check=True,
    )
    return [row["o"] for row in read_jsonl(Path(out) / "offsets.jsonl")]


class OracleResult(NamedTuple):
    buckets: list
    late: list
    accounting: dict


def run_oracle(data_dir, out, idle_grace_ms=0):
    """Recompute from the raw log with the oracle and return its output."""
    subprocess.run(
        [
            sys.executable,
            REPO_ROOT / "oracle" / "cli.py",
            "windows",
            "--data",
            data_dir,
            "--idle-grace-ms",
            str(idle_grace_ms),
            "--out",
            out,
        ],
        check=True,
    )
    out = Path(out)
    return OracleResult(
        read_jsonl(out / "buckets.jsonl"),
        [row["o"] for row in read_jsonl(out / "late.jsonl")],
        json.loads((out / "accounting.json").read_text()),
    )


def run_exact(data_dir, out, rules=None):
    """Return the exact bucket rows of the oracle and, with rules, its exact sketch windows."""
    command = [
        sys.executable,
        REPO_ROOT / "oracle" / "cli.py",
        "exact",
        "--data",
        data_dir,
        "--idle-grace-ms",
        "0",
        "--out",
        out,
    ]
    if rules is not None:
        command += ["--rules", rules]
    subprocess.run(command, check=True)
    out = Path(out)
    windows = read_jsonl(out / "exact_windows.jsonl") if rules is not None else []
    return read_jsonl(out / "exact_buckets.jsonl"), windows


def run_oracle_rules(data_dir, out, rules, engine_evals, idle_grace_ms=0):
    """Replay the rules with the oracle and return its transitions.

    engine_evals is the evals.jsonl whose values decide quantile and cardinality.
    """
    subprocess.run(
        [
            sys.executable,
            REPO_ROOT / "oracle" / "cli.py",
            "rules",
            "--data",
            data_dir,
            "--idle-grace-ms",
            str(idle_grace_ms),
            "--rules",
            rules,
            "--engine-evals",
            engine_evals,
            "--out",
            out,
        ],
        check=True,
    )
    return read_jsonl(Path(out) / "transitions.jsonl")


def run_oracle_incidents(transitions, out):
    """Return the notifications the oracle expects from a transitions.jsonl."""
    subprocess.run(
        [
            sys.executable,
            REPO_ROOT / "oracle" / "cli.py",
            "incidents",
            "--transitions",
            transitions,
            "--out",
            out,
        ],
        check=True,
    )
    return read_jsonl(Path(out) / "incidents.jsonl")


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")


def read_jsonl(path):
    """Return the rows of a JSONL file; kill -9 can leave a last line without its newline."""
    rows = []
    with open(path, "rb") as f:
        for line in f:
            if not line.endswith(b"\n"):
                break
            rows.append(json.loads(line))
    return rows


def export_runs(export_dir, process="serve"):
    """Return the run directories of a process, oldest first; run_id starts with the start time."""
    prefix = f"{process}-"
    return sorted(
        Path(export_dir).glob(f"{prefix}*"),
        key=lambda run: (int(run.name[len(prefix) :].split("-")[0]), run.name),
    )


def _bucket_key(row):
    return (row["tenant"], row["series"], row["end"])


def _bucket_values(row):
    return (row["count"], row["sum"], row["rate"])


def _eval_key(row):
    return (row["rule"], row["group"], row["t"])


def _eval_value(row):
    return row["value"]


def _transition_state(row):
    return row["state"]


def union_exports(runs, truncate_offset, key=_bucket_key, values=_bucket_values):
    """Join the rows of each run, oldest first, with the rule of section 4.8.

    Rows of earlier runs count only if closed_at is below the truncate offset;
    the last run is the one after the recovery. Returns the rows by key and the
    conflicts, one line per row with another value for a key already seen.
    """
    merged = {}
    conflicts = []
    for index, rows in enumerate(runs):
        earlier = index < len(runs) - 1
        for row in rows:
            if earlier and row["closed_at"] >= truncate_offset:
                continue
            k = key(row)
            seen = merged.setdefault(k, row)
            if values(seen) != values(row):
                conflicts.append(f"conflict {k}: {values(seen)} and {values(row)}")
    return merged, conflicts


def compare_buckets(engine, oracle_rows):
    """Compare buckets by key with exact equality of count, sum and rate.

    engine is the result of union_exports. Returns the number of keys compared
    and one line per bucket that differs, is missing or is left over.
    """
    oracle = {_bucket_key(row): row for row in oracle_rows}
    diffs = []
    keys = sorted(engine.keys() | oracle.keys())
    for key in keys:
        if key not in engine:
            diffs.append(f"missing {key}: oracle has {_bucket_values(oracle[key])}")
        elif key not in oracle:
            diffs.append(f"extra {key}: engine has {_bucket_values(engine[key])}")
        elif _bucket_values(engine[key]) != _bucket_values(oracle[key]):
            diffs.append(
                f"different {key}: engine {_bucket_values(engine[key])}"
                f" oracle {_bucket_values(oracle[key])}"
            )
    return len(keys), diffs


def check_windows(export_dir, oracle, truncate_offset=0):
    """Compare the export with the oracle, print the result and return whether it passes."""
    runs = [read_jsonl(run / "buckets.jsonl") for run in export_runs(export_dir)]
    merged, conflicts = union_exports(runs, truncate_offset)
    compared, diffs = compare_buckets(merged, oracle.buckets)
    diffs = conflicts + diffs
    for line in diffs[:_MAX_SHOWN]:
        print(line)
    print(f"ventanas comparadas: {compared}")
    print(f"ventanas: {len(diffs)} diferencias")
    return compared > 0 and not diffs


def evaluator_evals(export_dir, evaluator_id, truncate_offset=0):
    """Return the present evals of an evaluator by (rule, group, T), and the conflicts.

    Rows of absent groups carry a null value and are left out.
    """
    runs = [read_jsonl(run / "evals.jsonl") for run in export_runs(export_dir, evaluator_id)]
    merged, conflicts = union_exports(runs, truncate_offset, _eval_key, _eval_value)
    return {k: row for k, row in merged.items() if row["value"] is not None}, conflicts


def relative_error(value, exact):
    if exact == 0:
        return 0.0 if value == 0 else math.inf
    return abs(value - exact) / abs(exact)


def compare_relative(engine, exact, bound):
    """Compare two maps key -> number within a relative error bound.

    Returns the number of keys compared, the largest relative error among the
    keys on both sides, and one line per key that is out of bound, missing or
    left over.
    """
    diffs = []
    worst = 0.0
    keys = sorted(engine.keys() | exact.keys())
    for key in keys:
        if key not in engine:
            diffs.append(f"missing {key}: exact {exact[key]}")
        elif key not in exact:
            diffs.append(f"extra {key}: engine {engine[key]}")
        else:
            error = relative_error(engine[key], exact[key])
            worst = max(worst, error)
            if error > bound:
                diffs.append(
                    f"out of bound {key}: engine {engine[key]} exact {exact[key]}"
                    f" relative error {error:.6g}"
                )
    return len(keys), worst, diffs


def _metric_name(series):
    return series.split("{", 1)[0]


def bucket_quantiles(rows):
    """Return (tenant, series, end, q) -> value from bucket rows with q."""
    return {
        (row["tenant"], row["series"], row["end"], q): value
        for row in rows
        if "q" in row
        for q, value in row["q"].items()
    }


def exact_bucket_quantiles(rows, metrics=QUANTILE_METRICS):
    """Return the exact quantiles of the buckets of the sketched metrics, keyed like bucket_quantiles."""
    return bucket_quantiles(row for row in rows if _metric_name(row["series"]) in metrics)


def bucket_cardinalities(rows):
    """Return (tenant, series, end, field) -> n from bucket rows with card."""
    return {
        (row["tenant"], row["series"], row["end"], field): n
        for row in rows
        if "card" in row
        for field, n in row["card"].items()
    }


def exact_bucket_cardinalities(rows, fields=CARDINALITY_FIELDS):
    """Return the exact distinct counts of the sketched fields of every log bucket.

    A field without values in a bucket counts 0, as an empty HLL does.
    """
    return {
        (row["tenant"], row["series"], row["end"], field): row["card"].get(field, 0)
        for row in rows
        if "card" in row
        for field in fields
    }


def window_values(rows, rule_names):
    """Return (rule, group, T) -> value of the rows of the named rules."""
    return {_eval_key(row): row["value"] for row in rows if row["rule"] in rule_names}


def load_rules(path=ALERTS_RULES):
    return json.loads(Path(path).read_text())["rules"]


def print_relative(label, compared, worst, diffs):
    """Print the comparison of a sketch criterion and return whether it passes.

    label is the count line, with its participle agreeing in gender:
    "cuantiles comparados", "cardinalidades comparadas".
    """
    for line in diffs[:_MAX_SHOWN]:
        print(line)
    print(f"{label}: {compared}")
    print(f"error relativo máximo: {worst:.6g}")
    return compared > 0 and not diffs


def compare_transitions(engine_runs, oracle_rows, truncate_offset=0):
    """Compare the transitions (rule, group, T, state) of the engine with the oracle's.

    engine_runs are the rows of each export run, oldest first, joined with the
    rule of section 4.8. A (rule, group, T) has at most one transition, so a key
    repeated within a run or with two states is a difference too. Returns the
    number of keys compared and one line per difference.
    """
    diffs = []
    for index, rows in enumerate(engine_runs):
        seen = set()
        for row in rows:
            k = _eval_key(row)
            if k in seen:
                diffs.append(f"repeated {k} in run {index}")
            seen.add(k)
    merged, conflicts = union_exports(
        engine_runs, truncate_offset, _eval_key, _transition_state
    )
    diffs += conflicts
    engine = {k: row["state"] for k, row in merged.items()}
    oracle = {}
    for row in oracle_rows:
        k = _eval_key(row)
        if k in oracle:
            diffs.append(f"repeated {k} in the oracle")
        oracle[k] = row["state"]
    keys = sorted(engine.keys() | oracle.keys())
    for key in keys:
        if key not in engine:
            diffs.append(f"missing {key}: oracle {oracle[key]}")
        elif key not in oracle:
            diffs.append(f"extra {key}: engine {engine[key]}")
        elif engine[key] != oracle[key]:
            diffs.append(f"different {key}: engine {engine[key]} oracle {oracle[key]}")
    return len(keys), diffs


def evaluator_transitions(export_dir, evaluator_id, truncate_offset=0):
    """Return (rule, group, T) -> state of an evaluator, joined with section 4.8, and the conflicts."""
    runs = [
        read_jsonl(run / "transitions.jsonl") for run in export_runs(export_dir, evaluator_id)
    ]
    merged, conflicts = union_exports(runs, truncate_offset, _eval_key, _transition_state)
    return {k: row["state"] for k, row in merged.items()}, conflicts


def check_transitions(export_dir, evaluator_id, oracle_rows, truncate_offset=0):
    """Compare the transitions of an evaluator with the oracle's, print and return whether it passes."""
    runs = [
        read_jsonl(run / "transitions.jsonl") for run in export_runs(export_dir, evaluator_id)
    ]
    compared, diffs = compare_transitions(runs, oracle_rows, truncate_offset)
    for line in diffs[:_MAX_SHOWN]:
        print(line)
    print(f"transiciones comparadas: {compared}")
    print(f"transiciones: {len(diffs)} diferencias")
    return compared > 0 and not diffs


class NotifyOutcome(NamedTuple):
    incidents: int  # distinct (rule, group, T_f) expected
    lost: list  # expected notifications without an accepted request
    duplicates: list  # expected notifications with more than one accepted request
    unknown: list  # accepted requests the oracle does not expect


def notify_outcome(expected, requests):
    """Check the receiver log against the notifications the oracle expects (section 4.7).

    expected are the rows of the oracle incidents; requests the lines of the
    receiver. A (rule, group) has one transition per T, so (rule, group, kind, t)
    of the body names one notification of one incident (rule, group, T_f, kind)
    whatever key it came with: two accepted requests of it are an effective
    duplicate even with two different keys. Only accepted requests count.
    """
    counts = {}
    names = {}
    for row in expected:
        event = (row["rule"], row["group"], row["kind"], row["t"])
        counts[event] = 0
        names[event] = row["key"]
    unknown = []
    for request in requests:
        if request["status"] != "accepted":
            continue
        body = request["body"]
        event = None
        if isinstance(body, dict):
            event = (body.get("rule"), body.get("group"), body.get("kind"), body.get("t"))
        if event in counts:
            counts[event] += 1
        else:
            unknown.append(request["key"])
    return NotifyOutcome(
        incidents=len({row["incident"] for row in expected}),
        lost=[names[e] for e, n in counts.items() if n == 0],
        duplicates=[names[e] for e, n in counts.items() if n > 1],
        unknown=unknown,
    )


def notification_latencies(requests):
    """Return recv_ms - t of each accepted request, in ms (D25)."""
    return [
        request["recv_ms"] - request["body"]["t"]
        for request in requests
        if request["status"] == "accepted"
    ]


def percentile(values, q):
    """Return the nearest-rank q-quantile: the ceil(q n)-th smallest value."""
    if not values:
        raise ValueError("percentile of nothing")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]
