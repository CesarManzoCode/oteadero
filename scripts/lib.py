"""What the criterion scripts share: binaries, serve, otgen, oracle and bucket comparison."""

import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import NamedTuple

REPO_ROOT = Path(__file__).resolve().parent.parent

# Exit code of a criterion that lacks an external binary.
SKIPPED = 77

# Quota of the runs that must not drop anything.
HIGH_QUOTA = 100_000_000

_LISTEN_TIMEOUT_S = 60
_STOP_TIMEOUT_S = 60
_POLL_S = 0.01
_MAX_SHOWN = 20

_RECOVERED = re.compile(r"recovered next_offset=(\d+) truncated_bytes=(\d+)")
_LISTENING = re.compile(r"^listening ", re.MULTILINE)
_ACCOUNTING_LINE = re.compile(
    r"^oteadero_events_(accepted|dropped)_total\{(.*)\} (\d+)$", re.MULTILINE
)
_TENANT_LABEL = re.compile(r'tenant="([^"]*)"')


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
        with open(output, "wb") as out:
            self.proc = subprocess.Popen(
                [
                    binary("oteadero"),
                    "serve",
                    "--config",
                    self.config_path,
                    "--export",
                    self.export_dir,
                ],
                stdout=out,
                stderr=subprocess.STDOUT,
            )
        deadline = time.monotonic() + _LISTEN_TIMEOUT_S
        while True:
            text = output.read_text(errors="replace")
            if _LISTENING.search(text):
                break
            if self.proc.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError(f"serve did not listen: {text}")
            time.sleep(_POLL_S)
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


def start_otgen(profile, seed, url, report, duration_s=None):
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
    return subprocess.Popen(command)


def wait_otgen(proc, report):
    """Wait for otgen and return its report."""
    code = proc.wait()
    if code != 0:
        raise RuntimeError(f"otgen exited with {code}")
    return json.loads(Path(report).read_text())


def run_otgen(profile, seed, url, report, duration_s=None):
    return wait_otgen(start_otgen(profile, seed, url, report, duration_s), report)


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


def read_jsonl(path):
    """Return the rows of a JSONL file; kill -9 can leave a last line without its newline."""
    rows = []
    with open(path, "rb") as f:
        for line in f:
            if not line.endswith(b"\n"):
                break
            rows.append(json.loads(line))
    return rows


def export_runs(export_dir):
    """Return the run directories of serve, oldest first; run_id starts with the start time."""
    return sorted(
        Path(export_dir).glob("serve-*"),
        key=lambda run: (int(run.name.split("-")[1]), run.name),
    )


def _bucket_key(row):
    return (row["tenant"], row["series"], row["end"])


def _bucket_values(row):
    return (row["count"], row["sum"], row["rate"])


def union_exports(runs, truncate_offset):
    """Join the bucket rows of each run, oldest first, with the rule of section 4.8.

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
            key = _bucket_key(row)
            seen = merged.setdefault(key, row)
            if _bucket_values(seen) != _bucket_values(row):
                conflicts.append(
                    f"conflict {key}: {_bucket_values(seen)} and {_bucket_values(row)}"
                )
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
