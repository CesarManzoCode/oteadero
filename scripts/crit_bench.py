"""Criterion: the engine confirms at least 1 000 000 events/s over 60 s (D14, D18).

It builds the binaries, measures the rate of otgen alone with the null sink on
the load cores, then sends the bench profile for 60 s to serve pinned to the
engine cores. Throughput is the confirmed events over the duration of the
sustained phase in the otgen report. The peak resident memory of serve (VmHWM)
is read before it stops and left with the throughput in .work/bench/engine.json
for crit_bench_rss.py. Then the same load goes to VictoriaMetrics on the same
cores; without victoria-metrics-prod it exits with 77 once the engine passed.
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import crit_vmalert_replay
import lib

WORK = "bench"
RESULT = "engine.json"
SEED = 18
DURATION_S = 60
# Long enough to see the rate of the generator alone; it does not decide anything.
GENERATOR_S = 10
ENGINE_CPUS = "0-3,6-9"
ENGINE_CORES = 8
LOAD_CPUS = "4,5,10,11"
MIN_EPS = 1_000_000
VM_BINARY = "victoria-metrics-prod"
CONCLUSION = (
    "comparable, no equivalente: el motor confirma tras fsync y no guarda historial;"
    " VictoriaMetrics guarda historial y confirma antes de persistir"
)


def peak_rss_kib(pid, proc_dir="/proc"):
    """Return VmHWM of a process in KiB, from /proc/<pid>/status."""
    for line in (Path(proc_dir) / str(pid) / "status").read_text().splitlines():
        if line.startswith("VmHWM:"):
            value, unit = line.split()[1:3]
            if unit != "kB":
                raise ValueError(f"unexpected VmHWM unit {unit!r}")
            return int(value)
    raise ValueError(f"no VmHWM for pid {pid}")


def confirmed_rate(report):
    """Return the confirmed events of all tenants over the duration of the report."""
    acked = sum(tenant["acked"] for tenant in report["tenants"].values())
    return acked / report["duration_s"]


def cpu_model(path="/proc/cpuinfo"):
    for line in Path(path).read_text().splitlines():
        name, _, value = line.partition(":")
        if name.strip() == "model name":
            return value.strip()
    return "desconocida"


def command_output(command):
    """Return the first line a command prints, or 'desconocida' if it cannot run."""
    try:
        proc = subprocess.run(command, capture_output=True, text=True)
    except OSError:
        return "desconocida"
    lines = (proc.stdout + proc.stderr).strip().splitlines()
    return lines[0] if proc.returncode == 0 and lines else "desconocida"


def build():
    """Build the binaries of this tree into bin/; those of OTEADERO_BIN are used as they are."""
    if "OTEADERO_BIN" in os.environ:
        return True
    if shutil.which("go") is None:
        print("falta go para compilar")
        return False
    subprocess.run(
        ["go", "build", "-trimpath", "-o", f"{lib.bin_dir()}/", "./cmd/...", "./harness/cmd/..."],
        cwd=lib.REPO_ROOT,
        check=True,
    )
    return True


def print_load(report):
    sent = sum(tenant["sent"] for tenant in report["tenants"].values())
    acked = sum(tenant["acked"] for tenant in report["tenants"].values())
    print(f"enviados: {sent} confirmados: {acked} duración: {report['duration_s']:.3f} s")


def run_engine(work):
    """Run the bench load against serve, write RESULT under work and return it."""
    with lib.Serve(work, lib.tenants(1, lib.HIGH_QUOTA), cpus=ENGINE_CPUS) as engine:
        engine.start()
        report = lib.run_otgen(
            "bench", SEED, engine.url, work / "report.json", DURATION_S, cpus=LOAD_CPUS
        )
        # Before stopping: the status file goes away with the process.
        rss_kib = peak_rss_kib(engine.proc.pid)
        engine.stop()
    result = {"eps": confirmed_rate(report), "rss_kib": rss_kib, "report": report}
    (work / RESULT).write_text(json.dumps(result, indent=2) + "\n")
    return result


def run_vm(binary, work):
    """Run the bench load against VictoriaMetrics; return its rate and VmHWM."""
    work.mkdir(parents=True, exist_ok=True)
    with crit_vmalert_replay.VictoriaMetrics(binary, work, cpus=ENGINE_CPUS) as vm:
        vm.start()
        report = lib.run_otgen(
            "bench", SEED, vm.url, work / "report.json", DURATION_S, vm=True, cpus=LOAD_CPUS
        )
        rss_kib = peak_rss_kib(vm.proc.pid)
        vm.stop()
    return {"eps": confirmed_rate(report), "rss_kib": rss_kib, "report": report}


def main():
    if shutil.which("taskset") is None:
        print("falta taskset")
        return lib.SKIPPED
    if not build():
        return lib.SKIPPED
    work = lib.work_dir(WORK)

    generator = lib.run_otgen(
        "bench", SEED, "", work / "generator.json", GENERATOR_S, cpus=LOAD_CPUS, sink="null"
    )
    print(f"generador (sink null, {LOAD_CPUS}): {generator['rate_eps']:.0f} eventos/s")

    engine = run_engine(work)
    print_load(engine["report"])
    print(f"motor: {engine['eps']:.0f} eventos/s")
    print(f"núcleos: {ENGINE_CORES}")
    print(f"cpus: motor {ENGINE_CPUS}, generador {LOAD_CPUS}")
    print(f"cpu: {cpu_model()}")
    print(f"versión: {command_output([lib.binary('oteadero'), 'version'])}")
    print(f"go: {command_output(['go', 'version'])}")
    print(f"kernel: {Path('/proc/sys/kernel/osrelease').read_text().strip()}")
    print(f"rss máximo motor: {engine['rss_kib'] / 1024:.1f} MiB")
    passed = engine["eps"] >= MIN_EPS
    print(f"umbral: {MIN_EPS} eventos/s: {'pasa' if passed else 'no pasa'}")

    binary = lib.ref_binary(VM_BINARY)
    if binary is None:
        print(f"falta {VM_BINARY} en .refbin/ y en PATH (make refs)")
        # A failing engine is a failure, not a skip.
        return lib.SKIPPED if passed else 1
    vm = run_vm(binary, work / "vm")
    print_load(vm["report"])
    print(f"victoriametrics: {vm['eps']:.0f} eventos/s")
    print(f"versión victoriametrics: {command_output([binary, '-version'])}")
    print(f"rss máximo victoriametrics: {vm['rss_kib'] / 1024:.1f} MiB")
    if vm["eps"] > 0:
        print(f"cociente motor/vm: {engine['eps'] / vm['eps']:.3f}")
    else:
        print("cociente motor/vm: indefinido (victoriametrics no confirmó nada)")
    print(f"conclusión: {CONCLUSION}")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
