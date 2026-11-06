"""Download the reference binaries of D24 to .refbin/ and check their SHA-256.

A tar already in .refbin/ with the right SHA-256 is not downloaded again, and
a binary already extracted from it is not extracted again.
"""

import hashlib
import os
import sys
import tarfile
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
REFBIN = REPO_ROOT / ".refbin"

_PROMETHEUS = "https://github.com/prometheus/prometheus/releases/download/v3.16.0/"
_VM = "https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/v1.155.0/"

# (tar, URL, SHA-256, {member in the tar: binary in .refbin/}), from vault/RESEARCH.md.
ARCHIVES = [
    (
        "prometheus-3.16.0.linux-amd64.tar.gz",
        _PROMETHEUS + "prometheus-3.16.0.linux-amd64.tar.gz",
        "68727009472a2c02e1ff7f062fd5b3e637285bffc8fc2d340c76fb449d6e0f34",
        {"prometheus-3.16.0.linux-amd64/promtool": "promtool"},
    ),
    (
        "victoria-metrics-linux-amd64-v1.155.0.tar.gz",
        _VM + "victoria-metrics-linux-amd64-v1.155.0.tar.gz",
        "7180e65913d55ab99c0be8f4a3ad17a0fafe5598a21ff6327fecba3190d7b176",
        {"victoria-metrics-prod": "victoria-metrics-prod"},
    ),
    (
        "vmutils-linux-amd64-v1.155.0.tar.gz",
        _VM + "vmutils-linux-amd64-v1.155.0.tar.gz",
        "b6181fc16cf19835706ea5d67215eea56c1fe87350fdfd82a5ff803ea2c1a907",
        {"vmalert-prod": "vmalert-prod", "vmalert-tool-prod": "vmalert-tool-prod"},
    ),
]

_CHUNK = 1 << 20


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(_CHUNK):
            h.update(chunk)
    return h.hexdigest()


def download(url, dst, want):
    """Download url to dst through a temporary file; keep it only if its SHA-256 is want."""
    tmp = dst.with_name(dst.name + ".part")
    h = hashlib.sha256()
    with urllib.request.urlopen(url) as resp, open(tmp, "wb") as out:
        while chunk := resp.read(_CHUNK):
            h.update(chunk)
            out.write(chunk)
    if h.hexdigest() != want:
        tmp.unlink()
        raise RuntimeError(f"{dst.name}: SHA-256 {h.hexdigest()}, want {want}")
    os.replace(tmp, dst)


def extract(archive, members, refbin):
    """Write the named regular files of the tar to refbin, executable; nothing else leaves it."""
    with tarfile.open(archive, "r:gz") as tar:
        for member, name in members.items():
            dst = refbin / name
            if dst.exists():
                continue
            info = tar.getmember(member)
            if not info.isfile():
                raise RuntimeError(f"{archive.name}: {member} is not a regular file")
            tmp = dst.with_name(dst.name + ".part")
            with tar.extractfile(info) as src, open(tmp, "wb") as out:
                while chunk := src.read(_CHUNK):
                    out.write(chunk)
            tmp.chmod(0o755)
            os.replace(tmp, dst)
            print(f"{name}: extraído")


def fetch(refbin=REFBIN, archives=ARCHIVES):
    refbin.mkdir(exist_ok=True)
    for tar_name, url, want, members in archives:
        archive = refbin / tar_name
        if archive.exists() and sha256(archive) == want:
            print(f"{tar_name}: verificado")
        else:
            print(f"{tar_name}: descargando {url}")
            download(url, archive, want)
            print(f"{tar_name}: verificado")
        extract(archive, members, refbin)


def main():
    try:
        fetch()
    except (OSError, RuntimeError, KeyError, tarfile.TarError) as e:
        print(f"fetch_refs: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
