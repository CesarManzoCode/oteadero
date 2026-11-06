"""Criterion: after kill -9 and a torn log tail, buckets equal the oracle's on the final log."""

import os
import random
import struct
import sys
import time
from pathlib import Path

import lib

SEED = 3
CHECKPOINT_EVENTS = 50_000
# Well under what is left of the load after the first checkpoint.
MAX_KILL_DELAY_S = 0.1
POLL_S = 0.005

CHECKPOINT_MAGIC = b"OTCKPT01"
# magic + u32 version + u32 CRC-32 + u64 log id, then the position of the log.
CHECKPOINT_POS_AT = 24
SEGMENT_HEADER_SIZE = 16
RECORD_HEADER_SIZE = 8


def wait_for_checkpoint(data_dir, load):
    """Wait until a checkpoint covers events; the first one is not at offset 0."""
    ckpt_dir = Path(data_dir) / "ckpt"
    while not any(int(path.stem) > 0 for path in ckpt_dir.glob("*.ckpt")):
        if load.poll() is not None:
            raise RuntimeError("the load ended before the first checkpoint")
        time.sleep(POLL_S)


def newest_checkpoint_pos(data_dir):
    """Return (segment, byte) of the newest checkpoint, or None if there is none."""
    paths = sorted((Path(data_dir) / "ckpt").glob("*.ckpt"))
    if not paths:
        return None
    data = paths[-1].read_bytes()
    if data[: len(CHECKPOINT_MAGIC)] != CHECKPOINT_MAGIC:
        raise RuntimeError(f"{paths[-1]}: not a checkpoint")
    return struct.unpack_from("<Qq", data, CHECKPOINT_POS_AT)


def last_record_span(path):
    """Return (start, end) of the last whole record of a segment, or None."""
    size = path.stat().st_size
    span = None
    pos = SEGMENT_HEADER_SIZE
    with open(path, "rb") as f:
        while pos + RECORD_HEADER_SIZE <= size:
            f.seek(pos)
            length = struct.unpack("<I", f.read(4))[0]
            end = pos + RECORD_HEADER_SIZE + length
            if length == 0 or end > size:
                break
            span = (pos, end)
            pos = end
    return span


def pick_cut(span, floor, rng):
    """Pick a size that splits the record and keeps the bytes below floor, or None (D25)."""
    start, end = span
    low = max(start + 1, floor)
    high = end - 1
    return rng.randint(low, high) if low <= high else None


def cut_last_record(data_dir, rng):
    """Truncate the last segment inside its last record, never below the newest checkpoint."""
    segment = sorted((Path(data_dir) / "log").glob("*.seg"))[-1]
    span = last_record_span(segment)
    if span is None:
        raise RuntimeError(f"{segment}: no record to cut")
    pos = newest_checkpoint_pos(data_dir)
    floor = pos[1] if pos is not None and pos[0] == int(segment.stem) else 0
    cut = pick_cut(span, floor, rng)
    if cut is None:
        raise RuntimeError(f"{segment}: no record after the checkpoint at byte {floor}")
    os.truncate(segment, cut)


def main():
    rng = random.Random(SEED)
    work = lib.work_dir("crit_recovery")
    report = work / "report.json"
    tenants = lib.tenants(2, lib.HIGH_QUOTA)
    with lib.Serve(work, tenants, checkpoint_events=CHECKPOINT_EVENTS) as engine:
        engine.start()
        # Batches that fail during the crash count as errors of the generator.
        load = lib.start_otgen("late", SEED, engine.url, report)
        try:
            wait_for_checkpoint(engine.data_dir, load)
            time.sleep(rng.uniform(0, MAX_KILL_DELAY_S))
            if load.poll() is not None:
                raise RuntimeError("the load ended before the kill")
            engine.kill()
            cut_last_record(engine.data_dir, rng)
            recovered = engine.start()
            lib.wait_otgen(load, report)
        finally:
            if load.poll() is None:
                load.kill()
                load.wait()
        engine.stop()

    truncated = recovered["truncated_bytes"]
    print(f"truncated_bytes: {truncated}")
    oracle = lib.run_oracle(engine.data_dir, work / "oracle")
    windows_ok = lib.check_windows(engine.export_dir, oracle, recovered["next_offset"])
    return 0 if windows_ok and truncated > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
