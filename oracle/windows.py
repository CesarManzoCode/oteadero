"""Watermarks, late classification and buckets, as specified in section 4.4 of the contract."""

import heapq
from typing import NamedTuple

from logread import KIND_METRICS, Tick


class WindowResult(NamedTuple):
    buckets: list  # closed buckets, in closing order
    late: list  # offsets of late events, in log order
    accounting: dict  # tenant -> {"accepted": n, "late": n}


class _Tenant:
    def __init__(self):
        self.max_ts = None
        self.open = {}  # end -> series -> [kind, count, sum]
        self.ends = []  # heap with the keys of open
        self.accepted = 0
        self.late = 0


def bucket_end(ts_ms, bucket_ms):
    """Return ceil(ts / S) * S, the end of the left-open bucket holding ts."""
    return -(-ts_ms // bucket_ms) * bucket_ms


def compute_windows(entries, bucket_ms, lateness_ms, idle_grace_ms):
    """Replay log entries and return the closed buckets and the late events."""
    tenants = {}
    buckets = []
    late = []

    def close(name, tenant):
        watermark = tenant.max_ts - lateness_ms
        while tenant.ends and tenant.ends[0] <= watermark:
            end = heapq.heappop(tenant.ends)
            series_map = tenant.open.pop(end)
            for series in sorted(series_map):
                kind, count, total = series_map[series]
                rate_base = total if kind == KIND_METRICS else count
                buckets.append(
                    {
                        "tenant": name,
                        "series": series,
                        "end": end,
                        "count": count,
                        "sum": total,
                        "rate": rate_base / (bucket_ms / 1000),
                    }
                )

    for entry in entries:
        if isinstance(entry, Tick):
            for name in sorted(tenants):
                tenant = tenants[name]
                if entry.ts_ms - idle_grace_ms > tenant.max_ts:
                    tenant.max_ts = entry.ts_ms - idle_grace_ms
                    close(name, tenant)
            continue

        tenant = tenants.get(entry.tenant)
        if tenant is None:
            tenant = tenants[entry.tenant] = _Tenant()
        tenant.accepted += 1
        end = bucket_end(entry.ts_ms, bucket_ms)

        # A late event cannot move the watermark: ts <= end <= maxTs - L.
        if tenant.max_ts is not None and end <= tenant.max_ts - lateness_ms:
            tenant.late += 1
            late.append(entry.offset)
            continue

        if end not in tenant.open:
            tenant.open[end] = {}
            heapq.heappush(tenant.ends, end)
        bucket = tenant.open[end].setdefault(entry.series, [entry.kind, 0, 0.0])
        bucket[1] += 1
        bucket[2] += entry.value

        if tenant.max_ts is None or entry.ts_ms > tenant.max_ts:
            tenant.max_ts = entry.ts_ms
            close(entry.tenant, tenant)

    accounting = {
        name: {"accepted": tenants[name].accepted, "late": tenants[name].late}
        for name in sorted(tenants)
    }
    return WindowResult(buckets, late, accounting)
