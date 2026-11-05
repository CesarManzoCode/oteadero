"""Exact quantiles and distinct counts per bucket, the reference for the sketches of D07.

The quantile is the lower rank quantile: the value at index floor(q(n-1)), from 0,
of the sorted values. Cardinality is the number of distinct values of each field.
"""

import math
from fractions import Fraction
from typing import NamedTuple

from logread import KIND_METRICS, Event, parse_labels
from windows import bucket_end, compute_windows

QUANTILES = ("0.5", "0.9", "0.99", "0.999")


class Bucket(NamedTuple):
    tenant: str
    series: str
    kind: int
    end: int
    count: int
    sum: float  # in log order, as section 4.4 fixes it
    values: list  # metrics: the values of the bucket, in log order
    fields: dict  # logs: field name -> set of values


def lower_quantile(sorted_values, q):
    """Return the value at index floor(q(n-1)) of a sorted, non-empty list.

    q goes through its decimal text so that 0.29 means 29/100, not the float
    just below it, and floor(0.29 * 100) is 29.
    """
    index = math.floor(Fraction(str(q)) * (len(sorted_values) - 1))
    return sorted_values[index]


def collect_buckets(entries, bucket_ms, lateness_ms, idle_grace_ms):
    """Return the closed buckets, in closing order, with their values and fields."""
    entries = list(entries)
    result = compute_windows(entries, bucket_ms, lateness_ms, idle_grace_ms)
    late = set(result.late)
    raw = {}
    for entry in entries:
        if not isinstance(entry, Event) or entry.offset in late:
            continue
        key = (entry.tenant, entry.series, bucket_end(entry.ts_ms, bucket_ms))
        _, values, fields = raw.setdefault(key, (entry.kind, [], {}))
        if entry.kind == KIND_METRICS:
            values.append(entry.value)
        else:
            for name, value in parse_labels(entry.fields).items():
                fields.setdefault(name, set()).add(value)

    buckets = []
    for b in result.buckets:
        kind, values, fields = raw[(b["tenant"], b["series"], b["end"])]
        buckets.append(
            Bucket(b["tenant"], b["series"], kind, b["end"], b["count"], b["sum"], values, fields)
        )
    return buckets


def exact_row(bucket):
    """Return the exact export row of one bucket: q for metrics, card for logs."""
    row = {"tenant": bucket.tenant, "series": bucket.series, "end": bucket.end, "n": bucket.count}
    if bucket.kind == KIND_METRICS:
        ordered = sorted(bucket.values)
        row["q"] = {q: lower_quantile(ordered, q) for q in QUANTILES}
    else:
        row["card"] = {name: len(bucket.fields[name]) for name in sorted(bucket.fields)}
    return row


def window_quantile(buckets, q):
    """Return the exact lower rank quantile of the values of several buckets."""
    ordered = sorted(v for b in buckets for v in b.values)
    return lower_quantile(ordered, q)


def window_cardinality(buckets, field):
    """Return the exact number of distinct values of field over several buckets."""
    distinct = set()
    for b in buckets:
        distinct |= b.fields.get(field, set())
    return len(distinct)
