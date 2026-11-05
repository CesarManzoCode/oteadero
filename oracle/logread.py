"""Reader of the input log segments, as specified in section 4.3 of the contract."""

import os
import re
import struct
import zlib
from typing import NamedTuple

MAGIC = b"OTLOG001"
SEGMENT_HEADER_SIZE = 16
SEGMENT_NAME = re.compile(r"\d{20}\.seg")

TYPE_BATCH = 1
TYPE_TICK = 2
TYPE_LEASE = 3

KIND_METRICS = 1
KIND_LOGS = 2

_RECORD_HEADER = struct.Struct("<II")
_I64 = struct.Struct("<q")
_METRIC_TAIL = struct.Struct("<dq")


class Event(NamedTuple):
    offset: int
    tenant: str
    kind: int
    series: str
    value: float  # 0.0 for logs
    ts_ms: int
    fields: str = "{}"  # canonical fields of a log event; metrics have none


class Tick(NamedTuple):
    ts_ms: int


class _Invalid(Exception):
    pass


def read_log(data_dir):
    """Yield Event and Tick entries in log order, up to the first invalid record.

    Offsets start at the name of the oldest segment, since older segments may
    have been deleted. Anything that does not continue the log (bad header,
    another log id, a gap in offsets) ends it like an invalid record does.
    """
    log_dir = os.path.join(data_dir, "log")
    names = sorted(n for n in os.listdir(log_dir) if SEGMENT_NAME.fullmatch(n))
    log_id = None
    next_offset = None
    for name in names:
        first_offset = int(name[:20])
        if next_offset is None:
            next_offset = first_offset
        elif first_offset != next_offset:
            return
        with open(os.path.join(log_dir, name), "rb") as f:
            data = f.read()
        if len(data) < SEGMENT_HEADER_SIZE or data[:8] != MAGIC:
            return
        if log_id is None:
            log_id = data[8:16]
        elif data[8:16] != log_id:
            return
        pos = SEGMENT_HEADER_SIZE
        while pos < len(data):
            record = _next_record(data, pos)
            if record is None:
                return
            record_type, payload, pos = record
            try:
                entries = _decode_record(record_type, payload, next_offset)
            except (_Invalid, IndexError, struct.error):
                return
            if record_type == TYPE_BATCH:
                next_offset += len(entries)
            yield from entries


def _next_record(data, pos):
    """Return (type, payload, next position), or None if the record is invalid."""
    if pos + _RECORD_HEADER.size > len(data):
        return None
    length, crc = _RECORD_HEADER.unpack_from(data, pos)
    start = pos + _RECORD_HEADER.size
    end = start + length
    if length == 0 or end > len(data):
        return None
    body = data[start:end]
    if zlib.crc32(body) != crc:
        return None
    return body[0], body[1:], end


def _decode_record(record_type, payload, first_offset):
    if record_type == TYPE_BATCH:
        return _decode_batch(payload, first_offset)
    if record_type == TYPE_TICK:
        return [Tick(_I64.unpack(payload)[0])]
    if record_type == TYPE_LEASE:
        return []
    raise _Invalid


def _decode_batch(payload, first_offset):
    raw_tenant, pos = _read_bytes(payload, 0)
    tenant = _text(raw_tenant)
    kind = payload[pos]
    if kind not in (KIND_METRICS, KIND_LOGS):
        raise _Invalid
    pos += 1 + 8  # kind and ingest_ms, which windows do not depend on
    count, pos = read_uvarint(payload, pos)
    events = []
    for i in range(count):
        if kind == KIND_METRICS:
            series, pos = _read_bytes(payload, pos)
            value, ts_ms = _METRIC_TAIL.unpack_from(payload, pos)
            pos += _METRIC_TAIL.size
        else:
            ts_ms = _I64.unpack_from(payload, pos)[0]
            value = 0.0
            series, pos = _read_bytes(payload, pos + _I64.size)
            fields, pos = _read_bytes(payload, pos)
            _, pos = _read_bytes(payload, pos)  # msg
        event = Event(first_offset + i, tenant, kind, _text(series), value, ts_ms)
        if kind == KIND_LOGS:
            event = event._replace(fields=_text(fields))
        events.append(event)
    if pos != len(payload):
        raise _Invalid
    return events


def read_uvarint(buf, pos):
    """Decode an unsigned varint with the limits of Go's encoding/binary."""
    result = 0
    for i in range(10):
        byte = buf[pos + i]
        if byte < 0x80:
            if i == 9 and byte > 1:
                raise _Invalid
            return result | byte << (7 * i), pos + i + 1
        result |= (byte & 0x7F) << (7 * i)
    raise _Invalid


def _read_bytes(buf, pos):
    length, pos = read_uvarint(buf, pos)
    end = pos + length
    if end > len(buf):
        raise _Invalid
    return buf[pos:end], end


def _text(raw):
    # surrogateescape keeps two series with different invalid UTF-8 apart.
    return raw.decode("utf-8", "surrogateescape")


def parse_labels(text):
    """Decode canonical labels of section 4.2, such as {a="1",b="2"}, into a dict."""
    if not text.startswith("{") or not text.endswith("}"):
        raise ValueError(f"not canonical labels: {text!r}")
    labels = {}
    pos = 1
    end = len(text) - 1
    while pos < end:
        eq = text.index("=", pos)
        name = text[pos:eq]
        if text[eq + 1] != '"':
            raise ValueError(f"not canonical labels: {text!r}")
        pos = eq + 2
        value = []
        while text[pos] != '"':
            char = text[pos]
            if char == "\\":
                pos += 1
                char = {"\\": "\\", '"': '"', "n": "\n"}[text[pos]]
            value.append(char)
            pos += 1
        labels[name] = "".join(value)
        pos += 1
        if pos < end:
            if text[pos] != ",":
                raise ValueError(f"not canonical labels: {text!r}")
            pos += 1
    return labels


def format_labels(labels):
    """Encode a dict as canonical labels of section 4.2: sorted by name in bytes."""
    names = sorted(labels, key=_utf8)
    parts = []
    for name in names:
        value = labels[name].replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        parts.append(f'{name}="{value}"')
    return "{" + ",".join(parts) + "}"


def split_series(series):
    """Return (metric name or None for logs, labels dict) of a canonical series key."""
    brace = series.index("{")
    name = series[:brace] if brace > 0 else None
    return name, parse_labels(series[brace:])


def _utf8(text):
    return text.encode("utf-8", "surrogateescape")
