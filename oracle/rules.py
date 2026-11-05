"""Rules of section 4.5 and the alert state machine of section 4.6, replayed from the log.

count, sum and rate are recomputed exactly. For quantile and cardinality the
expected value is the exact one of exact.py, and the condition is decided with
the value the engine exported, since a sketch may sit on the other side of the
threshold within its published error; the sketch criteria judge that value.
"""

import json
import operator
from typing import NamedTuple

from exact import collect_buckets, window_cardinality, window_quantile
from logread import KIND_LOGS, KIND_METRICS, Event, Tick, format_labels, split_series

SOURCES = {"metrics": KIND_METRICS, "logs": KIND_LOGS}
AGGS = ("count", "sum", "rate", "quantile", "cardinality")
SKETCH_AGGS = ("quantile", "cardinality")
OPS = {">": operator.gt, ">=": operator.ge, "<": operator.lt, "<=": operator.le}

INACTIVE = "inactive"
PENDING = "pending"
FIRING = "firing"
RESOLVED = "resolved"

_MISSING = object()


class Rule(NamedTuple):
    name: str
    tenant: str
    kind: int
    match: dict
    agg: str
    quantile: float  # None unless agg is quantile
    field: str  # None unless agg is cardinality
    range_ms: int
    interval_ms: int
    by: list  # None groups by series
    op: str
    threshold: float
    resolve_threshold: float  # None without hysteresis
    for_ms: int
    keep_firing_for_ms: int


def load_rules(path, bucket_ms):
    """Read and validate a rules file; raise ValueError on any invalid rule."""
    with open(path, encoding="utf-8") as f:
        try:
            doc = json.load(f)
        except json.JSONDecodeError as err:
            raise ValueError(f"{path}: {err}") from None
    return parse_rules(doc, bucket_ms)


def parse_rules(doc, bucket_ms):
    if not isinstance(doc, dict) or not isinstance(doc.get("rules"), list):
        raise ValueError('rules: want {"rules": [...]}')
    rules = []
    names = set()
    for i, raw in enumerate(doc["rules"]):
        rule = _parse_rule(raw, bucket_ms, f"rules[{i}]")
        if rule.name in names:
            raise ValueError(f"rules[{i}]: duplicate name {rule.name!r}")
        names.add(rule.name)
        rules.append(rule)
    return rules


def _parse_rule(raw, bucket_ms, where):
    if not isinstance(raw, dict):
        raise ValueError(f"{where}: want an object")

    def need(key, check, default=_MISSING):
        if key not in raw:
            if default is _MISSING:
                raise ValueError(f"{where}: missing {key}")
            return default
        value = raw[key]
        if not check(value):
            raise ValueError(f"{where}: invalid {key}: {value!r}")
        return value

    def is_str(v):
        return isinstance(v, str) and v != ""

    def is_number(v):
        return isinstance(v, (int, float)) and not isinstance(v, bool)

    def is_int(v):
        return isinstance(v, int) and not isinstance(v, bool)

    def is_multiple(v):
        return is_int(v) and v > 0 and v % bucket_ms == 0

    name = need("name", is_str)
    tenant = need("tenant", is_str)
    source = need("source", lambda v: v in SOURCES)
    match = need(
        "match",
        lambda v: isinstance(v, dict) and all(isinstance(x, str) for x in v.values()),
        {},
    )
    agg = need("agg", lambda v: v in AGGS)
    quantile = field = None
    if agg == "quantile":
        if source != "metrics":
            raise ValueError(f"{where}: quantile is only for metrics")
        quantile = need("quantile", lambda v: is_number(v) and 0 <= v <= 1)
    if agg == "cardinality":
        if source != "logs":
            raise ValueError(f"{where}: cardinality is only for logs")
        field = need("field", is_str)
    range_ms = need("range_ms", is_multiple)
    interval_ms = need("interval_ms", is_multiple, bucket_ms)
    by = need(
        "by", lambda v: isinstance(v, list) and all(isinstance(x, str) for x in v), None
    )
    op = need("op", lambda v: v in OPS)
    threshold = need("threshold", is_number)
    resolve_threshold = need("resolve_threshold", is_number, None)
    for_ms = need("for_ms", lambda v: is_int(v) and v >= 0, 0)
    keep_ms = need("keep_firing_for_ms", lambda v: is_int(v) and v >= 0, 0)
    return Rule(
        name, tenant, SOURCES[source], match, agg, quantile, field, range_ms, interval_ms,
        by, op, threshold, resolve_threshold, for_ms, keep_ms,
    )


def watermark_advances(entries, lateness_ms, idle_grace_ms):
    """Yield (tenant, previous W, new W) each time a watermark moves, in log order.

    The previous W is None the first time the watermark of a tenant is defined.
    A late event never moves it, since its ts is at most W.
    """
    max_ts = {}
    for entry in entries:
        if isinstance(entry, Tick):
            floor = entry.ts_ms - idle_grace_ms
            for tenant in sorted(max_ts):
                if floor > max_ts[tenant]:
                    previous = max_ts[tenant]
                    max_ts[tenant] = floor
                    yield tenant, previous - lateness_ms, floor - lateness_ms
            continue
        if not isinstance(entry, Event):
            continue
        previous = max_ts.get(entry.tenant)
        if previous is None or entry.ts_ms > previous:
            max_ts[entry.tenant] = entry.ts_ms
            old = None if previous is None else previous - lateness_ms
            yield entry.tenant, old, entry.ts_ms - lateness_ms


def instants(previous_w, new_w, interval_ms):
    """Return the multiples T of interval_ms with previous_w < T <= new_w."""
    if previous_w is None:
        return range(0)
    first = (previous_w // interval_ms + 1) * interval_ms
    return range(first, new_w + 1, interval_ms)


def _utf8(text):
    return text.encode("utf-8", "surrogateescape")


def group_of(rule, series):
    """Return the group key of a series under a rule, or None if it does not match."""
    name, labels = split_series(series)
    visible = dict(labels)
    if name is not None:
        visible["__name__"] = name
    for key, value in rule.match.items():
        if visible.get(key) != value:
            return None
    if rule.by is None:
        return format_labels(labels)
    return format_labels({key: visible[key] for key in rule.by if key in visible})


def rule_windows(entries, rules, bucket_ms, lateness_ms, idle_grace_ms):
    """Yield (rule, T, groups) for every evaluation instant, in log order.

    groups maps each group key present in the window to its buckets, in
    increasing end and, within an end, in canonical series order: the order of
    the sum in section 4.5.
    """
    entries = list(entries)
    index = {}  # (tenant, kind) -> end -> buckets
    for b in collect_buckets(entries, bucket_ms, lateness_ms, idle_grace_ms):
        index.setdefault((b.tenant, b.kind), {}).setdefault(b.end, []).append(b)
    for ends in index.values():
        for buckets in ends.values():
            buckets.sort(key=lambda b: _utf8(b.series))

    by_tenant = {}
    for rule in rules:
        by_tenant.setdefault(rule.tenant, []).append(rule)
    groups_of = {}  # (rule name, series) -> group key or None

    for tenant, previous_w, new_w in watermark_advances(entries, lateness_ms, idle_grace_ms):
        for rule in by_tenant.get(tenant, ()):
            ends = index.get((tenant, rule.kind), {})
            for t in instants(previous_w, new_w, rule.interval_ms):
                groups = {}
                for end in range(t - rule.range_ms + bucket_ms, t + 1, bucket_ms):
                    for b in ends.get(end, ()):
                        cache_key = (rule.name, b.series)
                        if cache_key not in groups_of:
                            groups_of[cache_key] = group_of(rule, b.series)
                        key = groups_of[cache_key]
                        if key is not None:
                            groups.setdefault(key, []).append(b)
                yield rule, t, groups


def window_value(rule, buckets):
    """Return the exact value of a group in a window from its buckets in sum order."""
    if rule.agg == "quantile":
        return window_quantile(buckets, rule.quantile)
    if rule.agg == "cardinality":
        return window_cardinality(buckets, rule.field)
    count = 0
    total = 0.0
    for b in buckets:
        count += b.count
        total += b.sum
    if rule.agg == "count":
        return count
    if rule.agg == "sum":
        return total
    base = total if rule.kind == KIND_METRICS else count
    return base / (rule.range_ms / 1000)


class Alert:
    """State of one (rule, group) in the machine of section 4.6."""

    def __init__(self, for_ms, keep_firing_for_ms):
        self.for_ms = for_ms
        self.keep_firing_for_ms = keep_firing_for_ms
        self.status = INACTIVE
        self.active_at = None
        self.keep_since = None

    def step(self, t, fire, stay):
        """Advance to T; return the recorded transition, or None.

        fire is the firing condition and stay the one with resolve_threshold;
        both are false for an absent group.
        """
        if self.status == INACTIVE:
            if not fire:
                return None
            self.active_at = t
            self.status = FIRING if self.for_ms == 0 else PENDING
            return self.status
        if self.status == PENDING:
            if not fire:
                self.status = INACTIVE
                self.active_at = None
                return INACTIVE
            if t - self.active_at >= self.for_ms:
                self.status = FIRING
                return FIRING
            return None
        if stay:
            self.keep_since = None
            return None
        if self.keep_firing_for_ms > 0:
            if self.keep_since is None:
                self.keep_since = t
            if t - self.keep_since < self.keep_firing_for_ms:
                return None
        self.status = INACTIVE
        self.active_at = None
        self.keep_since = None
        return RESOLVED


def _holds(op, value, threshold):
    return value is not None and OPS[op](value, threshold)


def load_engine_values(paths):
    """Read engine evals.jsonl files into (rule, group, t) -> value; later lines win."""
    values = {}
    for path in paths:
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    row = json.loads(line)
                    values[(row["rule"], row["group"], row["t"])] = row["value"]
    return values


def evaluate(entries, rules, bucket_ms, lateness_ms, idle_grace_ms, engine_values=None):
    """Return (evals, transitions) as the rows of section 4.8, without closed_at.

    evals holds one row per group present in a window. An absent group gets no
    eval row but still steps its state machine with a false condition.
    """
    engine_values = engine_values or {}
    evals = []
    transitions = []
    states = {}  # rule name -> group -> Alert
    for rule, t, groups in rule_windows(entries, rules, bucket_ms, lateness_ms, idle_grace_ms):
        present = {key: window_value(rule, buckets) for key, buckets in groups.items()}
        alerts = states.setdefault(rule.name, {})
        stepping = set(present)
        stepping.update(key for key, alert in alerts.items() if alert.status != INACTIVE)
        for key in sorted(stepping, key=_utf8):
            value = present.get(key)
            if value is not None:
                evals.append({"rule": rule.name, "group": key, "t": t, "value": value})
            decide = value
            if rule.agg in SKETCH_AGGS and value is not None:
                decide = engine_values.get((rule.name, key, t))
            fire = _holds(rule.op, decide, rule.threshold)
            stay_threshold = rule.threshold if rule.resolve_threshold is None else rule.resolve_threshold
            stay = _holds(rule.op, decide, stay_threshold)
            alert = alerts.get(key)
            if alert is None:
                alert = alerts[key] = Alert(rule.for_ms, rule.keep_firing_for_ms)
            state = alert.step(t, fire, stay)
            if state is not None:
                transitions.append(
                    {"rule": rule.name, "group": key, "t": t, "state": state, "value": value}
                )
    return evals, transitions
