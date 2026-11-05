"""Translate the rules of the reference subset (D12, D22) to a Prometheus rule file.

count -> count_over_time(sel[R]), sum -> sum_over_time(sel[R]),
rate -> sum_over_time(sel[R]) / R_s; with by, the window function goes inside
sum by (...) (...) and rate divides the group sum, in the order of section 4.5.
One group per evaluation interval. Rules outside the subset are rejected.

usage: rules2prom.py <rules.json> <out.yaml>
"""

import json
import re
import sys
from pathlib import Path

# The default bucket_ms S of section 4.4, which is the default interval_ms.
DEFAULT_INTERVAL_MS = 1000

_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_FIELDS = {
    "name",
    "tenant",
    "source",
    "match",
    "agg",
    "quantile",
    "field",
    "range_ms",
    "interval_ms",
    "by",
    "op",
    "threshold",
    "resolve_threshold",
    "for_ms",
    "keep_firing_for_ms",
}
_WINDOW = {"count": "count_over_time", "sum": "sum_over_time", "rate": "sum_over_time"}
_OPS = {">", ">=", "<", "<="}


class Unsupported(ValueError):
    """A rule outside the subset that promtool and vmalert compare."""


def _positive_ms(rule, field, default=None, zero=False):
    value = rule.get(field, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or (value == 0 and not zero):
        raise Unsupported(f"rule {rule.get('name')!r}: {field} {value!r}")
    return value


def duration(ms):
    """Return a Prometheus duration for ms milliseconds."""
    return f"{ms // 1000}s" if ms % 1000 == 0 else f"{ms}ms"


def number(x):
    """Return x as PromQL writes it; integral values without a decimal point."""
    if isinstance(x, int) or float(x).is_integer():
        return str(int(x))
    return repr(float(x))


def _escape(value):
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def selector(match):
    """Return the series selector of match: the metric name and the other labels sorted."""
    labels = ",".join(
        f'{k}="{_escape(match[k])}"' for k in sorted(match, key=str.encode) if k != "__name__"
    )
    return match["__name__"] + (f"{{{labels}}}" if labels else "")


def check(rule):
    """Raise Unsupported unless the rule is in the subset of D12."""
    name = rule.get("name")
    unknown = set(rule) - _FIELDS
    if unknown:
        raise Unsupported(f"rule {name!r}: unknown fields {sorted(unknown)}")
    if not isinstance(name, str) or not _NAME.match(name):
        raise Unsupported(f"rule name {name!r} is not an alert name")
    if rule.get("source") != "metrics":
        raise Unsupported(f"rule {name!r}: source {rule.get('source')!r} is not metrics")
    if rule.get("agg") not in _WINDOW:
        raise Unsupported(f"rule {name!r}: agg {rule.get('agg')!r} is not count, sum or rate")
    for field in ("quantile", "field", "resolve_threshold"):
        if field in rule:
            raise Unsupported(f"rule {name!r}: {field} has no Prometheus equivalent")
    match = rule.get("match")
    if (
        not isinstance(match, dict)
        or not isinstance(match.get("__name__"), str)
        or not _NAME.match(match["__name__"])
        or any(not _NAME.match(k) or not isinstance(v, str) for k, v in match.items())
    ):
        raise Unsupported(f"rule {name!r}: match {match!r} does not name a metric")
    by = rule.get("by")
    if by is not None and (
        not isinstance(by, list) or any(not isinstance(k, str) or not _NAME.match(k) for k in by)
    ):
        raise Unsupported(f"rule {name!r}: by {by!r}")
    if rule.get("op") not in _OPS:
        raise Unsupported(f"rule {name!r}: op {rule.get('op')!r}")
    threshold = rule.get("threshold")
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
        raise Unsupported(f"rule {name!r}: threshold {threshold!r}")
    _positive_ms(rule, "range_ms")
    _positive_ms(rule, "interval_ms", DEFAULT_INTERVAL_MS)
    _positive_ms(rule, "for_ms", 0, zero=True)
    _positive_ms(rule, "keep_firing_for_ms", 0, zero=True)


def expr(rule):
    """Return the PromQL expression of a rule of the subset."""
    check(rule)
    r = rule["range_ms"]
    window = f"{_WINDOW[rule['agg']]}({selector(rule['match'])}[{duration(r)}])"
    by = rule.get("by")
    if by is not None:
        labels = ", ".join(sorted(set(by), key=str.encode))
        window = f"sum by ({labels}) ({window})"
    if rule["agg"] == "rate":
        window = f"{window} / {number(r / 1000)}"
    return f"{window} {rule['op']} {number(rule['threshold'])}"


def common_interval(rules):
    """Return the one interval_ms of the rules; a replay compares a single grid of instants."""
    intervals = {rule.get("interval_ms", DEFAULT_INTERVAL_MS) for rule in rules}
    if len(intervals) != 1:
        raise Unsupported(f"the rules have the intervals {sorted(map(str, intervals))}, not one")
    return intervals.pop()


def translate(rules):
    """Return the YAML rule file of the rules: one group per interval, rules in file order."""
    tenants = {rule.get("tenant") for rule in rules}
    if len(tenants) != 1:
        raise Unsupported(f"the subset has one tenant, not {sorted(map(str, tenants))}")
    names = [rule.get("name") for rule in rules]
    if len(set(names)) != len(names):
        raise Unsupported("rule names are not unique")
    groups = {}
    for rule in rules:
        line = expr(rule)
        groups.setdefault(rule.get("interval_ms", DEFAULT_INTERVAL_MS), []).append((rule, line))
    out = ["groups:"]
    for interval in sorted(groups):
        out.append(f"  - name: {json.dumps('oteadero_' + duration(interval))}")
        out.append(f"    interval: {duration(interval)}")
        out.append("    rules:")
        for rule, line in groups[interval]:
            out.append(f"      - alert: {json.dumps(rule['name'])}")
            out.append(f"        expr: {json.dumps(line)}")
            if rule.get("for_ms", 0):
                out.append(f"        for: {duration(rule['for_ms'])}")
            if rule.get("keep_firing_for_ms", 0):
                out.append(f"        keep_firing_for: {duration(rule['keep_firing_for_ms'])}")
    return "\n".join(out) + "\n"


def translate_file(src, dst):
    rules = json.loads(Path(src).read_text())["rules"]
    Path(dst).write_text(translate(rules))


def main(argv):
    if len(argv) != 2:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    try:
        translate_file(argv[0], argv[1])
    except Unsupported as e:
        print(f"rules2prom: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
