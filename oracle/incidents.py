"""Incidents and idempotency keys of section 4.7, from the transitions of the oracle."""

import hashlib

from rules import FIRING, RESOLVED


def incident_id(rule, group, t_f):
    """Return the first 32 hex characters of SHA-256 of rule NUL group NUL T_f."""
    text = f"{rule}\x00{group}\x00{t_f}".encode("utf-8", "surrogateescape")
    return hashlib.sha256(text).hexdigest()[:32]


def notifications(transitions):
    """Return the expected notifications, one per incident and kind, in transition order.

    Each row has the fields of the notify body that the log fixes, plus its
    Idempotency-Key. A resolved belongs to the last firing of its (rule, group).
    """
    rows = []
    open_incidents = {}  # (rule, group) -> incident id
    for tr in transitions:
        pair = (tr["rule"], tr["group"])
        if tr["state"] == FIRING:
            incident = incident_id(tr["rule"], tr["group"], tr["t"])
            open_incidents[pair] = incident
            kind = FIRING
        elif tr["state"] == RESOLVED and pair in open_incidents:
            incident = open_incidents.pop(pair)
            kind = RESOLVED
        else:
            continue
        rows.append(
            {
                "incident": incident,
                "kind": kind,
                "rule": tr["rule"],
                "group": tr["group"],
                "t": tr["t"],
                "value": tr["value"],
                "key": f"{incident}/{kind}",
            }
        )
    return rows
