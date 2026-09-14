"""E7 -- how much of the plausible configuration space is defective?

Not a field study, and not presented as one. What it asks is narrower and still
worth asking: if a team picks each setting independently from the values such
settings usually take -- a checkpoint interval from the handful people use, a
recovery assumption from a runbook, a key lifetime from what providers
document -- how often does the combination fail one of the duration relations?

Every value in the menus below is one a practitioner would recognise. None is
pathological. The point of the sweep is that the defect is not produced by a
strange choice anywhere; it is produced by ordinary choices meeting.
"""

from __future__ import annotations

from itertools import product

from _common import write

from streamcheck.checks import DURATION, analyse
from streamcheck.corpus import _build
from streamcheck.ir import Guarantee

SECOND, MINUTE, HOUR, DAY = 1000, 60_000, 3_600_000, 86_400_000

CHECKPOINT = (10 * SECOND, 30 * SECOND, 60 * SECOND, 5 * MINUTE)
DOWNTIME = (5 * MINUTE, 30 * MINUTE, 2 * HOUR, 8 * HOUR, 24 * HOUR)
KEY_TTL = (1 * HOUR, 6 * HOUR, 24 * HOUR, 7 * DAY)
STATE_TTL = (1 * HOUR, 24 * HOUR, 7 * DAY, 30 * DAY)
TXN_TIMEOUT = (1 * MINUTE, 15 * MINUTE, 60 * MINUTE)


def main() -> None:
    import random

    base = _build("capture", random.Random(0))
    rows = []
    for cp, down, ttl, sttl, txn in product(CHECKPOINT, DOWNTIME, KEY_TTL, STATE_TTL, TXN_TIMEOUT):
        p = _build("capture", random.Random(0))
        p.engine.checkpoint_interval = cp
        p.engine.min_pause_between_checkpoints = 0
        p.engine.max_downtime = down
        p.engine.state_ttl = sttl
        p.engine.transaction_timeout = txn
        p.engine.guarantee = Guarantee.EXACTLY_ONCE
        for i, o in enumerate(p.operators):
            if o.kind == "dedup":
                from dataclasses import replace
                p.operators[i] = replace(o, state_ttl=sttl)
        from dataclasses import replace as _r
        p.effects[0] = _r(p.effects[0], provider_key_ttl=ttl, client_retry_window=5 * MINUTE)

        findings = analyse(p)
        rules = sorted({f.rule for f in findings})
        rows.append({
            "checkpoint_ms": cp, "downtime_ms": down, "key_ttl_ms": ttl,
            "state_ttl_ms": sttl, "txn_timeout_ms": txn,
            "replay_horizon_ms": p.replay_horizon(),
            "rules": rules,
            "any": bool(rules),
            "duration_only": bool([f for f in findings if f.family == DURATION]),
        })

    n = len(rows)
    by_rule: dict[str, int] = {}
    for r in rows:
        for rule in r["rules"]:
            by_rule[rule] = by_rule.get(rule, 0) + 1

    # Which single choice matters most: hold everything else and vary one.
    sensitivity = {}
    for field, menu in (("downtime_ms", DOWNTIME), ("key_ttl_ms", KEY_TTL),
                        ("checkpoint_ms", CHECKPOINT), ("state_ttl_ms", STATE_TTL),
                        ("txn_timeout_ms", TXN_TIMEOUT)):
        sensitivity[field] = {
            str(v): sum(1 for r in rows if r[field] == v and r["any"])
                    / max(1, sum(1 for r in rows if r[field] == v))
            for v in menu
        }

    # The share defective is a property of these menus, not of the field, and
    # quoting it alone would overstate what was measured. The threshold is the
    # transferable part: above a certain recovery assumption no permitted
    # transaction timeout is enough, whatever else is configured.
    cap = base.engine.broker_max_transaction_timeout
    thresholds = {
        f"checkpoint={cp // 1000}s,restart={base.engine.restart_delay // 1000}s":
            (cap - cp - base.engine.restart_delay) / 60_000.0
        for cp in CHECKPOINT
    }

    write("e7_prevalence.json", {
        "sc106_threshold_minutes": thresholds,
        "sc106_note": (
            "recovery assumptions above these figures cannot be made transactionally "
            "safe by any setting of the job, because the required transaction timeout "
            "exceeds the broker's cap"
        ),
        "config": {
            "checkpoint_ms": CHECKPOINT, "downtime_ms": DOWNTIME, "key_ttl_ms": KEY_TTL,
            "state_ttl_ms": STATE_TTL, "txn_timeout_ms": TXN_TIMEOUT,
            "note": "menus of values practitioners use; not a field distribution",
        },
        "totals": {
            "configurations": n,
            "with_any_finding": sum(1 for r in rows if r["any"]),
            "share_defective": sum(1 for r in rows if r["any"]) / n,
            "with_duration_finding": sum(1 for r in rows if r["duration_only"]),
            "share_duration": sum(1 for r in rows if r["duration_only"]) / n,
        },
        "by_rule": dict(sorted(by_rule.items())),
        "sensitivity": sensitivity,
        "rows": rows,
    })


if __name__ == "__main__":
    main()
