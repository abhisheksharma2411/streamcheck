"""E3 -- witnesses against a real broker, each with its own control.

The interpreter in :mod:`executor` and the checks in :mod:`checks` are both
ours, so their agreement is worth little. Here every witness is replayed
against a real Kafka broker, a pipeline process that is really killed with
SIGKILL, and a provider that really forgets keys on a timer. Nothing about the
recovery is modelled: what gets redelivered is whatever the broker decides to
redeliver from the last committed offset.

Three arms, because two were not enough. Each witness runs as configured; then
with a provider that never forgets the key; then with a provider whose key
lifetime is finite but comfortably longer than the replay takes. The first
control rules out the replay alone being responsible. The second rules out the
harness: the kill, the group eviction and the restart cost about six seconds of
real time, and an earlier version of this experiment scaled key lifetimes below
that, so every key expired during the restart and defects whose configured
downtime was nineteen milliseconds appeared to reproduce. A sufficient-lifetime
arm that charges once is what shows the harness is not simply timing everything
out.

A finding is called attributable only when the defective run charges twice and
both controls charge once. The gap the provider actually observed between the
two attempts is recorded with every run, so the comparison against the key
lifetime can be checked rather than assumed.
"""

from __future__ import annotations

import random
from dataclasses import replace

from _common import write

from streamcheck.checks import analyse
from streamcheck.corpus import build_corpus
from streamcheck.live import (
    HARNESS_RESTART_MS,
    broker_available,
    live_scale,
    run_live,
)
from streamcheck.witness import synthesise_all

#: Witnesses per rule. Each arm costs the better part of a minute and there are
#: three of them, so the sample is capped rather than exhaustive.
PER_RULE = 2
SEED = 7


def main() -> None:
    if not broker_available():
        write("e3_live.json", {
            "status": "skipped",
            "reason": "no Kafka broker reachable; run `docker compose up -d` first",
        })
        return

    cases = build_corpus(seed=SEED, per_combination=2)
    rng = random.Random(SEED)

    # Collect candidate (case, witness) pairs, grouped by rule.
    by_rule: dict[str, list] = {}
    for c in cases:
        if c.is_clean:
            continue
        witnesses = synthesise_all(c.pipeline, analyse(c.pipeline))
        for key, w in witnesses.items():
            by_rule.setdefault(w.rule, []).append((c, w))

    runs = []
    for rule, pairs in sorted(by_rule.items()):
        rng.shuffle(pairs)
        for case, w in pairs[:PER_RULE]:
            scaled = w.scaled(live_scale(w))
            defective = run_live(case.pipeline, scaled)
            control = run_live(case.pipeline, replace(scaled, provider_key_ttl=None))
            # A lifetime long enough to survive this replay, but finite: if the
            # harness were expiring keys on its own, this would charge twice too.
            sufficient_ttl = int(max(scaled.provider_key_ttl or 0,
                                     (scaled.faults[0].down_for if scaled.faults else 0))
                                 + 10 * HARNESS_RESTART_MS)
            sufficient = run_live(case.pipeline,
                                  replace(scaled, provider_key_ttl=sufficient_ttl))
            runs.append({
                "sufficient_ttl_ms": sufficient_ttl,
                "sufficient": sufficient.as_dict(),
                "rule": rule,
                "case_id": case.case_id,
                "shape": case.shape,
                "mutation": case.mutation,
                "scale": scaled.scale,
                "scaled_key_ttl_ms": scaled.provider_key_ttl,
                "scaled_down_for_ms": scaled.faults[0].down_for if scaled.faults else 0,
                "expect_charges": scaled.expect_charges,
                "expect_invocations": scaled.expect_invocations,
                "defective": defective.as_dict(),
                "control": control.as_dict(),
                # The comparison that matters: the defect is attributable to the
                # key lifetime only if removing it removes the second charge.
                "attributable": (
                    defective.charges_confirmed
                    and not control.charges_confirmed
                    and not sufficient.charges_confirmed
                ),
                # True when the provider-observed gap exceeds the key lifetime,
                # which is the mechanism the rule claims.
                "gap_exceeds_ttl": (
                    scaled.provider_key_ttl is not None
                    and defective.observed_gap_ms > scaled.provider_key_ttl
                ),
            })
            print(f"  {rule} {case.case_id}: defective={defective.charges} "
                  f"inf-ttl={control.charges} long-ttl={sufficient.charges} "
                  f"gap={defective.observed_gap_ms:.0f}ms ttl={scaled.provider_key_ttl}ms "
                  f"attributable={runs[-1]['attributable']}")

    per_rule = {}
    for r in runs:
        slot = per_rule.setdefault(r["rule"], {"n": 0, "reproduced": 0,
                                               "double_charge": 0, "attributable": 0,
                                               "sufficient_charged_twice": 0,
                                               "errors": 0})
        slot["n"] += 1
        slot["reproduced"] += int(r["defective"]["confirmed"])
        slot["double_charge"] += int(r["defective"]["charges_confirmed"])
        slot["attributable"] += int(r["attributable"])
        slot["sufficient_charged_twice"] += int(r["sufficient"]["charges_confirmed"])
        slot["errors"] += int(bool(r["defective"]["error"]))

    write("e3_live.json", {
        "status": "ok",
        "infrastructure": "apache/kafka 3.9.0 (KRaft), SIGKILL restart, HTTP provider with key TTL",
        "config": {"per_rule": PER_RULE, "seed": SEED,
                   "harness_restart_ms": HARNESS_RESTART_MS,
                   "target_scaled_key_ttl_ms": 30_000},
        "runs": runs,
        "per_rule": per_rule,
        "totals": {
            "runs": len(runs),
            "reproduced": sum(v["reproduced"] for v in per_rule.values()),
            "double_charge": sum(v["double_charge"] for v in per_rule.values()),
            "attributable": sum(v["attributable"] for v in per_rule.values()),
            "sufficient_charged_twice": sum(
                v["sufficient_charged_twice"] for v in per_rule.values()),
            "errors": sum(v["errors"] for v in per_rule.values()),
        },
    })


if __name__ == "__main__":
    main()
