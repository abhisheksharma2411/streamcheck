"""E2 -- do the synthesised witnesses reproduce their defect?

Every witness in the corpus, run through the interpreter. Two things are
counted separately and the distinction was forced by the results rather than
planned: whether the predicted observable appeared at all, and whether money
actually moved twice. Some defects produce a second call to the provider that
its idempotency then absorbs. Those are worth reporting and are not double
charges, and two rules were demoted to medium severity when their witnesses
showed exactly that.

This arm is cheap enough to run exhaustively, which is its only advantage over
E3. Both the checker and the interpreter are ours, so agreement between them is
weak evidence; the real broker in E3 is what the claim rests on.
"""

from __future__ import annotations

from _common import write

from streamcheck.checks import DURATION, RULES, analyse
from streamcheck.corpus import build_corpus
from streamcheck.executor import run
from streamcheck.witness import coverage, synthesise_all

SEEDS = (1, 2, 3)


def main() -> None:
    per_rule: dict[str, dict] = {}
    totals = {"findings": 0, "witnessed": 0, "reproduced": 0, "double_charge": 0}
    cov_acc = {"findings": 0, "witnessed": 0, "duration_findings": 0, "duration_witnessed": 0}

    for seed in SEEDS:
        for c in build_corpus(seed=seed, per_combination=2):
            if c.is_clean:
                continue
            findings = analyse(c.pipeline)
            witnesses = synthesise_all(c.pipeline, findings)
            cov = coverage(findings, witnesses)
            for k in cov_acc:
                cov_acc[k] += cov[k]

            for f in findings:
                totals["findings"] += 1
                slot = per_rule.setdefault(f.rule, {
                    "name": RULES[f.rule], "family": f.family, "findings": 0,
                    "witnessed": 0, "reproduced": 0, "double_charge": 0,
                })
                slot["findings"] += 1
                key = f"{f.rule}:{f.subject}"
                w = witnesses.get(key)
                if w is None:
                    continue
                totals["witnessed"] += 1
                slot["witnessed"] += 1
                r = run(c.pipeline, w)
                totals["reproduced"] += int(r.confirmed)
                totals["double_charge"] += int(r.charges_confirmed)
                slot["reproduced"] += int(r.confirmed)
                slot["double_charge"] += int(r.charges_confirmed)

    write("e2_witness.json", {
        "config": {"seeds": SEEDS},
        "totals": {
            **totals,
            "witness_coverage": totals["witnessed"] / totals["findings"] if totals["findings"] else 0.0,
            "reproduction_rate": totals["reproduced"] / totals["witnessed"] if totals["witnessed"] else 0.0,
            "double_charge_rate": totals["double_charge"] / totals["witnessed"] if totals["witnessed"] else 0.0,
        },
        "coverage": {
            **cov_acc,
            "coverage": cov_acc["witnessed"] / cov_acc["findings"] if cov_acc["findings"] else 0.0,
            "duration_coverage": (cov_acc["duration_witnessed"] / cov_acc["duration_findings"]
                                  if cov_acc["duration_findings"] else 0.0),
        },
        "per_rule": dict(sorted(per_rule.items())),
        "sufficient_alone": sorted(
            r for r, v in per_rule.items()
            if v["witnessed"] and v["double_charge"] == v["witnessed"]
        ),
        "needs_a_second_failure": sorted(
            r for r, v in per_rule.items()
            if v["witnessed"] and v["double_charge"] == 0
        ),
    })


if __name__ == "__main__":
    main()
