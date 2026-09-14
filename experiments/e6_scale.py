"""E6 -- is a witness still the same witness when it is compressed?

Witnesses are executed at a fraction of the durations they were synthesised
with, because a replay horizon of hours cannot be waited out once per case, let
alone hundreds of times. The justification is that every duration check is an
inequality between durations, so scaling them all by one positive factor leaves
each comparison unchanged.

That is an argument, and arguments about one's own evaluation method deserve a
measurement. Each witness is run at factors spanning four orders of magnitude,
and a witness whose verdict changes with the factor would mean the compression
is doing something the argument says it cannot.
"""

from __future__ import annotations

from _common import write

from streamcheck.checks import analyse
from streamcheck.corpus import build_corpus
from streamcheck.executor import run
from streamcheck.witness import synthesise_all

SEEDS = (1, 2)
FACTORS = (1.0, 0.1, 0.01, 0.001, 0.0001)


def main() -> None:
    rows = []
    disagreements = []
    for seed in SEEDS:
        for c in build_corpus(seed=seed, per_combination=1):
            if c.is_clean:
                continue
            findings = analyse(c.pipeline)
            for key, w in synthesise_all(c.pipeline, findings).items():
                verdicts = {}
                for factor in FACTORS:
                    r = run(c.pipeline, w.scaled(factor))
                    verdicts[str(factor)] = {
                        "charges": r.charges,
                        "invocations": r.invocations,
                        "confirmed": r.confirmed,
                        "double_charge": r.charges_confirmed,
                    }
                stable = len({(v["confirmed"], v["double_charge"]) for v in verdicts.values()}) == 1
                rows.append({"case_id": c.case_id, "rule": w.rule, "key": key,
                             "stable": stable, "verdicts": verdicts})
                if not stable:
                    disagreements.append(rows[-1])

    write("e6_scale.json", {
        "config": {"seeds": SEEDS, "factors": FACTORS},
        "totals": {
            "witnesses": len(rows),
            "stable": sum(1 for r in rows if r["stable"]),
            "unstable": len(disagreements),
            "stability": (sum(1 for r in rows if r["stable"]) / len(rows)) if rows else 0.0,
        },
        "disagreements": disagreements[:20],
    })


if __name__ == "__main__":
    main()
