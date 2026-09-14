"""E5 -- what would it take to find these by testing?

The alternative to analysing the configuration is injecting faults and watching
for a double charge. That works, and the question is what it costs. A random
tester picks a moment to kill the job and a duration to stay down; the duration
defects only show when the downtime happens to exceed a lifetime that may be
hours long, so the interesting quantity is how often a plausible draw lands in
that region.

The tester here is given every advantage: it knows to kill the job, it kills at
the worst moment rather than a random one, and it checks the provider directly
rather than waiting for a customer complaint. Only the downtime is drawn, from
a distribution centred on the recovery times an operations team would actually
see. Anything it fails to find under those conditions, it would not find under
worse.
"""

from __future__ import annotations

import random
import statistics

from _common import load, write

from streamcheck.checks import DURATION, analyse
from streamcheck.corpus import build_corpus
from streamcheck.executor import run
from streamcheck.witness import synthesise_all

SEEDS = (1, 2, 3)
TRIALS = 200
#: Downtimes a tester would try, log-uniform between ten seconds and a ceiling.
MIN_DOWN_MS = 10_000
MAX_DOWN_MS = 4 * 3_600_000

#: The ceiling is the whole question. A tester draws its downtimes from what it
#: has seen -- last quarter's incidents, the recovery SLO -- and a defect whose
#: trigger is a longer outage than any yet observed is outside the distribution
#: it samples from. Sweeping the ceiling asks how the hit rate depends on
#: having guessed high enough, which no number of trials can fix.
CEILINGS_MS = (
    5 * 60_000,        # a tester that has only seen quick restarts
    30 * 60_000,
    2 * 3_600_000,
    4 * 3_600_000,
    12 * 3_600_000,
    48 * 3_600_000,    # a tester willing to simulate a two-day outage
)


def main() -> None:
    rng = random.Random(11)
    per_rule: dict[str, dict] = {}

    for seed in SEEDS:
        for c in build_corpus(seed=seed, per_combination=1):
            if c.is_clean:
                continue
            findings = analyse(c.pipeline)
            witnesses = synthesise_all(c.pipeline, findings)
            for f in findings:
                w = witnesses.get(f"{f.rule}:{f.subject}")
                if w is None:
                    continue
                slot = per_rule.setdefault(f.rule, {
                    "family": f.family, "cases": 0, "trials": 0, "hits": 0,
                    "cases_ever_found": 0,
                })
                slot["cases"] += 1
                hits = 0
                for _ in range(TRIALS):
                    # Same witness, but the downtime is drawn rather than chosen.
                    lo, hi = MIN_DOWN_MS, MAX_DOWN_MS
                    down = int(lo * (hi / lo) ** rng.random())
                    trial = w
                    if trial.faults:
                        from dataclasses import replace

                        from streamcheck.witness import Fault
                        trial = replace(trial, faults=[Fault(trial.faults[0].at, down)])
                    if run(c.pipeline, trial).charges_confirmed:
                        hits += 1
                slot["trials"] += TRIALS
                slot["hits"] += hits
                slot["cases_ever_found"] += int(hits > 0)

    for v in per_rule.values():
        p = v["hits"] / v["trials"] if v["trials"] else 0.0
        v["hit_rate"] = p
        v["expected_trials_to_find"] = (1 / p) if p > 0 else None
        v["cases_never_found_rate"] = 1 - (v["cases_ever_found"] / v["cases"]) if v["cases"] else 0.0

    # How the hit rate depends on the tester's assumed worst case.
    ceiling_sweep = []
    rng2 = random.Random(23)
    for ceiling in CEILINGS_MS:
        hits = trials = cases = found_cases = 0
        for seed in SEEDS:
            for c in build_corpus(seed=seed, per_combination=1):
                if c.is_clean:
                    continue
                findings = analyse(c.pipeline)
                witnesses = synthesise_all(c.pipeline, findings)
                for f in findings:
                    if f.family != DURATION:
                        continue
                    w = witnesses.get(f"{f.rule}:{f.subject}")
                    if w is None or not w.faults:
                        continue
                    cases += 1
                    local = 0
                    for _ in range(50):
                        lo, hi = MIN_DOWN_MS, ceiling
                        if hi <= lo:
                            down = lo
                        else:
                            down = int(lo * (hi / lo) ** rng2.random())
                        from dataclasses import replace

                        from streamcheck.witness import Fault
                        trial = replace(w, faults=[Fault(w.faults[0].at, down)])
                        if run(c.pipeline, trial).charges_confirmed:
                            local += 1
                        trials += 1
                    hits += local
                    found_cases += int(local > 0)
        ceiling_sweep.append({
            "ceiling_ms": ceiling,
            "ceiling_hours": ceiling / 3_600_000,
            "hit_rate": hits / trials if trials else 0.0,
            "cases": cases,
            "cases_found": found_cases,
            "cases_found_share": found_cases / cases if cases else 0.0,
        })

    analysis_us = load("e1_detection.json")["cost"]["mean_us_per_pipeline"]
    # One trial costs a real kill, a real wait and a real restart. E3 measures
    # that directly; fall back to a conservative estimate if it has not run.
    try:
        live = load("e3_live.json")
        walls = [r["defective"]["wall_ms"] for r in live.get("runs", [])]
        trial_ms = statistics.mean(walls) if walls else 25_000.0
        trial_source = "measured in E3"
    except FileNotFoundError:
        trial_ms = 25_000.0
        trial_source = "estimate; E3 had not run"

    write("e5_random.json", {
        "config": {"seeds": SEEDS, "trials_per_case": TRIALS,
                   "downtime_ms": [MIN_DOWN_MS, MAX_DOWN_MS],
                   "downtime_distribution": "log-uniform"},
        "per_rule": dict(sorted(per_rule.items())),
        "ceiling_sweep": ceiling_sweep,
        "cost_comparison": {
            "analysis_us_per_pipeline": analysis_us,
            "one_trial_ms": trial_ms,
            "trial_cost_source": trial_source,
            "rules": {
                r: {
                    "expected_trials": v["expected_trials_to_find"],
                    "expected_seconds": (v["expected_trials_to_find"] * trial_ms / 1000.0
                                         if v["expected_trials_to_find"] else None),
                }
                for r, v in sorted(per_rule.items())
            },
        },
    })


if __name__ == "__main__":
    main()
