"""E1 -- detection over the corpus.

Recall against injected defects and false positives on the negatives. The
number to be careful about is recall on the mutations named after the rule
meant to catch them: the same author wrote both, so a high score there
measures implementation correctness, not detection power, and the paper says so
rather than banking it.

The boundary cases carry the weight. Four mutations put a threshold within one
second on either side of correct, so distinguishing them requires the replay
horizon to be computed from all of its terms. A checker that got the arithmetic
wrong would fail them whichever way it erred, and no amount of knowing the
mutation names would help.
"""

from __future__ import annotations

import time

from _common import write

from streamcheck.checks import RULES, analyse
from streamcheck.corpus import (
    EXPECTED_RULE,
    NEGATIVE_MUTATIONS,
    build_corpus,
    corpus_stats,
)

SEEDS = (1, 2, 3)
PER_COMBINATION = 2


def main() -> None:
    per_rule: dict[str, dict] = {}
    per_mutation: dict[str, dict] = {}
    tp = fn = fp = tn = 0
    analysed = 0
    elapsed = 0.0

    for seed in SEEDS:
        cases = build_corpus(seed=seed, per_combination=PER_COMBINATION)
        for c in cases:
            t0 = time.perf_counter()
            findings = analyse(c.pipeline)
            elapsed += time.perf_counter() - t0
            analysed += 1
            rules = {f.rule for f in findings}
            slot = per_mutation.setdefault(
                c.mutation, {"n": 0, "correct": 0, "findings": 0, "rules": {}}
            )
            slot["n"] += 1
            slot["findings"] += len(findings)
            for r in rules:
                slot["rules"][r] = slot["rules"].get(r, 0) + 1

            if c.mutation in NEGATIVE_MUTATIONS:
                if rules:
                    fp += 1
                else:
                    tn += 1
                    slot["correct"] += 1
            else:
                expected = EXPECTED_RULE[c.mutation]
                r = per_rule.setdefault(expected, {"n": 0, "hit": 0, "extra": 0})
                r["n"] += 1
                if expected in rules:
                    tp += 1
                    r["hit"] += 1
                    slot["correct"] += 1
                else:
                    fn += 1
                r["extra"] += len(rules - {expected})

    positives = tp + fn
    negatives = fp + tn
    write("e1_detection.json", {
        "config": {"seeds": SEEDS, "per_combination": PER_COMBINATION},
        "corpus": corpus_stats(build_corpus(seed=SEEDS[0], per_combination=PER_COMBINATION)),
        "totals": {
            "cases": analysed,
            "positives": positives, "negatives": negatives,
            "true_positive": tp, "false_negative": fn,
            "false_positive": fp, "true_negative": tn,
            "recall": tp / positives if positives else 0.0,
            "false_positive_rate": fp / negatives if negatives else 0.0,
        },
        "per_rule": {k: {**v, "name": RULES[k], "recall": v["hit"] / v["n"] if v["n"] else 0.0}
                     for k, v in sorted(per_rule.items())},
        "per_mutation": dict(sorted(per_mutation.items())),
        "cost": {
            "pipelines_analysed": analysed,
            "total_seconds": elapsed,
            "mean_us_per_pipeline": 1_000_000 * elapsed / analysed if analysed else 0.0,
        },
    })


if __name__ == "__main__":
    main()
