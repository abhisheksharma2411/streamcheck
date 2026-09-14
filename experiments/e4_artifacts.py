"""E4 -- how many artifacts a finding needs, and what one view alone can see.

The paper's claim is that these defects survive review because no single
artifact contains them. That is testable. Every finding records the artifacts
its evidence came from, so for any subset of artifacts we can ask which
findings are derivable from that subset alone -- a finding is derivable when
every artifact it cites is present.

Two measurements. How many artifacts each rule needs, and what a reviewer
holding one artifact, or one plausible pair, would find. If the duration
defects need three files and a reviewer holds one, the claim stands; if one
file turns out to be enough, it does not.
"""

from __future__ import annotations

from itertools import combinations

from _common import write

from streamcheck.checks import DURATION, RULES, analyse
from streamcheck.corpus import build_corpus

SEEDS = (1, 2, 3)


def artifacts_of(finding) -> set[str]:
    return {e.split(":")[0] for e in finding.evidence}


def main() -> None:
    per_rule: dict[str, dict] = {}
    all_findings = []

    for seed in SEEDS:
        for c in build_corpus(seed=seed, per_combination=2):
            if c.is_clean:
                continue
            for f in analyse(c.pipeline):
                arts = artifacts_of(f)
                all_findings.append((c.case_id, f.rule, f.family, frozenset(arts)))
                slot = per_rule.setdefault(f.rule, {
                    "n": 0, "family": f.family, "name": RULES[f.rule],
                    "artifact_counts": {}, "example": sorted(arts),
                })
                slot["n"] += 1
                k = str(len(arts))
                slot["artifact_counts"][k] = slot["artifact_counts"].get(k, 0) + 1

    # Which artifacts exist across the corpus, normalised: each pipeline names
    # its SQL file after its shape, so those are pooled into one "job SQL" kind.
    def kind(a: str) -> str:
        if a.endswith(".sql"):
            return "job SQL"
        if a.endswith(".java"):
            return "effect code"
        return a

    kinds = sorted({kind(a) for _, _, _, arts in all_findings for a in arts})
    total = len(all_findings)
    dur_total = sum(1 for _, _, fam, _ in all_findings if fam == DURATION)

    def derivable(subset: set[str]) -> tuple[int, int]:
        """Findings whose every cited artifact is in ``subset``."""
        n = sum(1 for _, _, _, arts in all_findings
                if {kind(a) for a in arts} <= subset)
        d = sum(1 for _, _, fam, arts in all_findings
                if fam == DURATION and {kind(a) for a in arts} <= subset)
        return n, d

    singles = {}
    for k in kinds:
        n, d = derivable({k})
        singles[k] = {"findings": n, "share": n / total,
                      "duration_findings": d,
                      "duration_share": d / dur_total if dur_total else 0.0}

    pairs = {}
    for a, b in combinations(kinds, 2):
        n, d = derivable({a, b})
        pairs[f"{a} + {b}"] = {"findings": n, "share": n / total,
                               "duration_findings": d,
                               "duration_share": d / dur_total if dur_total else 0.0}

    triples = {}
    for combo in combinations(kinds, 3):
        n, d = derivable(set(combo))
        triples[" + ".join(combo)] = {
            "findings": n, "share": n / total,
            "duration_findings": d,
            "duration_share": d / dur_total if dur_total else 0.0,
        }

    best_single = max(singles.values(), key=lambda v: v["findings"])["findings"]
    best_pair = max(pairs.values(), key=lambda v: v["findings"])["findings"]
    best_triple = max(triples.values(), key=lambda v: v["findings"])["findings"]
    best_triple_dur = max(triples.values(), key=lambda v: v["duration_findings"])

    # How many artifacts each finding needs, pooled over the corpus.
    need_hist: dict[str, int] = {}
    dur_need_hist: dict[str, int] = {}
    for _, _, fam, arts in all_findings:
        k = str(len({kind(a) for a in arts}))
        need_hist[k] = need_hist.get(k, 0) + 1
        if fam == DURATION:
            dur_need_hist[k] = dur_need_hist.get(k, 0) + 1

    write("e4_artifacts.json", {
        "config": {"seeds": SEEDS},
        "totals": {"findings": total, "duration_findings": dur_total,
                   "artifact_kinds": kinds},
        "per_rule": dict(sorted(per_rule.items())),
        "single_artifact": dict(sorted(singles.items(), key=lambda kv: -kv[1]["findings"])),
        "artifact_pairs": dict(sorted(pairs.items(), key=lambda kv: -kv[1]["findings"])),
        "artifact_triples": dict(sorted(triples.items(), key=lambda kv: -kv[1]["findings"])[:10]),
        "artifacts_needed_histogram": dict(sorted(need_hist.items())),
        "duration_artifacts_needed_histogram": dict(sorted(dur_need_hist.items())),
        "headline": {
            "best_single_artifact_findings": best_single,
            "best_single_artifact_share": best_single / total,
            "best_pair_findings": best_pair,
            "best_pair_share": best_pair / total,
            "best_triple_findings": best_triple,
            "best_triple_share": best_triple / total,
            "best_triple_for_duration": best_triple_dur,
            "duration_findings_from_any_single": max(
                v["duration_findings"] for v in singles.values()),
            "duration_findings_from_any_pair": max(
                v["duration_findings"] for v in pairs.values()),
        },
    })


if __name__ == "__main__":
    main()
