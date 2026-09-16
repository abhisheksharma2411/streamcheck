"""Check every number in the paper against the results files.

The failure this exists to prevent is a figure surviving a re-run: a number is
written into the text, an experiment changes, and nothing complains. Each entry
names a claim, the value the paper states, and where it should come from. Run
after every re-run and before every release.

Usage:  python scripts/verify_claims.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
PAPER = ROOT / "paper" / "streamcheck.tex"


def load(name: str) -> dict | None:
    p = RESULTS / name
    return json.loads(p.read_text()) if p.exists() else None


def pct(x: float) -> float:
    return round(100 * x, 1)


def build_claims() -> list[tuple[str, object, object]]:
    e1, e2, e4 = load("e1_detection.json"), load("e2_witness.json"), load("e4_artifacts.json")
    e5, e6, e7 = load("e5_random.json"), load("e6_scale.json"), load("e7_prevalence.json")
    e8, e3 = load("e8_broker.json"), load("e3_live.json")
    e9 = load("e9_example.json")
    c: list[tuple[str, object, object]] = []

    if e1:
        t = e1["totals"]
        c.append(("corpus size", 630, t["cases"]))
        c.append(("negatives", 150, t["negatives"]))
        c.append(("injected defects", 480, t["positives"]))
        c.append(("recall %", 100.0, pct(t["recall"])))
        c.append(("false positives", 0, t["false_positive"]))
        c.append(("analysis us per pipeline", 3.8, round(e1["cost"]["mean_us_per_pipeline"], 1)))
        c.append(("shapes", 5, e1["corpus"]["shapes"]))
        c.append(("mutations", 21, e1["corpus"]["mutations"]))

    if e2:
        t = e2["totals"]
        c.append(("findings", 515, t["findings"]))
        c.append(("witnessed", 335, t["witnessed"]))
        c.append(("witness coverage %", 65.0, pct(t["witness_coverage"])))
        c.append(("duration witness coverage %", 70.5, pct(e2["coverage"]["duration_coverage"])))
        c.append(("reproduction %", 100.0, pct(t["reproduction_rate"])))
        c.append(("double charge count", 245, t["double_charge"]))
        c.append(("double charge %", 73.1, pct(t["double_charge_rate"])))
        c.append(("rules sufficient alone", 6, len(e2["sufficient_alone"])))
        c.append(("rules needing a second failure",
                  ["SC006", "SC103"], e2["needs_a_second_failure"]))

    if e4:
        h, tot = e4["headline"], e4["totals"]
        c.append(("duration findings", 305, tot["duration_findings"]))
        c.append(("findings needing 3 artifacts", 210,
                  e4["artifacts_needed_histogram"].get("3")))
        c.append(("duration findings needing 3 artifacts", 210,
                  e4["duration_artifacts_needed_histogram"].get("3")))
        c.append(("best single artifact findings", 155, h["best_single_artifact_findings"]))
        c.append(("best single artifact %", 30.1, pct(h["best_single_artifact_share"])))
        c.append(("best single artifact duration findings", 35,
                  h["duration_findings_from_any_single"]))
        c.append(("best single artifact duration %", 11.5,
                  pct(h["duration_findings_from_any_single"] / tot["duration_findings"])))
        c.append(("best pair findings", 185, h["best_pair_findings"]))
        c.append(("best pair %", 35.9, pct(h["best_pair_share"])))
        c.append(("best pair duration findings", 65, h["duration_findings_from_any_pair"]))
        c.append(("best pair duration %", 21.3,
                  pct(h["duration_findings_from_any_pair"] / tot["duration_findings"])))
        c.append(("best triple findings", 245, h["best_triple_findings"]))
        c.append(("best triple %", 47.6, pct(h["best_triple_share"])))

    if e5:
        by = {round(r["ceiling_hours"], 2): r for r in e5["ceiling_sweep"]}
        for hours, found, rate in ((0.08, 15, 2.9), (0.5, 59, 17.8), (2.0, 59, 26.5),
                                   (4.0, 71, 29.5), (12.0, 100, 38.6), (48.0, 100, 46.2)):
            row = by[hours]
            c.append((f"testing ceiling {hours}h cases found %", float(found),
                      float(round(100 * row["cases_found_share"]))))
            c.append((f"testing ceiling {hours}h hit rate %", rate, pct(row["hit_rate"])))

    if e6:
        t = e6["totals"]
        c.append(("scale-stable witnesses", 114, t["witnesses"]))
        c.append(("scale instability", 0, t["unstable"]))
        c.append(("compression factors", 5, len(e6["config"]["factors"])))

    if e7:
        t = e7["totals"]
        c.append(("configurations swept", 960, t["configurations"]))
        c.append(("share defective %", 86.7, pct(t["share_defective"])))
        c.append(("SC106 threshold at 60s checkpointing (min)", 13.8,
                  round(e7["sc106_threshold_minutes"]["checkpoint=60s,restart=10s"], 1)))
        lows = [round(v, 1) for v in e7["sc106_threshold_minutes"].values()]
        c.append(("SC106 threshold range low", 9.8, min(lows)))
        c.append(("SC106 threshold range high", 14.7, max(lows)))

    if e8:
        c.append(("broker transaction timeout cap ms", 900000, e8["transaction_max_timeout_ms"]))
        c.append(("cap is the broker default", True, e8["is_default"]))

    if e3 and e3.get("status") == "ok":
        t = e3["totals"]
        c.append(("live witness runs", 16, t["runs"]))
        c.append(("live executions (three arms)", 48, t["runs"] * 3))
        c.append(("live errors", 0, t["errors"]))
        c.append(("live reproduced", 16, t["reproduced"]))
        c.append(("live double charges", 12, t["double_charge"]))
        c.append(("live attributable to lifetime", 6, t["attributable"]))
        c.append(("live sufficient-arm double charges", 6, t["sufficient_charged_twice"]))
        c.append(("harness restart overhead ms", 6000.0, e3["config"]["harness_restart_ms"]))
        c.append(("scaled key lifetime ms", 30000, e3["config"]["target_scaled_key_ttl_ms"]))
        # The mechanism: the gap exceeds the lifetime exactly for the lifetime
        # defects, and for nothing else.
        exceed = {r["rule"] for r in e3["runs"] if r["gap_exceeds_ttl"]}
        c.append(("rules where the gap exceeds the lifetime",
                  ["SC101", "SC102", "SC105"], sorted(exceed)))
        c.append(("runs where the gap exceeds the lifetime", 6,
                  sum(1 for r in e3["runs"] if r["gap_exceeds_ttl"])))
        attributable_rules = sorted({r["rule"] for r in e3["runs"] if r["attributable"]})
        c.append(("attributable rules", ["SC101", "SC102", "SC105"], attributable_rules))
    if e9:
        h = e9["headline"]
        c.append(("example checkpoint interval, seconds", 60,
                  h["checkpoint_interval_seconds"]))
        c.append(("example restart, seconds", 30, h["restart_seconds"]))
        c.append(("example worst recovery, minutes", 45.0,
                  h["worst_recovery_minutes"]))
        c.append(("example key lifetime, hours", 24.0, h["key_ttl_hours"]))
        c.append(("example replay horizon, hours", 1.54,
                  h["replay_horizon_hours"]))
        c.append(("example lifetime needed, minutes", 46.5,
                  h["lifetime_needed_minutes"]))
        c.append(("example lifetime configured, minutes", 15.0,
                  h["lifetime_configured_minutes"]))
        c.append(("the horizon fits inside the key", True,
                  h["horizon_inside_the_key"]))
        c.append(("the transaction lifetime does not fit", False,
                  h["lifetime_is_enough"]))

    if e3:
        # The live arm runs real processes against a real broker, so the gap
        # between the two attempts is measured rather than chosen. The paper
        # quotes the tightest one -- the run that cleared the scaled key
        # lifetime by the smallest margin -- because that is the case where
        # the two arms are hardest to tell apart.
        over = [r for r in e3["runs"] if r.get("gap_exceeds_ttl")]
        if over:
            tightest = min(r["defective"]["observed_gap_ms"] for r in over)
            c.append(("tightest observed gap over the key lifetime, seconds",
                      30.6, round(tightest / 1000, 1)))
            c.append(("the scaled key lifetime, seconds", 30.0,
                      over[0]["scaled_key_ttl_ms"] / 1000))
            c.append(("the defective arm charges twice", 2,
                      max(r["defective"]["charges"] for r in over)))
            c.append(("the control arm charges once", 1,
                      max(r["control"]["charges"] for r in over)))

    if e8 and e8.get("status") == "ok":
        c.append(("broker cap, ms", 900000, e8["transaction_max_timeout_ms"]))
        c.append(("the cap is the broker default", True, e8["is_default"]))
        c.append(("the refusal is measured, not quoted", True,
                  e8.get("refusal_is_measured")))
        c.append(("largest timeout the broker grants, ms", 900000,
                  e8.get("largest_accepted_ms")))
        c.append(("smallest timeout the broker refuses, ms", 900001,
                  e8.get("smallest_refused_ms")))

    return c


def main() -> int:
    claims = build_claims()
    if not claims:
        print("no results files found; run the experiments first")
        return 1

    bad = []
    for desc, stated, computed in claims:
        if isinstance(stated, float) and isinstance(computed, (int, float)):
            ok = abs(float(stated) - float(computed)) <= 0.051
        else:
            ok = stated == computed
        if not ok:
            bad.append((desc, stated, computed))

    print(f"checked {len(claims)} numeric claims against results/")
    for desc, stated, computed in bad:
        print(f"  MISMATCH  {desc}: paper says {stated!r}, results give {computed!r}")

    text = PAPER.read_text() if PAPER.exists() else ""
    leftovers = [m for m in re.findall(r"[A-Z\\_]*PLACEHOLDER[A-Z\\_]*", text)
                 if m.replace("\\", "") != "ZENODO_DOI_PLACEHOLDER"]
    if leftovers:
        print(f"  LEFTOVER placeholders in paper: {sorted(set(leftovers))}")

    if not bad and not leftovers:
        print("all claims verified")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
