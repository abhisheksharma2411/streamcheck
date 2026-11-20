"""Regenerate every figure from results/. No number is typed in here."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
HERE = Path(__file__).resolve().parent

INK = "#1b4965"
WARM = "#c1666b"
LEAF = "#5f8a68"
SAND = "#d4a253"

plt.rcParams.update({
    "font.size": 8.5, "axes.grid": True, "grid.alpha": 0.25,
    "axes.spines.top": False, "axes.spines.right": False,
    "figure.dpi": 200, "savefig.bbox": "tight",
})


def load(name):
    path = RESULTS / name
    if not path.exists():
        print(f"  skip: {name} not found")
        return None
    return json.loads(path.read_text())


def fig_artifacts() -> None:
    d = load("e4_artifacts.json")
    if not d:
        return
    total = d["totals"]["findings"]
    dur_total = d["totals"]["duration_findings"]
    h = d["headline"]
    labels = ["one artifact", "two artifacts", "all artifacts"]
    allf = [h["best_single_artifact_findings"], h["best_pair_findings"], total]
    durf = [h["duration_findings_from_any_single"], h["duration_findings_from_any_pair"], dur_total]

    fig, ax = plt.subplots(figsize=(3.7, 2.8))
    x = range(len(labels))
    ax.bar([i - 0.19 for i in x], [100 * v / total for v in allf], width=0.38,
           color=INK, label="all findings")
    ax.bar([i + 0.19 for i in x], [100 * v / dur_total for v in durf], width=0.38,
           color=WARM, label="duration findings")
    for i, (a, b) in enumerate(zip(allf, durf)):
        ax.text(i - 0.19, 100 * a / total + 2, str(a), ha="center", fontsize=7)
        ax.text(i + 0.19, 100 * b / dur_total + 2, str(b), ha="center", fontsize=7)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, fontsize=7.5)
    ax.set_ylabel("share derivable (%)")
    ax.set_ylim(0, 112)
    ax.set_title("What a reviewer holding one file can see", fontsize=9)
    ax.legend(frameon=False, fontsize=7, loc="upper left")
    fig.savefig(HERE / "fig1_artifacts.pdf")
    plt.close(fig)
    print("  fig1_artifacts.pdf")


def fig_testing() -> None:
    d = load("e5_random.json")
    if not d:
        return
    sweep = d["ceiling_sweep"]
    xs = [r["ceiling_hours"] for r in sweep]
    found = [100 * r["cases_found_share"] for r in sweep]
    rate = [100 * r["hit_rate"] for r in sweep]

    fig, ax = plt.subplots(figsize=(3.7, 2.8))
    ax.plot(xs, found, color=INK, marker="o", ms=3.4, lw=1.8, label="defects ever found")
    ax.plot(xs, rate, color=SAND, marker="s", ms=3.0, lw=1.4, label="per-trial hit rate")
    ax.set_xscale("log")
    ax.set_xlabel("longest outage the tester is willing to simulate (h)")
    ax.set_ylabel("%")
    ax.set_ylim(-4, 104)
    ax.set_title("Fault injection finds these only if it guesses high enough", fontsize=9)
    ax.legend(frameon=False, fontsize=7, loc="upper left")
    fig.savefig(HERE / "fig2_testing.pdf")
    plt.close(fig)
    print("  fig2_testing.pdf")


def fig_broker_cap() -> None:
    d = load("e7_prevalence.json")
    if not d:
        return
    rows = d["rows"]
    cap = 900_000
    downtimes = sorted({r["downtime_ms"] for r in rows})
    cps = sorted({r["checkpoint_ms"] for r in rows})

    fig, ax = plt.subplots(figsize=(3.7, 2.8))
    for cp, colour in zip(cps, (INK, LEAF, SAND, WARM)):
        need = [(cp + dt + 10_000) / 60_000 for dt in downtimes]
        ax.plot([dt / 60_000 for dt in downtimes], need, marker="o", ms=3,
                color=colour, lw=1.5, label=f"checkpoint {cp // 1000}s")
    ax.axhline(cap / 60_000, color="#333", ls="--", lw=1.2)
    ax.annotate("broker cap on a requested\ntransaction timeout (15 min)",
                xy=(downtimes[1] / 60_000, cap / 60_000), xytext=(6, 60),
                fontsize=7, arrowprops=dict(arrowstyle="->", lw=0.8, color="#333"))
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("assumed recovery time (min)")
    ax.set_ylabel("transaction timeout required (min)")
    ax.set_title("Above the dashed line, no job setting is enough", fontsize=9)
    ax.legend(frameon=False, fontsize=6.5, loc="upper left")
    fig.savefig(HERE / "fig3_broker_cap.pdf")
    plt.close(fig)
    print("  fig3_broker_cap.pdf")


def fig_witness() -> None:
    d = load("e2_witness.json")
    if not d:
        return
    rules = [r for r, v in sorted(d["per_rule"].items()) if v["witnessed"]]
    repro = [100 * d["per_rule"][r]["reproduced"] / d["per_rule"][r]["witnessed"] for r in rules]
    dbl = [100 * d["per_rule"][r]["double_charge"] / d["per_rule"][r]["witnessed"] for r in rules]

    fig, ax = plt.subplots(figsize=(3.7, 2.8))
    y = range(len(rules))
    ax.barh([i + 0.19 for i in y], repro, height=0.38, color=INK, label="predicted effect reproduced")
    ax.barh([i - 0.19 for i in y], dbl, height=0.38, color=WARM, label="money moved twice")
    ax.set_yticks(list(y))
    ax.set_yticklabels(rules, fontsize=7.5)
    ax.invert_yaxis()
    ax.set_xlabel("% of witnesses")
    ax.set_xlim(0, 108)
    ax.set_title("Not every defect is a double charge", fontsize=9)
    # Anchored outside the axes: at "lower right" it printed over the
    # SC103 and SC105 bars.
    ax.legend(frameon=False, fontsize=6.5, loc="upper center",
              bbox_to_anchor=(0.5, -0.20), ncol=2)
    fig.savefig(HERE / "fig4_witness.pdf")
    plt.close(fig)
    print("  fig4_witness.pdf")


def main() -> None:
    print("regenerating figures from results/")
    fig_artifacts()
    fig_testing()
    fig_broker_cap()
    fig_witness()


if __name__ == "__main__":
    main()
