"""E9 -- the example pipeline's numbers, read from the artifacts.

Section~III walks the pipeline in ``data/example-pipeline``: a checkpoint
interval, a restart time, a producer transaction timeout, a worst observed
recovery from the runbook, a documented idempotency key, and then the replay
horizon those imply and the transaction lifetime they need. Every one of those
was typed into the prose, so editing an artifact would leave the paragraph
describing a pipeline that is no longer in the repository.

They are all readable. The first five are fields in the files themselves and
the last two are what ``replay_horizon`` computes from them, so this loads the
artifacts, computes the two derived figures, and writes them out. The check is
that the paper and the files it points at cannot disagree.
"""

from __future__ import annotations

from pathlib import Path

from _common import write

from streamcheck.frontend import load_pipeline

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = ROOT / "data" / "example-pipeline"


def main() -> None:
    pipeline, problems = load_pipeline(PIPELINE)
    if problems:
        raise SystemExit(f"the example pipeline does not load: {problems}")
    cfg = pipeline.engine
    horizon_ms = cfg.replay_horizon()
    needed = cfg.checkpoint_interval + cfg.max_downtime + cfg.restart_delay
    # The provider's documented key lifetime lives with the effects, not the
    # engine: it is the artifact the engine's authors never see, which is the
    # paper's point about findings that need more than one file.
    key_ttl = next((e.provider_key_ttl for e in pipeline.effects
                    if e.provider_key_ttl), None)

    write("e9_example.json", {
        "config": {"pipeline": str(PIPELINE.relative_to(ROOT))},
        "declared": {
            "checkpoint_interval_s": cfg.checkpoint_interval / 1000,
            "restart_s": cfg.restart_delay / 1000,
            "transaction_timeout_min": cfg.transaction_timeout / 60_000,
            "worst_recovery_min": cfg.max_downtime / 60_000,
            "idempotency_key_ttl_h": key_ttl / 3_600_000 if key_ttl else None,
        },
        "derived": {
            "replay_horizon_h": horizon_ms / 3_600_000,
            "replay_horizon_min": horizon_ms / 60_000,
            # What a transaction has to survive: one checkpoint, the worst
            # observed downtime, and the restart that follows it.
            "lifetime_needed_min": needed / 60_000,
            "lifetime_configured_min": cfg.transaction_timeout / 60_000,
        },
        "headline": {
            "horizon_inside_the_key": key_ttl is not None and horizon_ms <= key_ttl,
            "lifetime_is_enough": needed <= cfg.transaction_timeout,
            "replay_horizon_hours": round(horizon_ms / 3_600_000, 2),
            "lifetime_needed_minutes": round(needed / 60_000, 1),
            "lifetime_configured_minutes": round(
                cfg.transaction_timeout / 60_000, 1),
            "key_ttl_hours": (None if key_ttl is None
                              else round(key_ttl / 3_600_000, 1)),
            "worst_recovery_minutes": round(cfg.max_downtime / 60_000, 1),
            "checkpoint_interval_seconds": round(cfg.checkpoint_interval / 1000),
            "restart_seconds": round(cfg.restart_delay / 1000),
        },
    })


if __name__ == "__main__":
    main()
