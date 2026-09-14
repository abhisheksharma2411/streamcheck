"""Each rule fires on exactly the thing it claims to, and not on its neighbour."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest

from streamcheck.checks import RULES, analyse
from streamcheck.corpus import EXPECTED_RULE, NEGATIVE_MUTATIONS, build_corpus
from streamcheck.ir import BROKER_MAX_TXN_TIMEOUT_DEFAULT, EngineConfig, Guarantee

SECOND, MINUTE, HOUR = 1000, 60_000, 3_600_000


def rules_for(pipeline) -> set[str]:
    return {f.rule for f in analyse(pipeline)}


@pytest.mark.parametrize("mutation", sorted(EXPECTED_RULE))
def test_each_mutation_is_caught(mutation):
    cases = [c for c in build_corpus(seed=3, per_combination=1) if c.mutation == mutation]
    assert cases, f"no case generated for {mutation}"
    for c in cases:
        assert EXPECTED_RULE[mutation] in rules_for(c.pipeline), c.case_id


@pytest.mark.parametrize("mutation", sorted(NEGATIVE_MUTATIONS))
def test_negatives_stay_silent(mutation):
    """Clean pipelines, the safe side of each boundary, and the decoys."""
    for c in build_corpus(seed=3, per_combination=1):
        if c.mutation == mutation:
            assert not rules_for(c.pipeline), f"{c.case_id} fired {rules_for(c.pipeline)}"


def test_the_boundary_is_one_second_wide():
    """The pair that makes the corpus worth anything.

    Two pipelines differing by two seconds of key lifetime, one either side of
    the replay horizon. Getting these right requires computing the horizon from
    all of its terms; a checker that pattern-matched the mutation name would
    have no way to tell them apart.
    """
    cases = {c.mutation: c for c in build_corpus(seed=3, per_combination=1)
             if c.mutation in ("edge_ttl_just_below", "edge_ttl_just_above")
             and c.shape == "capture"}
    below, above = cases["edge_ttl_just_below"], cases["edge_ttl_just_above"]
    # The two draw independent engine configurations, so the comparison is
    # against each one's own horizon rather than against each other.
    for case, offset, expect in ((below, -SECOND, True), (above, +SECOND, False)):
        ttl = case.pipeline.effects[0].provider_key_ttl
        assert ttl == case.pipeline.replay_horizon() + offset
        assert ("SC101" in rules_for(case.pipeline)) is expect


def test_replay_horizon_uses_every_term():
    """Each term must move the horizon, or a check could not depend on it."""
    base = EngineConfig(checkpoint_interval=60 * SECOND, max_downtime=10 * MINUTE,
                        restart_delay=10 * SECOND, min_pause_between_checkpoints=0)
    h = base.replay_horizon()
    for attr, bump in (("checkpoint_interval", 30 * SECOND),
                       ("max_downtime", 5 * MINUTE),
                       ("restart_delay", 20 * SECOND),
                       ("min_pause_between_checkpoints", 10 * SECOND)):
        cfg = EngineConfig(**{**base.__dict__, attr: getattr(base, attr) + bump})
        assert cfg.replay_horizon() > h, f"{attr} does not affect the horizon"
    assert base.replay_horizon(source_lateness=75 * SECOND) == h + 75 * SECOND


def test_broker_cap_rule_is_about_impossibility_not_misconfiguration():
    """SC106 fires only when no permitted timeout would be enough.

    Raising the requested timeout clears SC103 and cannot clear SC106, which is
    the distinction between the two.
    """
    from streamcheck.corpus import _build
    import random

    p = _build("capture", random.Random(0))
    p.engine.guarantee = Guarantee.EXACTLY_ONCE
    p.engine.checkpoint_interval = 60 * SECOND
    p.engine.restart_delay = 10 * SECOND

    p.engine.max_downtime = 5 * MINUTE
    p.engine.transaction_timeout = BROKER_MAX_TXN_TIMEOUT_DEFAULT
    assert "SC106" not in rules_for(p)

    p.engine.max_downtime = 2 * HOUR
    fired = rules_for(p)
    assert "SC106" in fired
    p.engine.transaction_timeout = BROKER_MAX_TXN_TIMEOUT_DEFAULT
    assert "SC106" in rules_for(p), "raising the request cannot clear an impossibility"


def test_every_rule_has_a_name_and_every_name_a_rule():
    seen = set()
    for c in build_corpus(seed=3, per_combination=1):
        seen |= rules_for(c.pipeline)
    assert seen <= set(RULES), seen - set(RULES)
    assert "SC106" in RULES
