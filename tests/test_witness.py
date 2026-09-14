"""Witnesses say something falsifiable, and compressing them does not change it."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest

from streamcheck.checks import analyse
from streamcheck.corpus import build_corpus
from streamcheck.executor import run
from streamcheck.witness import synthesise_all

FACTORS = (1.0, 0.01, 0.0001)


def _witnessed(mutation: str, shape: str = "capture"):
    case = next(c for c in build_corpus(seed=5, per_combination=1)
                if c.mutation == mutation and c.shape == shape)
    witnesses = synthesise_all(case.pipeline, analyse(case.pipeline))
    assert witnesses, f"no witness for {mutation}"
    return case, next(iter(witnesses.values()))


@pytest.mark.parametrize("mutation", [
    "SC001_non_idempotent_effect", "SC002_uuid_in_key", "SC003_missing_key",
    "SC101_key_ttl_below_replay", "SC102_state_ttl_below_replay",
    "SC105_retry_above_ttl",
])
def test_these_defects_charge_twice_on_their_own(mutation):
    case, w = _witnessed(mutation)
    assert run(case.pipeline, w).charges_confirmed


@pytest.mark.parametrize("mutation", ["SC006_effect_before_dedup", "SC103_txn_timeout_too_low"])
def test_these_need_a_second_failure(mutation):
    """A duplicate call the provider absorbs is not a double charge.

    Both of these were predicted to charge twice until their witnesses were
    run. Pinning the corrected behaviour down stops it drifting back.
    """
    case, w = _witnessed(mutation)
    result = run(case.pipeline, w)
    assert result.invocations >= 2
    assert result.charges == 1


@pytest.mark.parametrize("factor", FACTORS)
def test_compression_preserves_the_verdict(factor):
    case, w = _witnessed("SC101_key_ttl_below_replay")
    assert run(case.pipeline, w.scaled(factor)).charges_confirmed


def test_a_witness_needs_the_defect_to_reproduce():
    """The discriminating check: same scenario, provider never forgets.

    If the second charge appeared without the key having expired, the witness
    would be demonstrating that replays happen, which nobody disputes, rather
    than that this key lifetime is too short.
    """
    from dataclasses import replace

    case, w = _witnessed("SC101_key_ttl_below_replay")
    assert run(case.pipeline, w).charges_confirmed
    forgiving = replace(w, provider_key_ttl=None)
    assert not run(case.pipeline, forgiving).charges_confirmed


def test_no_witness_is_claimed_for_rules_that_need_more_than_one_job():
    """Better to say a rule has no runnable scenario than to ship an empty one."""
    case = next(c for c in build_corpus(seed=5, per_combination=1)
                if c.mutation == "SC005_shared_txn_prefix")
    findings = analyse(case.pipeline)
    witnesses = synthesise_all(case.pipeline, findings)
    assert any(f.rule == "SC005" for f in findings)
    assert not any(k.startswith("SC005") for k in witnesses)
