"""Facts come out of the files, and a missing fact is reported not guessed."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest

from streamcheck.checks import analyse
from streamcheck.frontend import load_pipeline, parse_duration

EXAMPLE = Path(__file__).resolve().parents[1] / "data" / "example-pipeline"


@pytest.mark.parametrize("text,expected", [
    ("60000", 60_000), ("60 s", 60_000), ("15 min", 900_000),
    ("2 h", 7_200_000), ("7 d", 604_800_000), ("45 min", 2_700_000),
    ("nonsense", None),
])
def test_durations_in_the_spellings_these_files_use(text, expected):
    assert parse_duration(text) == expected


def test_facts_are_gathered_from_every_artifact():
    p, missing = load_pipeline(EXAMPLE)
    assert not missing, missing
    assert p.engine.checkpoint_interval == 60_000          # flink-conf.yaml
    assert p.engine.transaction_timeout == 900_000          # producer properties
    assert p.engine.max_downtime == 2_700_000               # runbook
    assert p.effects[0].provider_key_ttl == 86_400_000      # effects manifest
    assert p.sources[0].isolation.value == "read_committed"  # consumer properties
    assert p.sources[0].lateness_bound == 5_000             # job SQL


def test_the_horizon_needs_more_than_one_file():
    """Drop the runbook and the horizon changes, which is the whole argument."""
    p, _ = load_pipeline(EXAMPLE)
    with_runbook = p.replay_horizon()
    p.engine.max_downtime = 0
    assert p.replay_horizon() < with_runbook


def test_the_example_is_defective_and_says_why(tmp_path):
    p, _ = load_pipeline(EXAMPLE)
    findings = {f.rule: f for f in analyse(p)}
    assert "SC106" in findings
    q = findings["SC106"].quantities
    assert q["required_ms"] > q["broker_cap_ms"]
    assert any("runbook" in e for e in findings["SC106"].evidence)


def test_an_undeclared_key_lifetime_is_a_finding_not_a_default(tmp_path):
    for name in ("flink-conf.yaml", "kafka-producer.properties",
                 "kafka-consumer.properties", "runbook.json", "capture_settle.sql"):
        (tmp_path / name).write_text((EXAMPLE / name).read_text())
    manifest = json.loads((EXAMPLE / "effects.json").read_text())
    manifest["effects"][0].pop("provider_key_ttl")
    (tmp_path / "effects.json").write_text(json.dumps(manifest))

    p, missing = load_pipeline(tmp_path)
    assert any("provider_key_ttl" in m for m in missing)
    assert p.effects[0].provider_key_ttl is None
    assert "SC100" in {f.rule for f in analyse(p)}
