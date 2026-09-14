"""The defect corpus: pipelines whose faults we know because we put them there.

A detector evaluated only on cases its author considered is not evaluated. The
corpus here is built the other way round: five realistic payment pipeline
shapes, and a set of mutations that each introduce one specific defect. Ground
truth is the mutation applied, recorded independently of what any check looks
for, so a rule that fails to fire is a miss rather than a case nobody thought
of.

Clean pipelines matter as much as broken ones. A checker that reports a defect
on a correct pipeline costs an engineer an afternoon and costs the tool its
credibility, so a third of the corpus is deliberately correct and every finding
on those is a false positive.

The realistic part is the shapes. All five are drawn from things a payments
team actually builds: taking a capture to a processor, issuing a refund,
reconciling a ledger against a bank feed, paying out a batch, and retrying a
failed charge. The reconciliation shape is P5's setting, and its source
lateness bound is P5's, which is what makes the replay horizon in that pipeline
larger than the engine config alone would suggest.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field, replace

from .ir import (
    EngineConfig,
    Effect,
    Guarantee,
    Isolation,
    Millis,
    Operator,
    Pipeline,
    Provenance,
    Sink,
    Source,
)

SECOND = 1000
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
DAY = 24 * HOUR

#: Provider key lifetimes, as a payment provider would document them. These are
#: plausible values spanning the range practitioners report, not measurements of
#: any particular provider, and the sweep in E5 varies them.
KEY_TTLS: dict[str, Millis] = {
    "short": 6 * HOUR,
    "day": 24 * HOUR,
    "long": 7 * DAY,
}

SHAPES = ("capture", "refund", "reconcile", "payout", "dunning")

#: Each mutation introduces exactly one defect, named by the rule that should
#: catch it. ``clean`` introduces none.
#:
#: The ``edge_`` entries are the ones that carry evidential weight. A mutation
#: named for the rule meant to catch it can be matched by a checker that
#: recognises the mutation rather than computing anything, and since the same
#: author wrote both, a perfect score on those says little. The edge cases sit
#: a hair on either side of a threshold: telling them apart requires the replay
#: horizon to be computed correctly, from all of its terms, and a checker that
#: gets the arithmetic wrong fails them whichever way it errs. Two of them are
#: deliberately *not* defects while looking alarming.
MUTATIONS = (
    "clean",
    "SC001_non_idempotent_effect",
    "SC002_uuid_in_key",
    "SC003_missing_key",
    "SC004_uncommitted_read",
    "SC005_shared_txn_prefix",
    "SC006_effect_before_dedup",
    "SC007_sink_without_dedup",
    "SC100_undeclared_ttl",
    "SC101_key_ttl_below_replay",
    "SC102_state_ttl_below_replay",
    "SC103_txn_timeout_too_low",
    "SC104_delivery_above_txn",
    "SC105_retry_above_ttl",
    "SC106_downtime_exceeds_broker_cap",
    # Boundary cases. The first two straddle the key-lifetime threshold by one
    # second; the third and fourth straddle the dedup-state threshold.
    "edge_ttl_just_below",
    "edge_ttl_just_above",
    "edge_state_just_below",
    "edge_state_just_above",
    # Decoys: superficially alarming, actually correct. A very short checkpoint
    # interval reads as risky and is not, on its own, a defect; a long client
    # retry window is fine when the key outlives it.
    "decoy_fast_checkpoints",
    "decoy_long_retry_within_ttl",
)

#: Boundary and decoy cases whose correct answer is "no finding".
NEGATIVE_MUTATIONS = frozenset({
    "clean", "edge_ttl_just_above", "edge_state_just_above",
    "decoy_fast_checkpoints", "decoy_long_retry_within_ttl",
})

#: Which rule each mutation should trigger. Ground truth for the evaluation.
EXPECTED_RULE: dict[str, str] = {
    m: m.split("_")[0] for m in MUTATIONS if m.startswith("SC")
}
EXPECTED_RULE["edge_ttl_just_below"] = "SC101"
EXPECTED_RULE["edge_state_just_below"] = "SC102"


@dataclass
class Case:
    """One corpus entry: a pipeline, and what is wrong with it."""

    case_id: str
    shape: str
    mutation: str
    pipeline: Pipeline
    #: The rule that should fire, or "" for a clean pipeline.
    expected_rule: str

    @property
    def is_clean(self) -> bool:
        """True when the correct answer is "no finding".

        Covers the literally-clean pipelines and the boundary and decoy cases
        that are correct despite looking otherwise.
        """
        return self.mutation in NEGATIVE_MUTATIONS

    def as_dict(self) -> dict:
        return {
            "case_id": self.case_id,
            "shape": self.shape,
            "mutation": self.mutation,
            "expected_rule": self.expected_rule,
            "pipeline": self.pipeline.as_dict(),
        }


def _base_engine(rng: random.Random) -> EngineConfig:
    """A correctly configured engine: every duration relation holds."""
    cp = rng.choice([30 * SECOND, 60 * SECOND, 2 * MINUTE])
    # The recovery assumption has to fit under the broker's cap on a requested
    # transaction timeout, or the pipeline is defective before any mutation is
    # applied. The first version of this generator used 5, 15 and 30 minutes,
    # which are unremarkable numbers, and every "clean" case it produced
    # violated SC106 -- a checkpoint plus half an hour of downtime needs a
    # transaction timeout the broker will not grant. That is reported as a
    # result rather than quietly corrected here; what is corrected is the
    # baseline, so that "clean" means clean.
    downtime = rng.choice([2 * MINUTE, 5 * MINUTE, 8 * MINUTE])
    restart = rng.choice([10 * SECOND, 30 * SECOND])
    return EngineConfig(
        checkpoint_interval=cp,
        min_pause_between_checkpoints=cp // 4,
        restart_delay=restart,
        max_downtime=downtime,
        drain_factor=1.0,
        state_ttl=14 * DAY,
        # Comfortably above a checkpoint plus the assumed downtime.
        transaction_timeout=max(15 * MINUTE, cp + downtime + restart + 5 * MINUTE),
        delivery_timeout=2 * MINUTE,
        guarantee=Guarantee.EXACTLY_ONCE,
        transactional_id_prefix="pay-jobs-a",
        provenance={
            "checkpoint_interval": Provenance("flink-conf.yaml", "execution.checkpointing.interval"),
            "transaction_timeout": Provenance("kafka-producer.properties", "transaction.timeout.ms"),
            "delivery_timeout": Provenance("kafka-producer.properties", "delivery.timeout.ms"),
            "max_downtime": Provenance("runbook.md", "recovery SLO"),
            "transactional_id_prefix": Provenance("flink-conf.yaml", "sink.transactional-id-prefix"),
        },
    )


def _build(shape: str, rng: random.Random) -> Pipeline:
    """A correct pipeline of the given shape."""
    engine = _base_engine(rng)
    # The provider lifetime is chosen to clear the horizon, so the baseline is
    # genuinely clean rather than clean by luck.
    horizon = engine.replay_horizon(75 * SECOND if shape == "reconcile" else 0)
    ttl = next(v for v in sorted(KEY_TTLS.values()) if v > horizon)

    src_prov = Provenance("kafka-consumer.properties", "isolation.level")
    eff_prov = Provenance("ChargeFunction.java", "idempotencyKey")

    if shape == "reconcile":
        # P5's setting: a fast ledger stream against a batching bank feed. The
        # bank's declared lateness bound enters the replay horizon, which is
        # why this shape's horizon exceeds what the engine config shows.
        sources = [
            Source("ledger", "ledger.settlements", Isolation.READ_COMMITTED,
                   "recon", lateness_bound=2 * SECOND, upstream_transactional=True,
                   provenance=src_prov),
            Source("bank", "bank.postings", Isolation.READ_COMMITTED,
                   "recon", lateness_bound=75 * SECOND, upstream_transactional=False,
                   provenance=src_prov),
        ]
        ops = [
            Operator("join", "join", ["ledger", "bank"], keyed_by="order_id",
                     state_ttl=engine.state_ttl,
                     provenance=Provenance("ReconJob.sql", "interval join")),
            Operator("dedup", "dedup", ["join"], keyed_by="order_id",
                     state_ttl=engine.state_ttl,
                     provenance=Provenance("ReconJob.sql", "ROW_NUMBER dedup")),
            Operator("raise_break", "process", ["dedup"], keyed_by="order_id",
                     provenance=Provenance("ReconJob.java", "BreakFunction")),
        ]
        effects = [Effect("notify_break", "raise_break", key_expr="order_id || ':break'",
                          provider_key_ttl=ttl, client_retry_window=5 * MINUTE,
                          idempotent=True, provenance=eff_prov)]
        sinks = [Sink("breaks", "kafka", Guarantee.EXACTLY_ONCE, "order_id", ["raise_break"],
                      Provenance("ReconJob.sql", "breaks sink"))]
    else:
        topic = {"capture": "orders.captured", "refund": "refunds.requested",
                 "payout": "payouts.batched", "dunning": "payments.failed"}[shape]
        sources = [Source("in", topic, Isolation.READ_COMMITTED, shape,
                          lateness_bound=5 * SECOND, upstream_transactional=True,
                          provenance=src_prov)]
        ops = [
            Operator("dedup", "dedup", ["in"], keyed_by="order_id",
                     state_ttl=engine.state_ttl,
                     provenance=Provenance(f"{shape.title()}Job.sql", "ROW_NUMBER dedup")),
            Operator("enrich", "map", ["dedup"], keyed_by="order_id",
                     provenance=Provenance(f"{shape.title()}Job.java", "enrich")),
            Operator("call_provider", "process", ["enrich"], keyed_by="order_id",
                     provenance=Provenance(f"{shape.title()}Job.java", "ProviderFunction")),
        ]
        effects = [Effect("charge", "call_provider", key_expr="order_id || ':' || attempt_kind",
                          provider_key_ttl=ttl, client_retry_window=5 * MINUTE,
                          idempotent=True, provenance=eff_prov)]
        sinks = [Sink("ledger_out", "kafka", Guarantee.EXACTLY_ONCE, "order_id",
                      ["call_provider"], Provenance(f"{shape.title()}Job.sql", "ledger sink"))]

    return Pipeline(
        name=f"{shape}-pipeline",
        engine=engine,
        sources=sources,
        operators=ops,
        effects=effects,
        sinks=sinks,
        peer_transactional_prefixes=["pay-jobs-b", "recon-jobs"],
    )


def _mutate(p: Pipeline, mutation: str, rng: random.Random) -> Pipeline:
    """Introduce exactly one defect.  ``clean`` returns the pipeline unchanged."""
    if mutation == "clean":
        return p
    horizon = p.replay_horizon()

    if mutation == "SC001_non_idempotent_effect":
        p.effects[0] = replace(p.effects[0], idempotent=False, key_expr="")
    elif mutation == "SC002_uuid_in_key":
        p.effects[0] = replace(p.effects[0], key_expr="UUID.randomUUID().toString()")
    elif mutation == "SC003_missing_key":
        p.effects[0] = replace(p.effects[0], key_expr="")
    elif mutation == "SC004_uncommitted_read":
        tx = [i for i, s in enumerate(p.sources) if s.upstream_transactional]
        i = tx[0] if tx else 0
        p.sources[i] = replace(p.sources[i], isolation=Isolation.READ_UNCOMMITTED,
                               upstream_transactional=True)
    elif mutation == "SC005_shared_txn_prefix":
        p.peer_transactional_prefixes = list(p.peer_transactional_prefixes) + [
            p.engine.transactional_id_prefix]
    elif mutation == "SC006_effect_before_dedup":
        # Move the effect upstream of the deduplication that was guarding it.
        dedup = p.dedup_operators()[0]
        upstream = dedup.upstream[0]
        p.effects[0] = replace(p.effects[0], operator=upstream)
    elif mutation == "SC007_sink_without_dedup":
        p.sinks[0] = replace(p.sinks[0], guarantee=Guarantee.AT_LEAST_ONCE, dedup_key="")
    elif mutation == "SC100_undeclared_ttl":
        p.effects[0] = replace(p.effects[0], provider_key_ttl=None)
    elif mutation == "SC101_key_ttl_below_replay":
        p.effects[0] = replace(p.effects[0], provider_key_ttl=max(SECOND, horizon // 2))
    elif mutation == "SC102_state_ttl_below_replay":
        for i, o in enumerate(p.operators):
            if o.kind == "dedup":
                p.operators[i] = replace(o, state_ttl=max(SECOND, horizon // 2))
        p.engine.state_ttl = max(SECOND, horizon // 2)
    elif mutation == "SC103_txn_timeout_too_low":
        need = (p.engine.checkpoint_interval + p.engine.max_downtime
                + p.engine.restart_delay)
        p.engine.transaction_timeout = max(SECOND, need // 2)
        p.engine.delivery_timeout = min(p.engine.delivery_timeout,
                                        p.engine.transaction_timeout)
    elif mutation == "SC104_delivery_above_txn":
        p.engine.delivery_timeout = p.engine.transaction_timeout + 5 * MINUTE
    elif mutation == "SC106_downtime_exceeds_broker_cap":
        # A recovery assumption of an hour: ordinary, and impossible to make
        # transactionally safe on a default broker.
        p.engine.max_downtime = HOUR
        p.engine.transaction_timeout = p.engine.broker_max_transaction_timeout
        # Keep the other durations clear of the enlarged horizon so this case
        # tests one thing.
        h = p.engine.replay_horizon(p.source_lateness())
        p.engine.state_ttl = h + HOUR
        for i, o in enumerate(p.operators):
            if o.kind == "dedup":
                p.operators[i] = replace(o, state_ttl=h + HOUR)
        p.effects[0] = replace(p.effects[0], provider_key_ttl=h + HOUR)
    elif mutation == "SC105_retry_above_ttl":
        ttl = p.effects[0].provider_key_ttl or DAY
        p.effects[0] = replace(p.effects[0], client_retry_window=ttl + HOUR)
    elif mutation == "edge_ttl_just_below":
        p.effects[0] = replace(p.effects[0], provider_key_ttl=horizon - SECOND,
                               client_retry_window=SECOND)
    elif mutation == "edge_ttl_just_above":
        p.effects[0] = replace(p.effects[0], provider_key_ttl=horizon + SECOND,
                               client_retry_window=SECOND)
    elif mutation == "edge_state_just_below":
        for i, o in enumerate(p.operators):
            if o.kind == "dedup":
                p.operators[i] = replace(o, state_ttl=horizon - SECOND)
        p.engine.state_ttl = horizon - SECOND
    elif mutation == "edge_state_just_above":
        for i, o in enumerate(p.operators):
            if o.kind == "dedup":
                p.operators[i] = replace(o, state_ttl=horizon + SECOND)
        p.engine.state_ttl = horizon + SECOND
    elif mutation == "decoy_fast_checkpoints":
        # Checkpointing every two seconds looks reckless and shortens the
        # replay horizon, which makes every duration relation easier to
        # satisfy, not harder. Correct pipeline.
        p.engine.checkpoint_interval = 2 * SECOND
        p.engine.min_pause_between_checkpoints = 0
    elif mutation == "decoy_long_retry_within_ttl":
        ttl = p.effects[0].provider_key_ttl or DAY
        p.effects[0] = replace(p.effects[0], client_retry_window=max(SECOND, ttl - HOUR))
    else:
        raise ValueError(f"unknown mutation: {mutation}")
    return p


def build_corpus(*, seed: int = 0, per_combination: int = 2) -> list[Case]:
    """Every shape crossed with every mutation, ``per_combination`` times.

    Repeats differ in their engine configuration, drawn from the same
    distribution, so a rule cannot pass by matching one particular set of
    numbers.
    """
    rng = random.Random(seed)
    cases: list[Case] = []
    for shape in SHAPES:
        for mutation in MUTATIONS:
            for k in range(per_combination):
                p = _build(shape, rng)
                p = _mutate(p, mutation, rng)
                cases.append(Case(
                    case_id=f"{shape}-{mutation}-{k}",
                    shape=shape,
                    mutation=mutation,
                    pipeline=p,
                    expected_rule=EXPECTED_RULE.get(mutation, ""),
                ))
    return cases


def corpus_stats(cases: list[Case]) -> dict:
    by_mut: dict[str, int] = {}
    for c in cases:
        by_mut[c.mutation] = by_mut.get(c.mutation, 0) + 1
    return {
        "cases": len(cases),
        "clean": sum(1 for c in cases if c.is_clean),
        "defective": sum(1 for c in cases if not c.is_clean),
        "edge": sum(1 for c in cases if c.mutation.startswith("edge_")),
        "decoy": sum(1 for c in cases if c.mutation.startswith("decoy_")),
        "shapes": len(SHAPES),
        "mutations": len(MUTATIONS),
        "by_mutation": by_mut,
    }
