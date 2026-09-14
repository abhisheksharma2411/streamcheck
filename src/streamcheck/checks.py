"""The checks: what makes a payment pipeline lose exactly-once, and why.

Two families, and the split is the point of the paper.

**Structural** defects are about shape -- an effect in the wrong place, a key
built from the wrong thing, a consumer reading uncommitted output. A careful
reviewer can find these by reading the job, and several linters already look
for neighbouring things.

**Duration** defects are about arithmetic between facts that live in different
artifacts. Whether a replayed record charges the customer twice depends on
whether the provider still remembers the idempotency key when the replay
arrives, which is a comparison between a number in the provider's
documentation and a number nobody computes: the replay horizon, assembled from
a checkpoint interval, an operational assumption about downtime, a restart
delay and a source's lateness bound. No reviewer holds all four at once, and no
single file contains them, so these survive.

Every check returns findings that carry the exact quantities compared, so a
report says "72h key TTL against a 128h replay horizon" rather than "possible
idempotency issue".

Nothing here claims to prove exactly-once. Each check is a sufficient condition
for a *violation*, not a certificate of correctness, and the difference is
stated in the paper rather than blurred.

A further distinction came out of executing the witnesses rather than out of
designing the rules. Some defects are sufficient on their own to move money
twice: a key that changes on replay, or a key the provider has forgotten.
Others -- an effect placed upstream of its deduplication, a transaction
timeout too short to survive the downtime -- produce a second *call* to the
provider, which is a double charge only if the idempotency mechanism has also
failed. Both are worth reporting and they are not the same finding, so the
second kind is medium severity and its witnesses predict two invocations and
one charge. Two rules were demoted this way after their witnesses did not
reproduce the double charge originally predicted.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .ir import (
    Guarantee,
    Isolation,
    Millis,
    NONDETERMINISTIC_TOKENS,
    Pipeline,
)

STRUCTURAL = "structural"
DURATION = "duration"


@dataclass
class Finding:
    """One defect, with the evidence that makes it actionable."""

    rule: str
    family: str
    severity: str                  # high | medium | low
    subject: str                   # which effect/operator/sink
    message: str
    quantities: dict = field(default_factory=dict)
    evidence: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "rule": self.rule,
            "family": self.family,
            "severity": self.severity,
            "subject": self.subject,
            "message": self.message,
            "quantities": dict(self.quantities),
            "evidence": list(self.evidence),
        }


def _hours(ms: Millis) -> str:
    return f"{ms / 3_600_000:.1f}h" if ms >= 3_600_000 else f"{ms / 1000:.0f}s"


# --- structural ------------------------------------------------------------

def effect_not_transactional(p: Pipeline) -> list[Finding]:
    """SC001. A non-idempotent external effect cannot be rolled back.

    Flink's two-phase commit covers sinks that can participate in it. An HTTP
    call to a payment provider cannot: the money has moved before the
    checkpoint completes. If the call is not idempotent, a replay charges
    again, and no engine setting prevents it.
    """
    out = []
    for e in p.effects:
        if not e.idempotent:
            out.append(Finding(
                "SC001", STRUCTURAL, "high", e.name,
                "external effect is declared non-idempotent and sits outside the "
                "commit protocol, so any replay repeats it",
                {"operator": e.operator},
                [str(e.provenance)],
            ))
    return out


def nondeterministic_key(p: Pipeline) -> list[Finding]:
    """SC002. The idempotency key differs between an attempt and its replay.

    A key containing a fresh UUID, a wall clock reading or a processing-time
    function is not a function of the business event. The provider therefore
    sees the replay as a new request. This is the defect that makes an
    otherwise correct idempotency mechanism useless.
    """
    out = []
    for e in p.effects:
        expr = e.key_expr.lower()
        hits = [t for t in NONDETERMINISTIC_TOKENS if t in expr]
        if hits:
            out.append(Finding(
                "SC002", STRUCTURAL, "high", e.name,
                f"idempotency key is not a function of the event: {', '.join(sorted(hits))} "
                f"changes between an attempt and its replay",
                {"key_expr": e.key_expr, "tokens": sorted(hits)},
                [str(e.provenance)],
            ))
    return out


def missing_key(p: Pipeline) -> list[Finding]:
    """SC003. An idempotent-by-claim effect with no key at all."""
    return [
        Finding("SC003", STRUCTURAL, "high", e.name,
                "effect is declared idempotent but carries no idempotency key",
                {}, [str(e.provenance)])
        for e in p.effects if e.idempotent and not e.key_expr.strip()
    ]


def uncommitted_read(p: Pipeline) -> list[Finding]:
    """SC004. Reading uncommitted records from a transactional producer.

    The upstream job writes in transactions so that aborted work disappears. A
    consumer on ``read_uncommitted`` sees that work anyway, and acts on records
    that were later rolled back.
    """
    return [
        Finding("SC004", STRUCTURAL, "high", s.name,
                "source reads a transactionally-written topic with "
                "isolation.level=read_uncommitted, so it can act on records that "
                "are later aborted",
                {"topic": s.topic, "isolation": s.isolation.value},
                [str(s.provenance)])
        for s in p.sources
        if s.upstream_transactional and s.isolation is not Isolation.READ_COMMITTED
    ]


def shared_transactional_id(p: Pipeline) -> list[Finding]:
    """SC005. Two jobs sharing a transactional id prefix fence each other."""
    prefix = p.engine.transactional_id_prefix
    if not prefix or p.engine.guarantee is not Guarantee.EXACTLY_ONCE:
        return []
    clash = [o for o in p.peer_transactional_prefixes if o == prefix]
    if not clash:
        return []
    return [Finding(
        "SC005", STRUCTURAL, "high", prefix,
        "another job on this cluster uses the same transactional id prefix; the "
        "two will fence each other's transactions",
        {"prefix": prefix, "peers": clash},
        [str(p.engine.provenance.get("transactional_id_prefix", "unknown")),
         "cluster-inventory"],
    )]


def effect_before_dedup(p: Pipeline) -> list[Finding]:
    """SC006. The side effect fires upstream of the deduplication that guards it.

    Common when dedup is added later, to fix a downstream symptom, without
    moving the effect. The charge still happens twice; only the record stops
    being duplicated.
    """
    dedups = {d.name for d in p.dedup_operators()}
    if not dedups:
        return []
    out = []
    for e in p.effects:
        upstream_of_effect = p.ancestors(e.operator) | {e.operator}
        if not (dedups & upstream_of_effect):
            guarded = [d for d in dedups if e.operator in p.ancestors(d)]
            if guarded:
                # Severity is medium, not high, and execution is the reason.
                # A duplicate reaching the effect is not by itself a double
                # charge: if the key is sound and still alive, the provider
                # absorbs it. What this rule reports is that the pipeline has
                # no defence of its own and is correct only because the
                # provider is. Witnesses for it reproduce two invocations and
                # one charge, which is what the severity now reflects.
                out.append(Finding(
                    "SC006", STRUCTURAL, "medium", e.name,
                    f"effect fires upstream of the deduplication in "
                    f"{', '.join(sorted(guarded))}, so a duplicate reaches the "
                    f"provider and is absorbed only by its idempotency, not by "
                    f"anything this pipeline does",
                    {"effect_operator": e.operator, "dedup": sorted(guarded)},
                    [str(e.provenance)],
                ))
    return out


def non_idempotent_sink(p: Pipeline) -> list[Finding]:
    """SC007. At-least-once into a sink with no way to recognise a repeat."""
    return [
        Finding("SC007", STRUCTURAL, "medium", s.name,
                "sink is at-least-once and declares no deduplication key, so a "
                "replay writes the row twice",
                {"kind": s.kind, "guarantee": s.guarantee.value},
                [str(s.provenance)])
        for s in p.sinks
        if s.guarantee is not Guarantee.EXACTLY_ONCE and not s.dedup_key.strip()
    ]


# --- duration --------------------------------------------------------------

def key_ttl_below_replay(p: Pipeline) -> list[Finding]:
    """SC101. The provider forgets the key before the replay arrives.

    The headline duration defect. Everything about the pipeline can be correct
    -- transactional sink, deterministic key, effect properly guarded -- and a
    replay still charges twice, because the provider's memory of the key
    expired while the job was down. The comparison is between a number in the
    provider's documentation and a number that exists in no artifact at all.
    """
    horizon = p.replay_horizon()
    out = []
    for e in p.effects:
        if e.provider_key_ttl is None:
            out.append(Finding(
                "SC100", DURATION, "medium", e.name,
                "no idempotency-key lifetime is declared for this provider, so "
                "whether a replay is recognised cannot be decided",
                {"replay_horizon_ms": horizon},
                [str(e.provenance),
                 str(p.engine.provenance.get("checkpoint_interval", "engine config")),
                 str(p.engine.provenance.get("max_downtime", "operational assumption"))],
            ))
            continue
        if e.provider_key_ttl < horizon:
            out.append(Finding(
                "SC101", DURATION, "high", e.name,
                f"provider forgets the idempotency key after {_hours(e.provider_key_ttl)}, "
                f"but a record can be replayed up to {_hours(horizon)} after it was "
                f"first processed; a replay past that point is charged again",
                {
                    "provider_key_ttl_ms": e.provider_key_ttl,
                    "replay_horizon_ms": horizon,
                    "shortfall_ms": horizon - e.provider_key_ttl,
                    "horizon_terms": {
                        "checkpoint_interval": p.engine.checkpoint_interval,
                        "max_downtime": p.engine.max_downtime,
                        "restart_delay": p.engine.restart_delay,
                        "source_lateness": p.source_lateness(),
                    },
                },
                [str(e.provenance),
                 str(p.engine.provenance.get("checkpoint_interval", "engine config")),
                 str(p.engine.provenance.get("max_downtime", "operational assumption"))],
            ))
    return out


def state_ttl_below_replay(p: Pipeline) -> list[Finding]:
    """SC102. Dedup state expires before the duplicate it is meant to catch.

    The pipeline deduplicates correctly and the state that does the
    deduplicating is gone by the time the replay arrives, which makes the
    dedup operator a no-op in exactly the situation it exists for.
    """
    horizon = p.replay_horizon()
    out = []
    for d in p.dedup_operators():
        ttl = d.state_ttl if d.state_ttl is not None else p.engine.state_ttl
        if ttl is None:
            continue
        if ttl < horizon:
            out.append(Finding(
                "SC102", DURATION, "high", d.name,
                f"deduplication state is discarded after {_hours(ttl)} but a replay "
                f"can arrive up to {_hours(horizon)} later, so the duplicate meets "
                f"empty state",
                {"state_ttl_ms": ttl, "replay_horizon_ms": horizon,
                 "shortfall_ms": horizon - ttl},
                [str(d.provenance),
                 str(p.engine.provenance.get("checkpoint_interval", "engine config")),
                 str(p.engine.provenance.get("max_downtime", "operational assumption"))],
            ))
    return out


def txn_timeout_below_downtime(p: Pipeline) -> list[Finding]:
    """SC103. The broker aborts the transaction before the job comes back.

    Kafka expires a transaction that stays open past ``transaction.timeout.ms``.
    If that is shorter than a checkpoint interval plus the time the job spends
    dead, the in-flight transaction is aborted and its records are reprocessed
    -- so a setting chosen to bound broker memory silently becomes a
    correctness setting.
    """
    if p.engine.guarantee is not Guarantee.EXACTLY_ONCE:
        return []
    need = p.engine.checkpoint_interval + p.engine.max_downtime + p.engine.restart_delay
    if p.engine.transaction_timeout >= need:
        return []
    # Medium for the same reason as SC006: an aborted transaction causes a
    # replay, and a replay is only a double charge if the key has also failed.
    return [Finding(
        "SC103", DURATION, "medium", "transaction.timeout.ms",
        f"transaction timeout of {_hours(p.engine.transaction_timeout)} is below the "
        f"{_hours(need)} a checkpoint plus the assumed downtime and restart can take; "
        f"the broker aborts the transaction and the records are reprocessed, which "
        f"costs money only if the idempotency key has also expired",
        {"transaction_timeout_ms": p.engine.transaction_timeout, "required_ms": need,
         "shortfall_ms": need - p.engine.transaction_timeout},
        [str(p.engine.provenance.get("transaction_timeout", "engine config")),
         str(p.engine.provenance.get("checkpoint_interval", "engine config")),
         str(p.engine.provenance.get("max_downtime", "operational assumption"))],
    )]


def txn_timeout_exceeds_broker_cap(p: Pipeline) -> list[Finding]:
    """SC106. No transaction timeout large enough is permitted by the broker.

    Distinct from SC103 and worse. SC103 says the configured timeout is too
    small and can be raised; this says the timeout that *would* be large enough
    exceeds ``transaction.max.timeout.ms``, so the producer's request would be
    rejected and there is no setting of the job that fixes it. The remedies are
    a cluster-wide broker change, a shorter recovery assumption, or giving up
    the transactional sink -- all of them decisions above the pipeline.
    """
    if p.engine.guarantee is not Guarantee.EXACTLY_ONCE:
        return []
    need = p.engine.checkpoint_interval + p.engine.max_downtime + p.engine.restart_delay
    cap = p.engine.broker_max_transaction_timeout
    if need <= cap:
        return []
    return [Finding(
        "SC106", DURATION, "high", "transaction.max.timeout.ms",
        f"surviving the assumed downtime needs a transaction timeout of "
        f"{_hours(need)}, but the broker caps a producer's request at "
        f"{_hours(cap)}; no setting of this job is sufficient, so the recovery "
        f"assumption, the broker cap or the transactional sink has to change",
        {"required_ms": need, "broker_cap_ms": cap, "excess_ms": need - cap},
        [str(p.engine.provenance.get("max_downtime", "operational assumption")),
         "broker config:transaction.max.timeout.ms"],
    )]


def delivery_timeout_above_txn(p: Pipeline) -> list[Finding]:
    """SC104. A producer retry outlives the transaction it belongs to."""
    if p.engine.guarantee is not Guarantee.EXACTLY_ONCE:
        return []
    if p.engine.delivery_timeout <= p.engine.transaction_timeout:
        return []
    return [Finding(
        "SC104", DURATION, "medium", "delivery.timeout.ms",
        f"producer delivery timeout ({_hours(p.engine.delivery_timeout)}) exceeds the "
        f"transaction timeout ({_hours(p.engine.transaction_timeout)}), so a retry can "
        f"still be in flight after its transaction has been aborted",
        {"delivery_timeout_ms": p.engine.delivery_timeout,
         "transaction_timeout_ms": p.engine.transaction_timeout},
        [str(p.engine.provenance.get("delivery_timeout", "engine config")),
         str(p.engine.provenance.get("transaction_timeout", "engine config"))],
    )]


def retry_window_above_key_ttl(p: Pipeline) -> list[Finding]:
    """SC105. Our own client retries for longer than the provider remembers.

    Independent of any replay: a client configured to keep retrying for longer
    than the key lives will eventually send a retry the provider treats as new.
    """
    out = []
    for e in p.effects:
        if e.provider_key_ttl is None or not e.client_retry_window:
            continue
        if e.client_retry_window > e.provider_key_ttl:
            out.append(Finding(
                "SC105", DURATION, "high", e.name,
                f"client retries for up to {_hours(e.client_retry_window)} but the key "
                f"lives {_hours(e.provider_key_ttl)}; a late retry is charged again",
                {"client_retry_window_ms": e.client_retry_window,
                 "provider_key_ttl_ms": e.provider_key_ttl},
                [str(e.provenance)],
            ))
    return out


ALL_CHECKS = (
    effect_not_transactional,
    nondeterministic_key,
    missing_key,
    uncommitted_read,
    shared_transactional_id,
    effect_before_dedup,
    non_idempotent_sink,
    key_ttl_below_replay,
    state_ttl_below_replay,
    txn_timeout_below_downtime,
    txn_timeout_exceeds_broker_cap,
    delivery_timeout_above_txn,
    retry_window_above_key_ttl,
)

RULES = {
    "SC001": "effect outside the commit protocol",
    "SC002": "non-deterministic idempotency key",
    "SC003": "missing idempotency key",
    "SC004": "uncommitted read of transactional output",
    "SC005": "shared transactional id prefix",
    "SC006": "effect upstream of deduplication",
    "SC007": "non-idempotent sink under at-least-once",
    "SC100": "undeclared provider key lifetime",
    "SC101": "key lifetime below replay horizon",
    "SC102": "dedup state TTL below replay horizon",
    "SC103": "transaction timeout below downtime",
    "SC104": "delivery timeout above transaction timeout",
    "SC105": "client retry window above key lifetime",
    "SC106": "required transaction timeout exceeds the broker cap",
}


def analyse(p: Pipeline) -> list[Finding]:
    """Run every check.  Order is stable so reports diff cleanly."""
    out: list[Finding] = []
    for check in ALL_CHECKS:
        out.extend(check(p))
    return sorted(out, key=lambda f: (f.rule, f.subject))
