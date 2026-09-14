"""The pipeline model the analyser reasons over.

A streaming payment pipeline is not one artifact.  The topology lives in a
Flink job, the delivery guarantees live in connector properties, the consumer's
view of transactions lives in a Kafka client config, and the rule that actually
decides whether a retry charges the customer twice -- how long the payment
provider remembers an idempotency key -- lives in the provider's documentation
and in no file the team owns at all.

That scattering is why these defects survive review.  Each artifact is
defensible on its own; the fault is in the relation between them.  This module
gathers the facts into one structure so a relation can be stated, and keeps a
record of which artifact each fact came from, because a finding that cannot
name its evidence is not actionable.

Durations are milliseconds throughout.  Mixing units in a codebase whose whole
subject is comparing durations would be an unforced error.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

Millis = int


class Guarantee(str, Enum):
    """Delivery guarantee a stage claims."""

    NONE = "none"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


class Isolation(str, Enum):
    READ_UNCOMMITTED = "read_uncommitted"
    READ_COMMITTED = "read_committed"


#: Expression fragments that make an idempotency key differ between the first
#: attempt and a replay of the same record.  A key built from any of these is
#: not a function of the business event, so the provider cannot recognise the
#: retry.  ``offset`` and ``partition`` are included because they are stable
#: only until a topic is repartitioned or a job is rescaled -- which is exactly
#: when a replay is most likely.
NONDETERMINISTIC_TOKENS = (
    "uuid", "random", "rand(", "now(", "current_timestamp", "systime",
    "proctime", "processing_time", "nanotime", "currentmillis",
    "offset", "partition", "taskid", "subtask", "hostname",
)


#: Kafka's broker-side ceiling on a producer's requested transaction timeout,
#: ``transaction.max.timeout.ms``. A producer asking for more is rejected. The
#: default is fifteen minutes, read off a running broker rather than quoted --
#: ``experiments/e8_broker.py`` queries it and records what it found.
#:
#: This number is why SC106 exists. Raising it is a cluster-wide broker change,
#: which in most organisations is not the application team's to make, so a
#: pipeline whose recovery assumption exceeds it has no application-level fix.
BROKER_MAX_TXN_TIMEOUT_DEFAULT: "Millis" = 900_000


@dataclass(frozen=True, slots=True)
class Provenance:
    """Where a fact came from, so a finding can cite its evidence."""

    artifact: str
    locator: str = ""

    def __str__(self) -> str:
        return f"{self.artifact}:{self.locator}" if self.locator else self.artifact


@dataclass
class EngineConfig:
    """Facts about the stream processor's recovery behaviour.

    ``max_downtime`` is the operational assumption nobody writes down: how long
    a job may stay dead before someone restarts it.  It enters two of the
    duration checks, so leaving it implicit hides them.
    """

    checkpoint_interval: Millis = 60_000
    min_pause_between_checkpoints: Millis = 0
    restart_delay: Millis = 10_000
    max_downtime: Millis = 300_000
    #: Time to re-read and re-process the backlog accumulated while down.
    drain_factor: float = 1.0
    state_ttl: Millis | None = None
    transaction_timeout: Millis = 900_000
    delivery_timeout: Millis = 120_000
    guarantee: Guarantee = Guarantee.AT_LEAST_ONCE
    transactional_id_prefix: str = ""
    #: The cluster's cap on a requested transaction timeout.
    broker_max_transaction_timeout: Millis = BROKER_MAX_TXN_TIMEOUT_DEFAULT
    provenance: dict[str, Provenance] = field(default_factory=dict)

    def replay_horizon(self, source_lateness: Millis = 0) -> Millis:
        """How stale a record can be when it is processed for the second time.

        On recovery the job resumes from its last checkpoint, so anything
        processed since that checkpoint is replayed: up to one checkpoint
        interval of work, plus however long the job stayed down, plus the
        restart itself, plus the time to drain the backlog that piled up.
        ``source_lateness`` covers records that had not yet arrived when the
        job died and are still in flight -- the bound P5 makes explicit.

        This is the quantity every duration check below is stated against, and
        the reason those checks are not expressible inside any one artifact:
        two of its four terms are engine facts and two are operational ones.
        """
        backlog = int((self.max_downtime + self.restart_delay) * self.drain_factor)
        return (
            self.checkpoint_interval
            + self.min_pause_between_checkpoints
            + self.max_downtime
            + self.restart_delay
            + backlog
            + source_lateness
        )


@dataclass
class Source:
    """A Kafka topic the pipeline reads."""

    name: str
    topic: str
    isolation: Isolation = Isolation.READ_UNCOMMITTED
    group_id: str = ""
    #: Declared upper bound on how late a record may be, in the sense of P5's
    #: source model.  Feeds the replay horizon.
    lateness_bound: Millis = 0
    #: Whether the topic is written by a transactional producer.  If it is, a
    #: consumer reading uncommitted sees records that may be rolled back.
    upstream_transactional: bool = False
    provenance: Provenance = field(default_factory=lambda: Provenance("unknown"))


@dataclass
class Effect:
    """An external side effect: money moves and the engine cannot undo it.

    ``key_expr`` is the expression the pipeline uses as the provider's
    idempotency key, written as it appears in the job.  ``provider_key_ttl`` is
    how long the provider remembers it, which is a fact about the provider and
    not about this repository -- so it has to be declared, and the analyser says
    so rather than assuming a value.
    """

    name: str
    operator: str
    key_expr: str = ""
    provider_key_ttl: Millis | None = None
    client_retry_window: Millis = 0
    idempotent: bool = True
    amount_field: str = "amount"
    provenance: Provenance = field(default_factory=lambda: Provenance("unknown"))


@dataclass
class Operator:
    """One node of the dataflow."""

    name: str
    kind: str                      # map | filter | dedup | join | aggregate | process
    upstream: list[str] = field(default_factory=list)
    keyed_by: str = ""
    state_ttl: Millis | None = None
    provenance: Provenance = field(default_factory=lambda: Provenance("unknown"))


@dataclass
class Sink:
    name: str
    kind: str                      # kafka | jdbc | http
    guarantee: Guarantee = Guarantee.AT_LEAST_ONCE
    dedup_key: str = ""
    upstream: list[str] = field(default_factory=list)
    provenance: Provenance = field(default_factory=lambda: Provenance("unknown"))


@dataclass
class Pipeline:
    """Everything the analyser needs, gathered from every artifact."""

    name: str
    engine: EngineConfig = field(default_factory=EngineConfig)
    sources: list[Source] = field(default_factory=list)
    operators: list[Operator] = field(default_factory=list)
    effects: list[Effect] = field(default_factory=list)
    sinks: list[Sink] = field(default_factory=list)
    #: Other jobs sharing the cluster, for the transactional-id check.
    peer_transactional_prefixes: list[str] = field(default_factory=list)

    def operator(self, name: str) -> Operator | None:
        return next((o for o in self.operators if o.name == name), None)

    def source_lateness(self) -> Millis:
        return max((s.lateness_bound for s in self.sources), default=0)

    def replay_horizon(self) -> Millis:
        return self.engine.replay_horizon(self.source_lateness())

    def ancestors(self, name: str) -> set[str]:
        """Every node upstream of ``name``, transitively."""
        by_name = {o.name: o for o in self.operators}
        seen: set[str] = set()
        stack = list(by_name[name].upstream) if name in by_name else []
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            node = by_name.get(cur)
            if node is not None:
                stack.extend(node.upstream)
        return seen

    def dedup_operators(self) -> list[Operator]:
        return [o for o in self.operators if o.kind == "dedup"]

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "replay_horizon": self.replay_horizon(),
            "checkpoint_interval": self.engine.checkpoint_interval,
            "guarantee": self.engine.guarantee.value,
            "sources": [s.topic for s in self.sources],
            "operators": [(o.name, o.kind) for o in self.operators],
            "effects": [e.name for e in self.effects],
            "sinks": [(s.name, s.kind) for s in self.sinks],
        }
