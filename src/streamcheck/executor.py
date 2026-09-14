"""Run a witness and count the charges.

This is a small interpreter of the recovery semantics the engines document:
records are processed in order, a checkpoint periodically commits progress, a
failure discards everything since the last checkpoint, and on restart the job
resumes from it and reprocesses what was lost. A side effect is a call to a
provider that remembers an idempotency key for a stated lifetime and treats a
repeat within that lifetime as a no-op.

Written from those semantics rather than from :mod:`checks`, and deliberately
so: a witness verified by the same reasoning that produced it demonstrates
nothing. Even written independently, agreement between this and the checker is
weak evidence, because both are mine. The evidence that counts comes from
:mod:`kafka_flink`, which runs the same witness against a real broker, a real
job and a real HTTP provider, and the paper reports the two separately.

What this buys over the real executor is that it is exact and instant, so every
witness in the corpus can be run rather than a sample.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .ir import Guarantee, Millis, Pipeline
from .witness import Witness


@dataclass
class Provider:
    """An external payment API with an idempotency key of finite memory."""

    key_ttl: Millis | None
    #: key -> instant it was first accepted
    seen: dict[str, Millis] = field(default_factory=dict)
    charges: list[tuple[str, str, Millis]] = field(default_factory=list)
    invocations: list[tuple[str, Millis]] = field(default_factory=list)

    def charge(self, key: str, order_id: str, now: Millis) -> bool:
        """Attempt a charge.  Returns True if money actually moved."""
        self.invocations.append((order_id, now))
        if key:
            first = self.seen.get(key)
            if first is not None:
                if self.key_ttl is None or now - first <= self.key_ttl:
                    return False          # recognised as a repeat
                # The key expired. The provider has no memory of it, so this is
                # a new request as far as it is concerned.
            self.seen[key] = now
        self.charges.append((key, order_id, now))
        return True

    def count_for(self, order_id: str) -> int:
        return sum(1 for _, o, _ in self.charges if o == order_id)

    def invocations_for(self, order_id: str) -> int:
        return sum(1 for o, _ in self.invocations if o == order_id)


@dataclass
class DedupState:
    """Keyed state with a time-to-live, as a stream processor keeps it."""

    ttl: Millis | None
    seen: dict[str, Millis] = field(default_factory=dict)

    def is_duplicate(self, key: str, now: Millis) -> bool:
        first = self.seen.get(key)
        if first is not None and (self.ttl is None or now - first <= self.ttl):
            return True
        self.seen[key] = now
        return False

    def snapshot(self) -> dict:
        return dict(self.seen)

    def restore(self, snap: dict) -> None:
        self.seen = dict(snap)


@dataclass
class RunResult:
    """What running a witness showed.

    Two counts, and keeping them apart is what let execution correct the
    static rules. ``invocations`` is how many times the pipeline asked the
    provider to move money; ``charges`` is how many times money actually
    moved. A defect that produces a second invocation but not a second charge
    is a pipeline relying entirely on the provider's idempotency to stay
    correct -- worth reporting, and not the same thing as a double charge.
    """

    charges: int
    invocations: int
    order_id: str
    expected_charges: int
    expected_invocations: int
    confirmed: bool
    charges_confirmed: bool
    events: list[str] = field(default_factory=list)
    replays: int = 0

    def as_dict(self) -> dict:
        return {
            "charges": self.charges,
            "invocations": self.invocations,
            "order_id": self.order_id,
            "expected_charges": self.expected_charges,
            "expected_invocations": self.expected_invocations,
            "confirmed": self.confirmed,
            "charges_confirmed": self.charges_confirmed,
            "replays": self.replays,
            "events": list(self.events),
        }


def _key_for(pipeline: Pipeline, order_id: str, attempt: int, now: Millis) -> str:
    """Evaluate the pipeline's key expression for one record.

    Only the property the checks care about is modelled: whether the expression
    is a function of the event. A key naming a non-deterministic source gets a
    fresh value on every attempt, which is exactly what makes the provider treat
    a replay as new.
    """
    effect = pipeline.effects[0]
    expr = effect.key_expr
    if not expr:
        return ""                       # no key at all: never recognised
    low = expr.lower()
    if any(t in low for t in ("uuid", "random", "now(", "current_timestamp",
                              "proctime", "nanotime", "currentmillis")):
        return f"{order_id}:attempt{attempt}:{now}"
    if "offset" in low or "partition" in low:
        return f"{order_id}:offset{attempt}"
    return f"{order_id}:stable"


def run(pipeline: Pipeline, w: Witness) -> RunResult:
    """Execute a witness against the interpreter and count the charges."""
    effect = pipeline.effects[0] if pipeline.effects else None
    provider = Provider(key_ttl=w.provider_key_ttl)
    dedups = pipeline.dedup_operators()
    dedup = dedups[0] if dedups else None
    dedup_state = DedupState(
        ttl=(dedup.state_ttl if dedup and dedup.state_ttl is not None else pipeline.engine.state_ttl)
    ) if dedup else None

    # Is the effect downstream of the deduplication, or upstream of it?
    effect_guarded = False
    if dedup is not None and effect is not None:
        effect_guarded = dedup.name in (pipeline.ancestors(effect.operator) | {effect.operator})

    cp = max(1, w.checkpoint_interval)
    events: list[str] = []
    attempt = 0
    replays = 0

    #: Records not yet covered by a checkpoint. A failure loses exactly these.
    uncommitted: list = []
    committed_upto = 0
    dedup_committed = dedup_state.snapshot() if dedup_state else {}

    timeline = sorted(
        [("record", r.at, r) for r in w.records] + [("fault", f.at, f) for f in w.faults],
        key=lambda x: (x[1], 0 if x[0] == "record" else 1),
    )

    pending: list = [r for r in w.records]
    now = 0
    idx = 0
    faults = list(w.faults)
    processed: set[tuple[str, int]] = set()

    def process(rec, at: Millis, replay: bool) -> None:
        nonlocal attempt
        if dedup_state is not None and effect_guarded:
            if dedup_state.is_duplicate(rec.order_id, at):
                events.append(f"{at}: {rec.order_id} dropped by dedup")
                return
        attempt += 1
        key = _key_for(pipeline, rec.order_id, attempt, at)
        if effect is not None and not effect.idempotent:
            key = ""
        moved = provider.charge(key, rec.order_id, at)
        events.append(
            f"{at}: {'replay' if replay else 'first'} of {rec.order_id} "
            f"key={key or '<none>'} -> {'CHARGED' if moved else 'recognised as repeat'}"
        )
        if dedup_state is not None and not effect_guarded:
            dedup_state.is_duplicate(rec.order_id, at)

    # Advance through the timeline, checkpointing on the interval.
    for kind, at, item in timeline:
        # Checkpoints between the previous instant and this one.
        while committed_upto + cp <= at:
            committed_upto += cp
            uncommitted.clear()
            if dedup_state is not None:
                dedup_committed = dedup_state.snapshot()
            events.append(f"{committed_upto}: checkpoint")
        if kind == "record":
            process(item, at, replay=False)
            uncommitted.append((item, at))
        else:
            back = at + item.down_for
            events.append(f"{at}: job fails; {len(uncommitted)} record(s) not checkpointed")
            lost = list(uncommitted)
            uncommitted.clear()
            if dedup_state is not None:
                dedup_state.restore(dedup_committed)
            for rec, _orig in lost:
                replays += 1
                process(rec, back, replay=True)
            committed_upto = back
            events.append(f"{back}: job resumes")

    charges = provider.count_for(w.order_id)
    invocations = provider.invocations_for(w.order_id)
    return RunResult(
        charges=charges,
        invocations=invocations,
        order_id=w.order_id,
        expected_charges=w.expect_charges,
        expected_invocations=w.expect_invocations,
        confirmed=(invocations >= w.expect_invocations and charges >= w.expect_charges),
        charges_confirmed=charges >= 2,
        events=events,
        replays=replays,
    )
