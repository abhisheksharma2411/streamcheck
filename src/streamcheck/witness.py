"""Turn a finding into something you can run.

A static finding about a payment pipeline is easy to wave away. The engineer
who owns the job has seen linter output before, and "possible idempotency
issue" competes with real work. What ends that argument is a scenario: these
three records, kill the job at this instant, bring it back after this long, and
the provider will show two charges for one order.

Each witness carries the inputs, the fault schedule, and the observation it
predicts. Running it either produces the second charge or it does not, and a
witness that does not is a false positive we report as such.

**Why witnesses can run in seconds.** Real replay horizons are hours: a
checkpoint interval plus however long a job stays dead. Executing a witness at
that scale would make the evaluation impossible. Every duration check, though,
is a comparison between durations and nothing else -- ``key_ttl <
replay_horizon`` and the rest are homogeneous of degree one -- so multiplying
every duration in a witness by the same positive factor leaves every such
comparison, and therefore the defect, unchanged. Witnesses are synthesised in
the pipeline's own units and executed compressed. The factor is recorded with
every result, and Section 6 checks that a witness confirmed at one factor is
confirmed at another.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from .checks import DURATION, Finding
from .ir import Millis, Pipeline


@dataclass(frozen=True, slots=True)
class Record:
    """One input event, with the business identity the key should derive from."""

    at: Millis
    order_id: str
    amount: int
    key: str = ""

    def as_dict(self) -> dict:
        return {"at": self.at, "order_id": self.order_id, "amount": self.amount, "key": self.key}


@dataclass(frozen=True, slots=True)
class Fault:
    """Kill the job at ``at``; it comes back ``down_for`` later."""

    at: Millis
    down_for: Millis

    def as_dict(self) -> dict:
        return {"at": self.at, "down_for": self.down_for}


@dataclass
class Witness:
    """A runnable scenario that should exhibit one finding."""

    rule: str
    subject: str
    records: list[Record]
    faults: list[Fault]
    #: What running it should show. ``expect_invocations`` is how many times the
    #: pipeline asks the provider to move money; ``expect_charges`` is how many
    #: times money actually moves. They differ for defects that produce a
    #: repeated call which the provider's idempotency then absorbs -- a real
    #: weakness, and not a double charge.
    expect_charges: int
    expect_invocations: int
    order_id: str
    rationale: str
    horizon: Millis
    provider_key_ttl: Millis | None
    checkpoint_interval: Millis
    scale: float = 1.0

    def scaled(self, factor: float) -> "Witness":
        """Compress every duration by ``factor``.

        Sound for the duration checks because each is an inequality between
        durations, and for the structural ones because they do not involve
        durations at all.
        """
        def s(x: Millis) -> Millis:
            return max(1, int(round(x * factor)))

        return replace(
            self,
            records=[replace(r, at=s(r.at)) for r in self.records],
            faults=[Fault(s(f.at), s(f.down_for)) for f in self.faults],
            horizon=s(self.horizon),
            provider_key_ttl=None if self.provider_key_ttl is None else s(self.provider_key_ttl),
            checkpoint_interval=s(self.checkpoint_interval),
            scale=self.scale * factor,
        )

    def duration(self) -> Millis:
        ends = [r.at for r in self.records] + [f.at + f.down_for for f in self.faults]
        return max(ends, default=0) + max(1, self.checkpoint_interval)

    def as_dict(self) -> dict:
        return {
            "rule": self.rule,
            "subject": self.subject,
            "records": [r.as_dict() for r in self.records],
            "faults": [f.as_dict() for f in self.faults],
            "expect_charges": self.expect_charges,
            "order_id": self.order_id,
            "rationale": self.rationale,
            "horizon": self.horizon,
            "provider_key_ttl": self.provider_key_ttl,
            "checkpoint_interval": self.checkpoint_interval,
            "scale": self.scale,
            "duration": self.duration(),
        }


def _effect_for(p: Pipeline, subject: str):
    return next((e for e in p.effects if e.name == subject), None)


def synthesise(p: Pipeline, f: Finding) -> Witness | None:
    """Build a runnable scenario for one finding, or ``None`` if the rule is
    not one a single-record scenario can exhibit.

    The shape is the same throughout: process one order, fail after the effect
    has fired but before the work is checkpointed, and come back. What differs
    between rules is *when* the fault has to land for the defect to show, and
    that instant is computed from the quantities the finding already carries.
    """
    horizon = p.replay_horizon()
    cp = p.engine.checkpoint_interval
    order = "ord-witness-1"

    if f.rule in ("SC101", "SC102"):
        # The record must be replayed after the shorter of the two lifetimes
        # has expired but while replay is still possible. Failing just after
        # the effect fires, and staying down past the expiry, does it.
        ttl = f.quantities.get("provider_key_ttl_ms") or f.quantities.get("state_ttl_ms")
        effect = _effect_for(p, f.subject)
        # Down long enough to outlive the expiring thing, bounded by the
        # horizon the pipeline itself admits.
        down = min(max(ttl + cp, cp * 2), horizon)
        return Witness(
            rule=f.rule, subject=f.subject,
            records=[Record(at=0, order_id=order, amount=49_900, key=order)],
            faults=[Fault(at=cp // 2, down_for=down)],
            expect_charges=2, expect_invocations=2, order_id=order,
            rationale=(
                f"the effect fires before the first checkpoint, the job dies and stays "
                f"down for {down} ms, and the replay arrives after the "
                f"{'provider key' if f.rule == 'SC101' else 'dedup state'} lifetime of "
                f"{ttl} ms has expired, so the repeat is not recognised"
            ),
            horizon=horizon,
            provider_key_ttl=effect.provider_key_ttl if effect else ttl,
            checkpoint_interval=cp,
        )

    if f.rule in ("SC001", "SC002", "SC003"):
        # No waiting required. The key is wrong, or absent, or the effect is not
        # idempotent, so any replay at all repeats the charge.
        effect = _effect_for(p, f.subject)
        return Witness(
            rule=f.rule, subject=f.subject,
            records=[Record(at=0, order_id=order, amount=49_900, key=order)],
            faults=[Fault(at=cp // 2, down_for=p.engine.restart_delay)],
            expect_charges=2, expect_invocations=2, order_id=order,
            rationale=(
                "the effect is repeated on replay and the provider cannot recognise it, "
                "so no delay is needed: failing before the first checkpoint is enough"
            ),
            horizon=horizon,
            provider_key_ttl=effect.provider_key_ttl if effect else None,
            checkpoint_interval=cp,
        )

    if f.rule == "SC006":
        effect = _effect_for(p, f.subject)
        return Witness(
            rule=f.rule, subject=f.subject,
            records=[
                Record(at=0, order_id=order, amount=49_900, key=order),
                Record(at=cp // 4, order_id=order, amount=49_900, key=order),
            ],
            faults=[],
            # Two calls, but only one charge when the key still works: this
            # pipeline is correct only because the provider is.
            expect_charges=1, expect_invocations=2, order_id=order,
            rationale=(
                "two copies of the same order reach the effect before the deduplication "
                "downstream of it can remove either, so the provider is asked to move "
                "money twice; whether it does depends entirely on its idempotency"
            ),
            horizon=horizon,
            provider_key_ttl=effect.provider_key_ttl if effect else None,
            checkpoint_interval=cp,
        )

    if f.rule == "SC105":
        effect = _effect_for(p, f.subject)
        ttl = f.quantities["provider_key_ttl_ms"]
        return Witness(
            rule=f.rule, subject=f.subject,
            records=[Record(at=0, order_id=order, amount=49_900, key=order)],
            faults=[Fault(at=cp // 2, down_for=ttl + cp)],
            expect_charges=2, expect_invocations=2, order_id=order,
            rationale=(
                f"the client keeps retrying past the {ttl} ms key lifetime, so the retry "
                f"that eventually succeeds is treated as a new request"
            ),
            horizon=horizon,
            provider_key_ttl=effect.provider_key_ttl if effect else ttl,
            checkpoint_interval=cp,
        )

    if f.rule == "SC103":
        return Witness(
            rule=f.rule, subject=f.subject,
            records=[Record(at=0, order_id=order, amount=49_900, key=order)],
            faults=[Fault(at=cp // 2,
                          down_for=p.engine.transaction_timeout + cp)],
            # The aborted transaction causes a replay; the key decides whether
            # that replay costs anything.
            expect_charges=1, expect_invocations=2, order_id=order,
            rationale=(
                "the job stays down past the transaction timeout, the broker aborts the "
                "open transaction, and the records inside it are processed again; the "
                "repeat reaches the provider and is absorbed only if the key still lives"
            ),
            horizon=horizon,
            provider_key_ttl=next((e.provider_key_ttl for e in p.effects), None),
            checkpoint_interval=cp,
        )

    # SC004, SC005, SC007 and SC100 describe conditions whose manifestation
    # needs a second job, a second consumer, or a fact nobody declared. Saying
    # so is better than emitting a scenario that cannot show anything.
    return None


def synthesise_all(p: Pipeline, findings: list[Finding]) -> dict[str, Witness]:
    """Witnesses for every finding that admits one, keyed by rule and subject."""
    out: dict[str, Witness] = {}
    for f in findings:
        w = synthesise(p, f)
        if w is not None:
            out[f"{f.rule}:{f.subject}"] = w
    return out


def coverage(findings: list[Finding], witnesses: dict[str, Witness]) -> dict:
    """How much of a report is backed by something runnable."""
    total = len(findings)
    dur = [f for f in findings if f.family == DURATION]
    return {
        "findings": total,
        "witnessed": len(witnesses),
        "coverage": len(witnesses) / total if total else 0.0,
        "duration_findings": len(dur),
        "duration_witnessed": sum(
            1 for f in dur if f"{f.rule}:{f.subject}" in witnesses
        ),
    }
