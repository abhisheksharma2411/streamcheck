# Novelty and prior art

Live search on 2026-09-14, plus the guardrails in the project brief. The claim
below is narrower than the one this started as, because the general problem is
well covered and only one part of it is open.

## What is claimed

**Exactly-once for a payment pipeline is a conjunction of structural properties
and duration inequalities whose terms live in separate artifacts, and no tool
checks the conjunction.** Concretely: a checker that gathers facts from an
engine config, client properties, job SQL, a runbook and a declaration of the
provider's idempotency contract, derives a *replay horizon* from them, states
the defect classes as relations against it, and turns each finding into an
executable scenario whose observable is money moving twice.

The measured form of the claim is the one in Section 8.2 of the paper: 210 of
515 findings cannot be derived from fewer than three artifacts, and no single
artifact yields more than 11.5% of the duration findings.

## What is not claimed

Not claimed: a first static analyser for stream pipelines, novelty in
exactly-once semantics, or a proof of exactly-once. The checks are sufficient
conditions for a violation and never certify correctness, and the paper says so
in three places because it is the kind of thing readers assume.

Per the brief's banned list, nothing here claims "exactly-once guaranteed". The
whole argument is that the guarantee is conditional on a key lifetime, a fault
model and an evidence lifetime, which is precisely the condition the banned
phrase elides.

## Occupied ground, and how this differs

**Fault recovery benchmarking** — Vogel, Henning, Perez-Wohlfeil, Ertl and
Rabiser (DEBS 2024, arXiv:2404.06203) benchmark recovery across stream
frameworks, measuring recovery time and post-failure throughput.

*Difference*: they measure how long recovery takes; we ask whether a given
configuration is correct given how long it takes. Their measurements are the
empirical basis for the `max_downtime` term our horizon needs, and the two are
complementary rather than competing.

**Kafka pattern surveys** — Mohammad (arXiv:2512.16146) synthesises 42 studies
into nine recurring Kafka design patterns and audits benchmarking practice.

*Difference*: a description of what teams build. No correctness condition, no
checking.

**Exactly-once semantics literature and vendor documentation** — Kafka
transactions, Flink two-phase commit sinks, the interaction between them. All
mature, all about effects *inside* the system boundary.

*Difference*: an external payment call is outside that boundary by
construction. The money moves before the checkpoint completes and no commit
protocol retracts it. Everything here is about that gap.

**SV-COMP witness formats and correctness witnesses** — the idea that a verdict
should ship with a checkable artifact, validated by an independent tool.

*Difference*: ours are violation witnesses validated by execution rather than
by a second analyser, which suits a defect whose observable is unambiguous.
The debt is conceptual and acknowledged.

**Practitioner writing on idempotency TTLs and transaction timeouts** — the
advice to keep `transaction.timeout.ms` above the checkpoint interval plus
expected downtime, and never to expire a key before the client's longest retry
window, is common and correct.

*Difference*: it is advice. Nothing computes the replay horizon those
comparisons need, because its terms are in four files. One search result put it
plainly: "the search results don't specifically reference a static checker tool
for detecting these misconfigurations."

## Sibling papers

P5 (ReconStream) contributes the idea of a *declared* bound on a source's
lateness, which is one of the six terms in the replay horizon, and its
reconciliation setting is one of the five corpus shapes. The two papers apply
the same shape of argument — a correctness property that is a relation between
durations — at different points of the pipeline: P5 to when a reconciler may
decide, P6 to when a replay stops being safe.

T8 supplies operation identity and the provider-aware contract that the effects
manifest is an instance of. T4 supplies failure patterns the structural checks
encode.

## Narrowing that happened during the work

1. The original framing was "static detection of exactly-once violations",
   which overreaches: the checks detect violations, never certify their
   absence. Every claim is now one-directional.
2. Two rules were demoted from high to medium severity after their witnesses
   reproduced a duplicate provider call that the provider's idempotency
   absorbed, rather than a double charge. The taxonomy now distinguishes
   defects sufficient on their own from defects that need a second failure, and
   that distinction came out of execution rather than design.
3. The "static analysis beats testing" claim did not survive measurement in its
   strong form: fault injection finds most of these defects in a few trials
   *if* the tester simulates long enough outages. The claim is now about the
   dependence on that assumption, measured as a sweep.
