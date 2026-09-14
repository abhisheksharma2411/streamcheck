# Assumptions

Decisions taken without asking, with the reasoning, so a reader can disagree
with a specific one rather than with the whole artifact.

## Modelling

**The replay horizon has six terms and a drain factor of one.** The backlog
that accumulates during an outage takes about as long to drain as the outage
lasted. A busier pipeline drains slower and a quieter one faster; the factor is
a parameter and the sweep in E7 does not vary it, which is a gap.

**Recovery assumptions come from a runbook.** Nothing in a build system records
how long a job may stay dead. Where no runbook states it, the fact is reported
missing rather than defaulted, and the duration checks that need it say they
cannot be decided.

**The provider's key lifetime must be declared.** It is a fact about a third
party. Guessing it would make the tool assert something it has no basis for,
and the assumption would be invisible in exactly the way this paper complains
about. Absence is a finding (SC100).

**One effect per pipeline.** The corpus builds pipelines with a single external
effect. Multiple effects with different providers and different key lifetimes
are a straightforward generalisation of the same relations and are untested.

**Single partition.** Nothing exercises rescaling, partition reassignment or
key skew. The offset-derived key defect (`offset` in the key expression) is
recognised statically but its manifestation needs a repartition we do not
perform.

## Corpus

**We wrote both the mutations and the checks.** Recall against mutations named
after the rules meant to catch them is therefore a statement about
implementation correctness, not detection power. The boundary cases exist
because of this: separating configurations that differ by one second requires
the arithmetic to be right, which no amount of knowing the mutation names
supplies. The decoys exist so that "looks alarming" and "is defective" come
apart.

**Five shapes, drawn from practice, not sampled from it.** Capture, refund,
reconciliation, payout and dunning are what payments teams build. They are not
a sample of any population and no prevalence claim rests on them.

**The clean baseline was wrong once.** The first generator drew recovery
assumptions of 5, 15 and 30 minutes, and every "clean" pipeline it produced
violated the broker-cap rule added later. The baseline was corrected and the
episode is reported, because a corpus whose negatives are not negative
invalidates every false-positive number computed on it.

## Execution

**Witnesses run compressed.** Every duration check is an inequality between
durations, so scaling all durations by one factor preserves it. E6 measures
this rather than assuming it, across four orders of magnitude.

**The stream processor in the live arm is not Flink.** Expressing an external
HTTP effect in Flink SQL needs a custom connector; writing one would have put
more of our own code between the witness and the result. What is real is the
mechanism under test: a real broker, a real SIGKILL, and replay determined by
Kafka's offset commit rather than by anything we wrote.

**The kill is triggered by observing the effect, not by a timer.** Racing a
compressed checkpoint interval against real process startup produced a run in
which the worker committed before the kill landed, so nothing replayed and a
genuine defect looked absent. Killing on the first provider invocation realises
the witness's premise — failure after the effect and before the checkpoint —
without the race.

**Consumer session timeouts are shortened.** A killed member must be evicted
before its replacement can take the partition, and the default is long enough
that the replacement gives up first. This is test-harness tuning, not a claim
about production settings.

## Economics

There are none. The observable is a count of charges, not a dollar figure, and
no cost model is used anywhere. Deliberate: the loss from a double charge
depends on dispute rates and remediation practices we have no data on, and
inventing them would have added a decimal point to a result that does not need
one.
