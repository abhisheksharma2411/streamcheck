# Experiment log

Every experiment writes one JSON to `results/`, stamped with the git revision
and the seeds. This is the log of what was run, what it showed, and what had to
change because of it.

## Iterations that changed the design

**The witness did not reproduce, and the harness was why.** The first live run
of an SC101 witness produced one charge instead of two. The worker had
committed its offsets *before* the kill landed, so nothing was replayed: the
orchestrator slept for a fixed margin that swamped the compressed checkpoint
interval. Racing a timer was the wrong design. The kill is now triggered by
observing the provider's first invocation, with the commit interval set beyond
the length of a run, which realises the witness's premise — failure after the
effect, before the checkpoint — without a race.

**Then it reproduced only sometimes.** With the kill fixed, the defective run
replayed and the control did not, at random. A SIGKILLed consumer is not
evicted from its group until the session timeout expires, and the default is
long enough that the replacement gave up first. Shortening the session and
heartbeat intervals made it deterministic. Both of these were harness faults
that looked exactly like the defect being absent, which is the failure mode to
watch for in this kind of evaluation.

**Execution demoted two rules.** SC006 (effect upstream of its deduplication)
and SC103 (transaction timeout below the downtime) both predicted a double
charge. Their witnesses reproduce a second *call* to the provider, which a
sound key still inside its lifetime absorbs. The pipelines are wrong — they have
no defence of their own — but they are not double-charging. Both were demoted to
medium severity, their witnesses now predict two invocations and one charge,
and the observable was split into `invocations` and `charges` so the
distinction cannot be lost again. A test pins it.

**The clean baseline was not clean.** Adding SC106 (required transaction
timeout above the broker's cap) made every "clean" case in the corpus fire,
because the generator drew recovery assumptions of 5, 15 and 30 minutes and two
of those exceed what a default broker permits. Those are unremarkable numbers,
which is the finding; the generator was corrected so the negatives are actually
negative, and the episode is reported rather than quietly fixed.

**100% recall on the first try meant the corpus was too easy.** The mutations
were named after the rules meant to catch them, and the same author wrote both.
Four boundary mutations were added that put a lifetime one second either side
of the replay horizon, and two decoys that look alarming and are correct.
Separating those requires the horizon arithmetic to be right; recognising a
mutation name does not help.

**The strong form of "static beats testing" did not survive measurement.**
Random fault injection finds most of these defects within a few trials, if the
tester simulates long enough outages. The claim was narrowed to the dependence
on that assumption and measured as a sweep over the tester's assumed worst
case, which is where the real gap is: 15% found at a five-minute ceiling,
100% at twelve hours.

**A SQL precision limit and a missing client feature.** `INTERVAL 'n' SECOND`
caps at 99 in Flink SQL (inherited from P5's harness). The Python Kafka client
available does not implement transactions, so the planned rejection probe for
the broker cap could not be run; the cap's *value* is measured from a running
broker and the refusal is left as documented behaviour.

## The experiments

| | question | result file |
|---|---|---|
| E1 | Detection over the corpus: recall, false positives, cost | `e1_detection.json` |
| E2 | Do the synthesised witnesses reproduce, in the interpreter? | `e2_witness.json` |
| E3 | Do they reproduce on a real broker, with a matched control? | `e3_live.json` |
| E4 | How many artifacts does a finding need? | `e4_artifacts.json` |
| E5 | What would fault-injection testing cost? | `e5_random.json` |
| E6 | Does compressing a witness change its verdict? | `e6_scale.json` |
| E7 | How much of the plausible configuration space is affected? | `e7_prevalence.json` |
| E8 | The broker's transaction-timeout cap, measured | `e8_broker.json` |

## Protocol

Three seeds, 21 mutations across 5 pipeline shapes, two instances per
combination: 630 pipelines, 150 of them negatives. Each repeat draws a
different engine configuration from the same distribution, so no rule can pass
by matching one particular set of numbers.

Witnesses are executed compressed; E6 checks that the compression changes no
verdict across four orders of magnitude. The live arm scales each witness so
the provider's key lifetime lands near a second, pairs every run with a control
in which the provider never forgets, and reports a defect as attributable only
when the control does not reproduce it.

## Negative and null results

- Two rules do **not** cause a double charge on their own (SC006, SC103).
- The strong "testing cannot find these" claim is **false**; the dependence on
  the tester's assumed worst case is the real result.
- The corpus's headline recall number is **not** evidence of detection power
  and is labelled as such everywhere it appears.
- The broker-cap *refusal* was **not** reproduced, only the cap's value.
