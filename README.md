# StreamCheck

**Whether a restart charges your customer twice is arithmetic between numbers
in four different files.** StreamCheck gathers those numbers, states the
relations, and turns each finding into a scenario you can run.

A payment pipeline dies at lunchtime and someone restarts it at two. It
reprocesses everything since its last checkpoint. Some customers are charged
again — not because the idempotency key was wrong, but because the provider
stopped remembering it some hours before the replay arrived.

## The replay horizon

```
H = checkpoint_interval + min_pause + max_downtime + restart_delay
    + drain(max_downtime + restart_delay) + max_source_lateness
```

Two terms come from the engine config, one from a runbook, one from the job
SQL. `H` is how stale a record can be when it is processed the second time, and
the duration defects are all relations against it:

| | |
|---|---|
| **SC101** | the provider forgets the key before the replay arrives (`key_ttl < H`) |
| **SC102** | dedup state expires before the duplicate it exists to catch |
| **SC103** | the broker aborts the transaction before the job returns |
| **SC105** | the client's own retry window outlives the key |
| **SC106** | the transaction timeout required exceeds the broker's cap — **no fix at the job level** |

Plus seven structural checks (non-deterministic key, effect outside the commit
protocol, uncommitted read of transactional output, and so on).

Every check is a sufficient condition for a *violation*. A pipeline with no
findings has **not** been proved exactly-once, and the tool never says it has.

## What we found

- **210 of 515 findings cannot be derived from fewer than three artifacts.**
  The most informative single file yields 30% of findings and only **11.5%** of
  the duration ones. These defects survive review because review is done one
  file at a time.
- **On a real Kafka broker, witnesses reproduce the double charge** — and a
  matched control where the provider never forgets the key does not. Same
  records, same SIGKILL, same replay; only the lifetime differs.
- **Execution corrected two of our own rules.** Both predicted a double charge;
  both actually produce a repeated call the provider absorbs. Demoted to
  medium, and the taxonomy now separates defects sufficient alone from defects
  needing a second failure.
- **Fault injection finds these only if it guessed high enough.** A tester
  willing to simulate a 5-minute outage finds 15% of them; 12 hours is needed
  for all. More trials do not help — the trigger is outside the distribution.
- **Above a ~14-minute recovery assumption there is no fix.** Kafka caps a
  producer's requested transaction timeout at 15 minutes by default (measured
  on a running broker, not quoted). Surviving longer needs more than that.

## Try it

```bash
make setup up
./.venv/bin/python -c "
import sys; sys.path.insert(0,'src')
from streamcheck.frontend import load_pipeline
from streamcheck.checks import analyse
p, missing = load_pipeline('data/example-pipeline')
for f in analyse(p):
    print(f'[{f.severity}] {f.rule}: {f.message}')
    print(f'   evidence: {f.evidence}')
"
```

`data/example-pipeline` holds five ordinary files: 60-second checkpointing,
Kafka's default 15-minute transaction timeout, a runbook recording a 45-minute
worst recovery, a 24-hour provider key. Every one is defensible. The
combination is not.

## Reproducing

```bash
make reproduce     # broker up, E1-E9, figures, paper, tests, claim check
make fast          # everything that does not need a broker
make verify        # check every number in the paper against results/
```

## Layout

```
src/streamcheck/
  ir.py         the pipeline model, with provenance per fact
  checks.py     13 checks, split structural / duration
  witness.py    findings -> runnable scenarios (scale-invariant)
  executor.py   a small interpreter of replay semantics
  live.py       the same witness against a real broker
  worker.py     the pipeline, as a process that can be killed
  provider.py   a payment API that forgets keys on a timer
  frontend.py   read facts out of flink-conf / properties / SQL / runbook
  corpus.py     5 shapes x 21 mutations, with boundary cases and decoys
experiments/    E1-E9, each writing one JSON to results/
```

## Honest limitations

We wrote both the mutations and the checks, so the headline recall number
measures implementation correctness, not detection power — the boundary cases
and the live execution are what carry evidence. The execution arm uses a real
broker and a real SIGKILL but a purpose-built consumer rather than Flink.
Single partition throughout. The broker cap is measured; the *refusal* is
documented behaviour we did not reproduce. See `ASSUMPTIONS.md` and the paper's
Limitations section.

## Licence

MIT. Author: Abhishek Sharma (ORCID 0009-0007-1103-2103).
