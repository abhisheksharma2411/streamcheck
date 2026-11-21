# Claim-to-evidence matrix, limitations, readiness

Every claim, the experiment behind it, and how far it generalises. Numbers are
checked mechanically by `scripts/verify_claims.py`; this file is the argument,
that script is the audit.

## Claims

| # | Claim | Evidence | Strength |
|---|---|---|---|
| C1 | Exactly-once for a payment pipeline is a conjunction whose terms span artifacts | Construction; the replay horizon has six terms from four files | Definitional |
| C2 | These defects are not derivable from a single-artifact view | E4: 210/515 findings need three artifacts; best single artifact yields 11.5% of duration findings | **Strong**; the paper's central measurement |
| C3 | Findings can be turned into runnable scenarios | E2: 335/515 findings witnessed (65%), all 335 reproduce their predicted observable | Strong |
| C4 | Not every defect is a double charge | E2: 245/335 move money twice; SC006 and SC103 produce an absorbed repeat | **Strong; corrected our own rules** |
| C5 | Witnesses reproduce on real infrastructure, attributably | E3: real broker, SIGKILL, three arms incl. a sufficient-lifetime control | See E3 caveat below |
| C6 | Compression does not change a witness's verdict | E6: 114/114 identical across four orders of magnitude | Strong |
| C7 | Fault injection depends on guessing the outage length | E5: 15% found at a 5-min ceiling, 100% at 12 h; more trials do not help | Strong |
| C8 | Above ~14 min recovery assumption there is no application-level fix | E7 threshold + E8: cap measured at 900,000 ms (`DEFAULT_CONFIG`) and 900,001 ms refused by the same broker | Strong for a default broker |
| C9 | The checker is cheap enough to run in CI | E1: 3.8 µs per pipeline | Strong |

## What the evidence does not support

- **Detection power.** We wrote the mutations and the checks. The 100% recall
  figure measures implementation correctness. The boundary cases (one second
  either side of a threshold) and the decoys are what carry evidence there.
- **Prevalence.** E7's 86.7% is a property of our menus of plausible values,
  not of deployed systems. The transferable part is the threshold, not the
  share.
- **Flink.** The live arm uses a real broker and a real SIGKILL, but a
  purpose-built consumer. Operator checkpointing, rescaling and partition
  reassignment are not exercised.
- ~~**The broker's refusal.**~~ *Closed.* Both halves are now measured. The
  Python client does not implement transactions, so the probe is a small Java
  program (`probe/TxnTimeoutProbe.java`) compiled and run inside a pinned JDK
  image against client jars taken from the broker image. E8 records three
  attempts: 60,000 ms accepted, 900,000 ms accepted, 900,001 ms refused with
  `The transaction timeout is larger than the maximum value allowed by the
  broker (as configured by transaction.max.timeout.ms)`. E8 now exits non-zero
  if the probe cannot compile or run, rather than writing `"status": "ok"`.
- **The provider.** The live arm's provider is a local HTTP stub with a
  key expiry, not a commercial provider. The live results are evidence about
  the defect families under a provider that behaves as documented.
- **Completeness.** Every check is sufficient for a violation, never necessary.
  A clean report is not a proof.

## Adversarial review, and what it changed

1. **Two rules were wrong about their own severity.** SC006 and SC103 predicted
   a double charge; their witnesses produce a repeated call the provider
   absorbs. Demoted to medium, observable split into invocations and charges, a
   test pins it.
2. **The live arm was measuring the harness.** The first version scaled key
   lifetimes to 1.2 s while the kill-and-restart cycle costs about 6 s of real
   time, so every key expired during the restart — and SC103 witnesses whose
   configured downtime was 19 ms "reproduced". Caught by noticing that
   `down_ms` in the results was orders of magnitude below the lifetime it was
   supposed to outlast. Fixed by measuring the overhead, scaling lifetimes to
   30 s, and adding a third arm with a finite-but-sufficient lifetime that must
   charge once.
3. **The corpus's clean baseline was defective** under a rule added later, and
   the numbers involved (a 30-minute recovery assumption) are unremarkable.
   Baseline corrected; the episode is a result rather than a silent fix.
4. **100% recall meant the corpus was too easy.** Boundary cases and decoys
   added; they are what the evaluation now rests on.
5. **The "static beats testing" claim failed in its strong form.** Narrowed to
   the dependence on the tester's assumed worst case and measured as a sweep.
6. **A witness that did not reproduce was a harness bug twice** — an offset
   commit landing before the kill, then a consumer-group eviction outlasting
   the replacement's patience. Both looked exactly like the defect being
   absent, which is the failure mode to watch for.

## Reproducibility

`make reproduce` runs clean-room: preflight, broker up, **E1–E9**, figures and
generated table rows regenerated from `results/`, paper rebuilt, tests, claim
check. Budget **about 25 minutes**, most of it E3. Preflight runs before
`distclean`, so a missing tool does not cost you the committed results, and it
*executes* each tool rather than trusting that it is on PATH --- macOS ships a
`/usr/bin/javac` stub that exists and fails. No host JDK is needed at all now:
the refusal probe builds and runs in a pinned image.

Every results file records the git revision and seeds. No number in the paper
is typed by hand, and the live-execution table's rows are generated from
`results/e3_live.json` rather than copied.

Tests: 49, covering each rule's specificity, the boundary arithmetic, witness
reproduction and its corrected severity split, scale invariance, and the
front-end's refusal to default a fact it cannot find.

## Readiness

**Submit-ready.** All nine experiments run clean-room; 85 numeric claims and 70
prose numerals are checked mechanically against `results/`; 49 tests pass; the
arXiv archive builds standalone under `pdflatex` alone.

Release `v1.0.1` is public and archived at Zenodo concept DOI
`10.5281/zenodo.24412123`, which resolves to the latest version. The source now
prepares `v1.0.2`, which is **not yet tagged or published** --- see
`BLOCKERS.md` for the exact commands. The only step after that is the arXiv
upload, which needs the author's login.
