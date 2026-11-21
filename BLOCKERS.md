# Blockers

Everything here needs an account, a login or an authorisation. Nothing in the
code or the results is waiting on anything.

## v1.0.2 is prepared but not published

The paper's artifact statement and the arXiv metadata name release `v1.0.2`.
That tag does not exist yet. `make arxiv` says so on every build:

```
RELEASE     v1.0.2 does not exist yet -- the paper points readers at it
```

`v1.0.1` *is* public and archived; v1.0.2 is the corrected source on top of it.
Until the tag is pushed and the release published, the artifact statement
points at something a reader cannot fetch.

**To finish** — commit, tag, push, release:

```bash
git add -A
git commit -s -m "Correct the universal witness claims and harden the clean-room run"
git tag -a v1.0.2 -m "v1.0.2 -- corrected witness claims, containerised broker probe"
git push origin main
git push origin v1.0.2
gh release create v1.0.2 -R abhisheksharma2411/streamcheck \
  --title "v1.0.2" --verify-tag --notes-file RELEASE_NOTES_v1.0.2.md
```

Zenodo is already enabled on the repository, so publishing the release is all
that is needed — the webhook fires on `release` and deposits automatically.
Confirm it with:

```bash
HOOK=$(gh api repos/abhisheksharma2411/streamcheck/hooks --jq '.[0].id')
gh api "repos/abhisheksharma2411/streamcheck/hooks/$HOOK/deliveries" \
  --jq '.[] | "\(.delivered_at) \(.event)/\(.action) \(.status) \(.status_code)"' | head
```

GitHub fires three deliveries for one release (`created`, `published`,
`released`). Only one is accepted; the others come back 409 and 500. That is
Zenodo refusing duplicate work for the same release, not a failure. The
`published` delivery returning 202 is the one that matters.

## Zenodo DOIs — minted, nothing outstanding

| | |
|---|---|
| Concept DOI | `10.5281/zenodo.24412123` — resolves to the latest version; this is what the paper cites |
| v1.0.1 version DOI | `10.5281/zenodo.24412124` |
| v1.0.2 version DOI | assigned when the release above is published |

The paper deliberately cites the **concept** DOI, so a new release does not
strand the citation and no manuscript edit is needed after v1.0.2 deposits.
`scripts/insert_doi.py` is still there for a repository that has no DOI yet; on
this one it is a no-op and says so.

## arXiv submission

No submission API; it needs the author's login.

- Upload `paper/streamcheck-arxiv.tar.gz` — one file, five members. **Not** the
  PDF; arXiv compiles the source.
- Abstract to paste: `paper/arxiv_abstract.txt`.
- Everything else: `paper/arxiv_metadata.txt`.
- Leave **External DOI blank**. That field is for a journal DOI of *this
  paper*. A software DOI there asserts a published version that does not
  exist; the Zenodo DOI belongs in Comments and in Artifact Availability, where
  it already is.

## Broker rejection probe — CLOSED

Both halves of the cap are measured on one broker: its configured value,
900,000 ms, and its refusal of 900,001 ms with `The transaction timeout is
larger than the maximum value allowed by the broker (as configured by
transaction.max.timeout.ms)`. `results/e8_broker.json` records
`rejection_probe.completed: true` with all three attempts.

The probe no longer depends on a host JDK. It compiles and runs inside a pinned
`eclipse-temurin:21.0.5_11-jdk` image — the same build the broker image runs —
against client jars copied out of the broker. The old path used
`shutil.which("javac")`, which happily accepted the macOS stub at
`/usr/bin/javac` that exists and fails on execution.

E8 now raises rather than records. A run that cannot compile or execute the
probe exits non-zero carrying the compiler or runtime output, instead of
writing `"status": "ok"` with `refusal_is_measured: false` buried inside.

## Flink not used in the execution arm

Expressing an external HTTP effect in Flink SQL needs a custom connector.
Writing one would have put more of our own code between the witness and the
result, so the live arm uses a real broker with a purpose-built consumer. The
replay semantics exercised are Kafka's; operator checkpointing and rescaling
are not. Stated as a limitation rather than implied away.

## The provider in the live arm is a stub

Not a blocker, recorded so it is not mistaken for one. The live arm's provider
is a local HTTP stub that implements an idempotency key with an expiry. That is
deliberate — it is the one provider behaviour the rules depend on, and it lets
the key be expired on demand — but it means the live results are evidence about
the defect families under a provider that behaves as documented, not about any
commercial provider's implementation. The paper says this in Limitations.

## Witness coverage is not universal

Also not a blocker, recorded because the documentation used to overstate it.
Of 515 findings, **335** admit an executable witness and all 335 reproduce
their predicted observable. **245** of those move money twice; the rest
reproduce a repeated call the provider's idempotency absorbs, which is a
different outcome and is reported as one. The live arm covers **16** selected
witnesses, not the corpus. Every document in this repository now states the
counts rather than saying "each finding".
