# Blockers

Everything here is outside the repository: it needs an account, a login or an
authorisation this machine does not have. Nothing in the code or the results
is waiting on anything.

## The release the paper cites does not exist

`paper/streamcheck.tex` and `CITATION.cff` both name `v1.0.1`. The repository
is private and has no releases at all; the only tag is `v1.0.0`, four commits
behind. A reader following the artifact statement today gets a 404.

`make arxiv` now says so --- it prints a `RELEASE` warning when the tag it is
about to put in the metadata is missing or unpushed --- but it cannot cut the
tag, and should not.

**To finish:** make the repository public, push `main`, tag `v1.0.1` at the
commit the paper was built from, and publish the GitHub release.

## Zenodo DOI not minted

No Zenodo API token on this machine, and the GitHub--Zenodo webhook fires only
for releases published *after* the repository is enabled in Zenodo. Order
matters and it is not recoverable afterwards: enabling Zenodo after the release
archives nothing.

The paper does not carry a placeholder --- the artifact statement names the
repository and the release and stops there, which is true without a DOI. When
one exists, `python scripts/insert_doi.py 10.5281/zenodo.XXXXXXX` appends it to
the artifact statement, updates `CITATION.cff`, and is a no-op on a second run.

**To finish, in this order:** make the repository public, enable `streamcheck`
in Zenodo's GitHub settings, publish release `v1.0.1`, confirm the webhook
delivered, then run `insert_doi.py` and rebuild.

## arXiv submission

No submission API; it needs the author's login. The package is built and
verified: `paper/streamcheck-arxiv.tar.gz`, five files, compiles to 8 pages
under `pdflatex` alone in a directory holding nothing else. Metadata is in
`paper/arxiv_metadata.txt`; primary cs.SE, cross-list cs.DC.

## Broker rejection probe --- CLOSED

This entry said the refusal had not been demonstrated. It has been. The Java
probe in `probe/TxnTimeoutProbe.java` was written and run, and
`results/e8_broker.json` records `rejection_probe.completed: true` with three
attempts against apache/kafka 3.9.0: 60,000 ms accepted, 900,000 ms accepted,
and 900,001 ms refused with `The transaction timeout is larger than the maximum
value allowed by the broker (as configured by transaction.max.timeout.ms)`.
Both halves of the cap --- its configured value and the refusal one millisecond
past it --- are measured on one broker. The paper says so and this file was
stale.

## Flink not used in the execution arm

Expressing an external HTTP effect in Flink SQL needs a custom connector.
Writing one would have put more of our own code between the witness and the
result, so the live arm uses a real broker with a purpose-built consumer. The
replay semantics exercised are Kafka's; operator checkpointing and rescaling
are not. Stated as a limitation rather than implied away.

## The provider in the live arm is a stub

Not a blocker, recorded so it is not mistaken for one. The live arm's provider
is a local HTTP stub that implements an idempotency key with an expiry. That is
deliberate --- it is the one provider behaviour the rules depend on, and it lets
the key be expired on demand --- but it means the live results are evidence
about the defect families under a provider that behaves as documented, not
about any commercial provider's implementation. The paper says this in
Limitations.
