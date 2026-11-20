# Blockers

## Zenodo DOI not minted

No Zenodo API token on this machine, and the GitHub–Zenodo webhook fires only
for releases published after the repository is enabled in Zenodo. The paper
carries `ZENODO\_DOI\_PLACEHOLDER`.

**To finish:** enable `streamcheck` in Zenodo's GitHub settings *before* cutting
the release, publish the release, then
`python scripts/insert_doi.py 10.5281/zenodo.XXXXXXX` and rebuild.

## arXiv submission

No submission API. Package built and verified; primary cs.SE, cross-list cs.DB
and cs.DC. Needs the author's login.

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
