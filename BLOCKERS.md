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

## Broker rejection probe not completed

SC106 rests on `transaction.max.timeout.ms`, whose value is read from a running
broker (900,000 ms, `DEFAULT_CONFIG`). Demonstrating a producer being *refused*
for asking beyond it needs a client that implements transactions; the available
Python client does not, and the console producer expires its batch first. A
Java probe would close this and was not attempted.

## Flink not used in the execution arm

Expressing an external HTTP effect in Flink SQL needs a custom connector.
Writing one would have put more of our own code between the witness and the
result, so the live arm uses a real broker with a purpose-built consumer. The
replay semantics exercised are Kafka's; operator checkpointing and rescaling
are not. Stated as a limitation rather than implied away.
