The revision the arXiv submission was built from. `v1.0.1` remains archived and
untouched; this corrects claims and hardens the clean-room run on top of it.

## Corrected claims

The documentation said the synthesiser "turns each finding into a runnable
scenario whose observable is money moving twice". Neither half is true, and the
repository's own results said so all along:

- 515 findings in the corpus;
- **335** admit an executable witness, and all 335 reproduce their predicted
  observable;
- **245** of those 335 move money twice. The rest reproduce a repeated call the
  provider's idempotency absorbs — a different outcome, now reported as one;
- the live arm covers **16** selected witnesses, not the corpus.

Corrected in the paper, the abstract, `README.md`, `NOVELTY.md`, `REVIEW.md`
and `CITATION.cff`.

## The broker-refusal probe no longer needs a host JDK

It compiles and runs inside a pinned `eclipse-temurin:21.0.5_11-jdk` image, the
same build the broker image itself runs, against client jars copied out of the
broker. The old path asked `shutil.which("javac")`, which accepts the macOS
stub at `/usr/bin/javac` that exists and fails the moment you run it.

E8 now raises on any failure, carrying the compiler or runtime output. It used
to write `"status": "ok"` with `refusal_is_measured: false` buried inside, so a
reader whose probe never ran got a results file that looked fine.

The measurement is unchanged: 60,000 ms accepted, 900,000 ms accepted,
900,001 ms refused.

## Clean-room reproduction

- `make preflight` runs **before** `distclean`. The old order deleted every
  result file first and discovered the missing tool twenty minutes later.
- Preflight *executes* docker, compose, latexmk, pdflatex and the JDK image
  rather than checking PATH, because existence is not evidence.
- `make reproduce` takes about **25 minutes**, now documented.
- `docker-compose.yml` no longer pins `container_name`. That is a global name
  on the daemon, so a second checkout collided; the experiments resolve the
  container through `docker compose ps -q broker` instead.

## Paper

- The replay-horizon equation is broken across two lines, unchanged in content,
  clearing a 2.61pt overfull box.
- `\balance` moved into the final page's first column. After
  `\end{thebibliography}` it was in the second, and the package was warning
  that the columns might not be balanced.
- The artifact statement cites the Zenodo **concept** DOI
  `10.5281/zenodo.24412123` and labels it as such.

## Reproducing

```bash
git clone https://github.com/abhisheksharma2411/streamcheck
cd streamcheck && git checkout v1.0.2
make reproduce
```

Needs Docker with Compose v2, Python 3.11+ and a TeX distribution. No JDK. The
live arm's provider is a local HTTP stub, not a commercial payment provider; no
money moves.
