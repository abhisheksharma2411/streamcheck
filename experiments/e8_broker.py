"""E8 -- the broker's cap, measured rather than quoted.

SC106 says a pipeline can need a transaction timeout the broker will not grant.
That rests on ``transaction.max.timeout.ms``, and a paper asserting a default
from documentation is asserting something it did not check. So this queries a
running broker for the value, and then asks the producer to exceed it and
records what happens.

It then asks the broker to exceed the cap and records the reply. The Python
client in this project does not implement transactions and the console
producer never opens one, so the probe is a small Java program
(``probe/TxnTimeoutProbe.java``) compiled against the client jars the broker
image already ships: it calls ``initTransactions()`` with
``transaction.timeout.ms`` set above the cap and prints whatever comes back.
Three requests are made, one below the cap, one exactly at it, and one a
millisecond above, because a refusal is only informative next to an acceptance.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from _common import ROOT, write

from streamcheck.ir import BROKER_MAX_TXN_TIMEOUT_DEFAULT
from streamcheck.live import broker_available

#: The JDK the probe is compiled and run in. Pinned to the exact build the
#: broker image itself runs (Temurin 21.0.5+11), so the probe is not merely
#: "some Java" but the same runtime talking to the same client jars.
#:
#: This used to use whatever `javac` was on the author's PATH. Two things were
#: wrong with that. macOS ships a /usr/bin/javac stub that exists and fails, so
#: shutil.which() said yes and the compile blew up; and a reader with a
#: different JDK was running a different experiment from the one the paper
#: reports. Neither is visible in the results file.
JDK_IMAGE = "eclipse-temurin:21.0.5_11-jdk"
PROBE = ROOT / "probe" / "TxnTimeoutProbe.java"


class ProbeError(RuntimeError):
    """The refusal could not be measured.

    Raised rather than recorded. The paper states that the refusal one
    millisecond past the cap was observed on a real broker; a run that could
    not observe it must not leave behind a results file that says ``ok``.
    """


def container() -> str:
    """The broker container id, resolved through compose rather than by name.

    The compose file used to pin ``container_name: streamcheck-broker``, which
    is a global name on the Docker daemon: a second checkout, or anyone else's
    container of that name, collides and the error points at the wrong thing.
    Compose already labels its own containers, so ask it.
    """
    out = subprocess.run(["docker", "compose", "ps", "-q", "broker"],
                         cwd=ROOT, capture_output=True, text=True)
    cid = out.stdout.strip().splitlines()
    if not cid:
        raise ProbeError(
            "no broker container: `docker compose ps -q broker` returned "
            f"nothing (stderr: {out.stderr.strip()[:200]}). Run `make up`.")
    return cid[0]


def network() -> str:
    """The compose network the broker is on, so the probe can reach it."""
    out = subprocess.run(
        ["docker", "inspect", container(), "--format",
         "{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}"],
        capture_output=True, text=True, check=True)
    return out.stdout.strip()
#: Below the cap, exactly at it, and one millisecond past. The middle value is
#: what makes the third a boundary rather than just a large number.
PROBE_REQUESTS = (60_000, BROKER_MAX_TXN_TIMEOUT_DEFAULT,
                  BROKER_MAX_TXN_TIMEOUT_DEFAULT + 1)


def broker_setting(name: str) -> tuple[str | None, str]:
    """Read one broker config, returning the value and where it came from."""
    try:
        out = subprocess.run(
            ["docker", "exec", container(), "/opt/kafka/bin/kafka-configs.sh",
             "--bootstrap-server", "localhost:9092", "--entity-type", "brokers",
             "--entity-name", "1", "--describe", "--all"],
            capture_output=True, text=True, timeout=60,
        ).stdout
    except (subprocess.SubprocessError, FileNotFoundError) as exc:
        return None, f"unavailable: {exc}"
    m = re.search(rf"{re.escape(name)}=(\d+).*?synonyms=\{{([^}}]*)\}}", out)
    if not m:
        return None, "not reported by the broker"
    return m.group(1), m.group(2)


def run_probe() -> dict:
    """Compile the Java probe and ask the broker for each timeout in turn.

    The client jars come out of the broker image and the compiler out of a
    pinned JDK image, so the probe is built against exactly the version under
    test and there is no host JDK to get wrong. Every failure raises
    ProbeError carrying the compiler or runtime output: the paper says the
    refusal was measured, and a run that could not measure it must fail
    loudly rather than leave a results file behind that says otherwise.
    """
    work = Path(tempfile.mkdtemp(prefix="streamcheck-probe-"))
    try:
        if not PROBE.exists():
            raise ProbeError(f"{PROBE} is missing")
        shutil.copy(PROBE, work / PROBE.name)

        libs = work / "libs"
        cp = subprocess.run(["docker", "cp", f"{container()}:/opt/kafka/libs",
                             str(libs)], capture_output=True, text=True,
                            timeout=300)
        if cp.returncode != 0:
            raise ProbeError("could not copy the client jars out of the broker "
                             f"image: {cp.stderr.strip()[:400]}")

        in_jdk = ["docker", "run", "--rm", "-v", f"{work}:/w", "-w", "/w"]
        compile_ = subprocess.run(
            in_jdk + [JDK_IMAGE, "javac", "-cp", "libs/*", "-d", "classes",
                      PROBE.name],
            capture_output=True, text=True, timeout=600)
        if compile_.returncode != 0:
            raise ProbeError(
                f"the probe did not compile under {JDK_IMAGE}.\n"
                f"javac exit {compile_.returncode}\n"
                f"{(compile_.stderr or compile_.stdout).strip()[:1200]}")

        net = network()
        attempts = []
        for ms in PROBE_REQUESTS:
            out = subprocess.run(
                in_jdk[:2] + ["--network", net] + in_jdk[2:] +
                [JDK_IMAGE, "java", "-cp", "classes:libs/*", "TxnTimeoutProbe",
                 "broker:9092", str(ms), f"streamcheck-probe-{ms}"],
                capture_output=True, text=True, timeout=300)
            line = next((l for l in out.stdout.splitlines()
                         if l.startswith("{")), "")
            if not line:
                raise ProbeError(
                    f"the probe produced no result for {ms} ms.\n"
                    f"java exit {out.returncode}\n"
                    f"stdout: {out.stdout.strip()[:400]}\n"
                    f"stderr: {out.stderr.strip()[:800]}")
            row = json.loads(line)
            row["requested_ms"] = int(row["requested_ms"])
            row["refused"] = row["refused"] == "true"
            attempts.append(row)
        return {"completed": True, "jdk": JDK_IMAGE, "attempts": attempts}
    except subprocess.TimeoutExpired as exc:
        raise ProbeError(f"timed out: {exc}") from exc
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main() -> None:
    if not broker_available():
        raise ProbeError(
            "no broker reachable on localhost:19092. E8 used to write "
            '"status": "skipped" here and exit 0, which let a clean-room run '
            "finish with the broker results missing and nothing saying so. "
            "Run `make up` first.")

    value, synonyms = broker_setting("transaction.max.timeout.ms")
    measured = int(value) if value else None
    others = {
        name: broker_setting(name)[0]
        for name in ("transaction.state.log.min.isr",
                     "transaction.remove.expired.transaction.cleanup.interval.ms")
    }
    probe = run_probe()  # raises ProbeError rather than returning a failure
    attempts = probe["attempts"]
    accepted = [a["requested_ms"] for a in attempts if not a["refused"]]
    refused = [a["requested_ms"] for a in attempts if a["refused"]]
    largest_accepted = max(accepted) if accepted else None
    smallest_refused = min(refused) if refused else None
    refused_above_cap = (
        largest_accepted == measured and smallest_refused == measured + 1)

    if not refused_above_cap:
        raise ProbeError(
            "the probe ran but did not bracket the cap: largest accepted "
            f"{largest_accepted} ms, smallest refused {smallest_refused} ms, "
            f"broker cap {measured} ms. The paper reports the refusal one "
            "millisecond past the cap, so this is a failed measurement, not a "
            "result to record.")

    write("e8_broker.json", {
        "status": "ok",
        "broker": "apache/kafka 3.9.0 (KRaft)",
        "transaction_max_timeout_ms": measured,
        "synonyms": synonyms,
        "is_default": "DEFAULT_CONFIG" in synonyms,
        "constant_in_ir": BROKER_MAX_TXN_TIMEOUT_DEFAULT,
        "matches_constant": measured == BROKER_MAX_TXN_TIMEOUT_DEFAULT,
        "related_settings": others,
        "rejection_probe": probe,
        "refusal_is_measured": bool(probe.get("completed")) and refused_above_cap,
        "largest_accepted_ms": largest_accepted,
        "smallest_refused_ms": smallest_refused,
    })


if __name__ == "__main__":
    main()
