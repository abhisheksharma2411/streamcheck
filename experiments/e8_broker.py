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

CONTAINER = "streamcheck-broker"
PROBE = ROOT / "probe" / "TxnTimeoutProbe.java"
#: Below the cap, exactly at it, and one millisecond past. The middle value is
#: what makes the third a boundary rather than just a large number.
PROBE_REQUESTS = (60_000, BROKER_MAX_TXN_TIMEOUT_DEFAULT,
                  BROKER_MAX_TXN_TIMEOUT_DEFAULT + 1)


def broker_setting(name: str) -> tuple[str | None, str]:
    """Read one broker config, returning the value and where it came from."""
    try:
        out = subprocess.run(
            ["docker", "exec", CONTAINER, "/opt/kafka/bin/kafka-configs.sh",
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

    The client jars come out of the broker image, so the probe is built against
    exactly the version under test and the experiment pulls in no new
    dependency. A missing JDK is reported rather than silently skipped: the
    paper says the refusal was measured, and a run that could not measure it
    has to say so.
    """
    javac = shutil.which("javac")
    java = shutil.which("java")
    if not (javac and java and PROBE.exists()):
        return {"completed": False,
                "reason": "no JDK on PATH, or probe/TxnTimeoutProbe.java missing"}

    work = Path(tempfile.mkdtemp(prefix="streamcheck-probe-"))
    try:
        libs = work / "libs"
        subprocess.run(["docker", "cp", f"{CONTAINER}:/opt/kafka/libs", str(libs)],
                       capture_output=True, check=True, timeout=180)
        classes = work / "classes"
        subprocess.run([javac, "-cp", f"{libs}/*", "-d", str(classes), str(PROBE)],
                       capture_output=True, text=True, check=True, timeout=180)

        attempts = []
        for ms in PROBE_REQUESTS:
            out = subprocess.run(
                [java, "-cp", f"{classes}:{libs}/*", "TxnTimeoutProbe",
                 "localhost:19092", str(ms), f"streamcheck-probe-{ms}"],
                capture_output=True, text=True, timeout=120)
            line = next((l for l in out.stdout.splitlines() if l.startswith("{")), "")
            if not line:
                return {"completed": False,
                        "reason": f"probe produced no result for {ms}ms",
                        "stderr": out.stderr[-400:]}
            row = json.loads(line)
            row["requested_ms"] = int(row["requested_ms"])
            row["refused"] = row["refused"] == "true"
            attempts.append(row)
        return {"completed": True, "attempts": attempts}
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        return {"completed": False, "reason": f"{type(exc).__name__}: {exc}"}
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main() -> None:
    if not broker_available():
        write("e8_broker.json", {"status": "skipped",
                                 "reason": "no broker reachable"})
        return

    value, synonyms = broker_setting("transaction.max.timeout.ms")
    measured = int(value) if value else None
    others = {
        name: broker_setting(name)[0]
        for name in ("transaction.state.log.min.isr",
                     "transaction.remove.expired.transaction.cleanup.interval.ms")
    }
    probe = run_probe()
    attempts = probe.get("attempts", [])
    accepted = [a["requested_ms"] for a in attempts if not a["refused"]]
    refused = [a["requested_ms"] for a in attempts if a["refused"]]
    largest_accepted = max(accepted) if accepted else None
    smallest_refused = min(refused) if refused else None
    refused_above_cap = (
        largest_accepted == measured and smallest_refused == measured + 1)

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
