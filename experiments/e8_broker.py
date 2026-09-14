"""E8 -- the broker's cap, measured rather than quoted.

SC106 says a pipeline can need a transaction timeout the broker will not grant.
That rests on ``transaction.max.timeout.ms``, and a paper asserting a default
from documentation is asserting something it did not check. So this queries a
running broker for the value, and then asks the producer to exceed it and
records what happens.

A direct rejection probe -- asking for more than the cap and watching the
producer be refused -- would have been better still, and is not here. The
Python client available does not implement transactions, and the console
producer sets a transactional id without ever opening a transaction, so it
times out on the batch rather than being rejected on the timeout. What the
result records is therefore the cap as the broker reports it, with its synonym
chain showing it is the default, and the rejection behaviour is left as
documented rather than measured. The limitation is stated in the paper.
"""

from __future__ import annotations

import json
import re
import subprocess

from _common import write

from streamcheck.ir import BROKER_MAX_TXN_TIMEOUT_DEFAULT
from streamcheck.live import broker_available

CONTAINER = "streamcheck-broker"


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
    write("e8_broker.json", {
        "status": "ok",
        "broker": "apache/kafka 3.9.0 (KRaft)",
        "transaction_max_timeout_ms": measured,
        "synonyms": synonyms,
        "is_default": "DEFAULT_CONFIG" in synonyms,
        "constant_in_ir": BROKER_MAX_TXN_TIMEOUT_DEFAULT,
        "matches_constant": measured == BROKER_MAX_TXN_TIMEOUT_DEFAULT,
        "related_settings": others,
        "rejection_probe": {
            "attempted": True,
            "completed": False,
            "reason": "the available Python client does not implement transactions, and "
                      "the console producer never opens one, so it expires the batch "
                      "rather than being refused on the timeout",
        },
    })


if __name__ == "__main__":
    main()
