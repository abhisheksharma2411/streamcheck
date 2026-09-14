"""The pipeline, as a process that can actually be killed.

Consumes a Kafka topic, optionally deduplicates, calls the provider, and
commits its offsets on an interval. The offset commit is the checkpoint: when
this process dies, everything it processed since its last commit is replayed by
the next instance, because that is what Kafka's consumer group does. Nothing
here simulates recovery -- the broker decides what gets redelivered.

Keyed state is written beside the offset commit and restored on start, which is
how a stream processor checkpoints state together with its input position. It
is deliberately *not* written on every record: state that survived a failure it
should not have would hide the very defect under test.

Run as ``python -m streamcheck.worker <config.json>``.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from pathlib import Path


def _post(url: str, payload: dict, timeout: float = 5.0) -> dict:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def main(cfg_path: str) -> int:
    from kafka import KafkaConsumer

    cfg = json.loads(Path(cfg_path).read_text())
    state_path = Path(cfg["state_path"])
    checkpoint_ms = cfg["checkpoint_interval_ms"]
    state_ttl_ms = cfg.get("state_ttl_ms")
    dedup_enabled = cfg.get("dedup", False)
    dedup_guards_effect = cfg.get("dedup_guards_effect", True)
    key_mode = cfg.get("key_mode", "stable")
    provider_url = cfg["provider_url"]
    log = Path(cfg["log_path"]).open("a", buffering=1)

    # Restore the last checkpointed state, exactly as a job would on restart.
    seen: dict[str, float] = {}
    if state_path.exists():
        try:
            seen = json.loads(state_path.read_text())
        except json.JSONDecodeError:
            seen = {}
    log.write(f"start pid={os.getpid()} restored={len(seen)} keys\n")

    # A killed member has to be evicted before the next instance can take the
    # partition, and the default session timeout is long enough that the
    # replacement gives up first -- which made a replay look like it had not
    # happened. Short session and heartbeat intervals evict promptly; the
    # replacement then waits long enough to be assigned.
    consumer = KafkaConsumer(
        cfg["topic"],
        bootstrap_servers=cfg["bootstrap"],
        group_id=cfg["group_id"],
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode()),
        consumer_timeout_ms=cfg.get("idle_timeout_ms", 15000),
        session_timeout_ms=cfg.get("session_timeout_ms", 6000),
        heartbeat_interval_ms=cfg.get("heartbeat_interval_ms", 2000),
        max_poll_interval_ms=cfg.get("max_poll_interval_ms", 300000),
    )

    last_commit = time.monotonic() * 1000.0
    attempt = 0

    def checkpoint() -> None:
        """Commit the input position and the state together."""
        state_path.write_text(json.dumps(seen))
        consumer.commit()
        log.write(f"checkpoint offsets+state ({len(seen)} keys)\n")

    try:
        for msg in consumer:
            rec = msg.value
            order_id = rec["order_id"]
            now = time.monotonic() * 1000.0

            if dedup_enabled and dedup_guards_effect:
                first = seen.get(order_id)
                if first is not None and (state_ttl_ms is None or now - first <= state_ttl_ms):
                    log.write(f"drop {order_id} (dedup)\n")
                    continue
                seen[order_id] = now

            attempt += 1
            if key_mode == "stable":
                key = f"{order_id}:stable"
            elif key_mode == "none":
                key = ""
            elif key_mode == "nondeterministic":
                key = f"{order_id}:{os.getpid()}:{attempt}:{now:.0f}"
            elif key_mode == "offset":
                key = f"{order_id}:off{msg.offset}"
            else:
                raise ValueError(f"unknown key mode: {key_mode}")

            resp = _post(provider_url, {
                "idempotency_key": key,
                "order_id": order_id,
                "amount": rec.get("amount", 0),
            })
            log.write(f"effect {order_id} key={key or '<none>'} charged={resp['charged']}\n")

            if dedup_enabled and not dedup_guards_effect:
                seen[order_id] = now

            if (time.monotonic() * 1000.0) - last_commit >= checkpoint_ms:
                checkpoint()
                last_commit = time.monotonic() * 1000.0
    finally:
        # A clean shutdown checkpoints; a kill -9 does not, which is the point.
        try:
            checkpoint()
        except Exception:
            pass
        consumer.close()
        log.write("stop\n")
        log.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
