"""Run a witness against real infrastructure.

The orchestrator: create a topic on a real broker, start a provider that really
forgets keys on a timer, produce the witness's records, start the pipeline
process, kill it at the instant the witness says, restart it, and then ask the
provider what actually happened.

Nothing about the recovery is modelled here. The worker is killed with SIGKILL,
so it commits nothing on the way out, and the replay is whatever Kafka decides
to redeliver to the next instance from the last committed offset. That is the
mechanism the duration defects are about, and it is the reason this arm exists
alongside the interpreter in :mod:`executor`.

Two things this is not. The stream processor is a purpose-built consumer rather
than Flink: expressing an external HTTP effect in Flink SQL needs a custom
connector, and writing one would have put more of our own code between the
witness and the result, not less. And the pipeline is single-partition, so
nothing here exercises rescaling or partition reassignment.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

from .ir import Pipeline
from .witness import Witness

BOOTSTRAP = os.environ.get("STREAMCHECK_BOOTSTRAP", "localhost:19092")
ROOT = Path(__file__).resolve().parents[2]


def broker_available(timeout: float = 3.0) -> bool:
    host, _, port = BOOTSTRAP.partition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class LiveResult:
    """What the real system did."""

    charges: int
    invocations: int
    #: Milliseconds between the first attempt and the replay, from the
    #: provider's clock. Includes the harness's own restart cost.
    observed_gap_ms: float
    expected_charges: int
    expected_invocations: int
    confirmed: bool
    charges_confirmed: bool
    rule: str
    scale: float
    wall_ms: float
    worker_log: list[str] = field(default_factory=list)
    error: str = ""

    def as_dict(self) -> dict:
        return {
            "charges": self.charges,
            "invocations": self.invocations,
            "observed_gap_ms": self.observed_gap_ms,
            "expected_charges": self.expected_charges,
            "expected_invocations": self.expected_invocations,
            "confirmed": self.confirmed,
            "charges_confirmed": self.charges_confirmed,
            "rule": self.rule,
            "scale": self.scale,
            "wall_ms": self.wall_ms,
            "worker_log": list(self.worker_log)[:60],
            "error": self.error,
        }


def _key_mode(pipeline: Pipeline) -> str:
    if not pipeline.effects:
        return "stable"
    e = pipeline.effects[0]
    if not e.idempotent or not e.key_expr.strip():
        return "none"
    low = e.key_expr.lower()
    if any(t in low for t in ("uuid", "random", "now(", "current_timestamp",
                              "proctime", "nanotime", "currentmillis")):
        return "nondeterministic"
    if "offset" in low or "partition" in low:
        return "offset"
    return "stable"


#: Offset-commit interval used for the live run, in milliseconds.
#:
#: The witness says the failure happens before the first checkpoint. Racing a
#: scaled-down interval against real process startup does not reliably produce
#: that -- the first attempt committed before the kill landed and nothing was
#: replayed, which made a genuine defect look absent. So the commit interval is
#: set well beyond the length of a run and the kill is triggered by observing
#: the effect instead of by a timer. The premise being realised is unchanged;
#: only the way of hitting it is no longer a race.
LIVE_COMMIT_INTERVAL_MS = 600_000


#: Milliseconds the harness itself costs between the effect firing and the
#: replay reaching the provider: killing the worker, waiting for the group to
#: evict it, restarting, and being reassigned the partition. Measured at about
#: six seconds on the machine used here.
#:
#: This number is load-bearing. A first version of the live arm scaled key
#: lifetimes to 1.2 s, which is below it, so every key expired during the
#: restart whatever downtime the witness specified -- and defects whose
#: configured downtime was 19 ms appeared to reproduce. The scaled lifetime
#: has to dominate the overhead or the arm measures the harness.
HARNESS_RESTART_MS = 6000.0


def live_scale(w: Witness, *, target_ttl_ms: float = 30_000.0) -> float:
    """Factor that brings a witness's key lifetime to ``target_ttl_ms``.

    Every duration check is an inequality between durations, so scaling them
    all by one factor changes nothing about the defect; what it buys is a run
    that finishes in seconds rather than hours. The factor is chosen from the
    key lifetime because that is the quantity the wait has to outlast, and the
    target is several times :data:`HARNESS_RESTART_MS` so that a key expiring
    means the witness's downtime expired it.
    """
    if not w.provider_key_ttl:
        return 1.0 if w.horizon <= 0 else min(1.0, target_ttl_ms / max(1.0, w.horizon))
    return target_ttl_ms / float(w.provider_key_ttl)


def _invocations(port: int, order_id: str) -> int:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/state", timeout=2) as resp:
            state = json.loads(resp.read())
        return sum(1 for c in state["invocations"] if c["order_id"] == order_id)
    except Exception:                              # noqa: BLE001
        return 0


def run_live(pipeline: Pipeline, w: Witness, *, python: str | None = None,
             settle_ms: int = 1200, kill_timeout_s: float = 30.0) -> LiveResult:
    """Execute one witness end to end. Assumes the broker is up."""
    python = python or sys.executable
    started = time.monotonic()
    topic = f"sc-{w.rule.lower()}-{os.getpid()}-{int(time.monotonic()*1000) % 100000}"
    group = f"g-{topic}"
    port = _free_port()
    workdir = Path(tempfile.mkdtemp(prefix="streamcheck-live-"))
    provider_url = f"http://127.0.0.1:{port}/charge"

    ttl = -1 if w.provider_key_ttl is None else int(w.provider_key_ttl)
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    provider = subprocess.Popen(
        [python, "-m", "streamcheck.provider", str(port), str(ttl)],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    dedups = pipeline.dedup_operators()
    dedup = dedups[0] if dedups else None
    guarded = False
    if dedup is not None and pipeline.effects:
        eff_op = pipeline.effects[0].operator
        guarded = dedup.name in (pipeline.ancestors(eff_op) | {eff_op})
    state_ttl = None
    if dedup is not None:
        state_ttl = dedup.state_ttl if dedup.state_ttl is not None else pipeline.engine.state_ttl

    cfg = {
        "topic": topic, "group_id": group, "bootstrap": BOOTSTRAP,
        "checkpoint_interval_ms": LIVE_COMMIT_INTERVAL_MS,
        "state_ttl_ms": state_ttl,
        "dedup": dedup is not None,
        "dedup_guards_effect": guarded,
        "key_mode": _key_mode(pipeline),
        "provider_url": provider_url,
        "state_path": str(workdir / "state.json"),
        "log_path": str(workdir / "worker.log"),
        "idle_timeout_ms": 10000,
        "session_timeout_ms": 6000,
        "heartbeat_interval_ms": 2000,
    }
    cfg_path = workdir / "cfg.json"
    cfg_path.write_text(json.dumps(cfg))

    err = ""
    try:
        for _ in range(40):                      # wait for the provider socket
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                    break
            except OSError:
                time.sleep(0.1)

        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode(),
            retries=5,
        )
        for r in w.records:
            producer.send(topic, {"order_id": r.order_id, "amount": r.amount})
        producer.flush()
        producer.close()

        def start_worker() -> subprocess.Popen:
            return subprocess.Popen(
                [python, "-m", "streamcheck.worker", str(cfg_path)],
                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )

        proc = start_worker()
        if w.faults:
            fault = w.faults[0]
            # Wait for the money to move, then kill. Observing the provider is
            # what makes this deterministic: the kill is guaranteed to land
            # after the effect and before any offset commit.
            deadline = time.monotonic() + kill_timeout_s
            while time.monotonic() < deadline and _invocations(port, w.order_id) < 1:
                time.sleep(0.05)
            if _invocations(port, w.order_id) < 1:
                raise RuntimeError("effect never fired before the kill deadline")
            proc.send_signal(signal.SIGKILL)
            proc.wait(timeout=10)
            # Stay down as long as the witness says. The provider goes on
            # ageing its keys while we are gone, which is the whole point.
            time.sleep(max(0.0, fault.down_for / 1000.0))
            proc = start_worker()
        proc.wait(timeout=120)
        time.sleep(settle_ms / 1000.0)

        with urllib.request.urlopen(f"http://127.0.0.1:{port}/state", timeout=5) as resp:
            state = json.loads(resp.read())
        charges = sum(1 for c in state["charges"] if c["order_id"] == w.order_id)
        invs = [c["at"] for c in state["invocations"] if c["order_id"] == w.order_id]
        invocations = len(invs)
        # The gap between the first attempt and the replay, as the provider saw
        # it. This is the quantity the key lifetime has to be compared against,
        # and it is larger than the configured downtime by however long the
        # harness takes to kill and restart the worker. Measuring it is what
        # tells a genuine expiry from a harness artefact.
        observed_gap = (max(invs) - min(invs)) if len(invs) >= 2 else 0.0
        log_lines = (workdir / "worker.log").read_text().splitlines() \
            if (workdir / "worker.log").exists() else []
    except Exception as exc:                       # noqa: BLE001 - reported, not swallowed
        charges = invocations = 0
        observed_gap = 0.0
        log_lines = []
        err = f"{type(exc).__name__}: {exc}"
    finally:
        provider.send_signal(signal.SIGKILL)
        provider.wait(timeout=10)

    return LiveResult(
        charges=charges,
        invocations=invocations,
        observed_gap_ms=observed_gap,
        expected_charges=w.expect_charges,
        expected_invocations=w.expect_invocations,
        confirmed=(not err and invocations >= w.expect_invocations
                   and charges >= w.expect_charges),
        charges_confirmed=(not err and charges >= 2),
        rule=w.rule,
        scale=w.scale,
        wall_ms=(time.monotonic() - started) * 1000.0,
        worker_log=log_lines,
        error=err,
    )
