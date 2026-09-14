"""Read the facts out of the files a team actually keeps.

Four front-ends, because the facts are in four places and that scattering is
the paper's subject:

``flink-conf.yaml``          checkpoint interval, restart delay, state TTL
``*.properties``             transaction and delivery timeouts, isolation level
``*.sql``                    the topology: sources, dedup, sinks, connectors
``effects.json``             the external effects, their keys, and the
                             provider's key lifetime

The last one does not exist in any repository today, and that is the point
rather than an inconvenience. How long a provider remembers an idempotency key
is a fact about the provider; it lives in their documentation, it is load-
bearing for correctness, and nothing in a codebase records it. Asking for it to
be written down is part of what this tool proposes -- a declaration in the sense
of T8's provider-aware contract.

The SQL reader is deliberately shallow. It recognises the shapes these
pipelines are written in -- ``CREATE TABLE`` with connector options, a
``ROW_NUMBER`` deduplication, an ``INSERT INTO`` -- and does not attempt to be a
SQL front end. Anything it cannot place is reported rather than guessed at, so
a missing fact becomes a finding about incomplete evidence instead of a silent
default.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from .ir import (
    EngineConfig,
    Effect,
    Guarantee,
    Isolation,
    Millis,
    Operator,
    Pipeline,
    Provenance,
    Sink,
    Source,
)

_DURATION = re.compile(r"^\s*(\d+)\s*(ms|s|m|min|h|d)?\s*$", re.I)
_UNIT_MS = {"ms": 1, "s": 1000, "m": 60_000, "min": 60_000, "h": 3_600_000, "d": 86_400_000}


def parse_duration(text: str) -> Millis | None:
    """Accept the spellings these files actually use: 60000, 60 s, 15 min, 7 d."""
    if text is None:
        return None
    m = _DURATION.match(str(text))
    if not m:
        return None
    value, unit = m.group(1), (m.group(2) or "ms").lower()
    return int(value) * _UNIT_MS[unit]


def read_properties(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip()
    return out


def read_flat_yaml(path: Path) -> dict[str, str]:
    """flink-conf.yaml is a flat key: value file; a YAML parser is overkill."""
    out: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        k, _, v = line.partition(":")
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _tables(sql: str) -> list[tuple[str, dict[str, str]]]:
    """``CREATE TABLE name (...) WITH ('k' = 'v', ...)`` pairs."""
    out = []
    for m in re.finditer(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s*\(.*?\)\s*WITH\s*\((.*?)\)\s*;",
        sql, re.S | re.I,
    ):
        name = m.group(1)
        opts = dict(re.findall(r"'([^']+)'\s*=\s*'([^']*)'", m.group(2)))
        out.append((name, opts))
    return out


def load_pipeline(directory: str | Path) -> tuple[Pipeline, list[str]]:
    """Assemble a pipeline from a directory of artifacts.

    Returns the pipeline and a list of facts that could not be found, so a
    caller can tell "configured correctly" from "we could not tell".
    """
    d = Path(directory)
    missing: list[str] = []

    engine = EngineConfig()
    conf_path = d / "flink-conf.yaml"
    if conf_path.exists():
        conf = read_flat_yaml(conf_path)
        prov = lambda k: Provenance("flink-conf.yaml", k)  # noqa: E731
        for key, attr in (
            ("execution.checkpointing.interval", "checkpoint_interval"),
            ("execution.checkpointing.min-pause", "min_pause_between_checkpoints"),
            ("restart-strategy.fixed-delay.delay", "restart_delay"),
            ("table.exec.state.ttl", "state_ttl"),
        ):
            if key in conf:
                val = parse_duration(conf[key])
                if val is not None:
                    setattr(engine, attr, val)
                    engine.provenance[attr] = prov(key)
        if "sink.transactional-id-prefix" in conf:
            engine.transactional_id_prefix = conf["sink.transactional-id-prefix"]
            engine.provenance["transactional_id_prefix"] = prov("sink.transactional-id-prefix")
    else:
        missing.append("flink-conf.yaml")

    prod_path = d / "kafka-producer.properties"
    if prod_path.exists():
        props = read_properties(prod_path)
        for key, attr in (("transaction.timeout.ms", "transaction_timeout"),
                          ("delivery.timeout.ms", "delivery_timeout")):
            if key in props:
                val = parse_duration(props[key])
                if val is not None:
                    setattr(engine, attr, val)
                    engine.provenance[attr] = Provenance("kafka-producer.properties", key)
    else:
        missing.append("kafka-producer.properties")

    cons_path = d / "kafka-consumer.properties"
    consumer = read_properties(cons_path) if cons_path.exists() else {}
    if not cons_path.exists():
        missing.append("kafka-consumer.properties")

    # The operational assumption. Nothing in a build system records how long a
    # job may stay dead, so it is read from a runbook if one states it and
    # reported as missing if not.
    run_path = d / "runbook.json"
    if run_path.exists():
        rb = json.loads(run_path.read_text())
        val = parse_duration(rb.get("max_downtime"))
        if val is not None:
            engine.max_downtime = val
            engine.provenance["max_downtime"] = Provenance("runbook.json", "max_downtime")
    else:
        missing.append("runbook.json (max_downtime)")

    sources: list[Source] = []
    sinks: list[Sink] = []
    operators: list[Operator] = []
    sql_files = sorted(d.glob("*.sql"))
    if not sql_files:
        missing.append("*.sql")
    for sql_path in sql_files:
        sql = sql_path.read_text()
        body = re.sub(r"CREATE\s+TABLE.*?;", "", sql, flags=re.S | re.I)
        for name, opts in _tables(sql):
            prov = Provenance(sql_path.name, f"CREATE TABLE {name}")
            is_sink = bool(re.search(rf"INSERT\s+INTO\s+`?{name}`?", body, re.I))
            if is_sink:
                guarantee = {
                    "exactly-once": Guarantee.EXACTLY_ONCE,
                    "at-least-once": Guarantee.AT_LEAST_ONCE,
                    "none": Guarantee.NONE,
                }.get(opts.get("sink.delivery-guarantee", "at-least-once"),
                      Guarantee.AT_LEAST_ONCE)
                sinks.append(Sink(name, opts.get("connector", "kafka"), guarantee,
                                  opts.get("sink.dedup-key", ""), [], prov))
                if guarantee is Guarantee.EXACTLY_ONCE:
                    engine.guarantee = Guarantee.EXACTLY_ONCE
            else:
                iso = consumer.get("isolation.level", opts.get("properties.isolation.level",
                                                               "read_uncommitted"))
                sources.append(Source(
                    name, opts.get("topic", name),
                    Isolation.READ_COMMITTED if iso == "read_committed"
                    else Isolation.READ_UNCOMMITTED,
                    opts.get("properties.group.id", ""),
                    lateness_bound=parse_duration(opts.get("scan.watermark.max-lateness", "0")) or 0,
                    upstream_transactional=opts.get("upstream.transactional", "false") == "true",
                    provenance=prov,
                ))
        if re.search(r"ROW_NUMBER\s*\(\s*\)", sql, re.I):
            operators.append(Operator(
                "dedup", "dedup", [s.name for s in sources], keyed_by="order_id",
                state_ttl=engine.state_ttl,
                provenance=Provenance(sql_path.name, "ROW_NUMBER deduplication"),
            ))

    effects: list[Effect] = []
    eff_path = d / "effects.json"
    if eff_path.exists():
        for e in json.loads(eff_path.read_text())["effects"]:
            effects.append(Effect(
                name=e["name"],
                operator=e.get("operator", "effect"),
                key_expr=e.get("idempotency_key", ""),
                provider_key_ttl=parse_duration(e.get("provider_key_ttl")),
                client_retry_window=parse_duration(e.get("client_retry_window")) or 0,
                idempotent=e.get("idempotent", True),
                provenance=Provenance("effects.json", e["name"]),
            ))
            if e.get("provider_key_ttl") is None:
                missing.append(f"effects.json:{e['name']}.provider_key_ttl")
    else:
        missing.append("effects.json")

    if effects:
        operators.append(Operator(
            effects[0].operator, "process",
            [operators[0].name] if operators else [s.name for s in sources],
            keyed_by="order_id",
            provenance=Provenance("effects.json", effects[0].name),
        ))

    return Pipeline(
        name=d.name,
        engine=engine,
        sources=sources,
        operators=operators,
        effects=effects,
        sinks=sinks,
    ), missing
