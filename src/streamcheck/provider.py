"""A payment provider that forgets.

The whole duration argument rests on one behaviour: an idempotency key is
remembered for a finite time, and a retry arriving after that is a new request.
This is that behaviour and nothing else, served over HTTP so the pipeline talks
to it the way it would talk to a real provider -- across a socket, with no
shared memory and no way to cheat.

Run as ``python -m streamcheck.provider <port> <key_ttl_ms>``. A TTL of ``-1``
means the key is remembered forever, which is the configuration under which the
duration defects disappear and is used as a control.
"""

from __future__ import annotations

import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


class _State:
    def __init__(self, key_ttl_ms: int) -> None:
        self.key_ttl_ms = key_ttl_ms
        self.lock = threading.Lock()
        self.seen: dict[str, float] = {}
        self.charges: list[dict] = []
        self.invocations: list[dict] = []


class _Handler(BaseHTTPRequestHandler):
    state: _State = None  # type: ignore[assignment]

    def log_message(self, *args) -> None:      # keep the test output readable
        pass

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        key = body.get("idempotency_key", "")
        order_id = body.get("order_id", "")
        amount = int(body.get("amount", 0))
        now = time.monotonic() * 1000.0

        with self.state.lock:
            self.state.invocations.append({"order_id": order_id, "key": key, "at": now})
            moved = True
            if key:
                first = self.state.seen.get(key)
                if first is not None and (
                    self.state.key_ttl_ms < 0 or (now - first) <= self.state.key_ttl_ms
                ):
                    moved = False
                else:
                    self.state.seen[key] = now
            if moved:
                self.state.charges.append(
                    {"order_id": order_id, "key": key, "amount": amount, "at": now}
                )
            charged = sum(1 for c in self.state.charges if c["order_id"] == order_id)

        payload = json.dumps({"charged": moved, "total_for_order": charged}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        with self.state.lock:
            payload = json.dumps({
                "charges": self.state.charges,
                "invocations": self.state.invocations,
                "key_ttl_ms": self.state.key_ttl_ms,
            }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def serve(port: int, key_ttl_ms: int) -> None:
    handler = type("H", (_Handler,), {"state": _State(key_ttl_ms)})
    HTTPServer(("127.0.0.1", port), handler).serve_forever()


if __name__ == "__main__":
    serve(int(sys.argv[1]), int(sys.argv[2]))
