from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class ReceiverState:
    def __init__(self, latency_ms: int = 0):
        self._lock = threading.Lock()
        self._latency_ms = latency_ms
        self._receipts: list[dict[str, Any]] = []

    def configure(self, latency_ms: int) -> None:
        if not 0 <= latency_ms <= 4900:
            raise ValueError("latencyMs must be between 0 and 4900")
        with self._lock:
            self._latency_ms = latency_ms

    def latency_ms(self) -> int:
        with self._lock:
            return self._latency_ms

    def record(self, delivery_id: str | None, event_id: str | None, attempt: str | None) -> None:
        receipt = {
            "deliveryId": delivery_id, "eventId": event_id, "attempt": attempt,
            "receivedAt": datetime.now(timezone.utc).isoformat(),
        }
        with self._lock:
            self._receipts.append(receipt)

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._receipts)

    def reset(self) -> None:
        with self._lock:
            self._receipts.clear()


class BoundedThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], handler: type[BaseHTTPRequestHandler], max_concurrency: int):
        self.max_concurrency = max_concurrency
        self.request_queue_size = max(128, max_concurrency * 2)
        self._capacity = threading.BoundedSemaphore(max_concurrency)
        super().__init__(address, handler)

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        with self._capacity:
            super().process_request_thread(request, client_address)


class Handler(BaseHTTPRequestHandler):
    server: "BenchmarkReceiverServer"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _json_body(self, limit: int = 4096) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length > limit:
            raise ValueError("request body too large")
        value = json.loads(self.rfile.read(length) or b"{}")
        if not isinstance(value, dict):
            raise ValueError("JSON object required")
        return value

    def _respond(self, status: int, body: Any | None = None) -> None:
        encoded = b"" if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        if body is not None:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        if encoded:
            self.wfile.write(encoded)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._respond(200, {"status": "UP", "maxConcurrency": self.server.max_concurrency})
        elif self.path == "/received":
            receipts = self.server.state.snapshot()
            self._respond(200, {"count": len(receipts), "receipts": receipts})
        else:
            self._respond(404, {"error": "not_found"})

    def do_POST(self) -> None:
        if self.path == "/webhook":
            length = min(int(self.headers.get("Content-Length", "0")), 1_048_576)
            self.rfile.read(length)
            self.server.state.record(
                self.headers.get("X-EventFlow-Delivery-Id"),
                self.headers.get("X-EventFlow-Event-Id"),
                self.headers.get("X-EventFlow-Attempt"),
            )
            time.sleep(self.server.state.latency_ms() / 1000)
            self._respond(200)
            return
        try:
            body = self._json_body()
            if self.path == "/control":
                latency = body.get("latencyMs")
                if not isinstance(latency, int):
                    raise ValueError("latencyMs must be an integer")
                self.server.state.configure(latency)
                self._respond(204)
            elif self.path == "/reset":
                self.server.state.reset()
                self._respond(204)
            else:
                self._respond(404, {"error": "not_found"})
        except (ValueError, json.JSONDecodeError) as error:
            self._respond(400, {"error": str(error)})


class BenchmarkReceiverServer(BoundedThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], max_concurrency: int, latency_ms: int):
        self.state = ReceiverState(latency_ms)
        super().__init__(address, Handler, max_concurrency)


def create_server(host: str = "0.0.0.0", port: int = 8082, max_concurrency: int = 128, latency_ms: int = 0) -> BenchmarkReceiverServer:
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be at least 1")
    return BenchmarkReceiverServer((host, port), max_concurrency, latency_ms)


if __name__ == "__main__":
    server = create_server(
        max_concurrency=int(os.getenv("RECEIVER_MAX_CONCURRENCY", "128")),
        latency_ms=int(os.getenv("RECEIVER_LATENCY_MS", "0")),
    )
    server.serve_forever()
