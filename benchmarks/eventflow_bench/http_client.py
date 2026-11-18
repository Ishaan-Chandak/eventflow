from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from .config import RunIdentity
from .payloads import build_payload


class JsonTransport:
    def __init__(self, base_url: str, timeout_seconds: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def post_json(self, path: str, body: dict[str, Any], headers: dict[str, str] | None = None) -> tuple[int, Any]:
        encoded = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}{path}", data=encoded, method="POST",
            headers={"Content-Type": "application/json", **(headers or {})},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                content = response.read()
                return response.status, json.loads(content) if content else None
        except urllib.error.HTTPError as error:
            content = error.read()
            try:
                payload = json.loads(content) if content else None
            except json.JSONDecodeError:
                payload = {"message": content.decode("utf-8", "replace")}
            return error.code, payload

    def get_json(self, path: str) -> tuple[int, Any]:
        request = urllib.request.Request(f"{self.base_url}{path}", method="GET")
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            content = response.read()
            return response.status, json.loads(content) if content else None


class EventFlowHttpClient:
    def __init__(self, api_transport: Any, receiver_transport: Any | None = None):
        self.api = api_transport
        self.receiver = receiver_transport

    def wait_until_ready(self, timeout_seconds: float = 120, poll_interval_seconds: float = 0.5) -> None:
        deadline = time.monotonic() + timeout_seconds
        for transport, path in ((self.api, "/actuator/health"), (self.receiver, "/health")):
            if transport is None:
                continue
            while True:
                try:
                    status, body = transport.get_json(path)
                    if status == 200 and isinstance(body, dict) and body.get("status") == "UP":
                        break
                except Exception:
                    pass
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"service at {path} did not become ready")
                time.sleep(poll_interval_seconds)

    def configure_receiver(self, latency_ms: int) -> None:
        if self.receiver is None:
            return
        status, _ = self.receiver.post_json("/control", {"latencyMs": latency_ms})
        if status != 204:
            raise RuntimeError(f"receiver control returned HTTP {status}")

    def receiver_diagnostics(self) -> dict[str, Any]:
        if self.receiver is None:
            return {}
        health_status, health = self.receiver.get_json("/health")
        receipt_status, receipts = self.receiver.get_json("/received")
        if health_status != 200 or receipt_status != 200:
            raise RuntimeError("receiver diagnostics unavailable")
        return {
            "max_concurrency": health.get("maxConcurrency"),
            "receipt_count": receipts.get("count"),
        }

    def register_endpoint(self, identity: RunIdentity, receiver_url: str) -> str:
        status, response = self.api.post_json("/api/v1/endpoints", {
            "name": identity.endpoint_name,
            "url": f"{receiver_url.rstrip('/')}/webhook",
            "eventTypes": [identity.event_type],
            "enabled": True,
        })
        if status != 201 or not isinstance(response, dict) or "id" not in response:
            raise RuntimeError(f"endpoint registration returned HTTP {status}: {response}")
        return str(response["id"])

    def _publish(self, identity: RunIdentity, phase: str, sequence: int, payload_bytes: int) -> dict[str, Any]:
        payload = build_payload(payload_bytes, identity.run_id, phase, sequence)
        body = {"eventType": identity.event_type, "payload": payload}
        full_body_bytes = len(json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        start_monotonic = time.perf_counter()
        start_utc = datetime.now(timezone.utc)
        status: int | None = None
        response: Any = None
        error: str | None = None
        try:
            status, response = self.api.post_json(
                "/api/v1/events", body, {"Idempotency-Key": identity.idempotency_key(phase, sequence)}
            )
        except Exception as exception:
            error = f"{type(exception).__name__}: {exception}"
        end_monotonic = time.perf_counter()
        end_utc = datetime.now(timezone.utc)
        job_ids = response.get("jobIds", []) if isinstance(response, dict) else []
        success = status == 202 and len(job_ids) == 1
        return {
            "phase": phase, "sequence": sequence, "success": success, "status": status,
            "start_monotonic": start_monotonic, "end_monotonic": end_monotonic,
            "start_utc": start_utc.isoformat(), "end_utc": end_utc.isoformat(),
            "latency_ms": (end_monotonic - start_monotonic) * 1000,
            "payload_bytes": payload_bytes, "request_body_bytes": full_body_bytes,
            "event_id": response.get("eventId") if isinstance(response, dict) else None,
            "job_id": str(job_ids[0]) if len(job_ids) == 1 else None, "error": error,
        }

    def publish_many(
        self, identity: RunIdentity, phase: str, count: int, concurrency: int, payload_bytes: int
    ) -> list[dict[str, Any]]:
        if count == 0:
            return []
        with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="publisher") as pool:
            futures = {pool.submit(self._publish, identity, phase, sequence, payload_bytes): sequence for sequence in range(count)}
            rows: list[dict[str, Any]] = []
            for future in as_completed(futures):
                sequence = futures[future]
                try:
                    rows.append(future.result())
                except Exception as error:
                    now_monotonic = time.perf_counter()
                    now_utc = datetime.now(timezone.utc).isoformat()
                    rows.append({
                        "phase": phase, "sequence": sequence, "success": False, "status": None,
                        "start_monotonic": now_monotonic, "end_monotonic": now_monotonic,
                        "start_utc": now_utc, "end_utc": now_utc, "latency_ms": 0.0,
                        "payload_bytes": payload_bytes, "request_body_bytes": None,
                        "event_id": None, "job_id": None,
                        "error": f"{type(error).__name__}: {error}",
                    })
            return sorted(rows, key=lambda row: int(row["sequence"]))
