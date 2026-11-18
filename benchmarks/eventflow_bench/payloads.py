from __future__ import annotations

import json

from .config import RunIdentity

MAX_REQUEST_BYTES = 1_048_576


def serialized_size(value: object) -> int:
    return len(json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def build_payload(target_bytes: int, run_id: str, phase: str, sequence: int) -> dict[str, object]:
    payload: dict[str, object] = {
        "benchmark": {"runId": run_id, "phase": phase, "sequence": sequence},
        "padding": "",
    }
    base_size = serialized_size(payload)
    if target_bytes < base_size:
        raise ValueError(f"payload_bytes must be at least {base_size} for this run identity")
    payload["padding"] = "x" * (target_bytes - base_size)
    if serialized_size(payload) != target_bytes:
        raise AssertionError("payload sizing invariant failed")
    return payload


def validate_payload_size(
    target_bytes: int, identity: RunIdentity, event_count: int, warmup_count: int
) -> None:
    for phase, count in (("warmup", warmup_count), ("steady", event_count)):
        if count == 0:
            continue
        sequence = count - 1
        try:
            payload = build_payload(target_bytes, identity.run_id, phase, sequence)
        except ValueError as error:
            raise ValueError(f"payload_bytes is too small for the largest sequence in {phase}: {error}") from error
        body = {"eventType": identity.event_type, "payload": payload}
        full_size = serialized_size(body)
        if full_size > MAX_REQUEST_BYTES:
            raise ValueError(
                f"payload_bytes produces a {full_size}-byte request; the full request must not exceed 1 MiB"
            )
