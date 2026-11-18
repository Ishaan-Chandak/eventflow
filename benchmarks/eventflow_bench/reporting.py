from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

REQUEST_FIELDS = ["phase", "sequence", "success", "status", "start_monotonic", "end_monotonic", "start_utc", "end_utc", "latency_ms", "payload_bytes", "request_body_bytes", "event_id", "job_id", "error"]
DELIVERY_FIELDS = ["job_id", "event_id", "status", "attempt_count", "last_http_status", "last_error_code", "event_created_at", "finished_at", "outcome", "duration_ms", "e2e_latency_ms"]


def _json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _csv(path: Path, rows: list[dict[str, Any]], default_fields: list[str]) -> None:
    fields = list(dict.fromkeys(default_fields + sorted({key for row in rows for key in row})))
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_run_artifacts(
    output_dir: Path,
    config: dict[str, Any],
    machine: dict[str, Any],
    summary: dict[str, Any],
    requests: list[dict[str, Any]],
    deliveries: list[dict[str, Any]],
    errors: list[dict[str, Any]],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    _json(output_dir / "config.json", config)
    _json(output_dir / "machine.json", machine)
    _json(output_dir / "summary.json", summary)
    _json(output_dir / "errors.json", errors)
    _csv(output_dir / "requests.csv", requests, REQUEST_FIELDS)
    _csv(output_dir / "deliveries.csv", deliveries, DELIVERY_FIELDS)
    return output_dir
