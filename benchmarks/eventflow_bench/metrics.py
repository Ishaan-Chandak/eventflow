from __future__ import annotations

from collections import Counter
from datetime import datetime
from math import ceil
from typing import Iterable


def nearest_rank(values: Iterable[float], percentile: float) -> float | None:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    if not 0 < percentile <= 100:
        raise ValueError("percentile must be in (0, 100]")
    return ordered[max(0, ceil(percentile / 100 * len(ordered)) - 1)]


def _percentiles(values: Iterable[float]) -> dict[str, float | None]:
    materialized = list(values)
    return {f"p{percentile}": nearest_rank(materialized, percentile) for percentile in (50, 95, 99)}


def summarize_ingestion(requests: list[dict[str, object]]) -> dict[str, object]:
    measured = [row for row in requests if row.get("phase") == "steady"]
    successful = [row for row in measured if row.get("success") is True]
    if successful:
        started = min(float(row["start_monotonic"]) for row in measured)
        finished = max(float(row["end_monotonic"]) for row in successful)
        duration = max(finished - started, 1e-9)
    else:
        duration = 0.0
    return {
        "attempted": len(measured),
        "successful": len(successful),
        "failed": len(measured) - len(successful),
        "window_seconds": duration,
        "rps": len(successful) / duration if duration else 0.0,
        "latency_ms": _percentiles(float(row["latency_ms"]) for row in successful),
        "statuses": dict(Counter(str(row.get("status", "ERROR")) for row in measured)),
    }


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def summarize_delivery(
    deliveries: list[dict[str, object]], publication_start_utc: str, publication_end_utc: str
) -> dict[str, object]:
    successful = [row for row in deliveries if row.get("status") == "SUCCEEDED" and row.get("finished_at")]
    start = _instant(publication_start_utc)
    end = _instant(publication_end_utc)
    steady_seconds = max((end - start).total_seconds(), 1e-9)
    steady = [row for row in successful if start <= _instant(str(row["finished_at"])) <= end]
    drained_seconds = 0.0
    if successful:
        first_created = min(_instant(str(row["event_created_at"])) for row in deliveries)
        last_finished = max(_instant(str(row["finished_at"])) for row in successful)
        drained_seconds = max((last_finished - first_created).total_seconds(), 1e-9)
    return {
        "total": len(deliveries),
        "successful": len(successful),
        "incomplete_or_failed": len(deliveries) - len(successful),
        "statuses": dict(Counter(str(row.get("status", "MISSING")) for row in deliveries)),
        "steady_state_window_seconds": steady_seconds,
        "steady_state_completions": len(steady),
        "steady_state_rps": len(steady) / steady_seconds,
        "drained_window_seconds": drained_seconds,
        "drained_rps": len(successful) / drained_seconds if drained_seconds else 0.0,
        "e2e_latency_ms": _percentiles(
            float(row["e2e_latency_ms"]) for row in successful if row.get("e2e_latency_ms") is not None
        ),
    }
