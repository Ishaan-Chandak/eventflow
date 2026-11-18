from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Callable


TERMINAL_STATUSES = {"SUCCEEDED", "DEAD"}


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


def normalize_delivery_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = {key: _iso(value) for key, value in row.items()}
    created = row.get("event_created_at")
    finished = row.get("finished_at")
    normalized["e2e_latency_ms"] = (
        (finished - created).total_seconds() * 1000 if isinstance(created, datetime) and isinstance(finished, datetime) else None
    )
    return normalized


class PostgresCollector:
    """Read-only benchmark correlation against PostgreSQL's durable state."""

    def __init__(self, dsn: str, connect: Callable[..., Any] | None = None):
        if connect is None:
            from psycopg import connect as psycopg_connect

            connect = psycopg_connect
        self._dsn = dsn
        self._connect = connect

    def _rows(self, query: str, parameters: tuple[Any, ...]) -> list[dict[str, Any]]:
        from psycopg.rows import dict_row

        with self._connect(self._dsn, autocommit=True, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, parameters)
                return list(cursor.fetchall())

    def deliveries(self, event_type: str, phase: str) -> list[dict[str, Any]]:
        rows = self._rows(
            """
            SELECT j.id::text AS job_id, e.id::text AS event_id, j.status,
                   j.attempt_count, j.last_http_status, j.last_error_code,
                   e.created_at AS event_created_at,
                   a.finished_at, a.outcome, a.duration_ms
              FROM events e
              JOIN delivery_jobs j ON j.event_id = e.id
              LEFT JOIN LATERAL (
                  SELECT finished_at, outcome, duration_ms
                    FROM delivery_attempts
                   WHERE delivery_job_id = j.id AND outcome = 'SUCCESS'
                   ORDER BY attempt_number DESC LIMIT 1
              ) a ON true
             WHERE e.event_type = %s
               AND e.idempotency_key LIKE %s
             ORDER BY e.created_at, j.id
            """,
            (event_type, f"%-{phase}-%"),
        )
        return [normalize_delivery_row(row) for row in rows]

    def status_counts(self, event_type: str, phase: str) -> dict[str, int]:
        rows = self._rows(
            """
            SELECT j.status, count(*)::int AS count
              FROM events e JOIN delivery_jobs j ON j.event_id = e.id
             WHERE e.event_type = %s AND e.idempotency_key LIKE %s
             GROUP BY j.status
            """,
            (event_type, f"%-{phase}-%"),
        )
        return {str(row["status"]): int(row["count"]) for row in rows}

    def status_counts_for_jobs(self, job_ids: list[str]) -> dict[str, int]:
        if not job_ids:
            return {}
        rows = self._rows(
            "SELECT status, count(*)::int AS count FROM delivery_jobs WHERE id = ANY(%s::uuid[]) GROUP BY status",
            (job_ids,),
        )
        return {str(row["status"]): int(row["count"]) for row in rows}

    def wait_for_terminal(
        self, event_type: str, phase: str, expected: int, timeout_seconds: float, poll_interval_seconds: float,
        job_ids: list[str] | None = None,
    ) -> tuple[bool, dict[str, int]]:
        deadline = time.monotonic() + timeout_seconds
        counts: dict[str, int] = {}
        while True:
            counts = self.status_counts_for_jobs(job_ids) if job_ids is not None else self.status_counts(event_type, phase)
            terminal = sum(count for status, count in counts.items() if status in TERMINAL_STATUSES)
            if terminal >= expected:
                return counts.get("SUCCEEDED", 0) == expected, counts
            if time.monotonic() >= deadline:
                return False, counts
            time.sleep(poll_interval_seconds)
