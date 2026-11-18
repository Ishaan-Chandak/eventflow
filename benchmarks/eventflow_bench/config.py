from __future__ import annotations

from dataclasses import asdict, dataclass
from uuid import uuid4


@dataclass(frozen=True)
class BenchmarkConfig:
    event_count: int = 1000
    warmup_count: int = 100
    publisher_concurrency: int = 10
    worker_count: int = 1
    payload_bytes: int = 1024
    receiver_latency_ms: int = 0
    drain_timeout_seconds: float = 120.0
    poll_interval_seconds: float = 0.25
    api_url: str = "http://benchmark-api:8080"
    receiver_url: str = "http://benchmark-receiver:8082"
    postgres_dsn: str = "postgresql://eventflow:eventflow@postgres:5432/eventflow"
    output_dir: str = "/results"
    label: str = "benchmark"

    def __post_init__(self) -> None:
        positive = {
            "event_count": self.event_count,
            "publisher_concurrency": self.publisher_concurrency,
            "worker_count": self.worker_count,
            "payload_bytes": self.payload_bytes,
        }
        for name, value in positive.items():
            if value < 1:
                raise ValueError(f"{name} must be at least 1")
        if self.warmup_count < 0:
            raise ValueError("warmup_count cannot be negative")
        if not 0 <= self.receiver_latency_ms <= 4900:
            raise ValueError("receiver_latency_ms must be between 0 and 4900")
        if self.drain_timeout_seconds <= 0 or self.poll_interval_seconds <= 0:
            raise ValueError("timeouts and polling intervals must be positive")

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_public_dict(self) -> dict[str, object]:
        values = asdict(self)
        values.pop("postgres_dsn", None)
        return values


@dataclass(frozen=True)
class RunIdentity:
    run_id: str
    label: str

    @classmethod
    def create(cls, label: str = "benchmark", run_id: str | None = None) -> "RunIdentity":
        value = run_id or uuid4().hex
        if not value or any(character not in "0123456789abcdef" for character in value.lower()):
            raise ValueError("run_id must contain only hexadecimal characters")
        return cls(value.lower(), label)

    @property
    def event_type(self) -> str:
        return f"benchmark.{self.run_id}"

    @property
    def endpoint_name(self) -> str:
        return f"benchmark-{self.label}-{self.run_id}"

    def idempotency_key(self, phase: str, sequence: int) -> str:
        if phase not in {"warmup", "steady"}:
            raise ValueError("phase must be warmup or steady")
        return f"eventflow-bench-{self.run_id}-{phase}-{sequence}"
