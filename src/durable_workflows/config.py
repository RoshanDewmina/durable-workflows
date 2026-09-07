from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_PRINCIPALS = {
    "demo-alpha-token": "synthetic-alpha",
    "demo-beta-token": "synthetic-beta",
}


@dataclass(frozen=True, slots=True)
class Settings:
    db_path: Path = Path("data/durable.db")
    host: str = "127.0.0.1"
    port: int = 8111
    environment: str = "local"
    principals: dict[str, str] = field(default_factory=lambda: DEFAULT_PRINCIPALS.copy())
    start_workers: bool = True
    worker_count: int = 2
    poll_interval_seconds: float = 0.05
    lease_seconds: float = 2.0
    dependency_timeout_seconds: float = 0.5
    retry_base_seconds: float = 0.05
    retry_cap_seconds: float = 0.5
    max_attempts: int = 3
    dependency_mode: str = "normal"

    @classmethod
    def from_env(cls) -> Settings:
        environment = os.getenv("DW_ENV", "local")
        principals_raw = os.getenv("DW_PRINCIPALS_JSON")
        principals = DEFAULT_PRINCIPALS.copy()
        if principals_raw:
            parsed = json.loads(principals_raw)
            if not isinstance(parsed, dict) or not all(
                isinstance(key, str) and isinstance(value, str) for key, value in parsed.items()
            ):
                raise ValueError("DW_PRINCIPALS_JSON must be a string-to-string JSON object")
            principals = parsed

        settings = cls(
            db_path=Path(os.getenv("DW_DB_PATH", "data/durable.db")),
            host=os.getenv("HOST", "127.0.0.1"),
            port=int(os.getenv("PORT", "8111")),
            environment=environment,
            principals=principals,
            start_workers=os.getenv("DW_START_WORKERS", "true").lower() == "true",
            worker_count=int(os.getenv("DW_WORKER_COUNT", "2")),
            poll_interval_seconds=float(os.getenv("DW_POLL_SECONDS", "0.05")),
            lease_seconds=float(os.getenv("DW_LEASE_SECONDS", "2")),
            dependency_timeout_seconds=float(os.getenv("DW_DEPENDENCY_TIMEOUT_SECONDS", "0.5")),
            retry_base_seconds=float(os.getenv("DW_RETRY_BASE_SECONDS", "0.05")),
            retry_cap_seconds=float(os.getenv("DW_RETRY_CAP_SECONDS", "0.5")),
            max_attempts=int(os.getenv("DW_MAX_ATTEMPTS", "3")),
            dependency_mode=os.getenv("DW_DEPENDENCY_MODE", "normal"),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if not self.principals or len(set(self.principals.values())) != len(self.principals):
            raise ValueError("principals must map unique bearer tokens to unique tenants")
        if self.environment == "public":
            if self.principals == DEFAULT_PRINCIPALS or any(
                token.startswith("demo-") or len(token) < 32 for token in self.principals
            ):
                raise ValueError(
                    "public mode requires non-demo bearer tokens of at least 32 characters"
                )
        if self.worker_count < 0 or self.worker_count > 16:
            raise ValueError("DW_WORKER_COUNT must be between 0 and 16")
        if self.max_attempts < 1 or self.max_attempts > 10:
            raise ValueError("DW_MAX_ATTEMPTS must be between 1 and 10")
        if self.dependency_timeout_seconds <= 0:
            raise ValueError("dependency timeout must be positive")
        if self.lease_seconds <= self.dependency_timeout_seconds:
            raise ValueError("lease must outlast the dependency timeout")
        if self.dependency_mode not in {"normal", "timeout", "error"}:
            raise ValueError("DW_DEPENDENCY_MODE must be normal, timeout, or error")
