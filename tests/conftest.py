from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from durable_workflows.app import create_app
from durable_workflows.config import Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        db_path=tmp_path / "test.db",
        start_workers=False,
        worker_count=0,
        lease_seconds=0.2,
        dependency_timeout_seconds=0.03,
        retry_base_seconds=0.0,
        retry_cap_seconds=0.0,
    )


@pytest.fixture
def client(settings: Settings) -> Iterator[TestClient]:
    with TestClient(create_app(settings)) as test_client:
        yield test_client


@pytest.fixture
def alpha_headers() -> dict[str, str]:
    return {"Authorization": "Bearer demo-alpha-token"}


@pytest.fixture
def beta_headers() -> dict[str, str]:
    return {"Authorization": "Bearer demo-beta-token"}


@pytest.fixture
def payload() -> dict[str, object]:
    return {
        "kind": "data_import",
        "records": [{"value": 1}, {"value": 2}, {"value": 3}],
    }
