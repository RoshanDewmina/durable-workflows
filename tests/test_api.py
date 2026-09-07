from __future__ import annotations

from fastapi.testclient import TestClient

from durable_workflows.config import DEFAULT_PRINCIPALS, Settings


def submit(
    client: TestClient,
    headers: dict[str, str],
    payload: dict[str, object],
    key: str = "request-0001",
):
    return client.post("/jobs", headers={**headers, "Idempotency-Key": key}, json=payload)


def test_health_contract(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "durable-workflows",
        "version": "0.1.0",
    }


def test_authentication_and_owner_scope(
    client: TestClient,
    alpha_headers: dict[str, str],
    beta_headers: dict[str, str],
    payload: dict[str, object],
) -> None:
    assert client.get("/jobs").status_code == 401
    assert client.get("/jobs", headers={"Authorization": "Bearer wrong"}).status_code == 401
    created = submit(client, alpha_headers, payload)
    assert created.status_code == 201
    job_id = created.json()["id"]

    assert client.get(f"/jobs/{job_id}", headers=beta_headers).status_code == 404
    assert client.get(f"/jobs/{job_id}/events", headers=beta_headers).status_code == 404
    assert client.post(f"/jobs/{job_id}/cancel", headers=beta_headers).status_code == 404
    assert client.get("/jobs", headers=beta_headers).json() == []
    assert client.get("/events", headers=beta_headers).json() == []


def test_idempotency_replay_and_payload_collision(
    client: TestClient,
    alpha_headers: dict[str, str],
    payload: dict[str, object],
) -> None:
    first = submit(client, alpha_headers, payload)
    replay = submit(client, alpha_headers, payload)
    conflict_payload = {"kind": "data_import", "records": [{"value": 99}]}
    conflict = submit(client, alpha_headers, conflict_payload)

    assert first.status_code == 201
    assert replay.status_code == 200
    assert first.json()["id"] == replay.json()["id"]
    assert replay.json()["idempotent_replay"] is True
    assert conflict.status_code == 409
    assert len(client.get("/jobs", headers=alpha_headers).json()) == 1


def test_idempotency_keys_are_scoped_per_owner(
    client: TestClient,
    alpha_headers: dict[str, str],
    beta_headers: dict[str, str],
    payload: dict[str, object],
) -> None:
    alpha = submit(client, alpha_headers, payload)
    beta = submit(client, beta_headers, payload)
    assert alpha.status_code == beta.status_code == 201
    assert alpha.json()["id"] != beta.json()["id"]


def test_strict_validation_rejects_coercion_extras_and_bad_keys(
    client: TestClient, alpha_headers: dict[str, str]
) -> None:
    string_value = {"kind": "data_import", "records": [{"value": "1"}]}
    extra_value = {"kind": "data_import", "records": [{"value": 1, "name": "private"}]}
    empty = {"kind": "data_import", "records": []}

    assert submit(client, alpha_headers, string_value).status_code == 422
    assert submit(client, alpha_headers, extra_value).status_code == 422
    assert submit(client, alpha_headers, empty).status_code == 422
    assert client.post("/jobs", headers=alpha_headers, json=string_value).status_code == 422
    wrong_kind = {"kind": "other", "records": [{"value": 1}]}
    assert submit(client, alpha_headers, wrong_kind).status_code == 422


def test_event_envelope_contains_no_record_values(
    client: TestClient, alpha_headers: dict[str, str], payload: dict[str, object]
) -> None:
    job = submit(client, alpha_headers, payload).json()
    events = client.get(f"/jobs/{job['id']}/events", headers=alpha_headers).json()
    assert len(events) == 1
    event = events[0]
    assert event["schema_version"] == 1
    assert event["source"] == "durable-workflows"
    assert event["event_type"] == "job.state_changed"
    assert event["tenant_id"] == "synthetic-alpha"
    assert event["sequence"] == 1
    assert event["occurred_at"].endswith("Z")
    assert event["payload"] == {
        "attempt": 0,
        "kind": "data_import",
        "record_count": 3,
        "state": "queued",
    }
    assert "records" not in str(events)


def test_cancel_is_owner_scoped_and_terminal_is_immutable(
    client: TestClient, alpha_headers: dict[str, str], payload: dict[str, object]
) -> None:
    job_id = submit(client, alpha_headers, payload).json()["id"]
    cancelled = client.post(f"/jobs/{job_id}/cancel", headers=alpha_headers)
    assert cancelled.status_code == 200
    assert cancelled.json()["state"] == "cancelled"
    assert client.post(f"/jobs/{job_id}/cancel", headers=alpha_headers).status_code == 409


def test_metrics_require_authentication(
    client: TestClient, alpha_headers: dict[str, str], payload: dict[str, object]
) -> None:
    submit(client, alpha_headers, payload)
    assert client.get("/metrics").status_code == 401
    metrics = client.get("/metrics", headers=alpha_headers)
    assert metrics.status_code == 200
    assert 'durable_workflows_jobs{state="queued"} 1' in metrics.text
    assert "durable_workflows_events_total 1" in metrics.text


def test_public_mode_rejects_bundled_demo_credentials(tmp_path) -> None:
    settings = Settings(
        db_path=tmp_path / "public.db",
        environment="public",
        principals=DEFAULT_PRINCIPALS,
    )
    try:
        settings.validate()
    except ValueError as exc:
        assert "public mode" in str(exc)
    else:
        raise AssertionError("unsafe public credentials were accepted")
