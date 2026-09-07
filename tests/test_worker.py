from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from pathlib import Path

from durable_workflows.config import Settings
from durable_workflows.db import Database
from durable_workflows.worker import LocalImportDependency, Worker

PAYLOAD = {
    "kind": "data_import",
    "records": [{"value": 5}, {"value": -2}, {"value": 9}],
}


def initialized(settings: Settings) -> Database:
    database = Database(settings.db_path)
    database.initialize()
    return database


def test_worker_commits_result_and_ordered_state_events(settings: Settings) -> None:
    database = initialized(settings)
    job, _ = database.create_job("synthetic-alpha", PAYLOAD, "worker-test-0001", 3)

    assert Worker(database, settings, "worker-one").run_once() is True

    result = database.get_job("synthetic-alpha", job["id"])
    assert result["state"] == "succeeded"
    assert result["result"] == {"accepted_count": 3, "value_sum": 12}
    assert database.side_effect_count(job["id"]) == 1
    events = database.job_events("synthetic-alpha", job["id"])
    assert [event["sequence"] for event in events] == [1, 2, 3]
    assert [event["payload"]["state"] for event in events] == ["queued", "running", "succeeded"]


def test_dependency_timeout_retries_are_bounded_and_terminal(settings: Settings) -> None:
    database = initialized(settings)
    job, _ = database.create_job("synthetic-alpha", PAYLOAD, "timeout-test-01", 3)
    dependency = LocalImportDependency(
        mode="timeout", timeout_seconds=settings.dependency_timeout_seconds
    )
    worker = Worker(database, settings, "timeout-worker", dependency)

    assert worker.run_once() is True
    assert worker.run_once() is True
    assert worker.run_once() is True
    final = database.get_job("synthetic-alpha", job["id"])
    assert final["state"] == "failed"
    assert final["attempt"] == 3
    assert final["error"]["code"] == "dependency_timeout"
    assert database.side_effect_count(job["id"]) == 0
    assert worker.run_once() is False
    event_sequences = [
        event["sequence"] for event in database.job_events("synthetic-alpha", job["id"])
    ]
    assert event_sequences == list(range(1, 8))


def test_fencing_rejects_stale_worker_and_deduplicates_effect(settings: Settings) -> None:
    database = initialized(settings)
    job, _ = database.create_job("synthetic-alpha", PAYLOAD, "fencing-test-1", 3)
    first = database.claim_next("old-worker", lease_seconds=0.02)
    assert first is not None
    time.sleep(0.03)
    result = {"accepted_count": 3, "value_sum": 12}
    assert database.complete_job(job["id"], "old-worker", first["fencing_token"], result) is False
    assert database.recover_expired() == 1
    second = database.claim_next("new-worker", lease_seconds=0.2)
    assert second is not None
    assert second["fencing_token"] > first["fencing_token"]

    assert database.complete_job(job["id"], "old-worker", first["fencing_token"], result) is False
    assert database.complete_job(job["id"], "new-worker", second["fencing_token"], result) is True
    assert database.complete_job(job["id"], "new-worker", second["fencing_token"], result) is False
    assert database.side_effect_count(job["id"]) == 1


def test_competing_workers_claim_each_job_once(settings: Settings) -> None:
    database = initialized(settings)
    total = 48
    for index in range(total):
        database.create_job("synthetic-alpha", PAYLOAD, f"concurrent-{index:04d}", 3)

    barrier = threading.Barrier(4)

    def drain(worker_number: int) -> None:
        worker = Worker(database, settings, f"concurrent-worker-{worker_number}")
        barrier.wait()
        while worker.run_once():
            pass

    threads = [threading.Thread(target=drain, args=(index,)) for index in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    jobs = database.list_jobs("synthetic-alpha")
    assert len(jobs) == total
    assert all(job["state"] == "succeeded" and job["attempt"] == 1 for job in jobs)
    assert database.side_effect_count() == total


def test_process_kill_then_restart_recovers_expired_lease(tmp_path: Path) -> None:
    settings = Settings(
        db_path=tmp_path / "restart.db",
        start_workers=False,
        worker_count=0,
        lease_seconds=0.08,
        dependency_timeout_seconds=0.02,
        retry_base_seconds=0.0,
        retry_cap_seconds=0.0,
    )
    database = initialized(settings)
    job, _ = database.create_job("synthetic-alpha", PAYLOAD, "restart-test-1", 3)
    environment = {
        **os.environ,
        "DW_DB_PATH": str(settings.db_path),
        "DW_START_WORKERS": "false",
        "DW_LEASE_SECONDS": "0.08",
        "DW_DEPENDENCY_TIMEOUT_SECONDS": "0.02",
        "DW_RETRY_BASE_SECONDS": "0",
        "DW_RETRY_CAP_SECONDS": "0",
    }
    crashed = subprocess.run(
        [sys.executable, "-m", "durable_workflows.worker_cli", "--crash-after-claim"],
        env=environment,
        check=False,
    )
    assert crashed.returncode == 91
    assert database.get_job("synthetic-alpha", job["id"])["state"] == "running"

    time.sleep(0.1)
    restarted_worker = Worker(database, settings, "restart-worker")
    assert database.recover_expired() == 1
    assert restarted_worker.run_once() is True
    final = database.get_job("synthetic-alpha", job["id"])
    assert final["state"] == "succeeded"
    assert final["attempt"] == 2
    assert database.side_effect_count(job["id"]) == 1
