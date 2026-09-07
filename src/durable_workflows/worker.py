from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass
from typing import Protocol

from durable_workflows.config import Settings
from durable_workflows.db import Database

logger = logging.getLogger("durable_workflows.worker")


class ImportDependency(Protocol):
    def transform(self, records: list[dict[str, int]]) -> dict[str, int]: ...


@dataclass(slots=True)
class LocalImportDependency:
    mode: str = "normal"
    timeout_seconds: float = 0.5

    def transform(self, records: list[dict[str, int]]) -> dict[str, int]:
        if self.mode == "timeout":
            time.sleep(self.timeout_seconds * 3)
        if self.mode == "error":
            raise RuntimeError("controlled dependency error")
        values = [record["value"] for record in records]
        return {"accepted_count": len(values), "value_sum": sum(values)}


class Worker:
    def __init__(
        self,
        database: Database,
        settings: Settings,
        worker_id: str,
        dependency: ImportDependency | None = None,
    ):
        self.database = database
        self.settings = settings
        self.worker_id = worker_id
        self.dependency = dependency or LocalImportDependency(
            mode=settings.dependency_mode,
            timeout_seconds=settings.dependency_timeout_seconds,
        )

    def run_once(self) -> bool:
        job = self.database.claim_next(self.worker_id, self.settings.lease_seconds)
        if job is None:
            return False
        context = {
            "job_id": job["id"],
            "worker_id": self.worker_id,
            "attempt": job["attempt"],
        }
        logger.info("job claimed", extra={**context, "state": "running"})
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="bounded-dependency")
        future = executor.submit(self.dependency.transform, job["request"]["records"])
        try:
            result = future.result(timeout=self.settings.dependency_timeout_seconds)
            if set(result) != {"accepted_count", "value_sum"} or not all(
                isinstance(value, int) for value in result.values()
            ):
                raise ValueError("dependency returned an invalid result")
            committed = self.database.complete_job(
                job["id"], self.worker_id, job["fencing_token"], result
            )
            if committed:
                logger.info("job succeeded", extra={**context, "state": "succeeded"})
            else:
                logger.warning("stale worker completion rejected", extra=context)
        except FutureTimeout:
            future.cancel()
            state = self.database.fail_attempt(
                job["id"],
                self.worker_id,
                job["fencing_token"],
                "dependency_timeout",
                f"dependency exceeded {self.settings.dependency_timeout_seconds:.3f}s timeout",
                self.settings.retry_base_seconds,
                self.settings.retry_cap_seconds,
            )
            logger.warning(
                "job dependency timed out",
                extra={**context, "state": state, "error_code": "dependency_timeout"},
            )
        except Exception as exc:
            state = self.database.fail_attempt(
                job["id"],
                self.worker_id,
                job["fencing_token"],
                "dependency_error",
                str(exc),
                self.settings.retry_base_seconds,
                self.settings.retry_cap_seconds,
            )
            logger.exception(
                "job dependency failed",
                extra={**context, "state": state, "error_code": "dependency_error"},
            )
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        return True

    def run_forever(self, stop: threading.Event) -> None:
        recovered = self.database.recover_expired()
        if recovered:
            logger.warning(
                "expired leases recovered",
                extra={"worker_id": self.worker_id, "attempt": recovered},
            )
        while not stop.is_set():
            if not self.run_once():
                stop.wait(self.settings.poll_interval_seconds)


def run_worker_threads(
    database: Database, settings: Settings
) -> tuple[threading.Event, list[threading.Thread]]:
    stop = threading.Event()
    threads = []
    for index in range(settings.worker_count):
        worker = Worker(database, settings, f"embedded-{index + 1}")
        thread = threading.Thread(
            target=worker.run_forever,
            args=(stop,),
            daemon=True,
            name=f"durable-worker-{index + 1}",
        )
        thread.start()
        threads.append(thread)
    return stop, threads
