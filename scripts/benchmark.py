from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from durable_workflows.config import Settings
from durable_workflows.db import Database
from durable_workflows.worker import Worker


def git(command: list[str]) -> str:
    result = subprocess.run(["git", *command], capture_output=True, check=True, text=True)
    return result.stdout.strip()


def hardware_description() -> str:
    if platform.system() == "Darwin":
        result = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"],
            capture_output=True,
            check=False,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    return platform.processor() or "unavailable"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=300)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--records-per-job", type=int, default=12)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.jobs <= 10_000 or not 1 <= args.workers <= 16:
        raise SystemExit("jobs must be 1..10000 and workers 1..16")

    revision = git(["rev-parse", "HEAD"])
    dirty_tree = bool(git(["status", "--porcelain"]))
    command = (
        f"uv run python scripts/benchmark.py --jobs {args.jobs} --workers {args.workers} "
        f"--records-per-job {args.records_per_job} --output {args.output}"
    )

    with TemporaryDirectory(prefix="durable-benchmark-") as temp_dir:
        settings = Settings(
            db_path=Path(temp_dir) / "benchmark.db",
            start_workers=False,
            worker_count=0,
            lease_seconds=2,
            dependency_timeout_seconds=0.5,
            retry_base_seconds=0,
            retry_cap_seconds=0,
        )
        database = Database(settings.db_path)
        database.initialize()
        records = [{"value": offset - 5} for offset in range(args.records_per_job)]
        payload = {"kind": "data_import", "records": records}

        started = time.perf_counter()
        for index in range(args.jobs):
            database.create_job("synthetic-benchmark", payload, f"benchmark-{index:08d}", 3)
        submitted = time.perf_counter()

        barrier = threading.Barrier(args.workers)

        def drain(index: int) -> None:
            worker = Worker(database, settings, f"benchmark-worker-{index}")
            barrier.wait()
            while worker.run_once():
                pass

        threads = [threading.Thread(target=drain, args=(index,)) for index in range(args.workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        finished = time.perf_counter()

        jobs = database.list_jobs("synthetic-benchmark")
        successes = sum(job["state"] == "succeeded" for job in jobs)
        effects = database.side_effect_count()
        process_seconds = finished - submitted
        receipt = {
            "schema_version": 1,
            "source_revision": revision,
            "dirty_tree": dirty_tree,
            "command": command,
            "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "exit_status": 0 if successes == args.jobs and effects == args.jobs else 1,
            "environment": {
                "os": platform.platform(),
                "architecture": platform.machine(),
                "python": platform.python_version(),
                "hardware": hardware_description(),
                "logical_cpu_count": os.cpu_count(),
                "sqlite": __import__("sqlite3").sqlite_version,
            },
            "inputs": {
                "jobs": args.jobs,
                "workers": args.workers,
                "records_per_job": args.records_per_job,
                "synthetic": True,
                "seed": None,
            },
            "measured_results": {
                "submission_seconds": round(submitted - started, 6),
                "processing_seconds": round(process_seconds, 6),
                "wall_seconds": round(finished - started, 6),
                "processing_jobs_per_second": round(args.jobs / process_seconds, 3),
                "succeeded_jobs": successes,
                "deduplicated_side_effects": effects,
            },
            "limitations": [
                "Synthetic local workload; it is not evidence of production or "
                "multi-host capacity.",
                "SQLite WAL serializes writers; results depend on host and background load.",
                "The dependency is an in-process deterministic transform with no network latency.",
            ],
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(receipt, indent=2) + "\n")
        print(json.dumps(receipt["measured_results"], indent=2))
        raise SystemExit(receipt["exit_status"])


if __name__ == "__main__":
    main()
