from __future__ import annotations

import argparse
import os
import threading

from durable_workflows.config import Settings
from durable_workflows.db import Database
from durable_workflows.logging_config import configure_logging
from durable_workflows.worker import Worker


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a durable workflow worker")
    parser.add_argument("--worker-id", default=f"standalone-{os.getpid()}")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--drain", action="store_true")
    parser.add_argument("--crash-after-claim", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()

    settings = Settings.from_env()
    database = Database(settings.db_path)
    database.initialize()
    configure_logging()

    if args.crash_after_claim:
        claimed = database.claim_next(args.worker_id, settings.lease_seconds)
        os._exit(91 if claimed else 92)

    worker = Worker(database, settings, args.worker_id)
    if args.once:
        worker.run_once()
    elif args.drain:
        while worker.run_once():
            pass
    else:
        worker.run_forever(threading.Event())


if __name__ == "__main__":
    main()
