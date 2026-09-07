from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

TERMINAL_STATES = {"succeeded", "failed", "cancelled"}


class IdempotencyConflict(Exception):
    pass


class JobNotFound(Exception):
    pass


class TerminalStateConflict(Exception):
    pass


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical_json(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


class Database:
    def __init__(self, path: Path | str):
        self.path = Path(path)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=5,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.session() as connection:
            journal_mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(journal_mode).lower() != "wal":
                raise RuntimeError(f"SQLite WAL unavailable: {journal_mode}")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    kind TEXT NOT NULL CHECK (kind = 'data_import'),
                    state TEXT NOT NULL CHECK (
                        state IN ('queued','running','retry_wait','succeeded','failed','cancelled')
                    ),
                    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
                    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
                    next_run_at REAL NOT NULL,
                    revision INTEGER NOT NULL CHECK (revision > 0),
                    record_count INTEGER NOT NULL CHECK (record_count > 0),
                    request_json TEXT NOT NULL,
                    result_json TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    lease_owner TEXT,
                    lease_expires_at REAL,
                    fencing_token INTEGER NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                );

                CREATE INDEX IF NOT EXISTS jobs_claim_idx
                    ON jobs(state, next_run_at, created_at);
                CREATE INDEX IF NOT EXISTS jobs_owner_idx
                    ON jobs(tenant_id, created_at DESC);

                CREATE TABLE IF NOT EXISTS idempotency_keys (
                    tenant_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    job_id TEXT NOT NULL REFERENCES jobs(id),
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, idempotency_key)
                );

                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL REFERENCES jobs(id),
                    tenant_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL CHECK (sequence > 0),
                    occurred_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    UNIQUE(job_id, sequence)
                );
                CREATE INDEX IF NOT EXISTS events_owner_idx
                    ON events(tenant_id, occurred_at, event_id);

                CREATE TABLE IF NOT EXISTS side_effects (
                    job_id TEXT NOT NULL REFERENCES jobs(id),
                    effect_key TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    committed_at TEXT NOT NULL,
                    PRIMARY KEY(job_id, effect_key)
                );
                """
            )

    @contextmanager
    def session(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def immediate(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        began = False
        try:
            connection.execute("BEGIN IMMEDIATE")
            began = True
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            if began and connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def create_job(
        self,
        tenant_id: str,
        payload: dict[str, Any],
        idempotency_key: str,
        max_attempts: int,
    ) -> tuple[dict[str, Any], bool]:
        encoded = canonical_json(payload)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        now = utc_now()
        now_epoch = time.time()
        with self.immediate() as connection:
            existing = connection.execute(
                """SELECT payload_sha256, job_id FROM idempotency_keys
                   WHERE tenant_id = ? AND idempotency_key = ?""",
                (tenant_id, idempotency_key),
            ).fetchone()
            if existing:
                if existing["payload_sha256"] != digest:
                    raise IdempotencyConflict
                job = connection.execute(
                    "SELECT * FROM jobs WHERE id = ?", (existing["job_id"],)
                ).fetchone()
                return self._public_job(job), False

            job_id = str(uuid.uuid4())
            connection.execute(
                """INSERT INTO jobs (
                    id, tenant_id, kind, state, attempt, max_attempts, next_run_at,
                    revision, record_count, request_json, created_at, updated_at
                ) VALUES (?, ?, 'data_import', 'queued', 0, ?, ?, 1, ?, ?, ?, ?)""",
                (
                    job_id,
                    tenant_id,
                    max_attempts,
                    now_epoch,
                    len(payload["records"]),
                    encoded,
                    now,
                    now,
                ),
            )
            connection.execute(
                """INSERT INTO idempotency_keys (
                    tenant_id, idempotency_key, payload_sha256, job_id, created_at
                ) VALUES (?, ?, ?, ?, ?)""",
                (tenant_id, idempotency_key, digest, job_id, now),
            )
            job = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            self._append_event(connection, job)
            return self._public_job(job), True

    def list_jobs(self, tenant_id: str) -> list[dict[str, Any]]:
        with self.session() as connection:
            rows = connection.execute(
                "SELECT * FROM jobs WHERE tenant_id = ? ORDER BY created_at DESC, id DESC",
                (tenant_id,),
            ).fetchall()
        return [self._public_job(row) for row in rows]

    def get_job(self, tenant_id: str, job_id: str) -> dict[str, Any]:
        with self.session() as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE tenant_id = ? AND id = ?", (tenant_id, job_id)
            ).fetchone()
        if not row:
            raise JobNotFound
        return self._public_job(row)

    def cancel_job(self, tenant_id: str, job_id: str) -> dict[str, Any]:
        with self.immediate() as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE tenant_id = ? AND id = ?", (tenant_id, job_id)
            ).fetchone()
            if not row:
                raise JobNotFound
            if row["state"] in TERMINAL_STATES:
                raise TerminalStateConflict
            self._transition(
                connection,
                row,
                state="cancelled",
                error_code=None,
                error_message=None,
                terminal=True,
            )
            updated = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return self._public_job(updated)

    def job_events(self, tenant_id: str, job_id: str) -> list[dict[str, Any]]:
        self.get_job(tenant_id, job_id)
        with self.session() as connection:
            rows = connection.execute(
                "SELECT * FROM events WHERE tenant_id = ? AND job_id = ? ORDER BY sequence",
                (tenant_id, job_id),
            ).fetchall()
        return [self._event_envelope(row) for row in rows]

    def export_events(self, tenant_id: str) -> list[dict[str, Any]]:
        with self.session() as connection:
            rows = connection.execute(
                """SELECT * FROM events WHERE tenant_id = ?
                   ORDER BY occurred_at, job_id, sequence""",
                (tenant_id,),
            ).fetchall()
        return [self._event_envelope(row) for row in rows]

    def claim_next(self, worker_id: str, lease_seconds: float) -> dict[str, Any] | None:
        with self.immediate() as connection:
            self._recover_expired_locked(connection)
            now_epoch = time.time()
            row = connection.execute(
                """SELECT * FROM jobs
                   WHERE state IN ('queued', 'retry_wait') AND next_run_at <= ?
                   ORDER BY next_run_at, created_at, id LIMIT 1""",
                (now_epoch,),
            ).fetchone()
            if not row:
                return None
            now = utc_now()
            cursor = connection.execute(
                """UPDATE jobs SET state = 'running', attempt = attempt + 1,
                    revision = revision + 1, lease_owner = ?, lease_expires_at = ?,
                    fencing_token = fencing_token + 1, error_code = NULL,
                    error_message = NULL, updated_at = ?
                   WHERE id = ? AND revision = ? AND state IN ('queued', 'retry_wait')""",
                (worker_id, now_epoch + lease_seconds, now, row["id"], row["revision"]),
            )
            if cursor.rowcount != 1:
                return None
            claimed = connection.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone()
            self._append_event(connection, claimed)
            result = dict(claimed)
            result["request"] = json.loads(claimed["request_json"])
            return result

    def recover_expired(self) -> int:
        with self.immediate() as connection:
            return self._recover_expired_locked(connection)

    def complete_job(
        self,
        job_id: str,
        worker_id: str,
        fencing_token: int,
        result: dict[str, int],
    ) -> bool:
        with self.immediate() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if not self._owns_lease(row, worker_id, fencing_token):
                return False
            now = utc_now()
            encoded = canonical_json(result)
            connection.execute(
                """INSERT INTO side_effects (job_id, effect_key, result_json, committed_at)
                   VALUES (?, 'import-summary', ?, ?)
                   ON CONFLICT(job_id, effect_key) DO NOTHING""",
                (job_id, encoded, now),
            )
            stored = connection.execute(
                """SELECT result_json FROM side_effects
                   WHERE job_id = ? AND effect_key = 'import-summary'""",
                (job_id,),
            ).fetchone()
            if stored["result_json"] != encoded:
                raise RuntimeError("deduplication collision produced a different result")
            self._transition(connection, row, state="succeeded", result_json=encoded, terminal=True)
            return True

    def fail_attempt(
        self,
        job_id: str,
        worker_id: str,
        fencing_token: int,
        error_code: str,
        error_message: str,
        retry_base_seconds: float,
        retry_cap_seconds: float,
    ) -> str | None:
        with self.immediate() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if not self._owns_lease(row, worker_id, fencing_token):
                return None
            if row["attempt"] >= row["max_attempts"]:
                state = "failed"
                terminal = True
                next_run_at = row["next_run_at"]
            else:
                state = "retry_wait"
                terminal = False
                delay = min(retry_cap_seconds, retry_base_seconds * (2 ** (row["attempt"] - 1)))
                next_run_at = time.time() + delay
            self._transition(
                connection,
                row,
                state=state,
                error_code=error_code,
                error_message=error_message[:500],
                next_run_at=next_run_at,
                terminal=terminal,
            )
            return state

    def metrics(self, tenant_id: str) -> dict[str, Any]:
        with self.session() as connection:
            state_rows = connection.execute(
                """SELECT state, COUNT(*) AS count FROM jobs
                   WHERE tenant_id = ? GROUP BY state""",
                (tenant_id,),
            ).fetchall()
            events = connection.execute(
                "SELECT COUNT(*) FROM events WHERE tenant_id = ?", (tenant_id,)
            ).fetchone()[0]
            effects = connection.execute(
                """SELECT COUNT(*) FROM side_effects AS effects
                   JOIN jobs ON jobs.id = effects.job_id WHERE jobs.tenant_id = ?""",
                (tenant_id,),
            ).fetchone()[0]
        return {
            "states": {row["state"]: row["count"] for row in state_rows},
            "events_total": events,
            "side_effects_total": effects,
        }

    def side_effect_count(self, job_id: str | None = None) -> int:
        with self.session() as connection:
            if job_id:
                return connection.execute(
                    "SELECT COUNT(*) FROM side_effects WHERE job_id = ?", (job_id,)
                ).fetchone()[0]
            return connection.execute("SELECT COUNT(*) FROM side_effects").fetchone()[0]

    def _recover_expired_locked(self, connection: sqlite3.Connection) -> int:
        rows = connection.execute(
            "SELECT * FROM jobs WHERE state = 'running' AND lease_expires_at < ?",
            (time.time(),),
        ).fetchall()
        for row in rows:
            terminal = row["attempt"] >= row["max_attempts"]
            self._transition(
                connection,
                row,
                state="failed" if terminal else "retry_wait",
                error_code="lease_expired",
                error_message="worker lease expired before completion",
                next_run_at=time.time(),
                terminal=terminal,
            )
        return len(rows)

    @staticmethod
    def _owns_lease(row: sqlite3.Row | None, worker_id: str, fencing_token: int) -> bool:
        return bool(
            row
            and row["state"] == "running"
            and row["lease_owner"] == worker_id
            and row["fencing_token"] == fencing_token
            and row["lease_expires_at"] is not None
            and row["lease_expires_at"] > time.time()
        )

    def _transition(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        state: str,
        error_code: str | None = None,
        error_message: str | None = None,
        result_json: str | None = None,
        next_run_at: float | None = None,
        terminal: bool = False,
    ) -> None:
        if row["state"] in TERMINAL_STATES:
            raise TerminalStateConflict
        now = utc_now()
        completed_at = now if terminal else None
        connection.execute(
            """UPDATE jobs SET state = ?, revision = revision + 1, updated_at = ?,
                completed_at = ?, error_code = ?, error_message = ?,
                result_json = COALESCE(?, result_json), next_run_at = ?,
                lease_owner = NULL, lease_expires_at = NULL
               WHERE id = ? AND revision = ?""",
            (
                state,
                now,
                completed_at,
                error_code,
                error_message,
                result_json,
                row["next_run_at"] if next_run_at is None else next_run_at,
                row["id"],
                row["revision"],
            ),
        )
        updated = connection.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone()
        self._append_event(connection, updated)

    @staticmethod
    def _append_event(connection: sqlite3.Connection, job: sqlite3.Row) -> None:
        payload: dict[str, object] = {
            "state": job["state"],
            "kind": job["kind"],
            "attempt": job["attempt"],
            "record_count": job["record_count"],
        }
        if job["error_code"]:
            payload["error_code"] = job["error_code"]
        connection.execute(
            """INSERT INTO events (
                event_id, job_id, tenant_id, sequence, occurred_at, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?)""",
            (
                str(uuid.uuid4()),
                job["id"],
                job["tenant_id"],
                job["revision"],
                job["updated_at"],
                canonical_json(payload),
            ),
        )

    @staticmethod
    def _public_job(row: sqlite3.Row) -> dict[str, Any]:
        error = None
        if row["error_code"]:
            error = {"code": row["error_code"], "message": row["error_message"]}
        return {
            "id": row["id"],
            "kind": row["kind"],
            "state": row["state"],
            "attempt": row["attempt"],
            "max_attempts": row["max_attempts"],
            "record_count": row["record_count"],
            "revision": row["revision"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "completed_at": row["completed_at"],
            "error": error,
            "result": json.loads(row["result_json"]) if row["result_json"] else None,
        }

    @staticmethod
    def _event_envelope(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "event_id": row["event_id"],
            "source": "durable-workflows",
            "event_type": "job.state_changed",
            "occurred_at": row["occurred_at"],
            "job_id": row["job_id"],
            "tenant_id": row["tenant_id"],
            "sequence": row["sequence"],
            "payload": json.loads(row["payload_json"]),
        }
