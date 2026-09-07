import re
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Response, status
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles

from durable_workflows import __version__
from durable_workflows.config import Settings
from durable_workflows.db import (
    Database,
    IdempotencyConflict,
    JobNotFound,
    TerminalStateConflict,
)
from durable_workflows.logging_config import configure_logging
from durable_workflows.models import EventEnvelope, JobCreate, JobCreateResponse, JobView
from durable_workflows.worker import run_worker_threads

IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")
security = HTTPBearer(auto_error=False)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    settings.validate()
    database = Database(settings.db_path)
    configure_logging()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        database.initialize()
        stop = None
        threads = []
        if settings.start_workers and settings.worker_count:
            stop, threads = run_worker_threads(database, settings)
        application.state.worker_threads = threads
        yield
        if stop:
            stop.set()
            for thread in threads:
                thread.join(timeout=1)

    application = FastAPI(
        title="Durable Workflows",
        version=__version__,
        description="Owner-scoped, durable data import requests with restart recovery.",
        lifespan=lifespan,
    )
    application.state.settings = settings
    application.state.database = database

    static_dir = Path(__file__).resolve().parents[2] / "static"
    if not static_dir.exists():
        static_dir = Path(__file__).resolve().parent / "static"
    application.mount("/static", StaticFiles(directory=static_dir), name="static")

    def principal(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
    ) -> str:
        if not credentials or credentials.scheme.lower() != "bearer":
            raise HTTPException(status_code=401, detail="valid bearer credential required")
        for token, tenant_id in settings.principals.items():
            if secrets.compare_digest(credentials.credentials, token):
                return tenant_id
        raise HTTPException(status_code=401, detail="valid bearer credential required")

    @application.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    @application.get("/health")
    def health() -> dict[str, str]:
        try:
            if not database.path.is_file():
                raise RuntimeError("database file missing")
            with database.session() as connection:
                connection.execute("PRAGMA busy_timeout = 100")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for table in ("jobs", "idempotency_keys", "events", "side_effects"):
                        connection.execute(f"SELECT * FROM {table} LIMIT 0")
                finally:
                    connection.rollback()
            if settings.start_workers and settings.worker_count:
                threads = getattr(application.state, "worker_threads", [])
                if len(threads) != settings.worker_count or not all(t.is_alive() for t in threads):
                    raise RuntimeError("embedded worker unavailable")
        except Exception as exc:
            raise HTTPException(status_code=503, detail="database unavailable") from exc
        return {"status": "ok", "service": "durable-workflows", "version": __version__}

    @application.post(
        "/jobs",
        response_model=JobCreateResponse,
        status_code=status.HTTP_201_CREATED,
    )
    def create_job(
        payload: JobCreate,
        response: Response,
        owner: Annotated[str, Depends(principal)],
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ) -> dict[str, object]:
        if not idempotency_key or not IDEMPOTENCY_KEY.fullmatch(idempotency_key):
            raise HTTPException(
                status_code=422,
                detail="Idempotency-Key must be 8-128 safe ASCII characters",
            )
        try:
            job, created = database.create_job(
                owner, payload.model_dump(), idempotency_key, settings.max_attempts
            )
        except IdempotencyConflict as exc:
            raise HTTPException(
                status_code=409,
                detail="Idempotency-Key was already used with a different payload",
            ) from exc
        response.status_code = 201 if created else 200
        return {**job, "idempotent_replay": not created}

    @application.get("/jobs", response_model=list[JobView])
    def list_jobs(owner: Annotated[str, Depends(principal)]) -> list[dict[str, object]]:
        return database.list_jobs(owner)

    @application.get("/jobs/{job_id}", response_model=JobView)
    def get_job(job_id: str, owner: Annotated[str, Depends(principal)]) -> dict[str, object]:
        try:
            return database.get_job(owner, job_id)
        except JobNotFound as exc:
            raise HTTPException(status_code=404, detail="job not found") from exc

    @application.post("/jobs/{job_id}/cancel", response_model=JobView)
    def cancel_job(job_id: str, owner: Annotated[str, Depends(principal)]) -> dict[str, object]:
        try:
            return database.cancel_job(owner, job_id)
        except JobNotFound as exc:
            raise HTTPException(status_code=404, detail="job not found") from exc
        except TerminalStateConflict as exc:
            raise HTTPException(status_code=409, detail="terminal job state is immutable") from exc

    @application.get("/jobs/{job_id}/events", response_model=list[EventEnvelope])
    def job_events(
        job_id: str, owner: Annotated[str, Depends(principal)]
    ) -> list[dict[str, object]]:
        try:
            return database.job_events(owner, job_id)
        except JobNotFound as exc:
            raise HTTPException(status_code=404, detail="job not found") from exc

    @application.get("/events", response_model=list[EventEnvelope])
    def export_events(owner: Annotated[str, Depends(principal)]) -> list[dict[str, object]]:
        return database.export_events(owner)

    @application.get("/metrics", response_class=PlainTextResponse)
    def metrics(owner: Annotated[str, Depends(principal)]) -> str:
        values = database.metrics(owner)
        lines = [
            "# HELP durable_workflows_jobs Current jobs by state.",
            "# TYPE durable_workflows_jobs gauge",
        ]
        for state_name in ("queued", "running", "retry_wait", "succeeded", "failed", "cancelled"):
            count = values["states"].get(state_name, 0)
            lines.append(f'durable_workflows_jobs{{state="{state_name}"}} {count}')
        lines.extend(
            [
                "# HELP durable_workflows_events_total Immutable state events written.",
                "# TYPE durable_workflows_events_total counter",
                f"durable_workflows_events_total {values['events_total']}",
                "# HELP durable_workflows_side_effects_total "
                "Deduplicated import summaries committed.",
                "# TYPE durable_workflows_side_effects_total counter",
                f"durable_workflows_side_effects_total {values['side_effects_total']}",
            ]
        )
        return "\n".join(lines) + "\n"

    return application


app = create_app()
