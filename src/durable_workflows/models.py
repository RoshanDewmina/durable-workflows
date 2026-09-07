from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class ImportRecord(StrictModel):
    value: Annotated[StrictInt, Field(ge=-1_000_000, le=1_000_000)]


class JobCreate(StrictModel):
    kind: Literal["data_import"]
    records: Annotated[list[ImportRecord], Field(min_length=1, max_length=1_000)]


class JobView(BaseModel):
    id: str
    kind: str
    state: str
    attempt: int
    max_attempts: int
    record_count: int
    revision: int
    created_at: str
    updated_at: str
    completed_at: str | None
    error: dict[str, str] | None
    result: dict[str, int] | None


class JobCreateResponse(JobView):
    idempotent_replay: bool


class EventEnvelope(BaseModel):
    schema_version: int
    event_id: str
    source: str
    event_type: str
    occurred_at: str
    job_id: str
    tenant_id: str
    sequence: int
    payload: dict[str, object]
