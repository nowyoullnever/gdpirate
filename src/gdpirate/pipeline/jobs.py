from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
import tomllib

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from gdpirate.config import Settings, get_settings
from gdpirate.core.database import SessionLocal
from gdpirate.core.models import AccessStatus, ScheduledJobState, utc_now
from gdpirate.pipeline.collection import CollectionRunner, CollectionStateMode
from gdpirate.pipeline.validation import validate_links


@dataclass(frozen=True)
class JobConfig:
    name: str
    kind: str
    enabled: bool
    interval_seconds: int
    sources: list[str] = field(default_factory=list)
    state_mode: str = "persistent"
    max_items_per_source: int | None = None
    status: str = "UNKNOWN"
    stale_only: bool = False
    max_items: int | None = None
    concurrency: int | None = None


def load_jobs_config(path: str) -> list[JobConfig]:
    config_path = Path(path)
    if not config_path.exists():
        return []
    with config_path.open("rb") as handle:
        payload = tomllib.load(handle)
    jobs = [_parse_job(item) for item in payload.get("jobs", [])]
    names = [job.name for job in jobs]
    if len(names) != len(set(names)):
        raise ValueError("duplicate job names in jobs config")
    return jobs


def enabled_jobs(settings: Settings | None = None) -> list[JobConfig]:
    settings = settings or get_settings()
    return [job for job in load_jobs_config(settings.jobs_config_path) if job.enabled]


async def get_job_state(session, job_name: str) -> ScheduledJobState | None:
    return (
        await session.execute(
            select(ScheduledJobState).where(ScheduledJobState.job_name == job_name)
        )
    ).scalar_one_or_none()


async def ensure_job_state(session, job: JobConfig) -> ScheduledJobState:
    state = await get_job_state(session, job.name)
    if state is None:
        state = ScheduledJobState(
            job_name=job.name,
            next_run_at=utc_now(),
            consecutive_failures=0,
            updated_at=utc_now(),
        )
        session.add(state)
        await session.flush()
    return state


def is_due(state: ScheduledJobState, now: datetime | None = None) -> bool:
    now = now or utc_now()
    if state.next_run_at is None:
        return True
    next_run = state.next_run_at
    if next_run.tzinfo is None:
        next_run = next_run.replace(tzinfo=UTC)
    return next_run <= now


async def record_job_started(session, job_name: str) -> None:
    state = await get_job_state(session, job_name)
    if state is None:
        raise ValueError(f"unknown job state: {job_name}")
    state.last_started_at = utc_now()
    state.updated_at = utc_now()
    await session.flush()


async def record_job_success(session, job: JobConfig, result: dict) -> None:
    state = await get_job_state(session, job.name)
    if state is None:
        raise ValueError(f"unknown job state: {job.name}")
    now = utc_now()
    state.last_finished_at = now
    state.last_success_at = now
    state.next_run_at = now + timedelta(seconds=job.interval_seconds)
    state.consecutive_failures = 0
    state.last_error = None
    state.last_result_json = _bounded_result(result)
    state.updated_at = now
    await session.flush()


async def record_job_failure(
    session,
    job: JobConfig,
    error: Exception | str,
    *,
    settings: Settings | None = None,
) -> int:
    settings = settings or get_settings()
    state = await get_job_state(session, job.name)
    if state is None:
        raise ValueError(f"unknown job state: {job.name}")
    now = utc_now()
    failures = int(state.consecutive_failures or 0) + 1
    backoff = min(
        settings.worker_failure_base_backoff_seconds * (2 ** (failures - 1)),
        settings.worker_failure_max_backoff_seconds,
    )
    state.last_finished_at = now
    state.next_run_at = now + timedelta(seconds=backoff)
    state.consecutive_failures = failures
    state.last_error = f"{type(error).__name__}: {error}"[:4000]
    state.updated_at = now
    await session.flush()
    return backoff


class JobExecutor:
    def __init__(
        self,
        *,
        settings: Settings | None = None,
        session_factory: async_sessionmaker | None = None,
        collection_runner: CollectionRunner | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.session_factory = session_factory or SessionLocal
        self.collection_runner = collection_runner or CollectionRunner(
            self.settings, self.session_factory
        )

    async def execute(self, job: JobConfig) -> dict:
        if job.kind == "collect":
            return await self._collect(job)
        if job.kind == "validate":
            return await self._validate(job)
        raise ValueError(f"unknown job kind: {job.kind}")

    async def _collect(self, job: JobConfig) -> dict:
        summary = {
            "sources": 0,
            "scanned": 0,
            "candidates": 0,
            "created": 0,
            "duplicates": 0,
            "public": 0,
            "restricted": 0,
            "dead": 0,
            "unknown": 0,
            "errors": 0,
        }
        for source in job.sources:
            try:
                results = await self.collection_runner.collect(
                    source,
                    max_items=job.max_items_per_source,
                    state_mode=job.state_mode,
                )
            except Exception:
                summary["errors"] += 1
                continue
            for result in results:
                summary["sources"] += 1
                for key in ("scanned", "candidates", "created", "duplicates", "public", "restricted", "dead", "unknown"):
                    summary[key] += int(getattr(result, key))
                if result.error:
                    summary["errors"] += 1
        return summary

    async def _validate(self, job: JobConfig) -> dict:
        result = await validate_links(
            max_items=job.max_items,
            concurrency=job.concurrency,
            status=AccessStatus[job.status.upper()],
            stale_only=job.stale_only,
            settings=self.settings,
            session_factory=self.session_factory,
        )
        return result.__dict__.copy()


def _parse_job(item: dict) -> JobConfig:
    kind = str(item.get("kind") or "")
    if kind not in {"collect", "validate"}:
        raise ValueError(f"unknown job kind: {kind}")
    state_mode = str(item.get("state_mode") or "persistent")
    CollectionStateMode(state_mode)
    return JobConfig(
        name=str(item["name"]),
        kind=kind,
        enabled=bool(item.get("enabled", True)),
        interval_seconds=int(item.get("interval_seconds", 300)),
        sources=[str(source) for source in item.get("sources", [])],
        state_mode=state_mode,
        max_items_per_source=item.get("max_items_per_source"),
        status=str(item.get("status", "UNKNOWN")),
        stale_only=bool(item.get("stale_only", False)),
        max_items=item.get("max_items"),
        concurrency=item.get("concurrency"),
    )


def _bounded_result(result: dict) -> dict:
    return {str(key): value for key, value in result.items() if isinstance(value, int | str | bool | float | type(None))}
