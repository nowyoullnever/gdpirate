from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
import tomllib

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker

from gdpirate.config import Settings, get_settings
from gdpirate.core.database import SessionLocal
from gdpirate.core.models import AccessStatus, ScheduledJobState, utc_now
from gdpirate.pipeline.collection import (
    FRESH_HEAD_SOURCES,
    CollectionRunner,
    CollectionStateMode,
    build_collectors,
)
from gdpirate.pipeline.metrics import MetricContext
from gdpirate.pipeline.validation import validate_links


class TotalCollectionFailure(RuntimeError):
    def __init__(self, failed_sources: list[str], summary: dict) -> None:
        self.failed_sources = failed_sources
        self.summary = summary
        super().__init__(
            f"all configured sources failed: {', '.join(failed_sources[:20])}"
        )


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
    keep_days: int | None = None


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
        try:
            async with session.begin_nested():
                session.add(state)
                await session.flush()
        except IntegrityError:
            state = await get_job_state(session, job.name)
            if state is None:
                raise
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

    async def execute(
        self, job: JobConfig, *, metric_context: MetricContext | None = None
    ) -> dict:
        if job.kind == "collect":
            return await self._collect(job, metric_context=metric_context)
        if job.kind == "validate":
            return await self._validate(job, metric_context=metric_context)
        if job.kind == "prune_metrics":
            from gdpirate.pipeline.metrics import prune_metrics

            keep_days = job.keep_days or self.settings.metrics_retention_days
            return await prune_metrics(
                keep_days=keep_days, session_factory=self.session_factory
            )
        raise ValueError(f"unknown job kind: {job.kind}")

    async def _collect(
        self, job: JobConfig, *, metric_context: MetricContext | None = None
    ) -> dict:
        summary = {
            "sources": 0,
            "successful_sources": 0,
            "failed_sources": 0,
            "scanned": 0,
            "candidates": 0,
            "created": 0,
            "duplicates": 0,
            "access_checks": 0,
            "public": 0,
            "restricted": 0,
            "dead": 0,
            "unknown": 0,
            "errors": 0,
        }
        failed_source_names = []
        for source in job.sources:
            try:
                results = await self.collection_runner.collect(
                    source,
                    max_items=job.max_items_per_source,
                    state_mode=job.state_mode,
                    metric_context=metric_context,
                )
            except Exception:
                summary["errors"] += 1
                summary["failed_sources"] += 1
                failed_source_names.append(source)
                continue
            for result in results:
                summary["sources"] += 1
                for key in (
                    "scanned",
                    "candidates",
                    "created",
                    "duplicates",
                    "access_checks",
                    "public",
                    "restricted",
                    "dead",
                    "unknown",
                ):
                    summary[key] += int(getattr(result, key, 0))
                if result.error or result.unavailable:
                    summary["errors"] += 1
                    summary["failed_sources"] += 1
                    failed_source_names.append(result.source)
                else:
                    summary["successful_sources"] += 1
        if summary["successful_sources"] == 0 and summary["failed_sources"] > 0:
            raise TotalCollectionFailure(failed_source_names, summary)
        return summary

    async def _validate(
        self, job: JobConfig, *, metric_context: MetricContext | None = None
    ) -> dict:
        result = await validate_links(
            max_items=job.max_items,
            concurrency=job.concurrency,
            status=AccessStatus[job.status.upper()],
            stale_only=job.stale_only,
            settings=self.settings,
            session_factory=self.session_factory,
            metric_context=metric_context,
        )
        return {
            key: value
            for key, value in result.__dict__.items()
            if key not in {"started_at", "finished_at", "by_source"}
        }


def _parse_job(item: dict) -> JobConfig:
    kind = str(item.get("kind") or "")
    if kind not in {"collect", "validate", "prune_metrics"}:
        raise ValueError(f"unknown job kind: {kind}")
    name = str(item.get("name") or "").strip()
    if not name:
        raise ValueError("job name must not be empty")
    interval_seconds = int(item.get("interval_seconds", 300))
    if interval_seconds <= 0:
        raise ValueError(f"job {name} interval_seconds must be positive")
    state_mode = str(item.get("state_mode") or "persistent")
    CollectionStateMode(state_mode)
    sources = [str(source) for source in item.get("sources", [])]
    max_items_per_source = item.get("max_items_per_source")
    status = str(item.get("status", "UNKNOWN"))
    max_items = item.get("max_items")
    concurrency = item.get("concurrency")
    keep_days = item.get("keep_days")

    if kind == "collect":
        _validate_collect_job(name, sources, state_mode, max_items_per_source)
    elif kind == "validate":
        _validate_validate_job(name, status, max_items, concurrency)
    else:
        if keep_days is not None and int(keep_days) <= 0:
            raise ValueError(f"job {name} keep_days must be positive")

    return JobConfig(
        name=name,
        kind=kind,
        enabled=bool(item.get("enabled", True)),
        interval_seconds=interval_seconds,
        sources=sources,
        state_mode=state_mode,
        max_items_per_source=max_items_per_source,
        status=status,
        stale_only=bool(item.get("stale_only", False)),
        max_items=max_items,
        concurrency=concurrency,
        keep_days=keep_days,
    )


def _bounded_result(result: dict) -> dict:
    return {str(key): value for key, value in result.items() if isinstance(value, int | str | bool | float | type(None))}


def _validate_collect_job(
    name: str, sources: list[str], state_mode: str, max_items_per_source: int | None
) -> None:
    if not sources:
        raise ValueError(f"job {name} collect sources must not be empty")
    if len(sources) != len(set(sources)):
        raise ValueError(f"job {name} collect sources must be unique")
    known_sources = set(build_collectors().keys())
    unknown = sorted(set(sources) - known_sources)
    if unknown:
        raise ValueError(f"job {name} unknown collectors: {', '.join(unknown)}")
    if state_mode == CollectionStateMode.FRESH_HEAD and any(
        source not in FRESH_HEAD_SOURCES for source in sources
    ):
        raise ValueError(f"job {name} contains non-fresh-head source")
    if max_items_per_source is not None and int(max_items_per_source) <= 0:
        raise ValueError(f"job {name} max_items_per_source must be positive")


def _validate_validate_job(
    name: str,
    status: str,
    max_items: int | None,
    concurrency: int | None,
) -> None:
    try:
        AccessStatus[status.upper()]
    except KeyError as exc:
        raise ValueError(f"job {name} invalid validation status: {status}") from exc
    if max_items is not None and int(max_items) <= 0:
        raise ValueError(f"job {name} max_items must be positive")
    if concurrency is not None and int(concurrency) <= 0:
        raise ValueError(f"job {name} concurrency must be positive")
