import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import time

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from gdpirate.config import Settings, get_settings
from gdpirate.core.access_check import AccessChecker
from gdpirate.core.database import SessionLocal
from gdpirate.core.http import HttpClientFactory
from gdpirate.core.models import AccessStatus, DriveLink
from gdpirate.pipeline.ingestion import IngestionService
from gdpirate.pipeline.metrics import MetricContext, record_validation_metric


@dataclass
class ValidationResult:
    selected: int = 0
    checked: int = 0
    public: int = 0
    restricted: int = 0
    dead: int = 0
    unknown: int = 0
    errors: int = 0
    duration_ms: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    by_source: dict | None = None
    reason_counts: dict | None = None
    transition_counts: dict | None = None


async def validate_links(
    *,
    max_items: int | None = None,
    concurrency: int | None = None,
    status: AccessStatus = AccessStatus.UNKNOWN,
    source: str | None = None,
    stale_only: bool = False,
    settings: Settings | None = None,
    session_factory: async_sessionmaker | None = None,
    metric_context: MetricContext | None = None,
) -> ValidationResult:
    settings = settings or get_settings()
    session_factory = session_factory or SessionLocal
    concurrency = max(1, concurrency or settings.http_max_concurrency)
    result = ValidationResult()
    result.started_at = datetime.now(UTC)
    result.by_source = {}
    result.reason_counts = {}
    result.transition_counts = {}
    monotonic_started = time.monotonic()
    queue: asyncio.Queue[tuple[int, str, str, str, str, str] | None] = asyncio.Queue(
        maxsize=concurrency
    )

    factory = HttpClientFactory(settings)
    async with factory.client() as client:
        workers = [
            asyncio.create_task(
                _validation_worker(queue, client, result, settings, session_factory)
            )
            for _ in range(concurrency)
        ]
        last_id = 0
        remaining = max_items
        while remaining is None or remaining > 0:
            limit = settings.validation_db_batch_size
            if remaining is not None:
                limit = min(limit, remaining)
            async with session_factory() as session:
                rows = await _select_validation_rows(
                    session,
                    status=status,
                    source=source,
                    stale_only=stale_only,
                    settings=settings,
                    limit=limit,
                    last_id=last_id,
                )
            if not rows:
                break
            for row in rows:
                last_id = row[0]
                result.selected += 1
                if remaining is not None:
                    remaining -= 1
                await queue.put(row)
        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers)
    result.finished_at = datetime.now(UTC)
    result.duration_ms = int((time.monotonic() - monotonic_started) * 1000)
    await record_validation_metric(
        result,
        metric_context=metric_context,
        requested_status=status,
        source_filter=source,
        stale_only=stale_only,
        session_factory=session_factory,
    )
    return result


async def _select_validation_rows(
    session,
    *,
    status: AccessStatus,
    source: str | None,
    stale_only: bool,
    settings: Settings,
    limit: int | None,
    last_id: int,
) -> list[tuple[int, str, str, str, str]]:
    stmt = select(
        DriveLink.id,
        DriveLink.provider,
        DriveLink.resource_id,
        DriveLink.canonical_url,
        DriveLink.source_name,
        DriveLink.access_status,
    ).where(DriveLink.access_status == status, DriveLink.id > last_id)
    if source:
        stmt = stmt.where(DriveLink.source_name == source)
    if stale_only:
        recheck_hours = (
            settings.unknown_recheck_hours
            if status == AccessStatus.UNKNOWN
            else settings.access_recheck_hours
        )
        cutoff = datetime.now(UTC) - timedelta(hours=recheck_hours)
        stmt = stmt.where(DriveLink.last_checked_at.is_not(None)).where(
            DriveLink.last_checked_at < cutoff
        )
    elif status == AccessStatus.UNKNOWN:
        cutoff = datetime.now(UTC) - timedelta(hours=settings.unknown_recheck_hours)
        stmt = stmt.where(
            (DriveLink.last_checked_at.is_(None)) | (DriveLink.last_checked_at < cutoff)
        )
    stmt = stmt.order_by(DriveLink.id)
    if limit is not None:
        stmt = stmt.limit(limit)
    rows = await session.execute(stmt)
    return [
        (row_id, provider, resource_id, canonical_url, source_name, old_status)
        for row_id, provider, resource_id, canonical_url, source_name, old_status in rows
    ]


async def _validation_worker(
    queue,
    client,
    result: ValidationResult,
    settings: Settings,
    session_factory,
) -> None:
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            return
        _row_id, provider, resource_id, canonical_url, source_name, old_status = item
        source_counts = result.by_source.setdefault(
            source_name,
            {"selected": 0, "checked": 0, "public": 0, "restricted": 0, "dead": 0, "unknown": 0, "errors": 0},
        )
        source_counts["selected"] += 1
        try:
            check_result = await AccessChecker(settings, client).check_detailed(canonical_url)
            async with session_factory() as session:
                async with session.begin():
                    await IngestionService(
                        session, settings=settings, check_access=False
                    ).update_access_status(provider, resource_id, check_result)
            result.checked += 1
            source_counts["checked"] += 1
            status = check_result.status
            setattr(result, status.value.lower(), getattr(result, status.value.lower()) + 1)
            source_counts[status.value.lower()] += 1
            source_counts.setdefault("reasons", {})
            source_counts["reasons"][check_result.reason] = (
                source_counts["reasons"].get(check_result.reason, 0) + 1
            )
            result.reason_counts[check_result.reason] = (
                result.reason_counts.get(check_result.reason, 0) + 1
            )
            transition = f"{old_status.value}->{status.value}"
            result.transition_counts[transition] = (
                result.transition_counts.get(transition, 0) + 1
            )
        except Exception:
            result.errors += 1
            source_counts["errors"] += 1
        finally:
            queue.task_done()
