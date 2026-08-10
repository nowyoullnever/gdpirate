import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from gdpirate.config import Settings, get_settings
from gdpirate.core.access_check import AccessChecker
from gdpirate.core.database import SessionLocal
from gdpirate.core.http import HttpClientFactory
from gdpirate.core.models import AccessStatus, DriveLink
from gdpirate.pipeline.ingestion import IngestionService


@dataclass
class ValidationResult:
    selected: int = 0
    checked: int = 0
    public: int = 0
    restricted: int = 0
    dead: int = 0
    unknown: int = 0
    errors: int = 0


async def validate_links(
    *,
    max_items: int | None = None,
    concurrency: int | None = None,
    status: AccessStatus = AccessStatus.UNKNOWN,
    source: str | None = None,
    stale_only: bool = False,
    settings: Settings | None = None,
    session_factory: async_sessionmaker | None = None,
) -> ValidationResult:
    settings = settings or get_settings()
    session_factory = session_factory or SessionLocal
    concurrency = max(1, concurrency or settings.http_max_concurrency)
    result = ValidationResult()
    queue: asyncio.Queue[tuple[int, str, str, str] | None] = asyncio.Queue(
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
) -> list[tuple[int, str, str, str]]:
    stmt = select(
        DriveLink.id, DriveLink.provider, DriveLink.resource_id, DriveLink.canonical_url
    ).where(DriveLink.access_status == status, DriveLink.id > last_id)
    if source:
        stmt = stmt.where(DriveLink.source_name == source)
    if stale_only:
        cutoff = datetime.now(UTC) - timedelta(hours=settings.access_recheck_hours)
        stmt = stmt.where(DriveLink.last_checked_at.is_not(None)).where(
            DriveLink.last_checked_at < cutoff
        )
    elif status == AccessStatus.UNKNOWN:
        stmt = stmt.where(DriveLink.last_checked_at.is_(None))
    stmt = stmt.order_by(DriveLink.id)
    if limit is not None:
        stmt = stmt.limit(limit)
    rows = await session.execute(stmt)
    return [
        (row_id, provider, resource_id, canonical_url)
        for row_id, provider, resource_id, canonical_url in rows
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
        _row_id, provider, resource_id, canonical_url = item
        try:
            status = await AccessChecker(settings, client).check(canonical_url)
            async with session_factory() as session:
                async with session.begin():
                    await IngestionService(
                        session, settings=settings, check_access=False
                    ).update_access_status(provider, resource_id, status)
            result.checked += 1
            setattr(result, status.value.lower(), getattr(result, status.value.lower()) + 1)
        except Exception:
            result.errors += 1
        finally:
            queue.task_done()
