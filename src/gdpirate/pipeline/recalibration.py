from dataclasses import dataclass
import asyncio
import time

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from gdpirate.config import Settings, get_settings
from gdpirate.core.access_check import ACCESS_CHECK_VERSION, AccessChecker
from gdpirate.core.database import SessionLocal
from gdpirate.core.http import HttpClientFactory
from gdpirate.core.models import DriveLink
from gdpirate.pipeline.ingestion import IngestionService


@dataclass
class RecalibrationResult:
    selected: int = 0
    checked: int = 0
    errors: int = 0
    transitions: dict | None = None
    statuses: dict | None = None
    duration_ms: int = 0


async def recalibrate_access(
    *,
    max_items: int | None = None,
    concurrency: int | None = None,
    settings: Settings | None = None,
    session_factory: async_sessionmaker | None = None,
) -> RecalibrationResult:
    settings = settings or get_settings()
    session_factory = session_factory or SessionLocal
    concurrency = max(1, concurrency or settings.http_max_concurrency)
    result = RecalibrationResult(transitions={}, statuses={})
    started = time.monotonic()
    queue: asyncio.Queue[tuple[int, str, str, str, str] | None] = asyncio.Queue(
        maxsize=concurrency
    )

    factory = HttpClientFactory(settings)
    async with factory.client() as client:
        workers = [
            asyncio.create_task(
                _worker(queue, client, result, settings, session_factory)
            )
            for _ in range(concurrency)
        ]
        last_id = 0
        remaining = max_items
        while remaining is None or remaining > 0:
            limit = settings.validation_db_batch_size
            if remaining is not None:
                limit = min(limit, remaining)
            rows = await _select_rows(session_factory, last_id=last_id, limit=limit)
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
    result.duration_ms = int((time.monotonic() - started) * 1000)
    return result


async def _select_rows(session_factory, *, last_id: int, limit: int):
    async with session_factory() as session:
        rows = await session.execute(
            select(
                DriveLink.id,
                DriveLink.provider,
                DriveLink.resource_id,
                DriveLink.canonical_url,
                DriveLink.access_status,
            )
            .where(DriveLink.id > last_id)
            .where(DriveLink.last_checked_at.is_not(None))
            .where(
                (DriveLink.access_check_version.is_(None))
                | (DriveLink.access_check_version < ACCESS_CHECK_VERSION)
            )
            .order_by(DriveLink.id)
            .limit(limit)
        )
        return list(rows)


async def _worker(queue, client, result, settings, session_factory) -> None:
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            return
        _row_id, provider, resource_id, canonical_url, old_status = item
        try:
            check_result = await AccessChecker(settings, client).check_detailed(canonical_url)
            async with session_factory() as session:
                async with session.begin():
                    await IngestionService(
                        session, settings=settings, check_access=False
                    ).update_access_status(provider, resource_id, check_result)
            result.checked += 1
            result.statuses[check_result.status.value] = (
                result.statuses.get(check_result.status.value, 0) + 1
            )
            transition = f"{old_status.value}->{check_result.status.value}"
            result.transitions[transition] = result.transitions.get(transition, 0) + 1
        except Exception:
            result.errors += 1
        finally:
            queue.task_done()
