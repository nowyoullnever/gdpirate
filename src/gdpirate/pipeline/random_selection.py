from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import random

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from gdpirate.config import Settings, get_settings
from gdpirate.core.access_check import AccessChecker
from gdpirate.core.database import SessionLocal
from gdpirate.core.models import AccessStatus, DriveLink, utc_now


@dataclass(frozen=True)
class RandomLink:
    url: str
    source_name: str
    source_url: str


class RandomLinkService:
    def __init__(
        self,
        *,
        settings: Settings | None = None,
        session_factory: async_sessionmaker | None = None,
        access_checker: AccessChecker | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.session_factory = session_factory or SessionLocal
        self.access_checker = access_checker or AccessChecker(self.settings)

    async def pick(self, *, attempts: int = 20) -> RandomLink | None:
        stale_before = utc_now() - timedelta(hours=self.settings.access_recheck_hours)
        excluded_ids: set[int] = set()
        for _attempt in range(attempts):
            threshold = random.random()
            row = await self._select_candidate(threshold, excluded_ids)
            if row is None:
                return None
            excluded_ids.add(row.id)
            if not _is_stale(row.last_checked_at, stale_before):
                return _random_link(row)

            status = await self.access_checker.check(row.canonical_url)
            fresh_row = await self._update_status(row.id, status)
            if fresh_row is not None and fresh_row.access_status == AccessStatus.PUBLIC:
                return _random_link(fresh_row)
        return None

    async def _select_candidate(
        self, threshold: float, excluded_ids: set[int]
    ) -> DriveLink | None:
        async with self.session_factory() as session:
            base = _public_source_query().where(~DriveLink.id.in_(excluded_ids))
            row = (
                await session.execute(
                    base.where(DriveLink.random_key >= threshold)
                    .order_by(DriveLink.random_key)
                    .limit(1)
                )
            ).scalar_one_or_none()
            if row is not None:
                return row
            return (
                await session.execute(
                    base.order_by(DriveLink.random_key).limit(1)
                )
            ).scalar_one_or_none()

    async def _update_status(
        self, link_id: int, status: AccessStatus
    ) -> DriveLink | None:
        async with self.session_factory() as session:
            async with session.begin():
                row = await session.get(DriveLink, link_id)
                if row is None:
                    return None
                row.access_status = status
                row.last_checked_at = utc_now()
                row.updated_at = utc_now()
            return row


def _public_source_query() -> Select[tuple[DriveLink]]:
    return select(DriveLink).where(
        DriveLink.access_status == AccessStatus.PUBLIC,
        DriveLink.source_url.is_not(None),
    )


def _is_stale(checked_at: datetime | None, stale_before: datetime) -> bool:
    if checked_at is None:
        return True
    if checked_at.tzinfo is None:
        checked_at = checked_at.replace(tzinfo=UTC)
    return checked_at < stale_before


def _random_link(row: DriveLink) -> RandomLink:
    return RandomLink(
        url=row.canonical_url,
        source_name=row.source_name,
        source_url=str(row.source_url),
    )
