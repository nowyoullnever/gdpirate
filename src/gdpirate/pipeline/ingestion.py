from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import logging
import asyncio
from enum import Enum

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from gdpirate.collectors.base import CandidateLink
from gdpirate.core.access_check import AccessChecker
from gdpirate.config import Settings, get_settings
from gdpirate.core.drive_urls import ParsedGoogleUrl, canonical_url, parse_google_url
from gdpirate.core.models import AccessStatus, DriveLink
from gdpirate.core.random_key import stable_random_key
from gdpirate.core.resource_types import (
    is_contradictory_type,
    should_upgrade_resource_type,
)
from gdpirate.core.source_quality import should_replace_source

logger = logging.getLogger(__name__)


class AccessCheckPolicy(str, Enum):
    IMMEDIATE = "IMMEDIATE"
    DEFERRED = "DEFERRED"


@dataclass(frozen=True)
class IngestionResult:
    valid: bool
    created: bool = False
    duplicate: bool = False
    access_status: AccessStatus = AccessStatus.UNKNOWN
    canonical_url: str | None = None
    provider: str | None = None
    resource_id: str | None = None
    message: str | None = None
    access_check_needed: bool = False


class IngestionService:
    _lock_stripes = [asyncio.Lock() for _ in range(1024)]

    def __init__(
        self,
        session: AsyncSession,
        access_checker: AccessChecker | None = None,
        settings: Settings | None = None,
        *,
        check_access: bool = True,
    ) -> None:
        self.session = session
        self.settings = settings or get_settings()
        self.access_checker = access_checker or AccessChecker()
        self.check_access = check_access

    async def ingest(self, candidate: CandidateLink) -> IngestionResult:
        return await self.ingest_with_policy(candidate, AccessCheckPolicy.IMMEDIATE)

    async def ingest_with_policy(
        self, candidate: CandidateLink, access_policy: AccessCheckPolicy
    ) -> IngestionResult:
        parsed = parse_google_url(candidate.raw_url)
        if not parsed:
            return IngestionResult(valid=False, message="invalid_google_url")

        async with self._lock_for(parsed.provider, parsed.resource_id):
            return await self._ingest_locked(candidate, parsed, access_policy)

    async def _ingest_locked(
        self,
        candidate: CandidateLink,
        parsed: ParsedGoogleUrl,
        access_policy: AccessCheckPolicy,
    ) -> IngestionResult:
        existing = await self._find_existing(parsed)
        created = existing is None
        link = existing or DriveLink(
            provider=parsed.provider,
            resource_id=parsed.resource_id,
            resource_type=parsed.resource_type,
            canonical_url=parsed.canonical_url,
            random_key=stable_random_key(parsed.provider, parsed.resource_id),
            source_name=candidate.source_name,
            source_url=candidate.source_url,
        )

        if existing is None:
            try:
                async with self.session.begin_nested():
                    self.session.add(link)
                    await self.session.flush()
            except IntegrityError:
                existing = await self._find_existing(parsed)
                if existing is None:
                    raise
                link = existing
                created = False
        if not created:
            self._merge_discovery(link, candidate, parsed)

        if (
            access_policy == AccessCheckPolicy.IMMEDIATE
            and self.check_access
            and self._should_check_access(link, created)
        ):
            link.access_status = await self.access_checker.check(link.canonical_url)
            link.last_checked_at = datetime.now(UTC)

        await self.session.flush()
        return IngestionResult(
            valid=True,
            created=created,
            duplicate=not created,
            access_status=link.access_status,
            canonical_url=link.canonical_url,
            provider=link.provider,
            resource_id=link.resource_id,
            access_check_needed=(
                access_policy == AccessCheckPolicy.IMMEDIATE
                and self._should_check_access(link, created)
            ),
        )

    @classmethod
    def lock_count(cls) -> int:
        return len(cls._lock_stripes)

    @classmethod
    def _lock_for(cls, provider: str, resource_id: str) -> asyncio.Lock:
        return cls._lock_stripes[hash((provider, resource_id)) % len(cls._lock_stripes)]

    async def update_access_status(
        self, provider: str, resource_id: str, status: AccessStatus
    ) -> None:
        result = await self.session.execute(
            select(DriveLink).where(
                DriveLink.provider == provider,
                DriveLink.resource_id == resource_id,
            )
        )
        link = result.scalar_one_or_none()
        if link is not None:
            link.access_status = status
            link.last_checked_at = datetime.now(UTC)
            await self.session.flush()

    def _should_check_access(self, link: DriveLink, created: bool) -> bool:
        if created or link.last_checked_at is None:
            return True
        checked_at = link.last_checked_at
        if checked_at.tzinfo is None:
            checked_at = checked_at.replace(tzinfo=UTC)
        return datetime.now(UTC) - checked_at >= timedelta(
            hours=self.settings.access_recheck_hours
        )

    def _merge_discovery(
        self, link: DriveLink, candidate: CandidateLink, parsed: ParsedGoogleUrl
    ) -> None:
        if should_replace_source(
            link.source_name,
            link.source_url,
            candidate.source_name,
            candidate.source_url,
        ):
            link.source_name = candidate.source_name
            link.source_url = candidate.source_url
        if should_upgrade_resource_type(link.resource_type, parsed.resource_type):
            link.resource_type = parsed.resource_type
            link.canonical_url = canonical_url(parsed.resource_type, parsed.resource_id)
        elif is_contradictory_type(link.resource_type, parsed.resource_type):
            logger.warning(
                "conflicting resource types for %s/%s: existing=%s discovered=%s",
                parsed.provider,
                parsed.resource_id,
                link.resource_type.value,
                parsed.resource_type.value,
            )

    async def _find_existing(self, parsed: ParsedGoogleUrl) -> DriveLink | None:
        result = await self.session.execute(
            select(DriveLink).where(
                DriveLink.provider == parsed.provider,
                DriveLink.resource_id == parsed.resource_id,
            )
        )
        return result.scalar_one_or_none()


async def count_drive_links(session: AsyncSession) -> int:
    result = await session.execute(select(func.count(DriveLink.id)))
    return int(result.scalar_one())


async def count_drive_links_by_status(session: AsyncSession) -> dict[AccessStatus, int]:
    result = await session.execute(
        select(DriveLink.access_status, func.count(DriveLink.id)).group_by(
            DriveLink.access_status
        )
    )
    counts = {status: 0 for status in AccessStatus}
    for status, count in result.all():
        counts[status] = int(count)
    return counts
