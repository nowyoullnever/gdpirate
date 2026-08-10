from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from gdpirate.collectors.base import CandidateLink
from gdpirate.core.access_check import AccessChecker
from gdpirate.core.drive_urls import ParsedGoogleUrl, parse_google_url
from gdpirate.core.models import AccessStatus, DriveLink


@dataclass(frozen=True)
class IngestionResult:
    valid: bool
    created: bool = False
    duplicate: bool = False
    access_status: AccessStatus = AccessStatus.UNKNOWN
    canonical_url: str | None = None
    resource_id: str | None = None
    message: str | None = None


class IngestionService:
    def __init__(
        self,
        session: AsyncSession,
        access_checker: AccessChecker | None = None,
        *,
        check_access: bool = True,
    ) -> None:
        self.session = session
        self.access_checker = access_checker or AccessChecker()
        self.check_access = check_access

    async def ingest(self, candidate: CandidateLink) -> IngestionResult:
        parsed = parse_google_url(candidate.raw_url)
        if not parsed:
            return IngestionResult(valid=False, message="invalid_google_url")

        existing = await self._find_existing(parsed)
        created = existing is None
        link = existing or DriveLink(
            provider=parsed.provider,
            resource_id=parsed.resource_id,
            resource_type=parsed.resource_type,
            canonical_url=parsed.canonical_url,
            source_name=candidate.source_name,
            source_url=candidate.source_url,
        )

        if existing is None:
            self.session.add(link)
        elif not existing.source_url and candidate.source_url:
            existing.source_name = candidate.source_name
            existing.source_url = candidate.source_url

        if self.check_access:
            link.access_status = await self.access_checker.check(parsed.canonical_url)
            link.last_checked_at = datetime.now(UTC)

        await self.session.flush()
        return IngestionResult(
            valid=True,
            created=created,
            duplicate=not created,
            access_status=link.access_status,
            canonical_url=link.canonical_url,
            resource_id=link.resource_id,
        )

    async def _find_existing(self, parsed: ParsedGoogleUrl) -> DriveLink | None:
        result = await self.session.execute(
            select(DriveLink).where(
                DriveLink.provider == parsed.provider,
                DriveLink.resource_id == parsed.resource_id,
                DriveLink.resource_type == parsed.resource_type,
            )
        )
        return result.scalar_one_or_none()


async def count_drive_links(session: AsyncSession) -> int:
    result = await session.execute(select(func.count(DriveLink.id)))
    return int(result.scalar_one())
