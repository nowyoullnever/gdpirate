import pytest
from datetime import UTC, datetime, timedelta
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from gdpirate.collectors.base import CandidateLink
from gdpirate.config import Settings
from gdpirate.core.access_check import AccessChecker
from gdpirate.core.models import AccessStatus, Base, DriveLink
from gdpirate.pipeline.ingestion import IngestionService, count_drive_links


class StaticAccessChecker(AccessChecker):
    def __init__(self, status: AccessStatus = AccessStatus.PUBLIC) -> None:
        self.status = status
        self.calls = 0

    async def check(self, raw_url: str) -> AccessStatus:
        self.calls += 1
        return self.status


@pytest.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        async with session.begin():
            yield session
    await engine.dispose()


async def test_ingestion_stores_source_and_access_status(session):
    service = IngestionService(session, StaticAccessChecker(AccessStatus.PUBLIC))

    result = await service.ingest(
        CandidateLink(
            raw_url="https://drive.google.com/file/d/ABC123/view",
            source_name="manual",
            source_url="https://example.com/post",
        )
    )

    assert result.valid is True
    assert result.created is True
    assert result.access_status == AccessStatus.PUBLIC
    links = (await session.execute(select(DriveLink))).scalars().all()
    assert len(links) == 1
    assert links[0].source_name == "manual"
    assert links[0].source_url == "https://example.com/post"


async def test_duplicate_representations_do_not_create_history_or_rows(session):
    service = IngestionService(session, StaticAccessChecker(AccessStatus.RESTRICTED))

    first = await service.ingest(
        CandidateLink(
            raw_url="https://drive.google.com/file/d/ABC123/view",
            source_name="first",
            source_url="https://example.com/first",
        )
    )
    second = await service.ingest(
        CandidateLink(
            raw_url="https://drive.google.com/open?id=ABC123",
            source_name="second",
            source_url="https://example.com/second",
        )
    )

    assert first.created is True
    assert second.duplicate is True
    assert await count_drive_links(session) == 1
    link = (await session.execute(select(DriveLink))).scalar_one()
    assert link.source_name == "first"
    assert link.source_url == "https://example.com/first"


async def test_same_id_with_different_resource_types_deduplicates_and_upgrades(session):
    checker = StaticAccessChecker()
    service = IngestionService(session, checker, check_access=False)

    await service.ingest(
        CandidateLink(
            raw_url="https://drive.google.com/open?id=ABC123",
            source_name="manual",
        )
    )
    result = await service.ingest(
        CandidateLink(
            raw_url="https://docs.google.com/document/d/ABC123/edit",
            source_name="manual",
        )
    )

    assert result.duplicate is True
    assert await count_drive_links(session) == 1
    link = (await session.execute(select(DriveLink))).scalar_one()
    assert link.resource_type.value == "DOCUMENT"
    assert link.canonical_url == "https://docs.google.com/document/d/ABC123/edit"


async def test_duplicate_can_fill_missing_source_url(session):
    service = IngestionService(session, StaticAccessChecker())

    await service.ingest(
        CandidateLink(
            raw_url="https://drive.google.com/file/d/ABC123/view",
            source_name="first",
        )
    )
    await service.ingest(
        CandidateLink(
            raw_url="https://drive.google.com/open?id=ABC123",
            source_name="second",
            source_url="https://example.com/second",
        )
    )

    assert await count_drive_links(session) == 1
    link = (await session.execute(select(DriveLink))).scalar_one()
    assert link.source_name == "second"
    assert link.source_url == "https://example.com/second"


async def test_recent_duplicate_skips_access_recheck(session):
    checker = StaticAccessChecker(AccessStatus.PUBLIC)
    service = IngestionService(
        session, checker, settings=Settings(access_recheck_hours=24)
    )

    await service.ingest(
        CandidateLink(
            raw_url="https://drive.google.com/file/d/ABC123/view",
            source_name="manual",
        )
    )
    checker.status = AccessStatus.RESTRICTED
    result = await service.ingest(
        CandidateLink(
            raw_url="https://drive.google.com/file/d/ABC123/edit",
            source_name="manual",
        )
    )

    assert checker.calls == 1
    assert result.access_status == AccessStatus.PUBLIC


async def test_stale_duplicate_triggers_access_recheck(session):
    checker = StaticAccessChecker(AccessStatus.PUBLIC)
    service = IngestionService(
        session, checker, settings=Settings(access_recheck_hours=24)
    )

    await service.ingest(
        CandidateLink(
            raw_url="https://drive.google.com/file/d/ABC123/view",
            source_name="manual",
        )
    )
    link = (await session.execute(select(DriveLink))).scalar_one()
    link.last_checked_at = datetime.now(UTC) - timedelta(hours=25)
    checker.status = AccessStatus.RESTRICTED

    result = await service.ingest(
        CandidateLink(
            raw_url="https://drive.google.com/file/d/ABC123/edit",
            source_name="manual",
        )
    )

    assert checker.calls == 2
    assert result.access_status == AccessStatus.RESTRICTED


async def test_invalid_candidate_is_not_stored(session):
    service = IngestionService(session, StaticAccessChecker())

    result = await service.ingest(
        CandidateLink(raw_url="https://example.com/not-google", source_name="manual")
    )

    assert result.valid is False
    assert await count_drive_links(session) == 0


async def test_concurrent_duplicate_ingestion_keeps_one_row(tmp_path):
    db_path = tmp_path / "concurrent.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)

    async def ingest_once():
        async with maker() as session:
            async with session.begin():
                service = IngestionService(
                    session, StaticAccessChecker(), check_access=False
                )
                return await service.ingest(
                    CandidateLink(
                        raw_url="https://drive.google.com/file/d/ABC123/view",
                        source_name="manual",
                    )
                )

    results = await __import__("asyncio").gather(*(ingest_once() for _ in range(10)))

    async with maker() as session:
        assert await count_drive_links(session) == 1
    assert sum(result.created for result in results) == 1
    assert sum(result.duplicate for result in results) == 9
    await engine.dispose()
