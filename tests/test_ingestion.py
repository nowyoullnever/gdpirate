import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from gdpirate.collectors.base import CandidateLink
from gdpirate.core.access_check import AccessChecker
from gdpirate.core.models import AccessStatus, Base, DriveLink
from gdpirate.pipeline.ingestion import IngestionService, count_drive_links


class StaticAccessChecker(AccessChecker):
    def __init__(self, status: AccessStatus = AccessStatus.PUBLIC) -> None:
        self.status = status

    async def check(self, raw_url: str) -> AccessStatus:
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


async def test_invalid_candidate_is_not_stored(session):
    service = IngestionService(session, StaticAccessChecker())

    result = await service.ingest(
        CandidateLink(raw_url="https://example.com/not-google", source_name="manual")
    )

    assert result.valid is False
    assert await count_drive_links(session) == 0
