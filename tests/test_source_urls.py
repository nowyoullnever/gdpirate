from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
import pytest

from gdpirate.collectors.base import CandidateLink
from gdpirate.core.models import Base, DriveLink
from gdpirate.core.source_quality import source_quality
from gdpirate.core.source_urls import normalize_source_url
from gdpirate.pipeline.ingestion import IngestionService
from tests.test_ingestion import StaticAccessChecker


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


def test_source_url_validation_accepts_only_public_web_urls():
    assert normalize_source_url("https://Example.com/Post") == "https://example.com/Post"
    assert normalize_source_url("http://example.com/post") == "http://example.com/post"
    assert normalize_source_url("javascript:alert(1)") is None
    assert normalize_source_url("data:text/html,hi") is None
    assert normalize_source_url("file:///etc/passwd") is None
    assert normalize_source_url("https:///missing-host") is None
    assert normalize_source_url("https://user:pass@example.com/post") is None
    assert normalize_source_url("https://example.com/\x00bad") is None
    assert normalize_source_url("https://exa mple.com/post") is None


async def test_invalid_source_url_stored_as_none(session):
    service = IngestionService(session, StaticAccessChecker())

    await service.ingest(
        CandidateLink(
            raw_url="https://drive.google.com/file/d/ABC123/view",
            source_name="manual",
            source_url="javascript:alert(1)",
        )
    )

    link = (await session.execute(select(DriveLink))).scalar_one()
    assert link.source_url is None


def test_source_quality_order_and_invalid_zero():
    assert source_quality("Hacker News", "https://news.ycombinator.com/item?id=1") == 100
    assert source_quality("Hacker News", "https://example.com") > source_quality(
        "Common Crawl WAT", "https://archived.example/page"
    )
    assert source_quality("Common Crawl WAT", "https://archived.example/page") > source_quality(
        "gdURL", "https://gdurl.com/x"
    )
    assert source_quality("gdURL", "https://gdurl.com/x") > source_quality(
        "deDigger", "https://www.dedigger.com/search"
    )
    assert source_quality("deDigger", "https://www.dedigger.com/search") > source_quality(
        "Common Crawl URL Index", "https://index.commoncrawl.org/x"
    )
    assert source_quality("Hacker News", "javascript:alert(1)") == 0
