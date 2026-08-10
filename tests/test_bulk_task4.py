import httpx
import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from gdpirate.collectors.base import CollectorContext
from gdpirate.collectors.dedigger import DeDiggerCollector
from gdpirate.collectors.gdurl import GdUrlCollector
from gdpirate.config import Settings
from gdpirate.core.models import AccessStatus, Base, DriveLink
from gdpirate.core.robots import RobotsPolicy
from gdpirate.pipeline.ingestion import (
    AccessCheckPolicy,
    IngestionService,
)
from gdpirate.collectors.base import CandidateLink
from gdpirate.pipeline.validation import validate_links
from gdpirate.pipeline.collection import CollectionRunner


@pytest.fixture
async def session_factory(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'bulk.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


def test_ingestion_lock_pool_is_bounded():
    before = IngestionService.lock_count()
    for index in range(10_000):
        IngestionService._lock_for("google", f"id{index}")
    assert IngestionService.lock_count() == before == 1024


async def test_deferred_ingest_then_validate(session_factory):
    async with session_factory() as session:
        async with session.begin():
            result = await IngestionService(session).ingest_with_policy(
                CandidateLink(
                    raw_url="https://drive.google.com/file/d/ABC123/view",
                    source_name="gdURL",
                ),
                AccessCheckPolicy.DEFERRED,
            )
            assert result.access_status == AccessStatus.UNKNOWN
            assert result.access_check_needed is False

    with respx.mock:
        respx.get("https://drive.google.com/file/d/ABC123/view").mock(
            return_value=httpx.Response(200, text="<div id='drive-viewer'>ok</div>")
        )
        validation = await validate_links(
            max_items=1,
            concurrency=1,
            source="gdURL",
            session_factory=session_factory,
            settings=Settings(),
        )

    assert validation.selected == 1
    assert validation.public == 1
    async with session_factory() as session:
        link = (await session.execute(select(DriveLink))).scalar_one()
        assert link.access_status == AccessStatus.PUBLIC


async def test_robots_allow_disallow_and_failure():
    async def handler(request: httpx.Request) -> httpx.Response:
        if "down.example" in str(request.url):
            return httpx.Response(503)
        return httpx.Response(
            200,
            text="User-agent: GDPirate\nDisallow: /private\nAllow: /\n",
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        policy = RobotsPolicy("GDPirate")
        assert (await policy.allowed(client, "https://ok.example/public")).allowed
        assert not (await policy.allowed(client, "https://ok.example/private/x")).allowed
        assert not (await policy.allowed(client, "https://down.example/x")).allowed


async def test_gdurl_catalogue_resume_and_deferred_candidates():
    async def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.endswith("/robots.txt"):
            return httpx.Response(200, text="User-agent: *\nAllow: /\n")
        if url.endswith("/all"):
            return httpx.Response(
                200,
                text="""
                <a href="/a">one</a><a href="/b">two</a>
                <a href="/all?page=2">next</a>
                """,
            )
        if url.endswith("/a"):
            return httpx.Response(
                200,
                text="https://drive.google.com/file/d/ABC123/view",
            )
        if url.endswith("/b"):
            return httpx.Response(200, text="nothing")
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), follow_redirects=True
    ) as client:
        context = CollectorContext(client=client)
        collector = GdUrlCollector(
            Settings(gdurl_browse_url="https://gdurl.com/all", gdurl_request_delay_seconds=0)
        )
        items = [item async for item in collector.collect(context, max_items=1)]

    assert len(items) == 1
    assert items[0].source_name == "gdURL"
    assert items[0].source_url == "https://gdurl.com/a"
    assert context.cursor["catalogue"] == {
        "page_url": "https://gdurl.com/all",
        "offset": 1,
    }


async def test_dedigger_robots_denied_and_public_results(tmp_path):
    queries = tmp_path / "queries.toml"
    queries.write_text('[[queries]]\nquery = "*.pdf"\nenabled = true\n')

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text="User-agent: *\nAllow: /\n")
        return httpx.Response(
            200,
            text="https://docs.google.com/document/d/DOC123/edit",
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        context = CollectorContext(client=client)
        items = [
            item
            async for item in DeDiggerCollector(
                Settings(
                    dedigger_base_url="https://dedigger.example",
                    dedigger_query_config_path=str(queries),
                    dedigger_request_delay_seconds=0,
                )
            ).collect(context, max_items=1)
        ]

    assert len(items) == 1
    assert items[0].source_name == "deDigger"
    assert "q=%2A.pdf" in items[0].source_url


async def test_disabled_bulk_collector_returns_clean_result(session_factory):
    runner = CollectionRunner(Settings(enable_gdurl=False), session_factory=session_factory)

    result = (await runner.collect("gdurl", max_items=1))[0]

    assert result.source == "gdurl"
    assert result.error == "collector disabled"
