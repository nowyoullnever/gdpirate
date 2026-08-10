import gzip
import io
import json

import duckdb
import httpx
import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from gdpirate.collectors.base import CollectorContext
from gdpirate.collectors.commoncrawl import (
    CommonCrawlCollector,
    CommonCrawlRunOptions,
    commoncrawl_index_source_url,
    fetch_path_list,
    query_url_index_part,
    resolve_crawls,
)
from gdpirate.collectors.base import CandidateLink
from gdpirate.config import Settings
from gdpirate.core.models import AccessStatus, Base, DriveLink
from gdpirate.pipeline.ingestion import AccessCheckPolicy, IngestionService
from gdpirate.pipeline.validation import validate_links


@pytest.fixture
async def session_factory(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'cc.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


async def test_validation_uses_batches_and_max_items(session_factory):
    async with session_factory() as session:
        async with session.begin():
            service = IngestionService(session, check_access=False)
            for index in range(5):
                await service.ingest_with_policy(
                    CandidateLink(
                        raw_url=f"https://drive.google.com/file/d/ABC{index}/view",
                        source_name="Common Crawl",
                    ),
                    AccessCheckPolicy.DEFERRED,
                )

    with respx.mock:
        for index in range(5):
            respx.get(f"https://drive.google.com/file/d/ABC{index}/view").mock(
                return_value=httpx.Response(200, text="<div id='drive-viewer'>ok</div>")
            )
        result = await validate_links(
            max_items=3,
            concurrency=2,
            source="Common Crawl",
            settings=Settings(validation_db_batch_size=2),
            session_factory=session_factory,
        )

    assert result.selected == 3
    assert result.checked == 3
    async with session_factory() as session:
        rows = (await session.execute(select(DriveLink))).scalars().all()
    assert sum(row.access_status == AccessStatus.PUBLIC for row in rows) == 3
    assert sum(row.access_status == AccessStatus.UNKNOWN for row in rows) == 2


async def test_commoncrawl_crawl_resolution_and_path_list():
    gz = gzip.compress(b"a.parquet\nb.parquet\n")

    async def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).endswith("collinfo.json"):
            return httpx.Response(
                200,
                json=[
                    {"id": "CC-MAIN-2026-30"},
                    {"id": "CC-MAIN-2026-25"},
                ],
            )
        return httpx.Response(200, content=gz)

    settings = Settings(
        commoncrawl_collinfo_url="https://index.example/collinfo.json",
        commoncrawl_data_base="https://data.example",
        commoncrawl_crawls="latest",
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        assert await resolve_crawls(client, settings) == ["CC-MAIN-2026-30"]
        settings2 = Settings(
            commoncrawl_collinfo_url="https://index.example/collinfo.json",
            commoncrawl_data_base="https://data.example",
            commoncrawl_crawls="CC-MAIN-2026-25,CC-MAIN-2026-30",
        )
        assert await resolve_crawls(client, settings2) == [
            "CC-MAIN-2026-30",
            "CC-MAIN-2026-25",
        ]
        assert await fetch_path_list(
            client, settings, "CC-MAIN-2026-30", "cc-index-table.paths.gz"
        ) == ["a.parquet", "b.parquet"]


async def test_commoncrawl_invalid_crawl_rejected():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[{"id": "CC-MAIN-2026-30"}])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ValueError):
            await resolve_crawls(
                client,
                Settings(
                    commoncrawl_collinfo_url="https://index.example/collinfo.json",
                    commoncrawl_crawls="https://evil.example/x",
                ),
            )


def test_url_index_parquet_filters_google_urls(tmp_path):
    parquet = tmp_path / "part.parquet"
    duckdb.execute(
        "CREATE TABLE urls(url VARCHAR, url_host_name VARCHAR)"
    )
    duckdb.execute(
        "INSERT INTO urls VALUES "
        "('https://drive.google.com/file/d/ABC123/view','drive.google.com'),"
        "('https://docs.google.com/document/d/DOC123/edit','docs.google.com'),"
        "('https://example.com/x','example.com'),"
        "('https://drive.google.com.example.com/file/d/BAD/view','drive.google.com.example.com'),"
        "('https://drive.google.com/file/d/!!/view','drive.google.com')"
    )
    duckdb.execute(f"COPY urls TO '{parquet.as_posix()}' (FORMAT PARQUET)")
    duckdb.execute("DROP TABLE urls")

    assert query_url_index_part(str(parquet)) == [
        "https://drive.google.com/file/d/ABC123/view",
        "https://docs.google.com/document/d/DOC123/edit",
    ]
    assert "output=json" in commoncrawl_index_source_url(
        "CC-MAIN-2026-30", "https://drive.google.com/file/d/ABC123/view"
    )


async def test_commoncrawl_url_index_collector_resume_and_deferred(tmp_path):
    parquet = tmp_path / "part.parquet"
    duckdb.execute("CREATE TABLE ccurls(url VARCHAR, url_host_name VARCHAR)")
    duckdb.execute(
        "INSERT INTO ccurls VALUES "
        "('https://drive.google.com/file/d/ABC123/view','drive.google.com')"
    )
    duckdb.execute(f"COPY ccurls TO '{parquet.as_posix()}' (FORMAT PARQUET)")
    duckdb.execute("DROP TABLE ccurls")
    paths = gzip.compress(str(parquet).encode() + b"\n")

    async def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).endswith("collinfo.json"):
            return httpx.Response(200, json=[{"id": "CC-MAIN-2026-30"}])
        return httpx.Response(200, content=paths)

    settings = Settings(
        enable_common_crawl=True,
        commoncrawl_collinfo_url="https://index.example/collinfo.json",
        commoncrawl_data_base="https://data.example",
        commoncrawl_crawls="latest",
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        context = CollectorContext(client=client)
        items = [
            item
            async for item in CommonCrawlCollector(
                settings, CommonCrawlRunOptions(mode="url-index", max_files=1)
            ).collect(context, max_items=10)
        ]

    assert len(items) == 1
    assert items[0].source_name == "Common Crawl"
    assert context.cursor["url-index/CC-MAIN-2026-30"]["path_index"] == 1


def make_wat_gz(path, payloads):
    with path.open("wb") as raw:
        writer = WARCWriter(raw, gzip=True)
        for payload in payloads:
            record = writer.create_warc_record(
                payload["target"],
                "metadata",
                payload=io.BytesIO(json.dumps(payload["json"]).encode()),
                warc_headers_dict={"WARC-Type": "metadata"},
            )
            writer.write_record(record)


async def test_commoncrawl_wat_candidates_source_and_no_live_source_requests(tmp_path):
    wat = tmp_path / "part.wat.gz"
    make_wat_gz(
        wat,
        [
            {
                "target": "metadata://example",
                "json": {
                    "Envelope": {
                        "WARC-Header-Metadata": {
                            "WARC-Target-URI": "https://source.example/post"
                        },
                        "Payload-Metadata": {
                            "HTTP-Response-Metadata": {
                                "HTML-Metadata": {
                                    "Links": [
                                        {
                                            "url": "https://drive.google.com/file/d/ABC123/view"
                                        },
                                        {
                                            "url": "https://drive.google.com.example.com/file/d/BAD/view"
                                        },
                                    ]
                                }
                            }
                        },
                    }
                },
            }
        ],
    )
    paths = gzip.compress(str(wat).encode() + b"\n")
    requested = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        if str(request.url).endswith("collinfo.json"):
            return httpx.Response(200, json=[{"id": "CC-MAIN-2026-30"}])
        if str(request.url).endswith("wat.paths.gz"):
            return httpx.Response(200, content=paths)
        return httpx.Response(200, content=wat.read_bytes())

    settings = Settings(
        enable_common_crawl=True,
        commoncrawl_collinfo_url="https://index.example/collinfo.json",
        commoncrawl_data_base="https://data.example",
        commoncrawl_crawls="latest",
        commoncrawl_temp_dir=str(tmp_path / "tmp"),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        context = CollectorContext(client=client)
        items = [
            item
            async for item in CommonCrawlCollector(
                settings, CommonCrawlRunOptions(mode="wat", max_files=1)
            ).collect(context, max_items=10)
        ]

    assert len(items) == 1
    assert items[0].raw_url == "https://drive.google.com/file/d/ABC123/view"
    assert items[0].source_url == "https://source.example/post"
    assert not any("source.example" in url for url in requested)
    assert not list((tmp_path / "tmp").glob("*.wat.gz"))
