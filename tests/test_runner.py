from collections.abc import AsyncIterator

import httpx
import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from gdpirate.collectors.base import CandidateLink, CollectorContext
from gdpirate.config import Settings
from gdpirate.core.models import Base, CollectorState, DriveLink
from gdpirate.pipeline.collection import CollectionRunner


class FakeCollector:
    name = "fake"

    def __init__(self, urls: list[str], *, fail: bool = False) -> None:
        self.urls = urls
        self.fail = fail

    async def collect(
        self, context: CollectorContext, *, max_items: int | None = None
    ) -> AsyncIterator[CandidateLink]:
        if self.fail:
            raise RuntimeError("collector failed")
        count = 0
        for url in self.urls:
            if max_items is not None and count >= max_items:
                return
            yield CandidateLink(raw_url=url, source_name=self.name)
            count += 1
        context.set_cursor("scope", {"done": True})


@pytest.fixture
async def runner_session_factory(tmp_path):
    db_path = tmp_path / "runner.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@respx.mock
async def test_runner_collect_one_persists_state_and_ingests(runner_session_factory):
    url = "https://drive.google.com/file/d/ABC123/view"
    respx.get(url).mock(
        return_value=httpx.Response(200, text="<div id='drive-viewer'>ok</div>")
    )
    runner = CollectionRunner(
        Settings(http_max_concurrency=2),
        session_factory=runner_session_factory,
        collectors={"fake": FakeCollector([url])},
    )

    results = await runner.collect("fake", max_items=1)

    assert results[0].created == 1
    async with runner_session_factory() as session:
        links = (await session.execute(select(DriveLink))).scalars().all()
        states = (await session.execute(select(CollectorState))).scalars().all()
    assert len(links) == 1
    assert states[0].cursor_json == {"scope": {"done": True}}


@respx.mock
async def test_runner_collect_all_survives_one_collector_failure(runner_session_factory):
    url = "https://drive.google.com/file/d/ABC123/view"
    respx.get(url).mock(
        return_value=httpx.Response(200, text="<div id='drive-viewer'>ok</div>")
    )
    runner = CollectionRunner(
        Settings(http_max_concurrency=1),
        session_factory=runner_session_factory,
        collectors={
            "bad": FakeCollector([], fail=True),
            "good": FakeCollector([url]),
        },
    )

    results = await runner.collect("all", max_items_per_source=1)

    assert results[0].source == "bad"
    assert results[0].error == "collector failed"
    assert results[1].source == "good"
    assert results[1].created == 1


@respx.mock
async def test_runner_max_items_limits_collection(runner_session_factory):
    urls = [
        "https://drive.google.com/file/d/ABC123/view",
        "https://drive.google.com/file/d/DEF456/view",
    ]
    for url in urls:
        respx.get(url).mock(
            return_value=httpx.Response(200, text="<div id='drive-viewer'>ok</div>")
        )
    runner = CollectionRunner(
        Settings(http_max_concurrency=2),
        session_factory=runner_session_factory,
        collectors={"fake": FakeCollector(urls)},
    )

    results = await runner.collect("fake", max_items=1)

    assert results[0].scanned == 1
    assert results[0].created == 1
