from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from gdpirate.collectors.base import CandidateLink
from gdpirate.core.models import Base, CollectorState
from gdpirate.pipeline.collection import CollectionRunner


class CursorCollector:
    name = "hackernews"

    def __init__(self):
        self.seen_cursors = []

    async def collect(self, context, *, max_items=None):
        self.seen_cursors.append(dict(context.cursor))
        await context.checkpoint("page", {"cursor": "new"})
        yield CandidateLink(raw_url="https://example.com/not-google", source_name="test")


async def test_persistent_mode_loads_and_writes_collector_state(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'persistent.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    collector = CursorCollector()
    async with maker() as session:
        async with session.begin():
            session.add(
                CollectorState(
                    collector_name="hackernews",
                    scope="page",
                    cursor_json={"cursor": "old"},
                )
            )

    await CollectionRunner(
        session_factory=maker, collectors={"hackernews": collector}
    ).collect("hackernews")

    assert collector.seen_cursors == [{"page": {"cursor": "old"}}]
    async with maker() as session:
        state = (
            await session.execute(
                select(CollectorState).where(
                    CollectorState.collector_name == "hackernews",
                    CollectorState.scope == "page",
                )
            )
        ).scalar_one()
    assert state.cursor_json == {"cursor": "new"}
    await engine.dispose()


async def test_fresh_head_uses_empty_cursor_and_preserves_backfill_state(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'fresh.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    collector = CursorCollector()
    async with maker() as session:
        async with session.begin():
            session.add(
                CollectorState(
                    collector_name="hackernews",
                    scope="page",
                    cursor_json={"cursor": "old"},
                )
            )

    await CollectionRunner(
        session_factory=maker, collectors={"hackernews": collector}
    ).collect("hackernews", state_mode="fresh-head")

    assert collector.seen_cursors == [{}]
    async with maker() as session:
        state = (
            await session.execute(
                select(CollectorState).where(
                    CollectorState.collector_name == "hackernews",
                    CollectorState.scope == "page",
                )
            )
        ).scalar_one()
    assert state.cursor_json == {"cursor": "old"}
    await engine.dispose()
