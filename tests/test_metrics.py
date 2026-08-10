from datetime import UTC, datetime, timedelta

import httpx
import respx
from sqlalchemy import inspect, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from gdpirate.collectors.base import CandidateLink
from gdpirate.config import Settings
from gdpirate.core.models import (
    AccessStatus,
    Base,
    CollectionRunMetric,
    DriveLink,
    ResourceType,
    ValidationRunMetric,
)
from gdpirate.pipeline.collection import CollectionRunner
from gdpirate.pipeline.metrics import (
    MetricContext,
    metrics_summary,
    prune_metrics,
    record_collection_metrics,
    stats_by_source,
)
from gdpirate.pipeline.validation import validate_links


class OneCandidateCollector:
    name = "hackernews"

    async def collect(self, context, *, max_items=None):
        context.mark_scanned()
        yield CandidateLink(
            raw_url="https://drive.google.com/file/d/ABC123/view",
            source_name="Hacker News",
            source_url="https://news.ycombinator.com/item?id=1",
        )


async def _maker(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'metrics.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return engine, async_sessionmaker(engine, expire_on_commit=False)


def test_metrics_models_do_not_store_candidate_or_resource_urls():
    forbidden = {"resource_id", "canonical_url", "source_url", "candidate_url", "content"}

    assert forbidden.isdisjoint(CollectionRunMetric.__table__.columns.keys())
    assert forbidden.isdisjoint(ValidationRunMetric.__table__.columns.keys())


async def test_metrics_tables_and_indexes_exist(tmp_path):
    engine, _maker_instance = await _maker(tmp_path)
    async with engine.begin() as conn:
        tables = await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names())
        indexes = await conn.run_sync(
            lambda sync_conn: {
                "collection": inspect(sync_conn).get_indexes("collection_run_metrics"),
                "validation": inspect(sync_conn).get_indexes("validation_run_metrics"),
            }
        )

    assert "collection_run_metrics" in tables
    assert "validation_run_metrics" in tables
    assert any(index["name"] == "ix_collection_metrics_started" for index in indexes["collection"])
    assert any(index["name"] == "ix_validation_metrics_started" for index in indexes["validation"])
    await engine.dispose()


async def test_collection_metric_row_records_aggregate_counts(tmp_path):
    engine, maker = await _maker(tmp_path)
    runner = CollectionRunner(
        settings=Settings(http_max_concurrency=1),
        session_factory=maker,
        collectors={"hackernews": OneCandidateCollector()},
    )

    with respx.mock:
        respx.get("https://drive.google.com/file/d/ABC123/view").mock(
            return_value=httpx.Response(200, text="<div id='drive-viewer'>ok</div>")
        )
        await runner.collect(
            "hackernews",
            metric_context=MetricContext(run_id="run-1", trigger="cli"),
        )

    async with maker() as session:
        metric = (await session.execute(select(CollectionRunMetric))).scalar_one()
        link = (await session.execute(select(DriveLink))).scalar_one()

    assert metric.run_id == "run-1"
    assert metric.trigger == "cli"
    assert metric.source == "hackernews"
    assert metric.scanned == 1
    assert metric.candidates == 1
    assert metric.created == 1
    assert metric.access_checks == 1
    assert metric.duration_ms >= 0
    assert link.resource_id == "ABC123"
    await engine.dispose()


async def test_validation_metric_records_by_source_breakdown(tmp_path):
    engine, maker = await _maker(tmp_path)
    async with maker() as session:
        async with session.begin():
            for index, source in enumerate(["Hacker News", "Hacker News", "Common Crawl URL Index"]):
                session.add(
                    DriveLink(
                        provider="google",
                        resource_id=f"ABC{index}",
                        resource_type=ResourceType.FILE,
                        canonical_url=f"https://drive.google.com/file/d/ABC{index}/view",
                        random_key=0.1 + index,
                        source_name=source,
                        source_url="https://source.example/post",
                        access_status=AccessStatus.UNKNOWN,
                    )
                )

    with respx.mock:
        for index in range(3):
            respx.get(f"https://drive.google.com/file/d/ABC{index}/view").mock(
                return_value=httpx.Response(200, text="<div id='drive-viewer'>ok</div>")
            )
        await validate_links(
            max_items=3,
            concurrency=1,
            status=AccessStatus.UNKNOWN,
            session_factory=maker,
            metric_context=MetricContext(run_id="val-1", trigger="cli"),
            settings=Settings(),
        )

    async with maker() as session:
        metric = (await session.execute(select(ValidationRunMetric))).scalar_one()

    assert metric.run_id == "val-1"
    assert metric.selected == 3
    assert metric.checked == 3
    assert metric.by_source_json["Hacker News"]["selected"] == 2
    assert metric.by_source_json["Common Crawl URL Index"]["selected"] == 1
    assert "ABC0" not in str(metric.by_source_json)
    await engine.dispose()


async def test_metrics_summary_zero_denominators_stats_and_prune(tmp_path):
    engine, maker = await _maker(tmp_path)
    now = datetime.now(UTC)
    async with maker() as session:
        async with session.begin():
            session.add_all(
                [
                    CollectionRunMetric(
                        run_id="old",
                        trigger="cli",
                        source="hackernews",
                        state_mode="fresh-head",
                        started_at=now - timedelta(days=100),
                        finished_at=now - timedelta(days=100),
                        duration_ms=0,
                        success=True,
                    ),
                    CollectionRunMetric(
                        run_id="new",
                        trigger="cli",
                        source="hackernews",
                        state_mode="fresh-head",
                        started_at=now,
                        finished_at=now,
                        duration_ms=0,
                        success=True,
                    ),
                    ValidationRunMetric(
                        run_id="oldv",
                        trigger="cli",
                        requested_status="UNKNOWN",
                        stale_only=False,
                        started_at=now - timedelta(days=100),
                        finished_at=now - timedelta(days=100),
                        duration_ms=0,
                    ),
                    ValidationRunMetric(
                        run_id="newv",
                        trigger="cli",
                        requested_status="UNKNOWN",
                        stale_only=False,
                        started_at=now,
                        finished_at=now,
                        duration_ms=0,
                    ),
                    DriveLink(
                        provider="google",
                        resource_id="KEEP",
                        resource_type=ResourceType.FILE,
                        canonical_url="https://drive.google.com/file/d/KEEP/view",
                        random_key=0.5,
                        source_name="Hacker News",
                        source_url="https://news.ycombinator.com/item?id=1",
                        access_status=AccessStatus.PUBLIC,
                    ),
                ]
            )

    summary = await metrics_summary(hours=24, session_factory=maker)
    assert summary["collection"][0]["candidate_rate"] is None
    assert await stats_by_source(session_factory=maker) == [
        {
            "source": "Hacker News",
            "total": 1,
            "public": 1,
            "restricted": 0,
            "dead": 0,
            "unknown": 0,
        }
    ]
    pruned = await prune_metrics(keep_days=90, session_factory=maker)
    assert pruned == {"collection_deleted": 1, "validation_deleted": 1}
    async with maker() as session:
        assert len((await session.execute(select(CollectionRunMetric))).scalars().all()) == 1
        assert len((await session.execute(select(ValidationRunMetric))).scalars().all()) == 1
        assert len((await session.execute(select(DriveLink))).scalars().all()) == 1
    await engine.dispose()


async def test_metric_failure_does_not_break_primary_collection(tmp_path):
    engine, maker = await _maker(tmp_path)

    class BrokenSessionFactory:
        def __call__(self):
            raise RuntimeError("metric db down")

    runner = CollectionRunner(
        settings=Settings(http_max_concurrency=1),
        session_factory=maker,
        collectors={"hackernews": OneCandidateCollector()},
    )
    with respx.mock:
        respx.get("https://drive.google.com/file/d/ABC123/view").mock(
            return_value=httpx.Response(200, text="<div id='drive-viewer'>ok</div>")
        )
        results = await runner.collect("hackernews")
    await record_collection_metrics(
        results,
        metric_context=MetricContext(run_id="broken", trigger="cli"),
        session_factory=BrokenSessionFactory(),
    )

    async with maker() as session:
        assert len((await session.execute(select(DriveLink))).scalars().all()) == 1
    await engine.dispose()
