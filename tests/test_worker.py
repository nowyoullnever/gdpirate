import asyncio
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from gdpirate.config import Settings
from gdpirate.core.models import Base, ScheduledJobState
from gdpirate.pipeline.jobs import JobConfig, JobExecutor, TotalCollectionFailure
from gdpirate.worker import Worker


class FakeExecutor:
    def __init__(self, failures=None):
        self.failures = set(failures or [])
        self.ran = []

    async def execute(self, job):
        self.ran.append(job.name)
        if job.name in self.failures:
            raise RuntimeError("planned failure")
        return {"checked": 1}


class SlowExecutor(FakeExecutor):
    async def execute(self, job):
        await asyncio.sleep(0.01)
        return await super().execute(job)


async def test_worker_once_runs_due_jobs_and_records_success_failure(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'worker.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    jobs = [
        JobConfig(name="bad", kind="validate", enabled=True, interval_seconds=60),
        JobConfig(name="good", kind="validate", enabled=True, interval_seconds=60),
    ]
    executor = FakeExecutor(failures={"bad"})
    worker = Worker(
        settings=Settings(database_url="sqlite+aiosqlite:///:memory:"),
        session_factory=maker,
        executor=executor,
    )

    outcomes = await worker.run_due_jobs(jobs)

    assert [outcome.job for outcome in outcomes] == ["bad", "good"]
    assert executor.ran == ["bad", "good"]
    async with maker() as session:
        rows = (
            await session.execute(select(ScheduledJobState).order_by(ScheduledJobState.job_name))
        ).scalars().all()
    assert rows[0].job_name == "bad"
    assert rows[0].consecutive_failures == 1
    assert rows[0].last_error.startswith("RuntimeError")
    assert rows[1].job_name == "good"
    assert rows[1].consecutive_failures == 0
    assert rows[1].last_result_json == {"checked": 1}
    await engine.dispose()


async def test_lock_race_rechecks_due_state_after_acquire(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'race.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    job = JobConfig(name="recent", kind="validate", enabled=True, interval_seconds=3600)
    first = Worker(
        settings=Settings(database_url="sqlite+aiosqlite:///:memory:"),
        session_factory=maker,
        executor=FakeExecutor(),
    )
    second = Worker(
        settings=Settings(database_url="sqlite+aiosqlite:///:memory:"),
        session_factory=maker,
        executor=FakeExecutor(),
    )

    ran = await first.run_job(job, only_if_due=True)
    skipped = await second.run_job(job, only_if_due=True)

    assert ran.ran is True
    assert skipped.ran is False
    assert skipped.skipped_reason == "not_due"
    await engine.dispose()


async def test_due_scan_missing_state_skips_locked_without_creating_state(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'scan.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    job = JobConfig(name="recent", kind="validate", enabled=True, interval_seconds=60)
    worker = Worker(
        settings=Settings(database_url="sqlite+aiosqlite:///:memory:"),
        session_factory=maker,
        executor=FakeExecutor(),
    )

    from gdpirate.core.job_lock import JobLock

    async with JobLock("recent", settings=Settings(database_url="sqlite+aiosqlite:///:memory:")):
        outcomes = await worker.run_due_jobs([job])

    assert outcomes[0].skipped_reason == "locked"
    async with maker() as session:
        rows = (await session.execute(select(ScheduledJobState))).scalars().all()
    assert rows == []
    await engine.dispose()


async def test_concurrent_first_start_runs_once_and_creates_one_state(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'first.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    job = JobConfig(name="recent", kind="validate", enabled=True, interval_seconds=60)
    executor = SlowExecutor()
    first = Worker(
        settings=Settings(database_url="sqlite+aiosqlite:///:memory:"),
        session_factory=maker,
        executor=executor,
    )
    second = Worker(
        settings=Settings(database_url="sqlite+aiosqlite:///:memory:"),
        session_factory=maker,
        executor=executor,
    )

    outcomes = await asyncio.gather(first.run_due_jobs([job]), second.run_due_jobs([job]))

    assert sum(outcome.ran for batch in outcomes for outcome in batch) == 1
    assert executor.ran == ["recent"]
    async with maker() as session:
        rows = (await session.execute(select(ScheduledJobState))).scalars().all()
    assert len(rows) == 1
    await engine.dispose()


class FakeCollectionRunner:
    def __init__(self, results):
        self.results = results

    async def collect(self, source, **kwargs):
        result = self.results[source]
        if isinstance(result, Exception):
            raise result
        return [result]


async def test_collect_job_partial_failure_is_success():
    runner = FakeCollectionRunner(
        {
            "one": SimpleNamespace(
                source="one",
                scanned=1,
                candidates=0,
                created=0,
                duplicates=0,
                public=0,
                restricted=0,
                dead=0,
                unknown=0,
                error=None,
                unavailable=False,
            ),
            "two": RuntimeError("upstream down"),
        }
    )
    result = await JobExecutor(collection_runner=runner)._collect(
        JobConfig(
            name="recent",
            kind="collect",
            enabled=True,
            interval_seconds=60,
            sources=["one", "two"],
        )
    )

    assert result["successful_sources"] == 1
    assert result["failed_sources"] == 1


async def test_collect_job_total_failure_raises():
    runner = FakeCollectionRunner(
        {
            "one": RuntimeError("down"),
            "two": SimpleNamespace(
                source="two",
                scanned=0,
                candidates=0,
                created=0,
                duplicates=0,
                public=0,
                restricted=0,
                dead=0,
                unknown=0,
                error="rate limited",
                unavailable=False,
            ),
        }
    )

    with pytest.raises(TotalCollectionFailure) as exc:
        await JobExecutor(collection_runner=runner)._collect(
            JobConfig(
                name="recent",
                kind="collect",
                enabled=True,
                interval_seconds=60,
                sources=["one", "two"],
            )
        )

    assert exc.value.summary["failed_sources"] == 2


async def test_collect_job_zero_result_success():
    runner = FakeCollectionRunner(
        {
            "one": SimpleNamespace(
                source="one",
                scanned=0,
                candidates=0,
                created=0,
                duplicates=0,
                public=0,
                restricted=0,
                dead=0,
                unknown=0,
                error=None,
                unavailable=False,
            )
        }
    )
    result = await JobExecutor(collection_runner=runner)._collect(
        JobConfig(
            name="recent",
            kind="collect",
            enabled=True,
            interval_seconds=60,
            sources=["one"],
        )
    )

    assert result["successful_sources"] == 1
    assert result["candidates"] == 0


async def test_total_collect_failure_records_job_failure(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'total.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    job = JobConfig(
        name="recent",
        kind="collect",
        enabled=True,
        interval_seconds=60,
        sources=["one"],
    )
    worker = Worker(
        settings=Settings(
            database_url="sqlite+aiosqlite:///:memory:",
            worker_failure_base_backoff_seconds=60,
        ),
        session_factory=maker,
        executor=JobExecutor(
            collection_runner=FakeCollectionRunner({"one": RuntimeError("down")})
        ),
    )

    outcome = await worker.run_job(job, only_if_due=True)

    assert outcome.error is not None
    async with maker() as session:
        state = (
            await session.execute(select(ScheduledJobState).where(ScheduledJobState.job_name == "recent"))
        ).scalar_one()
    assert state.consecutive_failures == 1
    assert state.last_error.startswith("TotalCollectionFailure")
    await engine.dispose()


async def test_worker_survives_scheduler_failure_then_continues(monkeypatch, tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'survive.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    job = JobConfig(name="good", kind="validate", enabled=True, interval_seconds=60)
    calls = {"enabled": 0, "sleep": 0}

    def fake_enabled_jobs(settings):
        calls["enabled"] += 1
        if calls["enabled"] == 1:
            raise ValueError("bad config")
        return [job]

    worker = Worker(
        settings=Settings(database_url="sqlite+aiosqlite:///:memory:", worker_poll_seconds=0),
        session_factory=maker,
        executor=FakeExecutor(),
    )

    async def fake_sleep(seconds):
        calls["sleep"] += 1
        if calls["sleep"] >= 2:
            worker.request_stop()

    worker.sleep = fake_sleep
    monkeypatch.setattr("gdpirate.worker.enabled_jobs", fake_enabled_jobs)

    outcomes = await worker.run()

    assert [outcome.job for outcome in outcomes] == ["good"]
    assert calls["enabled"] >= 2
    await engine.dispose()
