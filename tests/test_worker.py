from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from gdpirate.config import Settings
from gdpirate.core.models import Base, ScheduledJobState
from gdpirate.pipeline.jobs import JobConfig
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
