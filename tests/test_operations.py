from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from gdpirate.config import Settings
from gdpirate.core.database import make_engine
from gdpirate.core.job_lock import JobLock, advisory_lock_key
from gdpirate.core.models import Base, ScheduledJobState
from gdpirate.pipeline.jobs import (
    JobConfig,
    ensure_job_state,
    is_due,
    load_jobs_config,
    record_job_failure,
    record_job_success,
)


def test_database_url_conversion_and_engine_settings():
    sqlite = Settings(database_url="sqlite:///./x.db")
    assert sqlite.async_database_url == "sqlite+aiosqlite:///./x.db"
    assert sqlite.sync_database_url == "sqlite:///./x.db"

    postgres = Settings(database_url="postgresql://u:p@localhost/db")
    assert postgres.async_database_url == "postgresql+asyncpg://u:p@localhost/db"
    assert postgres.sync_database_url == "postgresql+psycopg://u:p@localhost/db"

    engine = make_engine("sqlite+aiosqlite:///:memory:")
    assert engine.url.drivername == "sqlite+aiosqlite"


def test_stable_advisory_lock_key():
    assert advisory_lock_key("recent") == advisory_lock_key("recent")
    assert advisory_lock_key("recent") != advisory_lock_key("feeds")


async def test_job_state_success_and_failure_backoff(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'jobs.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    job = JobConfig(
        name="validate_unknown",
        kind="validate",
        enabled=True,
        interval_seconds=300,
    )
    settings = Settings(
        worker_failure_base_backoff_seconds=60,
        worker_failure_max_backoff_seconds=120,
    )

    async with maker() as session:
        async with session.begin():
            state = await ensure_job_state(session, job)
            assert is_due(state)
            await record_job_failure(session, job, "boom", settings=settings)
            assert state.consecutive_failures == 1
            first_next = state.next_run_at
            await record_job_failure(session, job, "boom", settings=settings)
            assert state.consecutive_failures == 2
            assert state.next_run_at > first_next
            await record_job_failure(session, job, "boom", settings=settings)
            assert state.consecutive_failures == 3
            await record_job_success(session, job, {"checked": 1})
            assert state.consecutive_failures == 0
            assert state.last_error is None
            assert state.last_result_json == {"checked": 1}
    await engine.dispose()


def test_jobs_config_validation(tmp_path):
    path = tmp_path / "jobs.toml"
    path.write_text(
        '[[jobs]]\nname = "a"\nkind = "collect"\ninterval_seconds = 1\n'
        '[[jobs]]\nname = "a"\nkind = "validate"\ninterval_seconds = 1\n'
    )
    with pytest.raises(ValueError, match="duplicate"):
        load_jobs_config(str(path))

    path.write_text('[[jobs]]\nname = "a"\nkind = "unknown"\ninterval_seconds = 1\n')
    with pytest.raises(ValueError, match="unknown job kind"):
        load_jobs_config(str(path))


async def test_sqlite_job_lock_prevents_same_process_duplicate():
    settings = Settings(database_url="sqlite+aiosqlite:///:memory:")
    async with JobLock("recent", settings=settings) as first:
        assert first.acquired
        async with JobLock("recent", settings=settings) as second:
            assert not second.acquired
    async with JobLock("recent", settings=settings) as third:
        assert third.acquired
