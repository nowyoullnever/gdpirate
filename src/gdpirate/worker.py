from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
import logging
import signal
import time

from sqlalchemy.ext.asyncio import async_sessionmaker

from gdpirate.config import Settings, get_settings
from gdpirate.core.database import SessionLocal
from gdpirate.core.job_lock import acquire_job_lock
from gdpirate.core.models import ScheduledJobState
from gdpirate.pipeline.jobs import (
    JobConfig,
    JobExecutor,
    enabled_jobs,
    ensure_job_state,
    get_job_state,
    is_due,
    load_jobs_config,
    record_job_failure,
    record_job_started,
    record_job_success,
)
from gdpirate.pipeline.metrics import MetricContext

logger = logging.getLogger(__name__)


@dataclass
class JobRunOutcome:
    job: str
    ran: bool
    skipped_reason: str | None = None
    result: dict | None = None
    error: str | None = None


class Worker:
    def __init__(
        self,
        *,
        settings: Settings | None = None,
        session_factory: async_sessionmaker | None = None,
        executor: JobExecutor | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.settings = settings or get_settings()
        self.session_factory = session_factory or SessionLocal
        self.executor = executor or JobExecutor(
            settings=self.settings, session_factory=self.session_factory
        )
        self.sleep = sleep
        self.stop_event = asyncio.Event()

    def request_stop(self) -> None:
        self.stop_event.set()

    async def run(self, *, once: bool = False) -> list[JobRunOutcome]:
        logger.info("worker_started")
        self._install_signal_handlers()
        outcomes: list[JobRunOutcome] = []
        while not self.stop_event.is_set():
            try:
                jobs = enabled_jobs(self.settings)
                loop_outcomes = await self.run_due_jobs(jobs)
                outcomes.extend(loop_outcomes)
            except Exception:
                logger.exception("worker scheduler scan failed")
                if once:
                    raise
            if once:
                return outcomes
            await self._wait_for_next_poll()
        logger.info("worker_stopping")
        return outcomes

    async def run_due_jobs(self, jobs: list[JobConfig]) -> list[JobRunOutcome]:
        outcomes = []
        for job in jobs:
            try:
                async with self.session_factory() as session:
                    state = await get_job_state(session, job.name)
                    due = state is None or is_due(state)
                if not due:
                    continue
                outcomes.append(await self.run_job(job, only_if_due=True))
            except Exception as exc:
                logger.exception("job infrastructure failed", extra={"job": job.name})
                outcomes.append(JobRunOutcome(job.name, ran=False, error=str(exc)))
        return outcomes

    async def run_job(
        self,
        job: JobConfig,
        *,
        only_if_due: bool = False,
        trigger: str = "worker",
    ) -> JobRunOutcome:
        async with acquire_job_lock(job.name, settings=self.settings) as lock:
            if not lock.acquired:
                logger.info("job_skipped_locked", extra={"job": job.name})
                return JobRunOutcome(job.name, ran=False, skipped_reason="locked")

            async with self.session_factory() as session:
                async with session.begin():
                    state = await ensure_job_state(session, job)
                    if only_if_due and not is_due(state):
                        return JobRunOutcome(job.name, ran=False, skipped_reason="not_due")
                    await record_job_started(session, job.name)

            started = time.monotonic()
            logger.info("job_started", extra={"job": job.name})
            try:
                result = await self.executor.execute(
                    job,
                    metric_context=MetricContext.create(trigger, job.name),
                )
            except Exception as exc:
                async with self.session_factory() as session:
                    async with session.begin():
                        await record_job_failure(
                            session, job, exc, settings=self.settings
                        )
                logger.exception(
                    "job_failed",
                    extra={"job": job.name, "error_type": type(exc).__name__},
                )
                return JobRunOutcome(job.name, ran=True, error=str(exc))

            async with self.session_factory() as session:
                async with session.begin():
                    await record_job_success(session, job, result)
            logger.info(
                "job_completed",
                extra={
                    "job": job.name,
                    "duration_ms": int((time.monotonic() - started) * 1000),
                    **{key: value for key, value in result.items() if isinstance(value, int)},
                },
            )
            return JobRunOutcome(job.name, ran=True, result=result)

    async def run_named_job(self, name: str) -> JobRunOutcome:
        jobs = {job.name: job for job in load_jobs_config(self.settings.jobs_config_path)}
        if name not in jobs:
            raise ValueError(f"unknown job: {name}")
        return await self.run_job(jobs[name], only_if_due=False, trigger="run-job")

    def _install_signal_handlers(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self.request_stop)
        except (NotImplementedError, RuntimeError):
            return

    async def _wait_for_next_poll(self) -> None:
        if self.sleep is asyncio.sleep:
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(), timeout=self.settings.worker_poll_seconds
                )
            except TimeoutError:
                return
        else:
            await self.sleep(self.settings.worker_poll_seconds)


async def list_job_rows(
    *,
    settings: Settings | None = None,
    session_factory: async_sessionmaker | None = None,
) -> list[tuple[JobConfig, ScheduledJobState | None]]:
    settings = settings or get_settings()
    session_factory = session_factory or SessionLocal
    jobs = load_jobs_config(settings.jobs_config_path)
    rows = []
    async with session_factory() as session:
        for job in jobs:
            rows.append((job, await get_job_state(session, job.name)))
    return rows
