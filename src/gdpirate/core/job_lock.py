from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import hashlib
import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from gdpirate.config import Settings, get_settings
from gdpirate.core.database import engine

logger = logging.getLogger(__name__)
_sqlite_locks: dict[str, asyncio.Lock] = {}


def advisory_lock_key(job_name: str) -> int:
    digest = hashlib.sha256(f"GDPirate scheduled job:{job_name}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


class JobLock:
    def __init__(
        self,
        job_name: str,
        *,
        settings: Settings | None = None,
        session_factory: async_sessionmaker | None = None,
        lock_engine=None,
    ) -> None:
        self.job_name = job_name
        self.settings = settings or get_settings()
        self.session_factory = session_factory
        self.lock_engine = lock_engine or engine
        self._sqlite_lock: asyncio.Lock | None = None
        self._connection = None
        self.acquired = False

    async def __aenter__(self) -> "JobLock":
        if self.settings.async_database_url.startswith("postgresql+asyncpg://"):
            self._connection = await self.lock_engine.connect()
            acquired = await self._connection.scalar(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": advisory_lock_key(self.job_name)},
            )
            await self._connection.commit()
            self.acquired = bool(acquired)
            if not self.acquired:
                await self._connection.close()
                self._connection = None
            return self

        lock = _sqlite_locks.setdefault(self.job_name, asyncio.Lock())
        if lock.locked():
            self.acquired = False
            return self
        await lock.acquire()
        self._sqlite_lock = lock
        self.acquired = True
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._connection is not None:
            try:
                try:
                    await self._connection.execute(
                        text("SELECT pg_advisory_unlock(:key)"),
                        {"key": advisory_lock_key(self.job_name)},
                    )
                    await self._connection.commit()
                except Exception:
                    logger.exception("job lock cleanup failed", extra={"job": self.job_name})
                    if exc_type is None:
                        raise
            finally:
                try:
                    await self._connection.close()
                except Exception:
                    logger.exception("job lock connection close failed", extra={"job": self.job_name})
                    if exc_type is None:
                        raise
        if self._sqlite_lock is not None and self._sqlite_lock.locked():
            self._sqlite_lock.release()


@asynccontextmanager
async def acquire_job_lock(job_name: str, *, settings: Settings | None = None):
    async with JobLock(job_name, settings=settings) as lock:
        yield lock
