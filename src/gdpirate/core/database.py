from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from gdpirate.config import get_settings


def make_engine(database_url: str | None = None):
    settings = get_settings()
    return create_async_engine(database_url or settings.async_database_url, future=True)


engine = make_engine()
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        async with session.begin():
            yield session
