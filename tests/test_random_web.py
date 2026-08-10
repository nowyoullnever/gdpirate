from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from gdpirate.config import Settings
from gdpirate.core.models import AccessStatus, Base, DriveLink, ResourceType
from gdpirate.pipeline.random_selection import RandomLink, RandomLinkService
from gdpirate.web import app


class StaticChecker:
    def __init__(self, status):
        self.status = status
        self.calls = 0

    async def check(self, raw_url):
        self.calls += 1
        return self.status


async def test_random_selection_rechecks_stale_without_order_by_random(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'random.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        async with session.begin():
            session.add(
                DriveLink(
                    provider="google",
                    resource_id="ABC123",
                    resource_type=ResourceType.FILE,
                    canonical_url="https://drive.google.com/file/d/ABC123/view",
                    random_key=0.25,
                    source_name="manual",
                    source_url="https://source.example/post",
                    access_status=AccessStatus.PUBLIC,
                    last_checked_at=datetime.now(UTC) - timedelta(hours=25),
                )
            )

    checker = StaticChecker(AccessStatus.PUBLIC)
    service = RandomLinkService(
        settings=Settings(access_recheck_hours=24),
        session_factory=maker,
        access_checker=checker,
    )
    link = await service.pick()

    assert link == RandomLink(
        url="https://drive.google.com/file/d/ABC123/view",
        source_name="manual",
        source_url="https://source.example/post",
    )
    assert checker.calls == 1
    await engine.dispose()


def test_random_api_no_store_and_empty_error(monkeypatch):
    class EmptyService:
        async def pick(self):
            return None

    monkeypatch.setattr("gdpirate.web.RandomLinkService", lambda: EmptyService())
    response = TestClient(app).get("/api/random")

    assert response.status_code == 503
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {"error": "no_verified_public_link_available"}
