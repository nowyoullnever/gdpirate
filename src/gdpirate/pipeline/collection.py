import asyncio
from collections.abc import Callable
from dataclasses import dataclass

import httpx
from sqlalchemy.ext.asyncio import async_sessionmaker

from gdpirate.collectors.base import CandidateLink, Collector, CollectorContext
from gdpirate.collectors.bluesky import BlueskyCollector
from gdpirate.collectors.feeds import FeedCollector
from gdpirate.collectors.fediverse import FediverseCollector
from gdpirate.collectors.hackernews import HackerNewsCollector
from gdpirate.collectors.lemmy import LemmyCollector
from gdpirate.collectors.misskey import MisskeyCollector
from gdpirate.collectors.nostr import NostrCollector
from gdpirate.config import Settings, get_settings
from gdpirate.core.access_check import AccessChecker
from gdpirate.core.database import SessionLocal
from gdpirate.core.http import HttpClientFactory
from gdpirate.pipeline.ingestion import IngestionService
from gdpirate.pipeline.state import (
    load_collector_cursors,
    record_collector_attempt,
    record_collector_error,
    record_collector_success,
)


@dataclass
class CollectionResult:
    source: str
    scanned: int = 0
    candidates: int = 0
    created: int = 0
    duplicates: int = 0
    public: int = 0
    restricted: int = 0
    dead: int = 0
    unknown: int = 0
    unavailable: bool = False
    error: str | None = None


def build_collectors(settings: Settings | None = None) -> dict[str, Collector]:
    settings = settings or get_settings()
    return {
        "hackernews": HackerNewsCollector(),
        "bluesky": BlueskyCollector(settings),
        "lemmy": LemmyCollector(settings),
        "misskey": MisskeyCollector(settings),
        "feeds": FeedCollector(settings),
        "fediverse": FediverseCollector(settings),
        "nostr": NostrCollector(settings),
    }


def source_statuses(settings: Settings | None = None) -> dict[str, str]:
    return {
        "hackernews": "enabled",
        "bluesky": "enabled",
        "lemmy": "enabled",
        "misskey": "enabled",
        "feeds": "enabled",
        "fediverse": "enabled",
        "nostr": "enabled",
        "gdurl": "enabled" if (settings or get_settings()).enable_gdurl else "disabled",
        "dedigger": "enabled"
        if (settings or get_settings()).enable_dedigger
        else "disabled",
        "commoncrawl": "enabled"
        if (settings or get_settings()).enable_common_crawl
        else "disabled",
    }


class CollectionRunner:
    def __init__(
        self,
        settings: Settings | None = None,
        session_factory: async_sessionmaker | None = None,
        collectors: dict[str, Collector] | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.session_factory = session_factory or SessionLocal
        self.collectors = collectors or build_collectors(self.settings)

    async def collect(
        self,
        source: str,
        *,
        max_items: int | None = None,
        max_items_per_source: int | None = None,
    ) -> list[CollectionResult]:
        if source == "all":
            results = []
            for name in self.collectors:
                results.append(
                    await self._collect_one(
                        name, max_items=max_items_per_source or max_items
                    )
                )
            return results
        if source not in self.collectors:
            raise ValueError(f"unknown collector: {source}")
        return [await self._collect_one(source, max_items=max_items)]

    async def _collect_one(
        self, source: str, *, max_items: int | None = None
    ) -> CollectionResult:
        result = CollectionResult(source=source)
        async with self.session_factory() as session:
            async with session.begin():
                await record_collector_attempt(session, source)
                cursor = await load_collector_cursors(session, source)

        factory = HttpClientFactory(self.settings)
        async with factory.client() as client:
            async def checkpoint(scope: str, value: dict) -> None:
                async with self.session_factory() as session:
                    async with session.begin():
                        await record_collector_success(session, source, scope, value)

            context = CollectorContext(
                client=client, cursor=cursor, checkpoint_callback=checkpoint
            )
            queue: asyncio.Queue[CandidateLink | None] = asyncio.Queue(
                maxsize=self.settings.http_max_concurrency
            )
            workers = [
                asyncio.create_task(self._ingest_worker(queue, client, result))
                for _ in range(self.settings.http_max_concurrency)
            ]
            try:
                async for candidate in self.collectors[source].collect(
                    context, max_items=max_items
                ):
                    await queue.put(candidate)
            except Exception as exc:
                result.error = str(exc)
            finally:
                for _ in workers:
                    await queue.put(None)
                await asyncio.gather(*workers)

        result.unavailable = context.unavailable
        result.scanned = context.scanned
        result.error = result.error or context.error
        async with self.session_factory() as session:
            async with session.begin():
                if result.error:
                    await record_collector_error(session, source, "default", result.error)
                else:
                    await record_collector_success(session, source, "default", {})
        return result

    async def _ingest_worker(
        self,
        queue: asyncio.Queue[CandidateLink | None],
        client: httpx.AsyncClient,
        result: CollectionResult,
    ) -> None:
        while True:
            candidate = await queue.get()
            if candidate is None:
                queue.task_done()
                return
            async with self.session_factory() as session:
                async with session.begin():
                    service = IngestionService(
                        session,
                        AccessChecker(self.settings, client),
                        settings=self.settings,
                        check_access=False,
                    )
                    ingestion = await service.ingest(candidate)
            if ingestion.valid and ingestion.access_check_needed:
                status = await AccessChecker(self.settings, client).check(
                    ingestion.canonical_url or candidate.raw_url
                )
                async with self.session_factory() as session:
                    async with session.begin():
                        service = IngestionService(
                            session,
                            AccessChecker(self.settings, client),
                            settings=self.settings,
                            check_access=False,
                        )
                        if ingestion.provider and ingestion.resource_id:
                            await service.update_access_status(
                                ingestion.provider, ingestion.resource_id, status
                            )
                ingestion = IngestionResultWithStatus(ingestion, status)
            if ingestion.valid:
                result.candidates += 1
                result.created += int(ingestion.created)
                result.duplicates += int(ingestion.duplicate)
                field = ingestion.access_status.value.lower()
                setattr(result, field, getattr(result, field) + 1)
            queue.task_done()


class IngestionResultWithStatus:
    def __init__(self, original, status):
        self.__dict__.update(original.__dict__)
        self.access_status = status
