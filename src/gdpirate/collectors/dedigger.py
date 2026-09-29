from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
import asyncio
import tomllib
from urllib.parse import urlencode

from gdpirate.collectors.base import CandidateLink, CollectorContext, distinct_google_urls
from gdpirate.config import Settings, get_settings
from gdpirate.core.drive_urls import extract_google_urls
from gdpirate.core.robots import RobotsPolicy

BLOCK_MARKERS = ("captcha", "challenge", "sign in", "login required", "blocked")


class DeDiggerCollector:
    name = "dedigger"
    source_name = "deDigger"
    bulk = True

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def collect(
        self, context: CollectorContext, *, max_items: int | None = None
    ) -> AsyncIterator[CandidateLink]:
        queries = _load_queries(self.settings.dedigger_query_config_path)
        robots = RobotsPolicy(self.settings.user_agent)
        for query in queries:
            scope = query
            state = context.get_cursor(scope)
            if state.get("complete"):
                continue
            page = int(state.get("page", 1))
            offset = int(state.get("offset", 0))
            while True:
                search_url = _search_url(self.settings.dedigger_base_url, query, page)
                decision = await robots.allowed(context.client, search_url)
                if not decision.allowed:
                    context.unavailable = True
                    context.error = decision.error or "robots disallow"
                    return
                response = await context.client.get(search_url)
                if response.status_code in {401, 403, 429}:
                    context.unavailable = True
                    context.error = f"deDigger unavailable: {response.status_code}"
                    return
                if response.status_code >= 400:
                    await context.checkpoint(scope, {"page": page, "offset": offset})
                    break
                if _blocked(response.text):
                    context.unavailable = True
                    context.error = "deDigger blocked or challenge page"
                    return
                results = distinct_google_urls(extract_google_urls(response.text))
                if not results:
                    await context.checkpoint(scope, {"page": page, "offset": offset, "complete": True})
                    break
                for index, raw_url in enumerate(results[offset:], start=offset):
                    if max_items is not None and context.scanned >= max_items:
                        await context.checkpoint(scope, {"page": page, "offset": index})
                        return
                    context.mark_scanned()
                    yield CandidateLink(
                        raw_url=raw_url,
                        source_name=self.source_name,
                        source_url=search_url,
                    )
                page += 1
                offset = 0
                await context.checkpoint(scope, {"page": page, "offset": 0})
                await asyncio.sleep(self.settings.dedigger_request_delay_seconds)


def _load_queries(path: str) -> list[str]:
    config_path = Path(path)
    if not config_path.exists():
        return []
    with config_path.open("rb") as handle:
        payload = tomllib.load(handle)
    return [
        item["query"]
        for item in payload.get("queries", [])
        if item.get("enabled", True) and item.get("query")
    ]


def _search_url(base_url: str, query: str, page: int) -> str:
    return f"{base_url.rstrip('/')}/?{urlencode({'q': query, 'page': page})}"


def _blocked(text: str) -> bool:
    lower = text.lower()
    return any(marker in lower for marker in BLOCK_MARKERS)
