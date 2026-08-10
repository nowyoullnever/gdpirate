from __future__ import annotations

from collections.abc import AsyncIterator
import asyncio
import html
import re
from urllib.parse import urljoin

import httpx

from gdpirate.collectors.base import CandidateLink, CollectorContext
from gdpirate.config import Settings, get_settings
from gdpirate.core.drive_urls import extract_google_urls
from gdpirate.core.robots import RobotsPolicy

LINK_RE = re.compile(r"""href=["']([^"']+)["']""", re.IGNORECASE)
ANTI_BOT_MARKERS = ("captcha", "challenge", "too many requests", "access denied")


class GdUrlCollector:
    name = "gdurl"
    source_name = "gdURL"
    bulk = True

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def collect(
        self, context: CollectorContext, *, max_items: int | None = None
    ) -> AsyncIterator[CandidateLink]:
        page_url = context.get_cursor("catalogue").get("page_url") or self.settings.gdurl_browse_url
        offset = int(context.get_cursor("catalogue").get("offset", 0))
        robots = RobotsPolicy(self.settings.user_agent)
        decision = await robots.allowed(context.client, page_url)
        if not decision.allowed:
            context.unavailable = True
            context.error = decision.error or "robots disallow"
            return

        while page_url:
            response = await context.client.get(page_url)
            if response.status_code in {403, 429}:
                context.error = f"gdURL blocked: {response.status_code}"
                return
            if response.status_code >= 400:
                context.error = f"gdURL failed: {response.status_code}"
                return
            if _looks_antibot(response.text):
                context.error = "gdURL anti-bot challenge"
                return

            entries = _catalogue_entries(response.text, page_url)
            next_url = _next_url(response.text, page_url)
            for index, permalink in enumerate(entries[offset:], start=offset):
                if max_items is not None and context.scanned >= max_items:
                    await context.checkpoint(
                        "catalogue", {"page_url": page_url, "offset": index}
                    )
                    return
                context.mark_scanned()
                for raw_url in await resolve_gdurl_permalink(
                    context.client, permalink, self.settings.gdurl_resolve_max_body_bytes
                ):
                    yield CandidateLink(
                        raw_url=raw_url,
                        source_name=self.source_name,
                        source_url=permalink,
                    )
                await asyncio.sleep(self.settings.gdurl_request_delay_seconds)
            page_url = next_url
            offset = 0
            await context.checkpoint("catalogue", {"page_url": page_url, "offset": 0})
            if not page_url:
                return


async def resolve_gdurl_permalink(
    client: httpx.AsyncClient, permalink: str, max_body_bytes: int
) -> list[str]:
    urls: list[str] = []
    async with client.stream("GET", permalink, follow_redirects=True) as response:
        for history in response.history:
            location = history.headers.get("Location")
            if location:
                urls.extend(extract_google_urls(location))
        urls.extend(extract_google_urls(str(response.url)))
        if urls:
            return urls
        content_type = response.headers.get("Content-Type", "").lower()
        if content_type and "html" not in content_type and "text" not in content_type:
            return []
        body = bytearray()
        async for chunk in response.aiter_bytes():
            remaining = max_body_bytes - len(body)
            if remaining <= 0:
                break
            body.extend(chunk[:remaining])
            if len(body) >= max_body_bytes:
                break
    return extract_google_urls(body.decode("utf-8", errors="ignore"))


def _catalogue_entries(text: str, base_url: str) -> list[str]:
    links = [urljoin(base_url, html.unescape(match)) for match in LINK_RE.findall(text)]
    return [
        link
        for link in links
        if "gdurl.com" in link and "/all" not in link and not link.endswith("/robots.txt")
    ]


def _next_url(text: str, base_url: str) -> str | None:
    for raw in LINK_RE.findall(text):
        lower = raw.lower()
        if "next" in lower or "page" in lower:
            return urljoin(base_url, html.unescape(raw))
    return None


def _looks_antibot(text: str) -> bool:
    lower = text.lower()
    return any(marker in lower for marker in ANTI_BOT_MARKERS)
