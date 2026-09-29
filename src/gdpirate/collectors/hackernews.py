from collections.abc import AsyncIterator

import httpx

from gdpirate.collectors.base import CandidateLink, CollectorContext, distinct_google_urls
from gdpirate.core.drive_urls import DISCOVERY_TERMS, extract_google_urls
from gdpirate.core.http import request_with_retries


class HackerNewsCollector:
    name = "hackernews"
    source_name = "Hacker News"
    base_url = "https://hn.algolia.com/api/v1/search_by_date"

    async def collect(
        self, context: CollectorContext, *, max_items: int | None = None
    ) -> AsyncIterator[CandidateLink]:
        for term in DISCOVERY_TERMS:
            for tag in ("story", "comment"):
                scope = f"{tag}/{term}"
                page = int(context.get_cursor(scope).get("page", 0))
                while True:
                    response = await request_with_retries(
                        context.client,
                        "GET",
                        self.base_url,
                        params={"query": term, "tags": tag, "page": page},
                    )
                    if response.status_code == 429:
                        context.error = "rate limited"
                        return
                    response.raise_for_status()
                    payload = response.json()
                    hits = payload.get("hits") or []
                    if not hits:
                        break
                    offset = int(context.get_cursor(scope).get("offset", 0))
                    for index, hit in enumerate(hits[offset:], start=offset):
                        if max_items is not None and context.scanned >= max_items:
                            await context.checkpoint(scope, {"page": page, "offset": index})
                            return
                        context.mark_scanned()
                        source_url = _hn_source_url(hit)
                        text = " ".join(
                            str(hit.get(key) or "")
                            for key in ("url", "title", "story_text", "comment_text")
                        )
                        for raw_url in distinct_google_urls(extract_google_urls(text)):
                            yield CandidateLink(
                                raw_url=raw_url,
                                source_name=self.source_name,
                                source_url=source_url,
                            )
                    page += 1
                    await context.checkpoint(scope, {"page": page, "offset": 0})


def _hn_source_url(hit: dict) -> str | None:
    object_id = hit.get("objectID")
    return f"https://news.ycombinator.com/item?id={object_id}" if object_id else None
