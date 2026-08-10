from collections.abc import AsyncIterator

import httpx

from gdpirate.collectors.base import CandidateLink, CollectorContext
from gdpirate.config import Settings, get_settings
from gdpirate.core.drive_urls import DISCOVERY_TERMS, extract_google_urls
from gdpirate.core.http import request_with_retries


class BlueskyCollector:
    name = "bluesky"
    source_name = "Bluesky"

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def collect(
        self, context: CollectorContext, *, max_items: int | None = None
    ) -> AsyncIterator[CandidateLink]:
        emitted = 0
        endpoint = f"{self.settings.bluesky_api_base.rstrip('/')}/xrpc/app.bsky.feed.searchPosts"
        for term in DISCOVERY_TERMS:
            scope = f"search/{term}"
            state = context.get_cursor(scope)
            cursor = state.get("cursor")
            offset = int(state.get("offset", 0))
            while max_items is None or emitted < max_items:
                response = await request_with_retries(
                    context.client,
                    "GET",
                    endpoint,
                    params={"q": term, "limit": 100, **({"cursor": cursor} if cursor else {})},
                )
                if response.status_code in {401, 403}:
                    context.unavailable = True
                    context.error = f"anonymous search unavailable: {response.status_code}"
                    return
                if response.status_code == 429:
                    context.error = "rate limited"
                    return
                response.raise_for_status()
                payload = response.json()
                posts = payload.get("posts") or []
                if not posts:
                    break
                for index, post in enumerate(posts[offset:], start=offset):
                    if max_items is not None and context.scanned >= max_items:
                        await context.checkpoint(
                            scope, {"cursor": cursor, "offset": index}
                        )
                        return
                    context.mark_scanned()
                    source_url = _bsky_source_url(post)
                    for raw_url in _post_urls(post):
                        yield CandidateLink(
                            raw_url=raw_url,
                            source_name=self.source_name,
                            source_url=source_url,
                        )
                        emitted += 1
                        if max_items is not None and emitted >= max_items:
                            await context.checkpoint(
                                scope, {"cursor": cursor, "offset": index + 1}
                            )
                            return
                cursor = payload.get("cursor")
                offset = 0
                await context.checkpoint(scope, {"cursor": cursor, "offset": 0})
                if not cursor:
                    break


def _post_urls(post: dict) -> list[str]:
    record = post.get("record") or {}
    values = [str(record.get("text") or "")]
    for facet in record.get("facets") or []:
        for feature in facet.get("features") or []:
            uri = feature.get("uri")
            if uri:
                values.append(str(uri))
    embed = post.get("embed") or record.get("embed") or {}
    external = embed.get("external") or {}
    if external.get("uri"):
        values.append(str(external["uri"]))
    urls: list[str] = []
    for value in values:
        urls.extend(extract_google_urls(value))
    return urls


def _bsky_source_url(post: dict) -> str | None:
    author = post.get("author") or {}
    handle = author.get("handle") or author.get("did")
    uri = post.get("uri") or ""
    rkey = uri.rstrip("/").split("/")[-1] if uri else None
    if handle and rkey:
        return f"https://bsky.app/profile/{handle}/post/{rkey}"
    return None
