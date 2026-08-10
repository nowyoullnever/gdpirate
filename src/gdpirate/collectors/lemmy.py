from collections.abc import AsyncIterator

from gdpirate.collectors.base import CandidateLink, CollectorContext
from gdpirate.config import Settings, get_settings
from gdpirate.core.drive_urls import DISCOVERY_TERMS, extract_google_urls
from gdpirate.core.http import request_with_retries


class LemmyCollector:
    name = "lemmy"
    source_name = "Lemmy"

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def collect(
        self, context: CollectorContext, *, max_items: int | None = None
    ) -> AsyncIterator[CandidateLink]:
        emitted = 0
        for instance in self.settings.lemmy_instance_list:
            for term in DISCOVERY_TERMS:
                for type_name in ("Posts", "Comments"):
                    scope = f"{instance}/{type_name}/{term}"
                    page = int(context.get_cursor(scope).get("page", 1))
                    while max_items is None or emitted < max_items:
                        try:
                            response = await request_with_retries(
                                context.client,
                                "GET",
                                f"{instance}/api/v3/search",
                                params={
                                    "q": term,
                                    "type_": type_name,
                                    "page": page,
                                    "limit": 50,
                                },
                            )
                            if response.status_code in {401, 403, 429}:
                                break
                            response.raise_for_status()
                        except Exception as exc:
                            context.error = str(exc)
                            break
                        payload = response.json()
                        items = (
                            payload.get("posts")
                            if type_name == "Posts"
                            else payload.get("comments")
                        ) or []
                        if not items:
                            break
                        for item in items:
                            if max_items is not None and context.scanned >= max_items:
                                await context.checkpoint(scope, {"page": page})
                                return
                            context.mark_scanned()
                            source_url = _lemmy_source_url(item, type_name)
                            text = _lemmy_text(item, type_name)
                            for raw_url in extract_google_urls(text):
                                yield CandidateLink(
                                    raw_url=raw_url,
                                    source_name=self.source_name,
                                    source_url=source_url,
                                )
                                emitted += 1
                                if max_items is not None and emitted >= max_items:
                                    await context.checkpoint(scope, {"page": page})
                                    return
                        page += 1
                        await context.checkpoint(scope, {"page": page})


def _lemmy_text(item: dict, type_name: str) -> str:
    if type_name == "Posts":
        post = item.get("post") or item
        return " ".join(
            str(post.get(key) or "") for key in ("url", "name", "body")
        )
    comment = item.get("comment") or item
    return str(comment.get("content") or "")


def _lemmy_source_url(item: dict, type_name: str) -> str | None:
    record = item.get("post") if type_name == "Posts" else item.get("comment")
    record = record or item
    return record.get("ap_id") or record.get("url")
