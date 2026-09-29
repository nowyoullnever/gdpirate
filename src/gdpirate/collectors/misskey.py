from collections.abc import AsyncIterator

from gdpirate.collectors.base import CandidateLink, CollectorContext, distinct_google_urls
from gdpirate.config import Settings, get_settings
from gdpirate.core.drive_urls import DISCOVERY_TERMS, extract_google_urls
from gdpirate.core.http import request_with_retries


class MisskeyCollector:
    name = "misskey"
    source_name = "Misskey"

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def collect(
        self, context: CollectorContext, *, max_items: int | None = None
    ) -> AsyncIterator[CandidateLink]:
        for instance in self.settings.misskey_instance_list:
            for term in DISCOVERY_TERMS:
                scope = f"{instance}/{term}"
                offset = int(context.get_cursor(scope).get("offset", 0))
                while True:
                    response = await request_with_retries(
                        context.client,
                        "POST",
                        f"{instance}/api/notes/search",
                        json={"query": term, "limit": 50, "offset": offset},
                    )
                    if response.status_code in {401, 403}:
                        context.unavailable = True
                        context.error = f"search unavailable: {response.status_code}"
                        break
                    if response.status_code == 429:
                        context.error = "rate limited"
                        break
                    response.raise_for_status()
                    notes = response.json()
                    if not notes:
                        break
                    for note in notes:
                        if max_items is not None and context.scanned >= max_items:
                            await context.checkpoint(scope, {"offset": offset})
                            return
                        context.mark_scanned()
                        source_url = _misskey_source_url(instance, note)
                        text = " ".join(
                            str(note.get(key) or "") for key in ("text", "url", "uri")
                        )
                        for raw_url in distinct_google_urls(extract_google_urls(text)):
                            yield CandidateLink(
                                raw_url=raw_url,
                                source_name=self.source_name,
                                source_url=source_url,
                            )
                    offset += len(notes)
                    await context.checkpoint(scope, {"offset": offset})


def _misskey_source_url(instance: str, note: dict) -> str | None:
    note_id = note.get("id")
    return f"{instance}/notes/{note_id}" if note_id else note.get("url") or note.get("uri")
