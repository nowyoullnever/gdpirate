from __future__ import annotations

from collections.abc import AsyncIterator, Callable
import asyncio
import json
import time
from uuid import uuid4

import websockets

from gdpirate.collectors.base import CandidateLink, CollectorContext, distinct_google_urls
from gdpirate.config import Settings, get_settings
from gdpirate.core.bech32 import note_id_from_event_id
from gdpirate.core.drive_urls import extract_google_urls


class NostrCollector:
    name = "nostr"
    source_name = "Nostr"

    def __init__(
        self,
        settings: Settings | None = None,
        connector: Callable | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.connector = connector or websockets.connect
        self._seen_event_ids: set[str] = set()

    async def collect(
        self, context: CollectorContext, *, max_items: int | None = None
    ) -> AsyncIterator[CandidateLink]:
        self._seen_event_ids = set()
        for relay in self.settings.nostr_relay_list:
            scope = relay
            state = context.get_cursor(scope)
            until = int(state.get("until") or time.time())
            boundary_event_ids = set(state.get("boundary_event_ids") or [])
            oldest_seen = until
            events_seen = 0
            oldest_event_ids: set[str] = set()
            try:
                async with self.connector(relay, open_timeout=10) as websocket:
                    sub_id = f"gdpirate-{uuid4().hex[:8]}"
                    await websocket.send(
                        json.dumps(
                            [
                                "REQ",
                                sub_id,
                                {
                                    "kinds": [1, 30023],
                                    "until": until,
                                    "limit": self.settings.nostr_batch_limit,
                                },
                            ]
                        )
                    )
                    while True:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=20)
                        except TimeoutError:
                            break
                        payload = json.loads(message)
                        msg_type = payload[0] if payload else None
                        if msg_type == "EVENT" and len(payload) >= 3:
                            event = payload[2]
                            event_id = event.get("id")
                            created_at = int(event.get("created_at") or until)
                            if created_at == until and event_id in boundary_event_ids:
                                continue
                            if event_id in self._seen_event_ids:
                                continue
                            if event_id:
                                self._seen_event_ids.add(event_id)
                            if max_items is not None and context.scanned >= max_items:
                                return
                            context.mark_scanned()
                            events_seen += 1
                            if created_at < oldest_seen:
                                oldest_seen = created_at
                                oldest_event_ids = {event_id} if event_id else set()
                            elif created_at == oldest_seen and event_id:
                                oldest_event_ids.add(event_id)
                            text = _event_text(event)
                            source_url = nostr_source_url(
                                self.settings.nostr_viewer_base, event_id
                            )
                            for raw_url in distinct_google_urls(extract_google_urls(text)):
                                yield CandidateLink(
                                    raw_url=raw_url,
                                    source_name=self.source_name,
                                    source_url=source_url,
                                )
                            if max_items is not None and context.scanned >= max_items:
                                return
                        elif msg_type == "EOSE":
                            if events_seen >= self.settings.nostr_batch_limit and oldest_seen == until:
                                await context.checkpoint(
                                    scope,
                                    {
                                        "until": until,
                                        "boundary_event_ids": sorted(oldest_event_ids),
                                        "status": "saturated_timestamp_boundary",
                                    },
                                )
                            else:
                                await context.checkpoint(
                                    scope,
                                    {
                                        "until": oldest_seen,
                                        "boundary_event_ids": sorted(oldest_event_ids),
                                    },
                                )
                            break
                        elif msg_type in {"CLOSED", "NOTICE"}:
                            text = str(payload[-1]).lower() if payload else ""
                            if any(word in text for word in ("restricted", "auth", "rate")):
                                context.error = text
                            break
            except Exception as exc:
                context.error = str(exc)
                continue


def _event_text(event: dict) -> str:
    parts = [event.get("content")]
    for tag in event.get("tags") or []:
        if isinstance(tag, list) and tag and tag[0] in {"r", "url"} and len(tag) > 1:
            parts.append(tag[1])
    return " ".join(str(part or "") for part in parts)


def nostr_source_url(viewer_base: str, event_id: str | None) -> str | None:
    if not event_id:
        return None
    return f"{viewer_base.rstrip('/')}/{note_id_from_event_id(event_id)}"
