from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
import tomllib

import httpx

from gdpirate.collectors.base import CandidateLink, CollectorContext
from gdpirate.config import Settings, get_settings
from gdpirate.core.drive_urls import extract_google_urls
from gdpirate.core.http import request_with_retries


@dataclass(frozen=True)
class FediverseInstance:
    url: str
    enabled: bool = True


@dataclass(frozen=True)
class FediverseDetection:
    url: str
    software: str
    compatible: bool
    public_timeline_available: bool
    status: str

    @property
    def source_name(self) -> str:
        normalized = self.software.lower()
        if normalized in {"mastodon", "pleroma", "akkoma", "pixelfed"}:
            return normalized.capitalize()
        return "Fediverse"


class FediverseCollector:
    name = "fediverse"

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def collect(
        self, context: CollectorContext, *, max_items: int | None = None
    ) -> AsyncIterator[CandidateLink]:
        emitted = 0
        for instance in load_fediverse_instances(
            self.settings.fediverse_instance_config_path
        ):
            if not instance.enabled:
                continue
            detection = await detect_fediverse_instance(context.client, instance.url)
            await context.checkpoint(
                f"{instance.url}/detection",
                {
                    "software": detection.software,
                    "status": detection.status,
                    "compatible": detection.compatible,
                },
            )
            if not detection.compatible or not detection.public_timeline_available:
                continue
            scope = instance.url
            max_id = context.get_cursor(scope).get("max_id")
            while max_items is None or emitted < max_items:
                response = await request_with_retries(
                    context.client,
                    "GET",
                    f"{instance.url}/api/v1/timelines/public",
                    params={
                        "limit": 40,
                        "local": str(self.settings.fediverse_local_only).lower(),
                        **({"max_id": max_id} if max_id else {}),
                    },
                )
                if response.status_code in {401, 403}:
                    await context.checkpoint(scope, {"status": "authentication_required"})
                    break
                if response.status_code == 429:
                    await context.checkpoint(scope, {"status": "rate_limited"})
                    break
                if response.status_code >= 400:
                    await context.checkpoint(scope, {"status": "temporarily_failed"})
                    break
                statuses = response.json()
                if not statuses:
                    break
                next_max_id = None
                for status in statuses:
                    if max_items is not None and context.scanned >= max_items:
                        await context.checkpoint(scope, {"max_id": max_id})
                        return
                    context.mark_scanned()
                    if status.get("visibility") != "public":
                        continue
                    next_max_id = status.get("id") or next_max_id
                    text = _status_text(status)
                    source_url = status.get("url") or status.get("uri")
                    for raw_url in extract_google_urls(text):
                        yield CandidateLink(
                            raw_url=raw_url,
                            source_name=detection.source_name,
                            source_url=source_url,
                        )
                        emitted += 1
                        if max_items is not None and emitted >= max_items:
                            await context.checkpoint(scope, {"max_id": max_id})
                            return
                max_id = next_max_id
                await context.checkpoint(scope, {"max_id": max_id})
                if not max_id:
                    break


async def detect_fediverse_instance(
    client: httpx.AsyncClient, base_url: str
) -> FediverseDetection:
    base_url = base_url.rstrip("/")
    software = "unknown"
    try:
        nodeinfo = await client.get(f"{base_url}/.well-known/nodeinfo")
        if nodeinfo.status_code < 400:
            links = nodeinfo.json().get("links") or []
            href = next((link.get("href") for link in links if link.get("href")), None)
            if href:
                info = await client.get(href)
                if info.status_code < 400:
                    software = (
                        (info.json().get("software") or {}).get("name") or software
                    ).lower()
        if software == "unknown":
            instance = await client.get(f"{base_url}/api/v2/instance")
            if instance.status_code >= 400:
                instance = await client.get(f"{base_url}/api/v1/instance")
            if instance.status_code < 400:
                version = str(instance.json().get("version") or "").lower()
                software = _software_from_version(version)
        timeline = await client.get(
            f"{base_url}/api/v1/timelines/public",
            params={"limit": 1, "local": "true"},
        )
        if timeline.status_code in {401, 403}:
            return FediverseDetection(
                base_url, software, True, False, "authentication_required"
            )
        if timeline.status_code == 429:
            return FediverseDetection(base_url, software, True, False, "rate_limited")
        compatible = timeline.status_code < 500 and timeline.status_code != 404
        return FediverseDetection(
            base_url,
            software,
            compatible,
            timeline.status_code < 400,
            "available" if timeline.status_code < 400 else "unsupported",
        )
    except Exception:
        return FediverseDetection(base_url, software, False, False, "temporarily_failed")


def load_fediverse_instances(path: str) -> list[FediverseInstance]:
    config_path = Path(path)
    if not config_path.exists():
        return []
    with config_path.open("rb") as handle:
        payload = tomllib.load(handle)
    return [
        FediverseInstance(url=item["url"].rstrip("/"), enabled=item.get("enabled", True))
        for item in payload.get("instances", [])
        if item.get("url")
    ]


def _software_from_version(version: str) -> str:
    for name in ("mastodon", "pleroma", "akkoma", "pixelfed"):
        if name in version:
            return name
    return "unknown"


def _status_text(status: dict) -> str:
    card = status.get("card") or {}
    parts = [
        status.get("content"),
        status.get("spoiler_text"),
        card.get("url"),
        card.get("title"),
        card.get("description"),
    ]
    return " ".join(str(part or "") for part in parts)
