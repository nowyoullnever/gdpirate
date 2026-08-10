from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
import hashlib
import json
import tomllib
from xml.etree import ElementTree

from gdpirate.collectors.base import CandidateLink, CollectorContext
from gdpirate.config import Settings, get_settings
from gdpirate.core.drive_urls import extract_google_urls
from gdpirate.core.http import request_with_retries


class FeedCollector:
    name = "feeds"
    source_name = "Feeds"

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def collect(
        self, context: CollectorContext, *, max_items: int | None = None
    ) -> AsyncIterator[CandidateLink]:
        emitted = 0
        for feed in _load_feed_config(self.settings.feed_config_path):
            feed_url = str(feed["url"])
            scope = feed_url
            state = context.get_cursor(scope)
            processed_entry_keys = set(state.get("processed_entry_keys") or [])
            headers = {}
            if not state.get("partial") and state.get("etag"):
                headers["If-None-Match"] = state["etag"]
            if not state.get("partial") and state.get("last_modified"):
                headers["If-Modified-Since"] = state["last_modified"]
            response = await request_with_retries(
                context.client, "GET", feed_url, headers=headers
            )
            if response.status_code == 304:
                await context.checkpoint(scope, state)
                continue
            if response.status_code >= 400:
                context.error = f"feed failed: {feed_url} {response.status_code}"
                continue
            entries = _parse_feed_entries(
                response.text,
                response.headers.get("Content-Type", ""),
            )
            for entry in entries:
                entry_key = _feed_entry_key(entry)
                if entry_key in processed_entry_keys:
                    continue
                if max_items is not None and context.scanned >= max_items:
                    await context.checkpoint(
                        scope, _partial_feed_state(state, processed_entry_keys)
                    )
                    return
                context.mark_scanned()
                processed_entry_keys.add(entry_key)
                source_url = entry.source_url or feed_url
                for raw_url in extract_google_urls(entry.text):
                    yield CandidateLink(
                        raw_url=raw_url,
                        source_name=str(feed.get("name") or self.source_name),
                        source_url=source_url,
                    )
                    emitted += 1
                    if max_items is not None and emitted >= max_items:
                        await context.checkpoint(
                            scope, _partial_feed_state(state, processed_entry_keys)
                        )
                        return
            latest = entries[0] if entries else None
            await context.checkpoint(scope, _feed_state(response, latest))


class FeedEntry:
    def __init__(
        self,
        *,
        source_url: str | None,
        text: str,
        entry_id: str | None = None,
        timestamp: str | None = None,
    ) -> None:
        self.source_url = source_url
        self.text = text
        self.entry_id = entry_id
        self.timestamp = timestamp


def _feed_state(response, entry: FeedEntry | None) -> dict:
    return {
        "etag": response.headers.get("ETag"),
        "last_modified": response.headers.get("Last-Modified"),
        "latest_entry_id": entry.entry_id if entry else None,
        "latest_entry_timestamp": entry.timestamp if entry else None,
        "partial": False,
        "processed_entry_keys": [],
    }


def _partial_feed_state(previous_state: dict, processed_entry_keys: set[str]) -> dict:
    return {
        "etag": previous_state.get("etag"),
        "last_modified": previous_state.get("last_modified"),
        "latest_entry_id": previous_state.get("latest_entry_id"),
        "latest_entry_timestamp": previous_state.get("latest_entry_timestamp"),
        "partial": True,
        "processed_entry_keys": sorted(processed_entry_keys)[-1000:],
    }


def _feed_entry_key(entry: FeedEntry) -> str:
    stable = entry.entry_id or entry.source_url
    if stable:
        return str(stable)
    return hashlib.sha256(entry.text.encode("utf-8")).hexdigest()


def _load_feed_config(path: str) -> list[dict]:
    config_path = Path(path)
    if not config_path.exists():
        return []
    with config_path.open("rb") as handle:
        payload = tomllib.load(handle)
    feeds = [feed for feed in payload.get("feeds", []) if feed.get("url")]
    feeds.extend(
        {
            "name": "Micro.blog",
            "url": f"https://{user['username']}.micro.blog/feed.xml",
            "enabled": user.get("enabled", True),
        }
        for user in payload.get("microblog_users", [])
        if user.get("username")
    )
    feeds.extend(
        {
            "name": "WriteFreely",
            "url": _writefreely_feed_url(blog["url"]),
            "enabled": blog.get("enabled", True),
        }
        for blog in payload.get("writefreely_blogs", [])
        if blog.get("url")
    )
    return [feed for feed in feeds if feed.get("enabled", True)]


def _writefreely_feed_url(url: str) -> str:
    return f"{url.rstrip('/')}/feed/"


def _parse_feed_entries(text: str, content_type: str = "") -> list[FeedEntry]:
    stripped = text.lstrip()
    if "json" in content_type.lower() or stripped.startswith("{"):
        return _parse_json_feed(text)
    try:
        root = ElementTree.fromstring(text)
    except ElementTree.ParseError:
        return []

    if _strip_ns(root.tag) == "rss" or root.find("./channel") is not None:
        return [_rss_item(item) for item in root.findall(".//item")]
    if _strip_ns(root.tag) == "feed":
        return [_atom_entry(entry) for entry in root.findall("{*}entry")]
    return []


def _parse_json_feed(text: str) -> list[FeedEntry]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []
    entries = []
    for item in payload.get("items", []):
        source_url = item.get("url") or item.get("external_url")
        parts = [
            item.get("title"),
            item.get("summary"),
            item.get("content_text"),
            item.get("content_html"),
            source_url,
        ]
        entries.append(
            FeedEntry(
                source_url=source_url,
                text=" ".join(str(part or "") for part in parts),
                entry_id=item.get("id"),
                timestamp=item.get("date_published") or item.get("date_modified"),
            )
        )
    return entries


def _rss_item(item: ElementTree.Element) -> FeedEntry:
    link = _child_text(item, "link")
    entry_id = _child_text(item, "guid") or link
    timestamp = _child_text(item, "pubDate")
    parts = [
        _child_text(item, "title"),
        _child_text(item, "description"),
        _child_text(item, "summary"),
        _child_text(item, "content"),
        link,
    ]
    return FeedEntry(
        source_url=link,
        text=" ".join(part or "" for part in parts),
        entry_id=entry_id,
        timestamp=timestamp,
    )


def _atom_entry(entry: ElementTree.Element) -> FeedEntry:
    link = None
    for link_node in entry.findall("{*}link"):
        if link_node.get("rel") in {None, "alternate"} and link_node.get("href"):
            link = link_node.get("href")
            break
    entry_id = _child_text(entry, "id") or link
    timestamp = _child_text(entry, "updated") or _child_text(entry, "published")
    parts = [
        _child_text(entry, "title"),
        _child_text(entry, "summary"),
        _child_text(entry, "content"),
        link,
    ]
    return FeedEntry(
        source_url=link,
        text=" ".join(part or "" for part in parts),
        entry_id=entry_id,
        timestamp=timestamp,
    )


def _child_text(node: ElementTree.Element, name: str) -> str | None:
    child = node.find(name)
    if child is None:
        child = node.find(f"{{*}}{name}")
    return child.text if child is not None else None


def _strip_ns(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]
