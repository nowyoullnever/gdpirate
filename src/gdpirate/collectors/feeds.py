from collections.abc import AsyncIterator
from pathlib import Path
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
            if not feed.get("enabled", True):
                continue
            feed_url = str(feed["url"])
            response = await request_with_retries(context.client, "GET", feed_url)
            if response.status_code >= 400:
                context.error = f"feed failed: {feed_url} {response.status_code}"
                continue
            for entry in _parse_feed_entries(response.text, feed_url):
                source_url = entry["source_url"] or feed_url
                for raw_url in extract_google_urls(entry["text"]):
                    yield CandidateLink(
                        raw_url=raw_url,
                        source_name=str(feed.get("name") or self.source_name),
                        source_url=source_url,
                    )
                    emitted += 1
                    if max_items is not None and emitted >= max_items:
                        return


def _load_feed_config(path: str) -> list[dict]:
    config_path = Path(path)
    if not config_path.exists():
        return []
    with config_path.open("rb") as handle:
        payload = tomllib.load(handle)
    return [feed for feed in payload.get("feeds", []) if feed.get("url")]


def _parse_feed_entries(xml_text: str, feed_url: str) -> list[dict[str, str | None]]:
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return []

    entries: list[dict[str, str | None]] = []
    if _strip_ns(root.tag) == "rss" or root.find("./channel") is not None:
        for item in root.findall(".//item"):
            entries.append(_rss_item(item))
    elif _strip_ns(root.tag) == "feed":
        for entry in root.findall("{*}entry"):
            entries.append(_atom_entry(entry))
    return entries


def _rss_item(item: ElementTree.Element) -> dict[str, str | None]:
    link = _child_text(item, "link")
    parts = [
        _child_text(item, "title"),
        _child_text(item, "description"),
        _child_text(item, "summary"),
        _child_text(item, "content"),
    ]
    return {"source_url": link, "text": " ".join(part or "" for part in parts + [link])}


def _atom_entry(entry: ElementTree.Element) -> dict[str, str | None]:
    link = None
    for link_node in entry.findall("{*}link"):
        if link_node.get("href"):
            link = link_node.get("href")
            break
    parts = [
        _child_text(entry, "title"),
        _child_text(entry, "summary"),
        _child_text(entry, "content"),
    ]
    return {"source_url": link, "text": " ".join(part or "" for part in parts + [link])}


def _child_text(node: ElementTree.Element, name: str) -> str | None:
    child = node.find(name)
    if child is None:
        child = node.find(f"{{*}}{name}")
    return child.text if child is not None else None


def _strip_ns(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]
