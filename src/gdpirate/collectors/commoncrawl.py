from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
import gzip
import io
import json
import re
from pathlib import Path
from urllib.parse import quote

import duckdb
import httpx
from warcio.archiveiterator import ArchiveIterator

from gdpirate.collectors.base import CandidateLink, CollectorContext
from gdpirate.config import Settings, get_settings
from gdpirate.core.drive_urls import GOOGLE_HOSTS, extract_google_urls, parse_google_url

CRAWL_RE = re.compile(r"^CC-MAIN-\d{4}-\d{2}$")


@dataclass
class CommonCrawlRunOptions:
    mode: str = "url-index"
    max_files: int | None = None


class CommonCrawlCollector:
    name = "commoncrawl"
    source_name = "Common Crawl"
    bulk = True

    def __init__(
        self,
        settings: Settings | None = None,
        options: CommonCrawlRunOptions | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.options = options or CommonCrawlRunOptions(self.settings.commoncrawl_default_mode)

    async def collect(
        self, context: CollectorContext, *, max_items: int | None = None
    ) -> AsyncIterator[CandidateLink]:
        crawls = await resolve_crawls(context.client, self.settings)
        modes = ["url-index", "wat"] if self.options.mode == "both" else [self.options.mode]
        for crawl in crawls:
            for mode in modes:
                if mode == "url-index":
                    async for item in self._collect_url_index(context, crawl, max_items):
                        yield item
                elif mode == "wat":
                    async for item in self._collect_wat(context, crawl, max_items):
                        yield item
                else:
                    context.error = f"unknown Common Crawl mode: {mode}"
                    return

    async def _collect_url_index(
        self, context: CollectorContext, crawl: str, max_items: int | None
    ) -> AsyncIterator[CandidateLink]:
        scope = f"url-index/{crawl}"
        state = context.get_cursor(scope)
        start_index = int(state.get("path_index", 0))
        paths = await fetch_path_list(context.client, self.settings, crawl, "cc-index-table.paths.gz")
        processed_files = 0
        for path_index, path in enumerate(paths[start_index:], start=start_index):
            if self.options.max_files is not None and processed_files >= self.options.max_files:
                return
            for raw_url in query_url_index_part(commoncrawl_data_url(self.settings, path)):
                if max_items is not None and context.scanned >= max_items:
                    await context.checkpoint(
                        scope, {"path_index": path_index, "current_path": path}
                    )
                    return
                context.mark_scanned()
                yield CandidateLink(
                    raw_url=raw_url,
                    source_name=self.source_name,
                    source_url=commoncrawl_index_source_url(crawl, raw_url),
                )
            processed_files += 1
            await context.checkpoint(scope, {"path_index": path_index + 1})

    async def _collect_wat(
        self, context: CollectorContext, crawl: str, max_items: int | None
    ) -> AsyncIterator[CandidateLink]:
        scope = f"wat/{crawl}"
        state = context.get_cursor(scope)
        start_index = int(state.get("path_index", 0))
        record_resume = int(state.get("record_index", 0))
        paths = await fetch_path_list(context.client, self.settings, crawl, "wat.paths.gz")
        temp_dir = Path(self.settings.commoncrawl_temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        processed_files = 0
        for path_index, path in enumerate(paths[start_index:], start=start_index):
            if self.options.max_files is not None and processed_files >= self.options.max_files:
                return
            temp_path = temp_dir / Path(path).name
            try:
                bytes_downloaded = await download_to_file(
                    context.client,
                    commoncrawl_data_url(self.settings, path),
                    temp_path,
                )
                context.cursor.setdefault(scope, {})["bytes_downloaded"] = bytes_downloaded
                async for record_index, candidate in iter_wat_candidates(
                    temp_path, record_resume
                ):
                    if max_items is not None and context.scanned >= max_items:
                        await context.checkpoint(
                            scope,
                            {
                                "path_index": path_index,
                                "current_path": path,
                                "record_index": record_index,
                            },
                        )
                        return
                    context.mark_scanned()
                    yield candidate
                    if record_index % self.settings.commoncrawl_checkpoint_record_interval == 0:
                        await context.checkpoint(
                            scope,
                            {
                                "path_index": path_index,
                                "current_path": path,
                                "record_index": record_index,
                            },
                        )
                processed_files += 1
                record_resume = 0
                await context.checkpoint(scope, {"path_index": path_index + 1, "record_index": 0})
            finally:
                temp_path.unlink(missing_ok=True)


async def fetch_collinfo(client: httpx.AsyncClient, settings: Settings) -> list[dict]:
    response = await client.get(settings.commoncrawl_collinfo_url)
    response.raise_for_status()
    return response.json()


async def resolve_crawls(client: httpx.AsyncClient, settings: Settings) -> list[str]:
    configured = settings.commoncrawl_crawl_list
    collinfo = await fetch_collinfo(client, settings)
    available = [item["id"] for item in collinfo if CRAWL_RE.fullmatch(item.get("id", ""))]
    if configured == ["latest"]:
        return available[:1]
    resolved = []
    for crawl in configured:
        if not CRAWL_RE.fullmatch(crawl):
            raise ValueError(f"invalid Common Crawl id: {crawl}")
        if available and crawl not in available:
            raise ValueError(f"Common Crawl id not in metadata: {crawl}")
        resolved.append(crawl)
    return sorted(resolved, reverse=True)


async def fetch_path_list(
    client: httpx.AsyncClient, settings: Settings, crawl: str, filename: str
) -> list[str]:
    url = f"{settings.commoncrawl_data_base.rstrip('/')}/crawl-data/{crawl}/{filename}"
    response = await client.get(url)
    response.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
        return [line.decode("utf-8").strip() for line in gz if line.strip()]


def query_url_index_part(parquet_url: str) -> list[str]:
    hosts = ", ".join(repr(host) for host in GOOGLE_HOSTS)
    query = (
        "SELECT url FROM read_parquet(?) "
        f"WHERE url_host_name IN ({hosts})"
    )
    try:
        rows = duckdb.execute(query, [parquet_url]).fetchall()
    except Exception:
        rows = duckdb.execute("SELECT url FROM read_parquet(?)", [parquet_url]).fetchall()
    urls = []
    for (raw_url,) in rows:
        parsed = parse_google_url(str(raw_url))
        if parsed:
            urls.append(str(raw_url))
    return urls


async def download_to_file(client: httpx.AsyncClient, url: str, path: Path) -> int:
    if re.match(r"^[A-Za-z]:[\\/]", url) or url.startswith("/"):
        source = Path(url)
        data = source.read_bytes()
        path.write_bytes(data)
        return len(data)
    total = 0
    async with client.stream("GET", url) as response:
        response.raise_for_status()
        with path.open("wb") as handle:
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                handle.write(chunk)
    return total


async def iter_wat_candidates(
    wat_path: Path, resume_record_index: int = 0
) -> AsyncIterator[tuple[int, CandidateLink]]:
    with wat_path.open("rb") as stream:
        for record_index, record in enumerate(ArchiveIterator(stream)):
            if record_index < resume_record_index:
                continue
            if record.rec_type != "metadata":
                continue
            try:
                payload = json.loads(record.content_stream().read().decode("utf-8"))
            except Exception:
                continue
            target = (
                payload.get("Envelope", {})
                .get("WARC-Header-Metadata", {})
                .get("WARC-Target-URI")
            )
            links = (
                payload.get("Envelope", {})
                .get("Payload-Metadata", {})
                .get("HTTP-Response-Metadata", {})
                .get("HTML-Metadata", {})
                .get("Links", [])
            )
            for link in links or []:
                raw = link.get("url") or link.get("href") or link.get("path")
                if not raw:
                    continue
                for google_url in extract_google_urls(str(raw)):
                    yield (
                        record_index,
                        CandidateLink(
                            raw_url=google_url,
                            source_name="Common Crawl",
                            source_url=target,
                        ),
                    )


def commoncrawl_index_source_url(crawl: str, raw_url: str) -> str:
    return f"https://index.commoncrawl.org/{crawl}-index?url={quote(raw_url, safe='')}&output=json"


def commoncrawl_data_url(settings: Settings, path: str) -> str:
    if re.match(r"^[A-Za-z]:[\\/]", path) or path.startswith("/") or path.startswith("http"):
        return path
    return f"{settings.commoncrawl_data_base.rstrip('/')}/{path}"
