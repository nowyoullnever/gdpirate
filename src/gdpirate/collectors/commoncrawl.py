from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
import asyncio
from contextlib import aclosing
import gzip
import io
import json
import re
from pathlib import Path
from urllib.parse import quote

import duckdb
import httpx
from warcio.archiveiterator import ArchiveIterator

from gdpirate.collectors.base import CandidateLink, CollectorContext, distinct_google_urls
from gdpirate.config import Settings, get_settings
from gdpirate.core.drive_urls import GOOGLE_HOSTS, extract_google_urls, parse_google_url

CRAWL_RE = re.compile(r"^CC-MAIN-\d{4}-\d{2}$")


@dataclass
class CommonCrawlRunOptions:
    mode: str = "url-index"
    max_files: int | None = None
    max_records: int | None = None


class CommonCrawlCollector:
    name = "commoncrawl"
    source_name = "Common Crawl"
    url_index_source_name = "Common Crawl URL Index"
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
        row_offset = int(state.get("row_offset", 0))
        processed_files = 0
        async for path_index, path in iter_path_list(
            context.client,
            self.settings,
            crawl,
            "cc-index-table.paths.gz",
            start_index,
        ):
            if self.options.max_files is not None and processed_files >= self.options.max_files:
                return
            iterator = iter_url_index_part_batches(
                commoncrawl_data_url(self.settings, path),
                self.settings.commoncrawl_url_index_batch_size,
                row_offset,
                include_offsets=True,
            )
            rows_seen_in_path = row_offset
            while True:
                try:
                    batch = await asyncio.to_thread(next, iterator, None)
                except UnsupportedUrlIndexSchema as exc:
                    context.error = str(exc)
                    return
                if batch is None:
                    break
                for query_row_offset, raw_url in batch:
                    if max_items is not None and context.scanned >= max_items:
                        await context.checkpoint(
                            scope,
                            {
                                "path_index": path_index,
                                "current_path": path,
                                "row_offset": rows_seen_in_path,
                            },
                        )
                        return
                    context.mark_scanned()
                    rows_seen_in_path = query_row_offset + 1
                    yield CandidateLink(
                        raw_url=raw_url,
                        source_name=self.url_index_source_name,
                        source_url=commoncrawl_index_source_url(crawl, raw_url),
                    )
            processed_files += 1
            row_offset = 0
            await context.checkpoint(scope, {"path_index": path_index + 1, "row_offset": 0})

    async def _collect_wat(
        self, context: CollectorContext, crawl: str, max_items: int | None
    ) -> AsyncIterator[CandidateLink]:
        scope = f"wat/{crawl}"
        state = context.get_cursor(scope)
        start_index = int(state.get("path_index", 0))
        record_resume = int(state.get("record_index", 0))
        temp_dir = Path(self.settings.commoncrawl_temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        processed_files = 0
        records_this_run = 0
        async for path_index, path in iter_path_list(
            context.client, self.settings, crawl, "wat.paths.gz", start_index
        ):
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
                async with aclosing(iter_wat_events(temp_path, record_resume)) as events:
                    async for event in events:
                        if event["type"] == "progress":
                            records_this_run += 1
                            if (
                                self.options.max_records is not None
                                and records_this_run > self.options.max_records
                            ):
                                await context.checkpoint(
                                    scope,
                                    {
                                        "path_index": path_index,
                                        "current_path": path,
                                        "record_index": event["record_index"],
                                        **_wat_metrics(context, scope),
                                    },
                                )
                                return
                            context.mark_scanned()
                            if max_items is not None and context.scanned >= max_items:
                                await context.checkpoint(
                                    scope,
                                    {
                                        "path_index": path_index,
                                        "current_path": path,
                                        "record_index": event["record_index"],
                                        **_wat_metrics(context, scope),
                                    },
                                )
                                return
                            if event["record_index"] % self.settings.commoncrawl_checkpoint_record_interval == 0:
                                await context.checkpoint(
                                    scope,
                                    {
                                        "path_index": path_index,
                                        "current_path": path,
                                        "record_index": event["record_index"],
                                        **_wat_metrics(context, scope),
                                    },
                                )
                            continue
                        if event["type"] == "links":
                            _inc_metric(context, scope, "links_scanned", event["links_scanned"])
                            continue
                        candidate = event["candidate"]
                        record_index = event["record_index"]
                        _inc_metric(context, scope, "google_candidates")
                        yield candidate
                processed_files += 1
                _inc_metric(context, scope, "files_processed")
                record_resume = 0
                await context.checkpoint(scope, {"path_index": path_index + 1, "record_index": 0, **_wat_metrics(context, scope)})
            finally:
                temp_path.unlink(missing_ok=True)


async def fetch_collinfo(client: httpx.AsyncClient, settings: Settings) -> list[dict]:
    response = await client.get(settings.commoncrawl_collinfo_url)
    response.raise_for_status()
    return response.json()


async def resolve_crawls(client: httpx.AsyncClient, settings: Settings) -> list[str]:
    configured = settings.commoncrawl_crawl_list
    collinfo = await fetch_collinfo(client, settings)
    available = sorted(
        [item["id"] for item in collinfo if CRAWL_RE.fullmatch(item.get("id", ""))],
        reverse=True,
    )
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
    return [
        path
        async for _index, path in iter_path_list(client, settings, crawl, filename)
    ]


async def iter_path_list(
    client: httpx.AsyncClient,
    settings: Settings,
    crawl: str,
    filename: str,
    start_index: int = 0,
) -> AsyncIterator[tuple[int, str]]:
    url = f"{settings.commoncrawl_data_base.rstrip('/')}/crawl-data/{crawl}/{filename}"
    response = await client.get(url)
    response.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
        for index, line in enumerate(gz):
            path = line.decode("utf-8").strip()
            if path and index >= start_index:
                yield index, path


class UnsupportedUrlIndexSchema(RuntimeError):
    pass


def validate_url_index_schema(parquet_url: str) -> None:
    rows = duckdb.execute(
        "DESCRIBE SELECT * FROM read_parquet(?) LIMIT 0", [parquet_url]
    ).fetchall()
    columns = {row[0] for row in rows}
    missing = {"url", "url_host_name"} - columns
    if missing:
        raise UnsupportedUrlIndexSchema(
            f"unsupported Common Crawl URL Index schema, missing: {', '.join(sorted(missing))}"
        )


def iter_url_index_part_batches(
    parquet_url: str,
    batch_size: int,
    row_offset: int = 0,
    *,
    include_offsets: bool = False,
):
    validate_url_index_schema(parquet_url)
    hosts = ", ".join(repr(host) for host in GOOGLE_HOSTS)
    query = (
        "SELECT url FROM read_parquet(?) "
        f"WHERE url_host_name IN ({hosts})"
        "LIMIT 9223372036854775807 OFFSET ?"
    )
    cursor = duckdb.execute(query, [parquet_url, row_offset])
    query_row_offset = row_offset
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        batch = []
        for (raw_url,) in rows:
            parsed = parse_google_url(str(raw_url))
            if parsed:
                if include_offsets:
                    batch.append((query_row_offset, str(raw_url)))
                else:
                    batch.append(str(raw_url))
            query_row_offset += 1
        yield batch


def iter_url_index_part(parquet_url: str, batch_size: int):
    for batch in iter_url_index_part_batches(parquet_url, batch_size):
        yield from batch


def query_url_index_part(parquet_url: str) -> list[str]:
    return list(iter_url_index_part(parquet_url, 1000))


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


async def iter_wat_events(
    wat_path: Path, resume_record_index: int = 0
) -> AsyncIterator[dict]:
    with wat_path.open("rb") as stream:
        for record_index, record in enumerate(ArchiveIterator(stream)):
            if record_index < resume_record_index:
                continue
            yield {"type": "progress", "record_index": record_index}
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
            links_scanned = len(links or [])
            yield {
                "type": "links",
                "record_index": record_index,
                "links_scanned": links_scanned,
            }
            for link in links or []:
                raw = link.get("url") or link.get("href") or link.get("path")
                if not raw:
                    continue
                for google_url in distinct_google_urls(extract_google_urls(str(raw))):
                    yield {
                        "type": "candidate",
                        "record_index": record_index,
                        "candidate": CandidateLink(
                            raw_url=google_url,
                            source_name="Common Crawl WAT",
                            source_url=target,
                        ),
                    }


async def iter_wat_candidates(
    wat_path: Path, resume_record_index: int = 0
) -> AsyncIterator[tuple[int, CandidateLink]]:
    async for event in iter_wat_events(wat_path, resume_record_index):
        if event["type"] == "candidate":
            yield event["record_index"], event["candidate"]


def commoncrawl_index_source_url(crawl: str, raw_url: str) -> str:
    return f"https://index.commoncrawl.org/{crawl}-index?url={quote(raw_url, safe='')}&output=json"


def commoncrawl_data_url(settings: Settings, path: str) -> str:
    if re.match(r"^[A-Za-z]:[\\/]", path) or path.startswith("/") or path.startswith("http"):
        return path
    return f"{settings.commoncrawl_data_base.rstrip('/')}/{path}"


def _metric(context: CollectorContext, scope: str, name: str) -> int:
    return int(context.cursor.setdefault(scope, {}).get(name, 0))


def _inc_metric(context: CollectorContext, scope: str, name: str, amount: int = 1) -> None:
    state = context.cursor.setdefault(scope, {})
    state[name] = int(state.get(name, 0)) + amount


def _wat_metrics(context: CollectorContext, scope: str) -> dict:
    return {
        key: _metric(context, scope, key)
        for key in ("files_processed", "links_scanned", "google_candidates", "bytes_downloaded")
    }
