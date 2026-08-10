from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
import asyncio
import html
import tomllib

from gdpirate.collectors.base import CandidateLink, CollectorContext
from gdpirate.config import Settings, get_settings
from gdpirate.core.drive_urls import extract_google_urls
from gdpirate.core.http import request_with_retries


def load_korea_queries(path: str) -> list[str]:
    config_path = Path(path)
    if not config_path.exists():
        return []
    with config_path.open("rb") as handle:
        payload = tomllib.load(handle)
    return [
        item["query"]
        for item in payload.get("queries", [])
        if item.get("enabled", True) and item.get("query")
    ]


class NaverCollector:
    name = "naver"

    search_types = {
        "blog": ("NAVER Blog", ("date", "sim")),
        "cafearticle": ("NAVER Cafe", ("date", "sim")),
        "webkr": ("NAVER Search", (None,)),
    }

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def configured(self) -> bool:
        return bool(
            self.settings.naver_api_hub_client_id
            and self.settings.naver_api_hub_client_secret
        )

    async def collect(
        self, context: CollectorContext, *, max_items: int | None = None
    ) -> AsyncIterator[CandidateLink]:
        if not self.configured():
            context.unavailable = True
            context.error = "unconfigured"
            return
        requests = 0
        headers = {
            "X-NCP-APIGW-API-KEY-ID": self.settings.naver_api_hub_client_id or "",
            "X-NCP-APIGW-API-KEY": self.settings.naver_api_hub_client_secret or "",
        }
        for search_type, (source_name, sorts) in self.search_types.items():
            for sort in sorts:
                for query in load_korea_queries(self.settings.korea_query_config_path):
                    scope = f"{search_type}/{query}" if sort is None else f"{search_type}/{sort}/{query}"
                    start = int(context.get_cursor(scope).get("start", 1))
                    if context.get_cursor(scope).get("complete"):
                        continue
                    while requests < self.settings.naver_max_requests_per_run:
                        if start > 1000:
                            await context.checkpoint(scope, {"start": start, "complete": True})
                            break
                        params = {"query": query, "display": 100, "start": start}
                        if sort is not None:
                            params["sort"] = sort
                        response = await request_with_retries(
                            context.client,
                            "GET",
                            f"{self.settings.naver_api_hub_base.rstrip('/')}/search/v1/{search_type}",
                            params=params,
                            headers=headers,
                            attempts=1,
                        )
                        requests += 1
                        if response.status_code in {401, 403}:
                            context.unavailable = True
                            context.error = f"naver authentication failed: {response.status_code}"
                            return
                        if response.status_code == 429:
                            context.error = "naver rate limited"
                            return
                        response.raise_for_status()
                        items = response.json().get("items") or []
                        if not items:
                            await context.checkpoint(scope, {"start": start, "complete": True})
                            break
                        for index, item in enumerate(items):
                            if max_items is not None and context.scanned >= max_items:
                                await context.checkpoint(scope, {"start": start + index})
                                return
                            context.mark_scanned()
                            text = " ".join(
                                html.unescape(str(item.get(key) or ""))
                                for key in ("title", "description", "link")
                            )
                            source_url = item.get("link")
                            for raw_url in extract_google_urls(text):
                                yield CandidateLink(
                                    raw_url=raw_url,
                                    source_name=source_name,
                                    source_url=source_url,
                                )
                            if max_items is not None and context.scanned >= max_items:
                                await context.checkpoint(scope, {"start": start + index + 1})
                                return
                        start += len(items)
                        await context.checkpoint(scope, {"start": start})
                        await asyncio.sleep(self.settings.naver_request_delay_seconds)


class DaumCollector:
    name = "daum"

    search_types = {
        "web": "Daum Web",
        "blog": "Daum Blog",
        "cafe": "Daum Cafe",
    }
    sorts = ("accuracy", "recency")

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def configured(self) -> bool:
        return bool(self.settings.kakao_rest_api_key)

    async def collect(
        self, context: CollectorContext, *, max_items: int | None = None
    ) -> AsyncIterator[CandidateLink]:
        if not self.configured():
            context.unavailable = True
            context.error = "unconfigured"
            return
        requests = 0
        headers = {"Authorization": f"KakaoAK {self.settings.kakao_rest_api_key}"}
        for search_type, source_name in self.search_types.items():
            for sort in self.sorts:
                for query in load_korea_queries(self.settings.korea_query_config_path):
                    scope = f"{search_type}/{sort}/{query}"
                    page = int(context.get_cursor(scope).get("page", 1))
                    offset = int(context.get_cursor(scope).get("offset", 0))
                    while requests < self.settings.daum_max_requests_per_run:
                        response = await request_with_retries(
                            context.client,
                            "GET",
                            f"{self.settings.kakao_daum_search_base.rstrip('/')}/v2/search/{search_type}",
                            params={"query": query, "size": 50, "page": page, "sort": sort},
                            headers=headers,
                            attempts=1,
                        )
                        requests += 1
                        if response.status_code in {401, 403}:
                            context.unavailable = True
                            context.error = f"daum authentication failed: {response.status_code}"
                            return
                        if response.status_code == 429:
                            context.error = "daum rate limited"
                            return
                        response.raise_for_status()
                        payload = response.json()
                        docs = payload.get("documents") or []
                        if not docs:
                            await context.checkpoint(scope, {"page": page, "complete": True})
                            break
                        for index, doc in enumerate(docs[offset:], start=offset):
                            if max_items is not None and context.scanned >= max_items:
                                await context.checkpoint(scope, {"page": page, "offset": index})
                                return
                            context.mark_scanned()
                            text = " ".join(
                                html.unescape(str(doc.get(key) or ""))
                                for key in ("title", "contents", "url")
                            )
                            source_url = doc.get("url")
                            for raw_url in extract_google_urls(text):
                                yield CandidateLink(
                                    raw_url=raw_url,
                                    source_name=source_name,
                                    source_url=source_url,
                                )
                            if max_items is not None and context.scanned >= max_items:
                                await context.checkpoint(
                                    scope, {"page": page, "offset": index + 1}
                                )
                                return
                        if (payload.get("meta") or {}).get("is_end"):
                            await context.checkpoint(scope, {"page": page, "complete": True})
                            break
                        page += 1
                        offset = 0
                        await context.checkpoint(scope, {"page": page, "offset": 0})
                        if page > 50:
                            break
                        await asyncio.sleep(self.settings.daum_request_delay_seconds)
