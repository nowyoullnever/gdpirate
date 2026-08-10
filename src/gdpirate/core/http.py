import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx

from gdpirate.config import Settings, get_settings


class HttpClientFactory:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._semaphore = asyncio.Semaphore(self.settings.http_max_concurrency)

    @asynccontextmanager
    async def client(self) -> AsyncIterator[httpx.AsyncClient]:
        timeout = httpx.Timeout(self.settings.http_timeout_seconds)
        limits = httpx.Limits(max_connections=self.settings.http_max_concurrency)
        async with self._semaphore:
            async with httpx.AsyncClient(
                follow_redirects=True,
                max_redirects=self.settings.http_max_redirects,
                timeout=timeout,
                limits=limits,
                headers={"User-Agent": self.settings.user_agent},
            ) as client:
                yield client


async def request_with_retries(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    attempts: int = 3,
    **kwargs,
) -> httpx.Response:
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = await client.request(method, url, **kwargs)
            if response.status_code not in {408, 429, 500, 502, 503, 504}:
                return response
            if attempt == attempts:
                return response
            retry_after = response.headers.get("Retry-After")
            delay = min(float(retry_after), 10.0) if retry_after and retry_after.isdigit() else attempt
            await asyncio.sleep(delay)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            last_exc = exc
            if attempt == attempts:
                raise
            await asyncio.sleep(attempt)
    if last_exc:
        raise last_exc
    raise RuntimeError("request retry loop exited unexpectedly")
