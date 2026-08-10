import logging
from collections.abc import Mapping

import httpx

from gdpirate.config import Settings, get_settings
from gdpirate.core.drive_urls import parse_google_url
from gdpirate.core.http import HttpClientFactory
from gdpirate.core.models import AccessStatus

logger = logging.getLogger(__name__)

RESTRICTED_MARKERS = (
    "request access",
    "access denied",
    "you need access",
    "you need permission",
    "you do not have permission",
    "ask for access",
    "access request",
    "sign in",
    "signin",
    "accounts.google.com",
    "ServiceLogin",
)
DEAD_MARKERS = (
    "file does not exist",
    "sorry, unable to open the file",
    "document not found",
    "cannot find",
    "not found",
    "404",
)
PUBLIC_MARKERS = (
    "drive-viewer",
    "docs-homescreen",
    "docs-title",
    "viewerng",
    "download",
    "preview",
    "Google Docs",
    "Google Sheets",
    "Google Slides",
)


class AccessChecker:
    def __init__(
        self,
        settings: Settings | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._client = client

    async def check(self, raw_url: str) -> AccessStatus:
        parsed = parse_google_url(raw_url)
        if not parsed:
            return AccessStatus.UNKNOWN

        try:
            if self._client is not None:
                return await self._check_with_client(self._client, parsed.canonical_url)
            factory = HttpClientFactory(self.settings)
            async with factory.client() as client:
                return await self._check_with_client(client, parsed.canonical_url)
        except (httpx.TimeoutException, httpx.NetworkError, httpx.TooManyRedirects) as exc:
            logger.warning("access check failed for %s: %s", parsed.canonical_url, exc)
            return AccessStatus.UNKNOWN

    async def _check_with_client(
        self, client: httpx.AsyncClient, canonical_url: str
    ) -> AccessStatus:
        headers = {
            "Range": f"bytes=0-{self.settings.access_check_max_body_bytes - 1}"
        }
        async with client.stream("GET", canonical_url, headers=headers) as response:
            initial = classify_limited_response(
                status_code=response.status_code,
                final_url=str(response.url),
                headers=response.headers,
                body=b"",
                body_complete=False,
            )
            if initial != AccessStatus.UNKNOWN:
                return initial

            body = bytearray()
            async for chunk in response.aiter_bytes():
                remaining = self.settings.access_check_max_body_bytes - len(body)
                if remaining <= 0:
                    break
                body.extend(chunk[:remaining])
                if len(body) >= self.settings.access_check_max_body_bytes:
                    break

            return classify_limited_response(
                status_code=response.status_code,
                final_url=str(response.url),
                headers=response.headers,
                body=bytes(body),
                body_complete=len(body) < self.settings.access_check_max_body_bytes,
                encoding=response.encoding,
            )


def classify_response(response: httpx.Response, max_body_bytes: int) -> AccessStatus:
    return classify_limited_response(
        status_code=response.status_code,
        final_url=str(response.url),
        headers=response.headers,
        body=response.content[:max_body_bytes],
        body_complete=len(response.content) <= max_body_bytes,
        encoding=response.encoding,
    )


def classify_limited_response(
    *,
    status_code: int,
    final_url: str,
    headers: Mapping[str, str],
    body: bytes,
    body_complete: bool,
    encoding: str | None = None,
) -> AccessStatus:
    final_url_lower = final_url.lower()

    if status_code in {404, 410}:
        return AccessStatus.DEAD
    if status_code in {401, 403}:
        return AccessStatus.RESTRICTED
    if status_code in {429, 500, 502, 503, 504}:
        return AccessStatus.UNKNOWN
    if "accounts.google.com" in final_url_lower or "servicelogin" in final_url_lower:
        return AccessStatus.RESTRICTED

    content_type = headers.get("Content-Type", "").lower()
    disposition = headers.get("Content-Disposition", "").lower()
    if status_code in {200, 206} and (
        "attachment" in disposition or (content_type and not _looks_like_html(content_type))
    ):
        return AccessStatus.PUBLIC

    body_text = body.decode(encoding or "utf-8", errors="ignore")
    body_lower = body_text.lower()

    if any(marker.lower() in body_lower for marker in DEAD_MARKERS):
        return AccessStatus.DEAD
    if any(marker.lower() in body_lower for marker in RESTRICTED_MARKERS):
        return AccessStatus.RESTRICTED
    if status_code in {200, 206} and any(
        marker.lower() in body_lower for marker in PUBLIC_MARKERS
    ):
        return AccessStatus.PUBLIC
    return AccessStatus.UNKNOWN


def _looks_like_html(content_type: str) -> bool:
    return not content_type or "html" in content_type or "text/" in content_type
