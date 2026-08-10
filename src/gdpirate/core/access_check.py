import logging

import httpx

from gdpirate.config import Settings, get_settings
from gdpirate.core.drive_urls import parse_google_url
from gdpirate.core.http import HttpClientFactory, request_with_retries
from gdpirate.core.models import AccessStatus

logger = logging.getLogger(__name__)

RESTRICTED_MARKERS = (
    "request access",
    "access denied",
    "you need access",
    "you need permission",
    "permission",
    "sign in",
    "signin",
    "accounts.google.com",
    "ServiceLogin",
    "로그인",
    "권한",
    "액세스 요청",
)
DEAD_MARKERS = (
    "file does not exist",
    "sorry, unable to open the file",
    "document not found",
    "cannot find",
    "404",
    "삭제",
    "찾을 수 없습니다",
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
                response = await request_with_retries(
                    self._client,
                    "GET",
                    parsed.canonical_url,
                    attempts=1,
                    headers={"Range": f"bytes=0-{self.settings.access_check_max_body_bytes - 1}"},
                )
            else:
                factory = HttpClientFactory(self.settings)
                async with factory.client() as client:
                    response = await request_with_retries(
                        client,
                        "GET",
                        parsed.canonical_url,
                        headers={
                            "Range": f"bytes=0-{self.settings.access_check_max_body_bytes - 1}"
                        },
                    )
        except (httpx.TimeoutException, httpx.NetworkError, httpx.TooManyRedirects) as exc:
            logger.warning("access check failed for %s: %s", parsed.canonical_url, exc)
            return AccessStatus.UNKNOWN

        return classify_response(response, self.settings.access_check_max_body_bytes)


def classify_response(response: httpx.Response, max_body_bytes: int) -> AccessStatus:
    final_url = str(response.url)
    final_url_lower = final_url.lower()

    if response.status_code in {404, 410}:
        return AccessStatus.DEAD
    if response.status_code in {401, 403}:
        return AccessStatus.RESTRICTED
    if response.status_code in {429, 500, 502, 503, 504}:
        return AccessStatus.UNKNOWN
    if "accounts.google.com" in final_url_lower or "servicelogin" in final_url_lower:
        return AccessStatus.RESTRICTED

    content_type = response.headers.get("Content-Type", "").lower()
    disposition = response.headers.get("Content-Disposition", "").lower()
    if response.status_code in {200, 206} and (
        "attachment" in disposition or not _looks_like_html(content_type)
    ):
        return AccessStatus.PUBLIC

    body = response.content[:max_body_bytes].decode(
        response.encoding or "utf-8", errors="ignore"
    )
    body_lower = body.lower()

    if any(marker.lower() in body_lower for marker in DEAD_MARKERS):
        return AccessStatus.DEAD
    if any(marker.lower() in body_lower for marker in RESTRICTED_MARKERS):
        return AccessStatus.RESTRICTED
    if response.status_code in {200, 206} and any(
        marker.lower() in body_lower for marker in PUBLIC_MARKERS
    ):
        return AccessStatus.PUBLIC
    return AccessStatus.UNKNOWN


def _looks_like_html(content_type: str) -> bool:
    return not content_type or "html" in content_type or "text/" in content_type
