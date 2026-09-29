import httpx
import pytest
import respx

from gdpirate.config import Settings
from gdpirate.core.access_check import AccessChecker
from gdpirate.core.models import AccessStatus


class CountingStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.chunks_read = 0

    async def __aiter__(self):
        for _ in range(100):
            self.chunks_read += 1
            yield b"x" * 64


def settings() -> Settings:
    return Settings(access_check_max_body_bytes=256, http_timeout_seconds=1)


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("<html><div id='drive-viewer'>Preview</div></html>", AccessStatus.PUBLIC),
        ("<html>Request access You need permission</html>", AccessStatus.RESTRICTED),
        ("<html>File does not exist</html>", AccessStatus.DEAD),
        ("<html>plain unrecognized page</html>", AccessStatus.UNKNOWN),
    ],
)
@respx.mock
async def test_classifies_html_responses(body, expected):
    url = "https://drive.google.com/file/d/ABC123/view"
    respx.get(url).mock(return_value=httpx.Response(200, text=body))

    assert await AccessChecker(settings()).check(url) == expected


@respx.mock
async def test_ambiguous_sign_in_text_is_not_restricted_without_redirect():
    url = "https://drive.google.com/file/d/ABC123/view"
    respx.get(url).mock(
        return_value=httpx.Response(
            200,
            text="<html>Sign in</html>",
            request=httpx.Request("GET", url),
            extensions={"history": []},
        )
    )

    result = await AccessChecker(settings()).check_detailed(url)

    assert result.status == AccessStatus.UNKNOWN
    assert result.reason == "ambiguous_html"


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (404, AccessStatus.DEAD),
        (410, AccessStatus.DEAD),
        (403, AccessStatus.RESTRICTED),
        (429, AccessStatus.UNKNOWN),
        (500, AccessStatus.UNKNOWN),
    ],
)
@respx.mock
async def test_classifies_status_codes(status_code, expected):
    url = "https://drive.google.com/file/d/ABC123/view"
    respx.get(url).mock(return_value=httpx.Response(status_code, text=""))

    assert await AccessChecker(settings()).check(url) == expected


@respx.mock
async def test_binary_success_is_public_without_downloading_full_file():
    url = "https://drive.google.com/file/d/ABC123/view"
    respx.get(url).mock(
        return_value=httpx.Response(
            206,
            content=b"abc",
            headers={"Content-Type": "application/pdf"},
        )
    )

    assert await AccessChecker(settings()).check(url) == AccessStatus.PUBLIC


@respx.mock
async def test_timeout_fails_closed_to_unknown():
    url = "https://drive.google.com/file/d/ABC123/view"
    respx.get(url).mock(side_effect=httpx.TimeoutException("slow"))

    assert await AccessChecker(settings()).check(url) == AccessStatus.UNKNOWN


async def test_streaming_access_check_reads_only_configured_limit():
    stream = CountingStream()
    url = "https://drive.google.com/file/d/ABC123/view"

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "text/html"},
            stream=stream,
            request=request,
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
        status = await AccessChecker(
            Settings(access_check_max_body_bytes=128), client
        ).check(url)

    assert status == AccessStatus.UNKNOWN
    assert stream.chunks_read == 2
