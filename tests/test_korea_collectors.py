import httpx

from gdpirate.collectors.base import CollectorContext
from gdpirate.collectors.korea import DaumCollector, NaverCollector
from gdpirate.config import Settings
from gdpirate.pipeline.collection import source_statuses


async def collect_items(collector, handler, max_items=10):
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        context = CollectorContext(client=client)
        items = [item async for item in collector.collect(context, max_items=max_items)]
        return items, context


async def test_naver_missing_credentials_is_unconfigured(tmp_path):
    settings = Settings(enable_naver=True, korea_query_config_path=str(tmp_path / "none.toml"))
    items, context = await collect_items(
        NaverCollector(settings), lambda request: httpx.Response(500)
    )

    assert items == []
    assert context.error == "unconfigured"
    assert source_statuses(settings)["naver"] == "unconfigured"


async def test_naver_extracts_blog_cafe_web_without_storing_secrets(tmp_path):
    queries = tmp_path / "korea.toml"
    queries.write_text('[[queries]]\nquery = "drive.google.com"\nenabled = true\n')
    seen_headers = []
    seen_urls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.append(request.headers)
        seen_urls.append(str(request.url))
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "title": "https://drive.google.com/file/d/ABC123/view",
                        "description": "docs",
                        "link": "https://blog.naver.com/post",
                    }
                ]
            },
        )

    settings = Settings(
        enable_naver=True,
        naver_api_hub_client_id="client-id",
        naver_api_hub_client_secret="secret-value",
        naver_max_requests_per_run=1,
        naver_request_delay_seconds=0,
        korea_query_config_path=str(queries),
    )
    items, context = await collect_items(NaverCollector(settings), handler, max_items=1)

    assert items[0].source_name == "NAVER Blog"
    assert items[0].source_url == "https://blog.naver.com/post"
    assert "secret-value" not in str(context.cursor)
    assert seen_headers[0]["X-NCP-APIGW-API-KEY-ID"] == "client-id"
    assert "/search/v1/blog" in seen_urls[0]


async def test_naver_429_stops_gracefully(tmp_path):
    queries = tmp_path / "korea.toml"
    queries.write_text('[[queries]]\nquery = "drive.google.com"\nenabled = true\n')

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429)

    settings = Settings(
        enable_naver=True,
        naver_api_hub_client_id="id",
        naver_api_hub_client_secret="secret",
        korea_query_config_path=str(queries),
    )
    items, context = await collect_items(NaverCollector(settings), handler)

    assert items == []
    assert context.error == "naver rate limited"


async def test_naver_webkr_omits_sort_and_uses_exact_resume(tmp_path):
    queries = tmp_path / "korea.toml"
    queries.write_text('[[queries]]\nquery = "drive.google.com"\nenabled = true\n')
    seen_urls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_urls.append(str(request.url))
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "title": "https://drive.google.com/file/d/ABC123/view",
                        "description": "",
                        "link": "https://naver.example/1",
                    }
                ]
            },
        )

    settings = Settings(
        enable_naver=True,
        naver_api_hub_client_id="id",
        naver_api_hub_client_secret="secret",
        naver_max_requests_per_run=10,
        naver_request_delay_seconds=0,
        korea_query_config_path=str(queries),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        context = CollectorContext(client=client)
        context.cursor["webkr/drive.google.com"] = {"start": 9}
        collector = NaverCollector(settings)
        collector.search_types = {"webkr": ("NAVER Search", (None,))}
        items = [item async for item in collector.collect(context, max_items=1)]

    assert items
    web_request = [url for url in seen_urls if "/search/v1/webkr" in url][0]
    assert "start=9" in web_request
    assert "sort=" not in web_request
    assert context.cursor["webkr/drive.google.com"]["start"] == 10


async def test_daum_missing_key_is_unconfigured(tmp_path):
    settings = Settings(enable_daum=True, korea_query_config_path=str(tmp_path / "none.toml"))
    items, context = await collect_items(
        DaumCollector(settings), lambda request: httpx.Response(500)
    )

    assert items == []
    assert context.error == "unconfigured"
    assert source_statuses(settings)["daum"] == "unconfigured"


async def test_daum_extracts_documents_and_paginates(tmp_path):
    queries = tmp_path / "korea.toml"
    queries.write_text('[[queries]]\nquery = "docs.google.com"\nenabled = true\n')
    pages = []

    async def handler(request: httpx.Request) -> httpx.Response:
        pages.append(request.url.params.get("page"))
        return httpx.Response(
            200,
            json={
                "documents": [
                    {
                        "title": "x",
                        "contents": "https://docs.google.com/document/d/DOC123/edit",
                        "url": "https://daum.example/post",
                    }
                ],
                "meta": {"is_end": True},
            },
        )

    settings = Settings(
        enable_daum=True,
        kakao_rest_api_key="kakao-secret",
        daum_request_delay_seconds=0,
        korea_query_config_path=str(queries),
    )
    items, context = await collect_items(DaumCollector(settings), handler, max_items=1)

    assert items[0].source_name == "Daum Web"
    assert items[0].source_url == "https://daum.example/post"
    assert pages == ["1"]
    assert "kakao-secret" not in str(context.cursor)


async def test_daum_resume_offset_on_same_page(tmp_path):
    queries = tmp_path / "korea.toml"
    queries.write_text('[[queries]]\nquery = "docs.google.com"\nenabled = true\n')

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "documents": [
                    {
                        "title": "skip",
                        "contents": "https://docs.google.com/document/d/SKIP/edit",
                        "url": "https://daum.example/skip",
                    },
                    {
                        "title": "take",
                        "contents": "https://docs.google.com/document/d/TAKE/edit",
                        "url": "https://daum.example/take",
                    },
                ],
                "meta": {"is_end": True},
            },
        )

    settings = Settings(
        enable_daum=True,
        kakao_rest_api_key="key",
        daum_request_delay_seconds=0,
        korea_query_config_path=str(queries),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        context = CollectorContext(client=client)
        context.cursor["web/accuracy/docs.google.com"] = {"page": 1, "offset": 1}
        items = [item async for item in DaumCollector(settings).collect(context, max_items=1)]

    assert items[0].raw_url == "https://docs.google.com/document/d/TAKE/edit"
