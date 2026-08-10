import httpx

from gdpirate.collectors.base import CollectorContext
from gdpirate.collectors.feeds import FeedCollector
from gdpirate.config import Settings


async def test_feed_partial_resume_preserves_old_validators_and_skips_processed(tmp_path):
    config = tmp_path / "feeds.toml"
    config.write_text('[[feeds]]\nname = "Test Feed"\nurl = "https://feed.example/rss"\n')
    seen_headers = []
    body = """<rss><channel>
<item><guid>one</guid><link>https://source.example/1</link><title>https://drive.google.com/file/d/ONE123/view</title></item>
<item><guid>two</guid><link>https://source.example/2</link><title>https://drive.google.com/file/d/TWO123/view</title></item>
</channel></rss>"""

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.append(dict(request.headers))
        return httpx.Response(
            200,
            text=body,
            headers={"ETag": '"new"', "Last-Modified": "Tue, 11 Aug 2026 00:00:00 GMT"},
        )

    settings = Settings(feed_config_path=str(config))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        context = CollectorContext(
            client=client,
            cursor={
                "https://feed.example/rss": {
                    "etag": '"old"',
                    "last_modified": "Mon, 10 Aug 2026 00:00:00 GMT",
                }
            },
        )
        first = [item async for item in FeedCollector(settings).collect(context, max_items=1)]
        second = [item async for item in FeedCollector(settings).collect(context, max_items=10)]

    assert first[0].raw_url == "https://drive.google.com/file/d/ONE123/view"
    assert second[0].raw_url == "https://drive.google.com/file/d/TWO123/view"
    assert seen_headers[0]["if-none-match"] == '"old"'
    assert "if-none-match" not in seen_headers[1]
    state = context.cursor["https://feed.example/rss"]
    assert state["etag"] == '"new"'
    assert state["partial"] is False
