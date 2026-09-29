import httpx
import pytest

from gdpirate.collectors.base import CollectorContext
from gdpirate.collectors.bluesky import BlueskyCollector
from gdpirate.collectors.feeds import FeedCollector
from gdpirate.collectors.hackernews import HackerNewsCollector
from gdpirate.collectors.lemmy import LemmyCollector
from gdpirate.collectors.misskey import MisskeyCollector
from gdpirate.config import Settings


async def collect_all(collector, handler, max_items=10):
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        context = CollectorContext(client=client, cursor={})
        items = [
            item
            async for item in collector.collect(context, max_items=max_items)
        ]
        return items, context


async def test_hackernews_story_comment_pagination_and_resume():
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(dict(request.url.params))
        page = int(request.url.params.get("page", 0))
        tag = request.url.params.get("tags")
        if page > 0:
            return httpx.Response(200, json={"hits": []})
        if tag == "story":
            return httpx.Response(
                200,
                json={
                    "hits": [
                        {
                            "objectID": "1",
                            "url": "https://drive.google.com/file/d/ABC123/view",
                            "title": "x",
                        }
                    ]
                },
            )
        return httpx.Response(
            200,
            json={
                "hits": [
                    {
                        "objectID": "2",
                        "comment_text": "https://docs.google.com/document/d/DOC123/edit",
                    }
                ]
            },
        )

    items, context = await collect_all(HackerNewsCollector(), handler, max_items=2)

    assert [item.source_name for item in items] == ["Hacker News", "Hacker News"]
    assert items[0].source_url == "https://news.ycombinator.com/item?id=1"
    assert items[1].raw_url == "https://docs.google.com/document/d/DOC123/edit"
    assert context.cursor
    assert calls


async def test_hackernews_rate_limit_records_error():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={})

    items, context = await collect_all(HackerNewsCollector(), handler)

    assert items == []
    assert context.error == "rate limited"


async def test_bluesky_extracts_text_facets_and_external_embed():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "posts": [
                    {
                        "uri": "at://did:plc:abc/app.bsky.feed.post/rkey1",
                        "author": {"handle": "example.test"},
                        "record": {
                            "text": "text https://drive.google.com/file/d/ABC123/view",
                            "facets": [
                                {
                                    "features": [
                                        {
                                            "uri": "https://docs.google.com/spreadsheets/d/SHEET123/edit"
                                        }
                                    ]
                                }
                            ],
                        },
                        "embed": {
                            "external": {
                                "uri": "https://docs.google.com/document/d/DOC123/edit"
                            }
                        },
                    }
                ]
            },
        )

    items, _ = await collect_all(BlueskyCollector(Settings()), handler, max_items=1)

    assert len(items) == 3
    assert items[0].source_url == "https://bsky.app/profile/example.test/post/rkey1"


@pytest.mark.parametrize("status", [401, 403])
async def test_bluesky_auth_required_is_unavailable(status):
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status)

    items, context = await collect_all(BlueskyCollector(Settings()), handler)

    assert items == []
    assert context.unavailable is True


async def test_lemmy_multiple_instances_one_fails():
    async def handler(request: httpx.Request) -> httpx.Response:
        if "bad.example" in str(request.url):
            return httpx.Response(500)
        return httpx.Response(
            200,
            json={
                "posts": [
                    {
                        "post": {
                            "url": "https://drive.google.com/file/d/ABC123/view",
                            "ap_id": "https://lemmy.world/post/1",
                        }
                    }
                ],
                "comments": [],
            },
        )

    settings = Settings(lemmy_instances="https://bad.example,https://lemmy.world")
    items, context = await collect_all(LemmyCollector(settings), handler, max_items=1)

    assert len(items) == 1
    assert items[0].source_name == "Lemmy"
    assert items[0].source_url == "https://lemmy.world/post/1"
    assert context.error


async def test_misskey_note_text_and_unavailable():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {
                    "id": "note1",
                    "text": "see https://drive.google.com/file/d/ABC123/view",
                }
            ],
        )

    items, _ = await collect_all(MisskeyCollector(Settings()), handler, max_items=1)

    assert items[0].source_url == "https://misskey.io/notes/note1"


async def test_feed_collector_rss_and_atom(tmp_path):
    rss = tmp_path / "rss.xml"
    atom = tmp_path / "atom.xml"
    config = tmp_path / "feeds.toml"
    rss.write_text(
        """
        <rss><channel><item><title>x</title>
        <link>https://example.com/a</link>
        <description>https://drive.google.com/file/d/ABC123/view.</description>
        </item></channel></rss>
        """,
        encoding="utf-8",
    )
    atom.write_text(
        """
        <feed xmlns="http://www.w3.org/2005/Atom"><entry>
        <title>https://docs.google.com/document/d/DOC123/edit</title>
        <link href="https://example.com/b" />
        </entry></feed>
        """,
        encoding="utf-8",
    )
    config.write_text(
        f"""
        [[feeds]]
        name = "RSS"
        url = "https://feeds.example/rss"
        enabled = true

        [[feeds]]
        name = "Atom"
        url = "https://feeds.example/atom"
        enabled = true
        """,
        encoding="utf-8",
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        text = rss.read_text() if "rss" in str(request.url) else atom.read_text()
        return httpx.Response(200, text=text)

    items, _ = await collect_all(
        FeedCollector(Settings(feed_config_path=str(config))), handler
    )

    assert [item.source_name for item in items] == ["RSS", "Atom"]
    assert items[0].source_url == "https://example.com/a"
    assert items[1].source_url == "https://example.com/b"


async def test_feed_conditional_request_and_json_feed(tmp_path):
    config = tmp_path / "feeds.toml"
    config.write_text(
        """
        [[feeds]]
        name = "JSON"
        url = "https://feeds.example/json"
        enabled = true
        """,
        encoding="utf-8",
    )
    requests = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.headers.get("If-None-Match") == '"abc"':
            return httpx.Response(304)
        return httpx.Response(
            200,
            headers={"ETag": '"abc"', "Last-Modified": "Mon, 10 Aug 2026 00:00:00 GMT"},
            json={
                "items": [
                    {
                        "id": "1",
                        "url": "https://example.com/j",
                        "content_text": "https://drive.google.com/file/d/ABC123/view",
                    }
                ]
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        context = CollectorContext(client=client, cursor={})
        items = [
            item
            async for item in FeedCollector(Settings(feed_config_path=str(config))).collect(
                context
            )
        ]
        context2 = CollectorContext(client=client, cursor=context.cursor)
        items2 = [
            item
            async for item in FeedCollector(Settings(feed_config_path=str(config))).collect(
                context2
            )
        ]

    assert len(items) == 1
    assert items2 == []
    assert requests[1].headers["If-None-Match"] == '"abc"'


def test_feed_platform_config_expands_microblog_and_writefreely(tmp_path):
    from gdpirate.collectors.feeds import _load_feed_config

    config = tmp_path / "feeds.toml"
    config.write_text(
        """
        [[microblog_users]]
        username = "alice"
        enabled = true

        [[writefreely_blogs]]
        url = "https://write.example/alice"
        enabled = true
        """,
        encoding="utf-8",
    )

    feeds = _load_feed_config(str(config))

    assert {"name": "Micro.blog", "url": "https://alice.micro.blog/feed.xml", "enabled": True} in feeds
    assert {"name": "WriteFreely", "url": "https://write.example/alice/feed/", "enabled": True} in feeds
