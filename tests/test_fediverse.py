import httpx

from gdpirate.collectors.base import CollectorContext
from gdpirate.collectors.fediverse import FediverseCollector, detect_fediverse_instance
from gdpirate.config import Settings


async def test_detects_mastodon_and_collects_public_timeline(tmp_path):
    config = tmp_path / "fediverse.toml"
    config.write_text(
        '[[instances]]\nurl = "https://mastodon.example"\nenabled = true\n',
        encoding="utf-8",
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/.well-known/nodeinfo":
            return httpx.Response(
                200,
                json={"links": [{"href": "https://mastodon.example/nodeinfo/2.0"}]},
            )
        if path == "/nodeinfo/2.0":
            return httpx.Response(200, json={"software": {"name": "mastodon"}})
        if path == "/api/v1/timelines/public":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": "9",
                        "visibility": "public",
                        "content": "<p>https://drive.google.com/file/d/ABC123/view</p>",
                        "url": "https://mastodon.example/@a/9",
                    },
                    {
                        "id": "8",
                        "visibility": "private",
                        "content": "https://drive.google.com/file/d/PRIVATE/view",
                    },
                ],
            )
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        context = CollectorContext(client=client)
        items = [
            item
            async for item in FediverseCollector(
                Settings(fediverse_instance_config_path=str(config))
            ).collect(context, max_items=1)
        ]

    assert items[0].source_name == "Mastodon"
    assert items[0].source_url == "https://mastodon.example/@a/9"
    assert context.scanned == 1


async def test_fediverse_detection_auth_required_and_unknown_compatible():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/instance":
            return httpx.Response(200, json={"version": "unknown"})
        if request.url.path == "/api/v1/timelines/public":
            return httpx.Response(401)
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        detection = await detect_fediverse_instance(client, "https://unknown.example")

    assert detection.compatible is True
    assert detection.public_timeline_available is False
    assert detection.status == "authentication_required"


async def test_detects_akkoma_and_pixelfed_from_nodeinfo():
    async def run_detection(software: str):
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/.well-known/nodeinfo":
                return httpx.Response(
                    200,
                    json={"links": [{"href": f"https://{software}.example/nodeinfo"}]},
                )
            if request.url.path == "/nodeinfo":
                return httpx.Response(200, json={"software": {"name": software}})
            if request.url.path == "/api/v1/timelines/public":
                return httpx.Response(200, json=[])
            return httpx.Response(404)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await detect_fediverse_instance(client, f"https://{software}.example")

    akkoma = await run_detection("akkoma")
    pixelfed = await run_detection("pixelfed")

    assert akkoma.software == "akkoma"
    assert akkoma.compatible is True
    assert pixelfed.software == "pixelfed"
    assert pixelfed.public_timeline_available is True


async def test_fediverse_one_instance_fails_another_works(tmp_path):
    config = tmp_path / "fediverse.toml"
    config.write_text(
        """
        [[instances]]
        url = "https://bad.example"
        enabled = true
        [[instances]]
        url = "https://pleroma.example"
        enabled = true
        """,
        encoding="utf-8",
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        if "bad.example" in str(request.url):
            return httpx.Response(500)
        if request.url.path == "/.well-known/nodeinfo":
            return httpx.Response(200, json={"links": [{"href": "https://pleroma.example/nodeinfo"}]})
        if request.url.path == "/nodeinfo":
            return httpx.Response(200, json={"software": {"name": "pleroma"}})
        if request.url.path == "/api/v1/timelines/public":
            return httpx.Response(200, json=[])
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        context = CollectorContext(client=client)
        items = [
            item
            async for item in FediverseCollector(
                Settings(fediverse_instance_config_path=str(config))
            ).collect(context, max_items=1)
        ]

    assert items == []
    assert context.cursor["https://pleroma.example/detection"]["software"] == "pleroma"
