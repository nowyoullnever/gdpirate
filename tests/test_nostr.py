import json

from gdpirate.collectors.base import CollectorContext
from gdpirate.collectors.nostr import NostrCollector, nostr_source_url
from gdpirate.config import Settings
from gdpirate.core.bech32 import note_id_from_event_id


class FakeWebSocket:
    def __init__(self, messages):
        self.messages = list(messages)
        self.sent = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def send(self, message):
        self.sent.append(json.loads(message))

    async def recv(self):
        if not self.messages:
            return json.dumps(["EOSE", "sub"])
        return self.messages.pop(0)


def connector_factory(sockets):
    def connect(relay, **kwargs):
        return sockets[relay]

    return connect


async def test_nostr_req_event_eose_duplicate_and_source_url():
    event_id = "00" * 32
    event = {
        "id": event_id,
        "kind": 1,
        "created_at": 100,
        "content": "https://drive.google.com/file/d/ABC123/view",
    }
    sockets = {
        "wss://one": FakeWebSocket(
            [json.dumps(["EVENT", "sub", event]), json.dumps(["EOSE", "sub"])]
        ),
        "wss://two": FakeWebSocket(
            [json.dumps(["EVENT", "sub", event]), json.dumps(["EOSE", "sub"])]
        ),
    }
    collector = NostrCollector(
        Settings(nostr_relays="wss://one,wss://two"),
        connector=connector_factory(sockets),
    )
    context = CollectorContext(client=None)

    items = [item async for item in collector.collect(context, max_items=10)]

    assert len(items) == 1
    assert items[0].source_name == "Nostr"
    assert items[0].source_url == nostr_source_url("https://njump.me", event_id)
    assert sockets["wss://one"].sent[0][0] == "REQ"
    assert context.cursor["wss://one"]["until"] == 100
    assert context.cursor["wss://one"]["boundary_event_ids"] == [event_id]


async def test_nostr_skips_previous_boundary_and_only_checkpoints_eose():
    old_event = {
        "id": "11" * 32,
        "kind": 1,
        "created_at": 100,
        "content": "https://drive.google.com/file/d/OLD123/view",
    }
    new_event = {
        "id": "22" * 32,
        "kind": 1,
        "created_at": 99,
        "content": "https://drive.google.com/file/d/NEW123/view",
    }
    sockets = {
        "wss://one": FakeWebSocket(
            [
                json.dumps(["EVENT", "sub", old_event]),
                json.dumps(["EVENT", "sub", new_event]),
                json.dumps(["EOSE", "sub"]),
            ]
        )
    }
    collector = NostrCollector(
        Settings(nostr_relays="wss://one", nostr_batch_limit=10),
        connector=connector_factory(sockets),
    )
    context = CollectorContext(
        client=None,
        cursor={"wss://one": {"until": 100, "boundary_event_ids": ["11" * 32]}},
    )

    items = [item async for item in collector.collect(context, max_items=10)]

    assert [item.raw_url for item in items] == [
        "https://drive.google.com/file/d/NEW123/view"
    ]
    assert context.cursor["wss://one"]["until"] == 99


async def test_nostr_max_items_does_not_advance_checkpoint():
    event = {
        "id": "33" * 32,
        "kind": 1,
        "created_at": 100,
        "content": "https://drive.google.com/file/d/ABC123/view",
    }
    sockets = {"wss://one": FakeWebSocket([json.dumps(["EVENT", "sub", event])])}
    collector = NostrCollector(
        Settings(nostr_relays="wss://one"),
        connector=connector_factory(sockets),
    )
    context = CollectorContext(client=None)

    items = [item async for item in collector.collect(context, max_items=1)]

    assert items
    assert "wss://one" not in context.cursor


def test_note_bech32_encoding_known_zero_id():
    assert note_id_from_event_id("00" * 32).startswith("note1")
