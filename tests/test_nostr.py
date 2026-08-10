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
    assert context.cursor["wss://one"]["until"] == 99


def test_note_bech32_encoding_known_zero_id():
    assert note_id_from_event_id("00" * 32).startswith("note1")
