from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol

from pydantic import BaseModel, Field

from gdpirate.core.drive_urls import parse_google_url


class CandidateLink(BaseModel):
    raw_url: str
    source_name: str = Field(min_length=1, max_length=128)
    source_url: str | None = None


class Collector(Protocol):
    name: str

    async def collect(
        self, context: "CollectorContext", *, max_items: int | None = None
    ) -> AsyncIterator[CandidateLink]:
        ...


@dataclass
class CollectorContext:
    client: object
    cursor: dict = field(default_factory=dict)
    checkpoint_callback: Callable[[str, dict], Awaitable[None]] | None = None
    unavailable: bool = False
    error: str | None = None
    scanned: int = 0

    def set_cursor(self, scope: str, value: dict) -> None:
        self.cursor[scope] = value

    async def checkpoint(self, scope: str, value: dict) -> None:
        self.set_cursor(scope, value)
        if self.checkpoint_callback is not None:
            await self.checkpoint_callback(scope, value)

    def get_cursor(self, scope: str) -> dict:
        value = self.cursor.get(scope)
        return value if isinstance(value, dict) else {}

    def mark_scanned(self, count: int = 1) -> None:
        self.scanned += count


def distinct_google_urls(raw_urls: list[str]) -> list[str]:
    seen: set[tuple[str, str]] = set()
    urls: list[str] = []
    for raw_url in raw_urls:
        parsed = parse_google_url(raw_url)
        if parsed is None:
            continue
        identity = (parsed.provider, parsed.resource_id)
        if identity in seen:
            continue
        seen.add(identity)
        urls.append(raw_url)
    return urls
