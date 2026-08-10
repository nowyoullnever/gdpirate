from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol

from pydantic import BaseModel, Field


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
