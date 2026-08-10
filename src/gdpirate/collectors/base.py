from collections.abc import AsyncIterator
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
    unavailable: bool = False
    error: str | None = None

    def set_cursor(self, scope: str, value: dict) -> None:
        self.cursor[scope] = value

    def get_cursor(self, scope: str) -> dict:
        value = self.cursor.get(scope)
        return value if isinstance(value, dict) else {}
