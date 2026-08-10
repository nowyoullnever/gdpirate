from collections.abc import AsyncIterator
from typing import Protocol

from pydantic import BaseModel, Field


class CandidateLink(BaseModel):
    raw_url: str
    source_name: str = Field(min_length=1, max_length=128)
    source_url: str | None = None


class Collector(Protocol):
    name: str

    async def collect(self) -> AsyncIterator[CandidateLink]:
        ...
