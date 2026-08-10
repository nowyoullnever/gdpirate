from dataclasses import dataclass
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx


@dataclass(frozen=True)
class RobotsDecision:
    allowed: bool
    crawl_delay: float | None = None
    error: str | None = None


class RobotsPolicy:
    def __init__(self, user_agent: str) -> None:
        self.user_agent = user_agent
        self._cache: dict[str, RobotFileParser | None] = {}

    async def allowed(self, client: httpx.AsyncClient, target_url: str) -> RobotsDecision:
        parsed = urlparse(target_url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        if origin not in self._cache:
            robots_url = urljoin(origin, "/robots.txt")
            try:
                response = await client.get(robots_url)
            except httpx.HTTPError as exc:
                self._cache[origin] = None
                return RobotsDecision(False, error=str(exc))
            if response.status_code >= 500:
                self._cache[origin] = None
                return RobotsDecision(False, error=f"robots status {response.status_code}")
            parser = RobotFileParser()
            parser.set_url(robots_url)
            if response.status_code == 404:
                parser.parse([])
            elif response.status_code < 400:
                parser.parse(response.text.splitlines())
            else:
                self._cache[origin] = None
                return RobotsDecision(False, error=f"robots status {response.status_code}")
            self._cache[origin] = parser
        parser = self._cache[origin]
        if parser is None:
            return RobotsDecision(False, error="robots unavailable")
        return RobotsDecision(
            parser.can_fetch(self.user_agent, target_url),
            crawl_delay=parser.crawl_delay(self.user_agent),
        )
