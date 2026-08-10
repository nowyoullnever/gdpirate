from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "sqlite:///./gdpirate.db"
    http_timeout_seconds: float = 20
    http_max_concurrency: int = 5
    http_max_redirects: int = 10
    access_check_max_body_bytes: int = 524288
    access_recheck_hours: int = 24
    enable_gdurl: bool = False
    enable_dedigger: bool = False
    enable_common_crawl: bool = False
    user_agent: str = Field(
        default="GDPirate/0.1 (+https://github.com/nowyoullnever/gdpirate)"
    )

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    @property
    def async_database_url(self) -> str:
        if self.database_url.startswith("sqlite:///"):
            return self.database_url.replace("sqlite:///", "sqlite+aiosqlite:///", 1)
        if self.database_url.startswith("postgresql://"):
            return self.database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return self.database_url

    @property
    def sync_database_url(self) -> str:
        if self.database_url.startswith("sqlite+aiosqlite:///"):
            return self.database_url.replace("sqlite+aiosqlite:///", "sqlite:///", 1)
        if self.database_url.startswith("postgresql+asyncpg://"):
            return self.database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
        return self.database_url


@lru_cache
def get_settings() -> Settings:
    return Settings()
