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
    db_pool_size: int = 5
    db_max_overflow: int = 10
    db_pool_timeout_seconds: int = 30
    db_pool_recycle_seconds: int = 1800
    worker_poll_seconds: float = 15
    worker_failure_base_backoff_seconds: int = 60
    worker_failure_max_backoff_seconds: int = 1800
    jobs_config_path: str = "./config/jobs.toml"
    log_level: str = "INFO"
    log_format: str = "text"
    bluesky_api_base: str = "https://api.bsky.app"
    lemmy_instances: str = "https://lemmy.world,https://lemmy.ml"
    misskey_instances: str = "https://misskey.io"
    feed_config_path: str = "./config/feeds.toml"
    fediverse_instance_config_path: str = "./config/fediverse_instances.toml"
    fediverse_local_only: bool = True
    nostr_relays: str = "wss://nos.lol,wss://relay.primal.net"
    nostr_viewer_base: str = "https://njump.me"
    nostr_batch_limit: int = 100
    gdurl_browse_url: str = "https://gdurl.com/all"
    gdurl_request_delay_seconds: float = 1.0
    gdurl_max_concurrency: int = 1
    gdurl_resolve_max_body_bytes: int = 262144
    dedigger_base_url: str = "https://www.dedigger.com"
    dedigger_query_config_path: str = "./config/dedigger_queries.toml"
    dedigger_request_delay_seconds: float = 2.0
    dedigger_max_concurrency: int = 1
    validation_db_batch_size: int = 500
    commoncrawl_collinfo_url: str = "https://index.commoncrawl.org/collinfo.json"
    commoncrawl_data_base: str = "https://data.commoncrawl.org"
    commoncrawl_crawls: str = "latest"
    commoncrawl_default_mode: str = "url-index"
    commoncrawl_wat_concurrency: int = 1
    commoncrawl_checkpoint_record_interval: int = 5000
    commoncrawl_temp_dir: str = "./commoncrawl-data"
    commoncrawl_url_index_batch_size: int = 1000
    enable_naver: bool = False
    naver_api_hub_base: str = "https://naverapihub.apigw.ntruss.com"
    naver_api_hub_client_id: str | None = None
    naver_api_hub_client_secret: str | None = None
    naver_max_requests_per_run: int = 500
    naver_request_delay_seconds: float = 0.1
    enable_daum: bool = False
    kakao_rest_api_key: str | None = None
    kakao_daum_search_base: str = "https://dapi.kakao.com"
    daum_max_requests_per_run: int = 500
    daum_request_delay_seconds: float = 0.1
    korea_query_config_path: str = "./config/korea_queries.toml"
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
        if self.database_url.startswith("postgresql+psycopg://"):
            return self.database_url.replace("postgresql+psycopg://", "postgresql+asyncpg://", 1)
        return self.database_url

    @property
    def sync_database_url(self) -> str:
        if self.database_url.startswith("sqlite+aiosqlite:///"):
            return self.database_url.replace("sqlite+aiosqlite:///", "sqlite:///", 1)
        if self.database_url.startswith("postgresql+asyncpg://"):
            return self.database_url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
        if self.database_url.startswith("postgresql+psycopg://"):
            return self.database_url
        if self.database_url.startswith("postgresql://"):
            return self.database_url.replace("postgresql://", "postgresql+psycopg://", 1)
        return self.database_url

    @property
    def lemmy_instance_list(self) -> list[str]:
        return _split_csv_urls(self.lemmy_instances)

    @property
    def misskey_instance_list(self) -> list[str]:
        return _split_csv_urls(self.misskey_instances)

    @property
    def nostr_relay_list(self) -> list[str]:
        return _split_csv_urls(self.nostr_relays)

    @property
    def commoncrawl_crawl_list(self) -> list[str]:
        return [item.strip() for item in self.commoncrawl_crawls.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()


def _split_csv_urls(value: str) -> list[str]:
    return [item.rstrip("/") for item in value.split(",") if item.strip()]
