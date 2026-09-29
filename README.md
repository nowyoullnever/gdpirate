# GDPirate

GDPirate is a small Python project for storing Google Drive and Google Docs links that were discovered on public internet sources and are accessible to an anonymous visitor.

This repository implements project structure, configuration, database schema, Google URL parsing and normalization, deduplication, source storage, anonymous access checking, collectors, validation, a random-link API, a minimal web UI, CLI utilities, and tests.

It does not implement brute forcing, Google account access, OAuth, Drive API credentials, file downloading, folder crawling, public statistics, admin UI, source selectors, or permission bypassing.

## Install

```bash
uv sync --extra test
```

## Configure

Copy `.env.example` to `.env` if you need local overrides. No API keys are required.

Important settings:

```env
DATABASE_URL=sqlite:///./gdpirate.db
HTTP_TIMEOUT_SECONDS=20
HTTP_MAX_CONCURRENCY=5
ACCESS_CHECK_MAX_BODY_BYTES=524288
ACCESS_RECHECK_HOURS=24
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
DB_POOL_TIMEOUT_SECONDS=30
DB_POOL_RECYCLE_SECONDS=1800
WORKER_POLL_SECONDS=15
LOG_LEVEL=INFO
LOG_FORMAT=text
```

Future source toggles such as `ENABLE_GDURL`, `ENABLE_DEDIGGER`, and `ENABLE_COMMON_CRAWL` are administrator configuration only and default to `false`.

## Database

Initialize or upgrade the database with Alembic:

```bash
uv run gdpirate init-db
```

Equivalent manual command:

```bash
uv run alembic upgrade head
```

## CLI

```bash
uv run gdpirate parse-url "https://drive.google.com/file/d/ABC123/view"
uv run gdpirate check-url "https://drive.google.com/file/d/ABC123/view"
uv run gdpirate ingest-url "https://drive.google.com/file/d/ABC123/view" --source-name manual --source-url "https://example.com/post"
uv run gdpirate stats
uv run gdpirate sources
uv run gdpirate collect hackernews --max-items 10
uv run gdpirate collect all --max-items-per-source 10
uv run gdpirate collect fediverse --max-items 20
uv run gdpirate collect nostr --max-items 20
uv run gdpirate collect gdurl --max-items 10
uv run gdpirate collect dedigger --max-items 10
uv run gdpirate validate --max-items 100 --concurrency 3
uv run gdpirate commoncrawl-crawls
uv run gdpirate collect commoncrawl --mode url-index --max-files 1 --max-items 100
uv run gdpirate collect commoncrawl --mode wat --max-files 1 --max-items 100
uv run gdpirate collect commoncrawl --mode wat --max-files 1 --max-records 10000
uv run gdpirate collect naver --max-items 10
uv run gdpirate collect daum --max-items 10
uv run gdpirate collect hackernews --max-items 200 --fresh-head
uv run gdpirate worker
uv run gdpirate worker --once
uv run gdpirate jobs
uv run gdpirate run-job recent
uv run gdpirate metrics
uv run gdpirate metrics --hours 168 --json
uv run gdpirate stats --by-source
uv run gdpirate metrics-prune
uv run gdpirate collector-state gdurl
uv run gdpirate live-access-check
uv run gdpirate serve
```

## Web

Initialize the database, collect links, validate them, then run:

```bash
uv run gdpirate serve
```

The local app listens on `http://127.0.0.1:8000` by default. `GET /healthz` is process liveness. `GET /readyz` performs a lightweight database/schema readiness check. `GET /api/random` returns one verified public Drive URL and its source with `Cache-Control: no-store`, or `503 {"error":"no_verified_public_link_available"}` when no eligible row exists or the database is unavailable.

## Public UI

The public GitHub Pages UI lives at `https://nowyoullnever.github.io/gdpirate/`.
It is currently a minimal static UI shell. Backend `/api/random` integration is intentionally a later step.

## Local SQLite

SQLite remains supported for local development and tests:

```bash
uv run gdpirate init-db
uv run gdpirate collect hackernews --max-items 20
uv run gdpirate validate --max-items 100
uv run gdpirate serve
```

SQLite worker locking is a same-process fallback only. Use PostgreSQL for persistent deployment or multiple worker processes.

## Fresh-Head And Backfill

Persistent collection loads and updates `collector_state`, so it is used for historical cursor/backfill progress. Fresh-head collection starts from an empty ephemeral cursor and does not write checkpoints:

```bash
uv run gdpirate collect hackernews --max-items 200 --fresh-head
```

Fresh-head accepts repeated work; database deduplication keeps it cheap while ensuring newest upstream posts continue to be checked.

## Worker

Scheduled jobs live in `config/jobs.toml`:

```bash
uv run gdpirate worker
uv run gdpirate worker --once
uv run gdpirate jobs
uv run gdpirate run-job recent
```

The worker runs recent fresh-head collection, persistent feed polling, historical backfill, UNKNOWN validation, and stale PUBLIC revalidation. Partial collection failures are recorded in the job summary but use the normal interval when at least one source succeeds. If every configured source fails, the job is marked failed and bounded backoff applies. Heavy collectors (`gdurl`, `dedigger`, `commoncrawl`) and credentialed Korean APIs (`naver`, `daum`) are not scheduled by default.

The FastAPI process does not run collectors, validation, or scheduling.

## Private Metrics

Operational metrics are private CLI-only telemetry. They are not exposed by the public website.

```bash
uv run gdpirate metrics
uv run gdpirate metrics --hours 168
uv run gdpirate metrics --json
uv run gdpirate stats --by-source
uv run gdpirate metrics-prune --keep-days 90
```

`metrics` reports historical aggregate run telemetry: one collection row per source run and one validation row per validation invocation. It stores counts, source keys/names, job names, timings, and status totals only. It does not store Google URLs, resource IDs, source URLs, candidate URLs, response bodies, or content.

`stats --by-source` reports current database state grouped by the currently stored best source for each DriveLink. It is not complete discovery history.

Bulk/deferred collectors such as `gdurl`, `dedigger`, and `commoncrawl` may show high UNKNOWN counts immediately after collection; their usefulness should be judged with later validation metrics.

## Docker Compose

For a production-like PostgreSQL stack:

```bash
copy .env.docker.example .env.docker
# edit POSTGRES_PASSWORD and DATABASE_URL
docker compose --env-file .env.docker up --build
```

Compose starts PostgreSQL 18, runs `gdpirate init-db` in a migration service, then starts separate web and worker services. PostgreSQL data is stored in the named `postgres-data` volume mounted at `/var/lib/postgresql`. `docker compose down` preserves that volume; `docker compose down -v` deletes it. The public app is available at `http://127.0.0.1:8000` by default.

## Tests

```bash
uv run pytest
```

The normal test suite uses mocked HTTP responses for access checks. Optional live checks should be added as explicit opt-in tests only.

## Architecture

Future collectors emit `CandidateLink` objects and call the central ingestion service. Collectors do not parse Google URLs and do not talk to SQLAlchemy directly.
The collector set discovers links from Hacker News, anonymous Bluesky search when available, Lemmy, Misskey, configured RSS/Atom/JSON feeds, Mastodon-compatible Fediverse public timelines, and read-only Nostr relay windows.
Bulk collectors `gdurl` and `dedigger` are implemented but disabled by default. Enable them with administrator environment flags only: `ENABLE_GDURL=true` or `ENABLE_DEDIGGER=true`. Bulk sources use deferred Google access validation, so newly discovered resources remain `UNKNOWN` until `gdpirate validate` checks them anonymously.
Common Crawl is also disabled by default with `ENABLE_COMMON_CRAWL=false`. It supports URL Index mode through DuckDB/Parquet and WAT mode through archived WAT metadata. It reads only Common Crawl archive infrastructure, never live source websites, and uses deferred Google validation.
Common Crawl URL Index processing validates the Parquet schema, consumes DuckDB results in bounded batches, and runs DuckDB work outside the asyncio event loop. WAT scans can be bounded with `--max-records`.
NAVER and Daum collectors are optional official API collectors. NAVER uses NAVER API HUB headers, and Daum uses the Kakao REST API key. Both are disabled by default, use empty secret values in `.env.example`, and report `unconfigured` when enabled without credentials.

The URL parser accepts known Google Drive, Docs, Sheets, Slides, Forms, and Drawings URL shapes, extracts a stable Google resource identity, and produces deterministic canonical URLs. Lookalike domains are rejected.

Deduplication is based on the actual Google resource identity through a unique `(provider, resource_id)` constraint. GDPirate stores one source name and source URL per resource; it does not keep discovery history or occurrence counts. Source URLs are normalized to public HTTP/HTTPS links at ingestion; invalid schemes such as `javascript:`, `data:`, and `file:` are discarded. If a later URL reveals a more specific Google resource type, the stored type and canonical URL are upgraded.
Random web selection uses a deterministic indexed `random_key` and wraps around the keyspace instead of using `ORDER BY RANDOM()`. Only `PUBLIC` rows with a stored source URL are eligible, and stale links are anonymously rechecked before they are returned.

Anonymous access checking uses `httpx.AsyncClient` without Google cookies, OAuth, browser state, or stored credentials. Uncertain results fail closed to `UNKNOWN`, not `PUBLIC`.
Access checks use bounded streaming and only inspect up to the configured body byte limit.

Optional live access calibration uses environment-provided URLs only:

```env
GDPIRATE_LIVE_PUBLIC_URL=
GDPIRATE_LIVE_RESTRICTED_URL=
GDPIRATE_LIVE_DEAD_URL=
```

Run `uv run gdpirate live-access-check` after setting one or more values.

Fediverse instances live in `config/fediverse_instances.toml`. Feed seeds live in `config/feeds.toml`, including SpaceHey feed seeds plus disabled examples for Micro.blog and WriteFreely.
deDigger query seeds live in `config/dedigger_queries.toml`. gdURL follows only the configured public Browse All catalogue URL and never guesses shortcodes.
Validation uses keyset-paginated DB batches controlled by `VALIDATION_DB_BATCH_SIZE`, so unbounded validation does not load the full UNKNOWN population into memory.
