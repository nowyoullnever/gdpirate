# GDPirate

GDPirate is a small Python project for storing Google Drive and Google Docs links that were discovered on public internet sources and are accessible to an anonymous visitor.

This repository currently implements Task 1 only: project structure, configuration, database schema, Google URL parsing and normalization, deduplication, source storage, anonymous access checking, CLI utilities, and tests.

It does not implement collectors, a frontend, brute forcing, Google account access, OAuth, Drive API credentials, file downloading, folder crawling, or permission bypassing.

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
```

## Tests

```bash
uv run pytest
```

The normal test suite uses mocked HTTP responses for access checks. Optional live checks should be added as explicit opt-in tests only.

## Architecture

Future collectors emit `CandidateLink` objects and call the central ingestion service. Collectors do not parse Google URLs and do not talk to SQLAlchemy directly.

The URL parser accepts known Google Drive, Docs, Sheets, Slides, Forms, and Drawings URL shapes, extracts a stable Google resource identity, and produces deterministic canonical URLs. Lookalike domains are rejected.

Deduplication is based on the actual Google resource identity through a unique `(provider, resource_id, resource_type)` constraint. GDPirate stores one source name and source URL per resource; it does not keep discovery history or occurrence counts.

Anonymous access checking uses `httpx.AsyncClient` without Google cookies, OAuth, browser state, or stored credentials. Uncertain results fail closed to `UNKNOWN`, not `PUBLIC`.
