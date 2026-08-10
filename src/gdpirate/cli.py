import asyncio
import os
from pathlib import Path

import typer
import uvicorn
from alembic import command
from alembic.config import Config

from gdpirate.collectors.base import CandidateLink
from gdpirate.core.access_check import AccessChecker
from gdpirate.core.database import session_scope
from gdpirate.core.drive_urls import parse_google_url
from gdpirate.core.logging import configure_logging
from gdpirate.core.models import AccessStatus
from gdpirate.core.database import SessionLocal
from gdpirate.core.models import CollectorState
from gdpirate.pipeline.collection import CollectionRunner, source_statuses
from gdpirate.pipeline.ingestion import (
    IngestionService,
    count_drive_links,
    count_drive_links_by_status,
)
from gdpirate.pipeline.validation import validate_links
from gdpirate.collectors.commoncrawl import fetch_collinfo
from gdpirate.config import get_settings
from gdpirate.core.http import HttpClientFactory
from gdpirate.worker import Worker, list_job_rows
from sqlalchemy import delete, select

app = typer.Typer(no_args_is_help=True)


@app.command("init-db")
def init_db() -> None:
    """Initialize or upgrade the database."""
    command.upgrade(_alembic_config(), "head")
    typer.echo("database initialized")


@app.command("parse-url")
def parse_url(url: str) -> None:
    parsed = parse_google_url(url)
    typer.echo(f"valid: {str(parsed is not None).lower()}")
    if not parsed:
        return
    typer.echo(f"provider: {parsed.provider}")
    typer.echo(f"resource_id: {parsed.resource_id}")
    typer.echo(f"resource_type: {parsed.resource_type.value}")
    typer.echo(f"canonical_url: {parsed.canonical_url}")


@app.command("check-url")
def check_url(url: str) -> None:
    async def run() -> None:
        status = await AccessChecker().check(url)
        typer.echo(status.value)

    asyncio.run(run())


@app.command("ingest-url")
def ingest_url(
    url: str,
    source_name: str = typer.Option(..., "--source-name"),
    source_url: str | None = typer.Option(None, "--source-url"),
) -> None:
    async def run() -> None:
        async with session_scope() as session:
            service = IngestionService(session)
            result = await service.ingest(
                CandidateLink(raw_url=url, source_name=source_name, source_url=source_url)
            )
            if not result.valid:
                typer.echo("valid: false")
                typer.echo(f"message: {result.message}")
                raise typer.Exit(1)
            typer.echo(f"created: {str(result.created).lower()}")
            typer.echo(f"duplicate: {str(result.duplicate).lower()}")
            typer.echo(f"resource_id: {result.resource_id}")
            typer.echo(f"canonical_url: {result.canonical_url}")
            typer.echo(f"access_status: {result.access_status.value}")

    asyncio.run(run())


@app.command("stats")
def stats() -> None:
    async def run() -> None:
        async with session_scope() as session:
            total = await count_drive_links(session)
            counts = await count_drive_links_by_status(session)
            typer.echo("Drive links")
            typer.echo("-----------")
            typer.echo(f"Total: {total}")
            typer.echo(f"Public: {counts[AccessStatus.PUBLIC]}")
            typer.echo(f"Restricted: {counts[AccessStatus.RESTRICTED]}")
            typer.echo(f"Dead: {counts[AccessStatus.DEAD]}")
            typer.echo(f"Unknown: {counts[AccessStatus.UNKNOWN]}")

    asyncio.run(run())


@app.command("sources")
def sources() -> None:
    for name, status in source_statuses().items():
        typer.echo(f"{name}\t{status}")


@app.command("collect")
def collect(
    source: str,
    max_items: int | None = typer.Option(None, "--max-items"),
    max_items_per_source: int | None = typer.Option(None, "--max-items-per-source"),
    mode: str | None = typer.Option(None, "--mode"),
    max_files: int | None = typer.Option(None, "--max-files"),
    max_records: int | None = typer.Option(None, "--max-records"),
    fresh_head: bool = typer.Option(False, "--fresh-head"),
) -> None:
    async def run() -> None:
        runner = CollectionRunner()
        results = await runner.collect(
            source,
            max_items=max_items,
            max_items_per_source=max_items_per_source,
            commoncrawl_mode=mode,
            max_files=max_files,
            max_records=max_records,
            state_mode="fresh-head" if fresh_head else "persistent",
        )
        for result in results:
            typer.echo(
                f"{result.source}: scanned={result.scanned} candidates={result.candidates} "
                f"created={result.created} duplicates={result.duplicates} "
                f"public={result.public} restricted={result.restricted} "
                f"dead={result.dead} unknown={result.unknown} "
                f"unavailable={str(result.unavailable).lower()} "
                f"error={result.error or ''}"
            )

    asyncio.run(run())


@app.command("commoncrawl-crawls")
def commoncrawl_crawls() -> None:
    async def run() -> None:
        settings = get_settings()
        factory = HttpClientFactory(settings)
        async with factory.client() as client:
            for item in await fetch_collinfo(client, settings):
                crawl_id = item.get("id")
                if crawl_id:
                    typer.echo(crawl_id)

    asyncio.run(run())


@app.command("serve")
def serve(
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(8000, "--port"),
) -> None:
    """Serve the minimal random-link web app."""
    uvicorn.run("gdpirate.web:app", host=host, port=port)


@app.command("worker")
def worker(once: bool = typer.Option(False, "--once")) -> None:
    """Run scheduled collection and validation jobs."""
    settings = get_settings()
    configure_logging(settings)

    async def run() -> None:
        await Worker(settings=settings).run(once=once)

    asyncio.run(run())


@app.command("run-job")
def run_job(job_name: str) -> None:
    """Run one configured job now."""
    settings = get_settings()
    configure_logging(settings)

    async def run() -> None:
        outcome = await Worker(settings=settings).run_named_job(job_name)
        if not outcome.ran:
            typer.echo(f"{job_name}: skipped {outcome.skipped_reason}")
            return
        if outcome.error:
            typer.echo(f"{job_name}: failed {outcome.error}")
            raise typer.Exit(1)
        typer.echo(f"{job_name}: completed {outcome.result}")

    asyncio.run(run())


@app.command("jobs")
def jobs() -> None:
    """Show configured scheduled jobs and latest state."""

    async def run() -> None:
        for job, state in await list_job_rows():
            typer.echo(
                "\t".join(
                    [
                        job.name,
                        f"enabled={str(job.enabled).lower()}",
                        f"kind={job.kind}",
                        f"next={state.next_run_at if state else ''}",
                        f"last_success={state.last_success_at if state else ''}",
                        f"failures={state.consecutive_failures if state else 0}",
                        f"error={(state.last_error or '') if state else ''}",
                    ]
                )
            )

    asyncio.run(run())


@app.command("validate")
def validate(
    max_items: int | None = typer.Option(None, "--max-items"),
    concurrency: int | None = typer.Option(None, "--concurrency"),
    source: str | None = typer.Option(None, "--source"),
    status: str = typer.Option("UNKNOWN", "--status"),
    stale_only: bool = typer.Option(False, "--stale-only"),
) -> None:
    async def run() -> None:
        result = await validate_links(
            max_items=max_items,
            concurrency=concurrency,
            source=source,
            status=AccessStatus[status.upper()],
            stale_only=stale_only,
        )
        typer.echo(f"selected={result.selected}")
        typer.echo(f"checked={result.checked}")
        typer.echo(f"public={result.public}")
        typer.echo(f"restricted={result.restricted}")
        typer.echo(f"dead={result.dead}")
        typer.echo(f"unknown={result.unknown}")
        typer.echo(f"errors={result.errors}")

    asyncio.run(run())


@app.command("collector-state")
def collector_state(source: str) -> None:
    async def run() -> None:
        async with SessionLocal() as session:
            rows = (
                await session.execute(
                    select(CollectorState).where(CollectorState.collector_name == source)
                )
            ).scalars().all()
            for row in rows:
                typer.echo(f"{row.scope}: {row.cursor_json} error={row.last_error or ''}")

    asyncio.run(run())


@app.command("reset-collector")
def reset_collector(source: str, yes: bool = typer.Option(False, "--yes")) -> None:
    if not yes:
        typer.confirm(f"Reset collector state for {source}?", abort=True)

    async def run() -> None:
        async with SessionLocal() as session:
            async with session.begin():
                await session.execute(
                    delete(CollectorState).where(CollectorState.collector_name == source)
                )
        typer.echo(f"reset {source}")

    asyncio.run(run())


@app.command("live-access-check")
def live_access_check() -> None:
    """Opt-in live access calibration using GDPIRATE_LIVE_* environment URLs."""

    async def run() -> None:
        checker = AccessChecker()
        for name, env_name in {
            "PUBLIC": "GDPIRATE_LIVE_PUBLIC_URL",
            "RESTRICTED": "GDPIRATE_LIVE_RESTRICTED_URL",
            "DEAD": "GDPIRATE_LIVE_DEAD_URL",
        }.items():
            url = os.environ.get(env_name)
            if not url:
                typer.echo(f"{name}: skipped")
                continue
            status = await checker.check(url)
            typer.echo(f"{name}: {status.value}")

    asyncio.run(run())


def _alembic_config() -> Config:
    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    return config


if __name__ == "__main__":
    app()
