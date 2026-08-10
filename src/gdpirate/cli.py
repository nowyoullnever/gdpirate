import asyncio
from pathlib import Path

import typer
from alembic import command
from alembic.config import Config

from gdpirate.collectors.base import CandidateLink
from gdpirate.core.access_check import AccessChecker
from gdpirate.core.database import session_scope
from gdpirate.core.drive_urls import parse_google_url
from gdpirate.core.models import AccessStatus
from gdpirate.pipeline.collection import CollectionRunner, source_statuses
from gdpirate.pipeline.ingestion import (
    IngestionService,
    count_drive_links,
    count_drive_links_by_status,
)

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
) -> None:
    async def run() -> None:
        runner = CollectionRunner()
        results = await runner.collect(
            source,
            max_items=max_items,
            max_items_per_source=max_items_per_source,
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


def _alembic_config() -> Config:
    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    return config


if __name__ == "__main__":
    app()
