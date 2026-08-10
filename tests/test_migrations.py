from pathlib import Path

import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from gdpirate.config import get_settings


def test_identity_migration_merges_duplicate_resource_ids(tmp_path, monkeypatch):
    db_path = tmp_path / "migration.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    get_settings.cache_clear()
    config = Config(str(Path("alembic.ini").resolve()))
    config.set_main_option("script_location", str(Path("alembic").resolve()))

    command.upgrade(config, "0001_create_drive_links")
    engine = sa.create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT INTO drive_links
                (provider, resource_id, resource_type, canonical_url, source_name,
                 source_url, access_status, last_checked_at, created_at, updated_at)
                VALUES
                ('google', 'ABC123', 'FILE',
                 'https://drive.google.com/file/d/ABC123/view', 'first', NULL,
                 'UNKNOWN', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
                ('google', 'ABC123', 'DOCUMENT',
                 'https://docs.google.com/document/d/ABC123/edit', 'second',
                 'https://example.com/source', 'PUBLIC', CURRENT_TIMESTAMP,
                 CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
            )
        )

    command.upgrade(config, "head")

    with engine.begin() as conn:
        rows = conn.execute(sa.text("SELECT * FROM drive_links")).mappings().all()
        states = conn.execute(
            sa.text(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='collector_state'"
            )
        ).all()

    assert len(rows) == 1
    assert rows[0]["resource_type"] == "DOCUMENT"
    assert rows[0]["source_url"] == "https://example.com/source"
    assert states
    get_settings.cache_clear()
