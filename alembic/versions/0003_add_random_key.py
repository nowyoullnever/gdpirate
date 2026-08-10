"""add random key

Revision ID: 0003_add_random_key
Revises: 0002_identity_and_collector_state
Create Date: 2026-08-10
"""

from collections.abc import Sequence
import hashlib

import sqlalchemy as sa
from alembic import op

revision: str = "0003_add_random_key"
down_revision: str | None = "0002_identity_and_collector_state"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("drive_links") as batch_op:
        batch_op.add_column(sa.Column("random_key", sa.Float(), nullable=True))

    bind = op.get_bind()
    rows = bind.execute(sa.text("SELECT id, provider, resource_id FROM drive_links"))
    for row in rows:
        bind.execute(
            sa.text("UPDATE drive_links SET random_key = :random_key WHERE id = :id"),
            {
                "id": row.id,
                "random_key": _stable_random_key(row.provider, row.resource_id),
            },
        )

    with op.batch_alter_table("drive_links") as batch_op:
        batch_op.alter_column("random_key", nullable=False)
        batch_op.create_index(
            "ix_drive_links_status_random", ["access_status", "random_key"]
        )


def downgrade() -> None:
    with op.batch_alter_table("drive_links") as batch_op:
        batch_op.drop_index("ix_drive_links_status_random")
        batch_op.drop_column("random_key")


def _stable_random_key(provider: str, resource_id: str) -> float:
    digest = hashlib.sha256(f"{provider}:{resource_id}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") / 2**64
