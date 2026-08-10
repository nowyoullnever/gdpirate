"""operations

Revision ID: 0004_operations
Revises: 0003_add_random_key
Create Date: 2026-08-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_operations"
down_revision: str | None = "0003_add_random_key"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("drive_links") as batch_op:
        batch_op.create_index("ix_drive_links_status_id", ["access_status", "id"])
        batch_op.create_index(
            "ix_drive_links_status_checked_id",
            ["access_status", "last_checked_at", "id"],
        )

    op.create_table(
        "scheduled_job_state",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("job_name", sa.String(length=128), nullable=False),
        sa.Column("last_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_result_json", sa.JSON(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("job_name", name="uq_scheduled_job_state_job_name"),
    )


def downgrade() -> None:
    op.drop_table("scheduled_job_state")
    with op.batch_alter_table("drive_links") as batch_op:
        batch_op.drop_index("ix_drive_links_status_checked_id")
        batch_op.drop_index("ix_drive_links_status_id")
