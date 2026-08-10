"""operational metrics

Revision ID: 0005_operational_metrics
Revises: 0004_operations
Create Date: 2026-08-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_operational_metrics"
down_revision: str | None = "0004_operations"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "collection_run_metrics",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("trigger", sa.String(length=32), nullable=False),
        sa.Column("job_name", sa.String(length=128), nullable=True),
        sa.Column("source", sa.String(length=128), nullable=False),
        sa.Column("state_mode", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("scanned", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("candidates", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duplicates", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("access_checks", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("public", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("restricted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("dead", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unknown", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unavailable", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("success", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.create_index("ix_collection_metrics_started", "collection_run_metrics", ["started_at"])
    op.create_index(
        "ix_collection_metrics_source_started",
        "collection_run_metrics",
        ["source", "started_at"],
    )

    op.create_table(
        "validation_run_metrics",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("trigger", sa.String(length=32), nullable=False),
        sa.Column("job_name", sa.String(length=128), nullable=True),
        sa.Column("requested_status", sa.String(length=32), nullable=False),
        sa.Column("source_filter", sa.String(length=128), nullable=True),
        sa.Column("stale_only", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("selected", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("checked", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("public", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("restricted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("dead", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unknown", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("errors", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("by_source_json", sa.JSON(), nullable=True),
    )
    op.create_index("ix_validation_metrics_started", "validation_run_metrics", ["started_at"])


def downgrade() -> None:
    op.drop_index("ix_validation_metrics_started", table_name="validation_run_metrics")
    op.drop_table("validation_run_metrics")
    op.drop_index("ix_collection_metrics_source_started", table_name="collection_run_metrics")
    op.drop_index("ix_collection_metrics_started", table_name="collection_run_metrics")
    op.drop_table("collection_run_metrics")
