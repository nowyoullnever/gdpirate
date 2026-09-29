"""access checker v2

Revision ID: 0006_access_checker_v2
Revises: 0005_operational_metrics
Create Date: 2026-09-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006_access_checker_v2"
down_revision: str | None = "0005_operational_metrics"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("drive_links", sa.Column("last_check_reason", sa.String(length=64), nullable=True))
    op.add_column("drive_links", sa.Column("access_check_version", sa.Integer(), nullable=True))
    op.add_column("collection_run_metrics", sa.Column("access_reasons_json", sa.JSON(), nullable=True))
    op.add_column("validation_run_metrics", sa.Column("reason_counts_json", sa.JSON(), nullable=True))
    op.add_column("validation_run_metrics", sa.Column("transition_counts_json", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("validation_run_metrics", "transition_counts_json")
    op.drop_column("validation_run_metrics", "reason_counts_json")
    op.drop_column("collection_run_metrics", "access_reasons_json")
    op.drop_column("drive_links", "access_check_version")
    op.drop_column("drive_links", "last_check_reason")
