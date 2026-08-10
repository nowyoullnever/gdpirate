"""use resource identity and add collector state

Revision ID: 0002_identity_and_collector_state
Revises: 0001_create_drive_links
Create Date: 2026-08-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_identity_and_collector_state"
down_revision: str | None = "0001_create_drive_links"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SPECIFICITY = {
    "UNKNOWN": 0,
    "FILE": 1,
    "DOCUMENT": 2,
    "SPREADSHEET": 2,
    "PRESENTATION": 2,
    "FORM": 2,
    "DRAWING": 2,
    "FOLDER": 2,
}


def upgrade() -> None:
    _merge_duplicate_drive_links()
    with op.batch_alter_table("drive_links") as batch_op:
        batch_op.drop_constraint("uq_drive_links_identity", type_="unique")
        batch_op.create_unique_constraint(
            "uq_drive_links_identity", ["provider", "resource_id"]
        )

    op.create_table(
        "collector_state",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("collector_name", sa.String(length=128), nullable=False),
        sa.Column("scope", sa.String(length=256), nullable=False),
        sa.Column("cursor_json", sa.JSON(), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "collector_name", "scope", name="uq_collector_state_scope"
        ),
    )


def downgrade() -> None:
    op.drop_table("collector_state")
    with op.batch_alter_table("drive_links") as batch_op:
        batch_op.drop_constraint("uq_drive_links_identity", type_="unique")
        batch_op.create_unique_constraint(
            "uq_drive_links_identity", ["provider", "resource_id", "resource_type"]
        )


def _merge_duplicate_drive_links() -> None:
    bind = op.get_bind()
    groups = bind.execute(
        sa.text(
            """
            SELECT provider, resource_id, COUNT(*) AS count
            FROM drive_links
            GROUP BY provider, resource_id
            HAVING COUNT(*) > 1
            """
        )
    ).mappings()

    for group in groups:
        rows = list(
            bind.execute(
                sa.text(
                    """
                    SELECT *
                    FROM drive_links
                    WHERE provider = :provider AND resource_id = :resource_id
                    ORDER BY id ASC
                    """
                ),
                {
                    "provider": group["provider"],
                    "resource_id": group["resource_id"],
                },
            ).mappings()
        )
        winner = rows[0]
        chosen_type = max(
            (row["resource_type"] for row in rows),
            key=lambda value: SPECIFICITY.get(value, 0),
        )
        source_row = next((row for row in rows if row["source_url"]), winner)
        checked_rows = [row for row in rows if row["last_checked_at"] is not None]
        status_row = (
            max(checked_rows, key=lambda row: row["last_checked_at"])
            if checked_rows
            else winner
        )
        updated_at = max(row["updated_at"] for row in rows if row["updated_at"])
        canonical = _canonical_url(chosen_type, winner["resource_id"])

        bind.execute(
            sa.text(
                "DELETE FROM drive_links WHERE provider = :provider "
                "AND resource_id = :resource_id AND id != :winner_id"
            ),
            {
                "provider": group["provider"],
                "resource_id": group["resource_id"],
                "winner_id": winner["id"],
            },
        )
        bind.execute(
            sa.text(
                """
                UPDATE drive_links
                SET resource_type = :resource_type,
                    canonical_url = :canonical_url,
                    source_name = :source_name,
                    source_url = :source_url,
                    access_status = :access_status,
                    last_checked_at = :last_checked_at,
                    updated_at = :updated_at
                WHERE id = :id
                """
            ),
            {
                "id": winner["id"],
                "resource_type": chosen_type,
                "canonical_url": canonical,
                "source_name": source_row["source_name"],
                "source_url": source_row["source_url"],
                "access_status": status_row["access_status"],
                "last_checked_at": status_row["last_checked_at"],
                "updated_at": updated_at,
            },
        )


def _canonical_url(resource_type: str, resource_id: str) -> str:
    if resource_type == "FILE":
        return f"https://drive.google.com/file/d/{resource_id}/view"
    if resource_type == "FOLDER":
        return f"https://drive.google.com/drive/folders/{resource_id}"
    doc_paths = {
        "DOCUMENT": "document",
        "SPREADSHEET": "spreadsheets",
        "PRESENTATION": "presentation",
        "FORM": "forms",
        "DRAWING": "drawings",
    }
    if resource_type in doc_paths:
        return f"https://docs.google.com/{doc_paths[resource_type]}/d/{resource_id}/edit"
    return f"https://drive.google.com/open?id={resource_id}"
