import enum
from datetime import UTC, datetime

from sqlalchemy import JSON, DateTime, Enum, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ResourceType(str, enum.Enum):
    FILE = "FILE"
    FOLDER = "FOLDER"
    DOCUMENT = "DOCUMENT"
    SPREADSHEET = "SPREADSHEET"
    PRESENTATION = "PRESENTATION"
    FORM = "FORM"
    DRAWING = "DRAWING"
    UNKNOWN = "UNKNOWN"


class AccessStatus(str, enum.Enum):
    UNKNOWN = "UNKNOWN"
    PUBLIC = "PUBLIC"
    RESTRICTED = "RESTRICTED"
    DEAD = "DEAD"


def utc_now() -> datetime:
    return datetime.now(UTC)


class DriveLink(Base):
    __tablename__ = "drive_links"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "resource_id",
            name="uq_drive_links_identity",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    provider: Mapped[str] = mapped_column(String(32), nullable=False, default="google")
    resource_id: Mapped[str] = mapped_column(String(512), nullable=False)
    resource_type: Mapped[ResourceType] = mapped_column(
        Enum(ResourceType, native_enum=False, length=32), nullable=False
    )
    canonical_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    source_name: Mapped[str] = mapped_column(String(128), nullable=False)
    source_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    access_status: Mapped[AccessStatus] = mapped_column(
        Enum(AccessStatus, native_enum=False, length=32),
        nullable=False,
        default=AccessStatus.UNKNOWN,
    )
    last_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class CollectorState(Base):
    __tablename__ = "collector_state"
    __table_args__ = (
        UniqueConstraint("collector_name", "scope", name="uq_collector_state_scope"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    collector_name: Mapped[str] = mapped_column(String(128), nullable=False)
    scope: Mapped[str] = mapped_column(String(256), nullable=False)
    cursor_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    last_success_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )
