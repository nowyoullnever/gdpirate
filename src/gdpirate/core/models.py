import enum
from datetime import UTC, datetime

from sqlalchemy import Float, Index, JSON, DateTime, Enum, String, Text, UniqueConstraint
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
        Index("ix_drive_links_status_random", "access_status", "random_key"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    provider: Mapped[str] = mapped_column(String(32), nullable=False, default="google")
    resource_id: Mapped[str] = mapped_column(String(512), nullable=False)
    resource_type: Mapped[ResourceType] = mapped_column(
        Enum(ResourceType, native_enum=False, length=32), nullable=False
    )
    canonical_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    random_key: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
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
    last_check_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    access_check_version: Mapped[int | None] = mapped_column(nullable=True)
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


class ScheduledJobState(Base):
    __tablename__ = "scheduled_job_state"
    __table_args__ = (
        UniqueConstraint("job_name", name="uq_scheduled_job_state_job_name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    job_name: Mapped[str] = mapped_column(String(128), nullable=False)
    last_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_result_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class CollectionRunMetric(Base):
    __tablename__ = "collection_run_metrics"
    __table_args__ = (
        Index("ix_collection_metrics_started", "started_at"),
        Index("ix_collection_metrics_source_started", "source", "started_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False)
    trigger: Mapped[str] = mapped_column(String(32), nullable=False)
    job_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source: Mapped[str] = mapped_column(String(128), nullable=False)
    state_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    duration_ms: Mapped[int] = mapped_column(nullable=False, default=0)
    scanned: Mapped[int] = mapped_column(nullable=False, default=0)
    candidates: Mapped[int] = mapped_column(nullable=False, default=0)
    created: Mapped[int] = mapped_column(nullable=False, default=0)
    duplicates: Mapped[int] = mapped_column(nullable=False, default=0)
    access_checks: Mapped[int] = mapped_column(nullable=False, default=0)
    public: Mapped[int] = mapped_column(nullable=False, default=0)
    restricted: Mapped[int] = mapped_column(nullable=False, default=0)
    dead: Mapped[int] = mapped_column(nullable=False, default=0)
    unknown: Mapped[int] = mapped_column(nullable=False, default=0)
    access_reasons_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    unavailable: Mapped[bool] = mapped_column(nullable=False, default=False)
    success: Mapped[bool] = mapped_column(nullable=False, default=True)


class ValidationRunMetric(Base):
    __tablename__ = "validation_run_metrics"
    __table_args__ = (
        Index("ix_validation_metrics_started", "started_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False)
    trigger: Mapped[str] = mapped_column(String(32), nullable=False)
    job_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    requested_status: Mapped[str] = mapped_column(String(32), nullable=False)
    source_filter: Mapped[str | None] = mapped_column(String(128), nullable=True)
    stale_only: Mapped[bool] = mapped_column(nullable=False, default=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    duration_ms: Mapped[int] = mapped_column(nullable=False, default=0)
    selected: Mapped[int] = mapped_column(nullable=False, default=0)
    checked: Mapped[int] = mapped_column(nullable=False, default=0)
    public: Mapped[int] = mapped_column(nullable=False, default=0)
    restricted: Mapped[int] = mapped_column(nullable=False, default=0)
    dead: Mapped[int] = mapped_column(nullable=False, default=0)
    unknown: Mapped[int] = mapped_column(nullable=False, default=0)
    errors: Mapped[int] = mapped_column(nullable=False, default=0)
    by_source_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    reason_counts_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    transition_counts_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
