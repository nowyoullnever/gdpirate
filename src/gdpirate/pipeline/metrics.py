from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import logging
from uuid import uuid4

from sqlalchemy import case, delete, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from gdpirate.core.database import SessionLocal
from gdpirate.core.models import (
    AccessStatus,
    CollectionRunMetric,
    DriveLink,
    ValidationRunMetric,
    utc_now,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MetricContext:
    run_id: str
    trigger: str
    job_name: str | None = None

    @classmethod
    def create(cls, trigger: str, job_name: str | None = None) -> "MetricContext":
        return cls(run_id=str(uuid4()), trigger=trigger, job_name=job_name)


async def record_collection_metrics(
    results,
    *,
    metric_context: MetricContext | None,
    session_factory: async_sessionmaker | None = None,
) -> None:
    if metric_context is None:
        return
    session_factory = session_factory or SessionLocal
    try:
        async with session_factory() as session:
            async with session.begin():
                for result in results:
                    session.add(
                        CollectionRunMetric(
                            run_id=metric_context.run_id,
                            trigger=metric_context.trigger,
                            job_name=metric_context.job_name,
                            source=result.source,
                            state_mode=str(getattr(result, "state_mode", "persistent")),
                            started_at=result.started_at,
                            finished_at=result.finished_at,
                            duration_ms=result.duration_ms,
                            scanned=result.scanned,
                            candidates=result.candidates,
                            created=result.created,
                            duplicates=result.duplicates,
                            access_checks=result.access_checks,
                            public=result.public,
                            restricted=result.restricted,
                            dead=result.dead,
                            unknown=result.unknown,
                            unavailable=result.unavailable,
                            success=not bool(result.error or result.unavailable),
                        )
                    )
    except Exception:
        logger.exception("collection metrics persistence failed")


async def record_validation_metric(
    result,
    *,
    metric_context: MetricContext | None,
    requested_status: AccessStatus,
    source_filter: str | None,
    stale_only: bool,
    session_factory: async_sessionmaker | None = None,
) -> None:
    if metric_context is None:
        return
    session_factory = session_factory or SessionLocal
    try:
        async with session_factory() as session:
            async with session.begin():
                session.add(
                    ValidationRunMetric(
                        run_id=metric_context.run_id,
                        trigger=metric_context.trigger,
                        job_name=metric_context.job_name,
                        requested_status=requested_status.value,
                        source_filter=source_filter,
                        stale_only=stale_only,
                        started_at=result.started_at,
                        finished_at=result.finished_at,
                        duration_ms=result.duration_ms,
                        selected=result.selected,
                        checked=result.checked,
                        public=result.public,
                        restricted=result.restricted,
                        dead=result.dead,
                        unknown=result.unknown,
                        errors=result.errors,
                        by_source_json=result.by_source,
                    )
                )
    except Exception:
        logger.exception("validation metrics persistence failed")


async def metrics_summary(
    *,
    hours: float,
    source: str | None = None,
    kind: str | None = None,
    session_factory: async_sessionmaker | None = None,
) -> dict:
    session_factory = session_factory or SessionLocal
    since = utc_now() - timedelta(hours=hours)
    payload = {"window": {"hours": hours, "since": since.isoformat()}, "collection": [], "validation": []}
    async with session_factory() as session:
        if kind in {None, "collect"}:
            stmt = select(
                CollectionRunMetric.source,
                func.count(CollectionRunMetric.id),
                func.sum(CollectionRunMetric.scanned),
                func.sum(CollectionRunMetric.candidates),
                func.sum(CollectionRunMetric.created),
                func.sum(CollectionRunMetric.duplicates),
                func.sum(CollectionRunMetric.access_checks),
                func.sum(CollectionRunMetric.public),
                func.sum(CollectionRunMetric.restricted),
                func.sum(CollectionRunMetric.dead),
                func.sum(CollectionRunMetric.unknown),
                func.sum(case((CollectionRunMetric.success.is_(False), 1), else_=0)),
            ).where(CollectionRunMetric.started_at >= since)
            if source:
                stmt = stmt.where(CollectionRunMetric.source == source)
            stmt = stmt.group_by(CollectionRunMetric.source).order_by(CollectionRunMetric.source)
            for row in await session.execute(stmt):
                item = _collection_row(row)
                payload["collection"].append(item)
        if kind in {None, "validate"}:
            rows = (
                await session.execute(
                    select(ValidationRunMetric).where(ValidationRunMetric.started_at >= since)
                )
            ).scalars().all()
            if source:
                rows = [
                    row
                    for row in rows
                    if source in (row.by_source_json or {}) or row.source_filter == source
                ]
            payload["validation"] = [_validation_summary(rows)]
    return payload


async def stats_by_source(
    *,
    session_factory: async_sessionmaker | None = None,
) -> list[dict]:
    session_factory = session_factory or SessionLocal
    async with session_factory() as session:
        rows = (
            await session.execute(
                select(DriveLink.source_name, DriveLink.access_status, func.count(DriveLink.id))
                .group_by(DriveLink.source_name, DriveLink.access_status)
            )
        ).all()
    grouped: dict[str, dict] = {}
    for source_name, status, count in rows:
        item = grouped.setdefault(
            source_name,
            {"source": source_name, "total": 0, "public": 0, "restricted": 0, "dead": 0, "unknown": 0},
        )
        key = status.value.lower()
        item[key] += int(count)
        item["total"] += int(count)
    return sorted(grouped.values(), key=lambda item: item["total"], reverse=True)


async def prune_metrics(
    *,
    keep_days: int,
    session_factory: async_sessionmaker | None = None,
) -> dict:
    if keep_days <= 0:
        raise ValueError("keep_days must be positive")
    session_factory = session_factory or SessionLocal
    cutoff = utc_now() - timedelta(days=keep_days)
    async with session_factory() as session:
        async with session.begin():
            collection = await session.execute(
                delete(CollectionRunMetric)
                .where(CollectionRunMetric.started_at < cutoff)
                .execution_options(synchronize_session=False)
            )
            validation = await session.execute(
                delete(ValidationRunMetric)
                .where(ValidationRunMetric.started_at < cutoff)
                .execution_options(synchronize_session=False)
            )
    return {
        "collection_deleted": int(collection.rowcount or 0),
        "validation_deleted": int(validation.rowcount or 0),
    }


def _collection_row(row) -> dict:
    (
        source,
        runs,
        scanned,
        candidates,
        created,
        duplicates,
        access_checks,
        public,
        restricted,
        dead,
        unknown,
        errors,
    ) = row
    item = {
        "source": source,
        "runs": int(runs or 0),
        "scanned": int(scanned or 0),
        "candidates": int(candidates or 0),
        "created": int(created or 0),
        "duplicates": int(duplicates or 0),
        "access_checks": int(access_checks or 0),
        "public": int(public or 0),
        "restricted": int(restricted or 0),
        "dead": int(dead or 0),
        "unknown": int(unknown or 0),
        "errors": int(errors or 0),
    }
    resolved = item["public"] + item["restricted"] + item["dead"]
    item["candidate_rate"] = _rate(item["candidates"], item["scanned"])
    item["new_rate"] = _rate(item["created"], item["candidates"])
    item["duplicate_rate"] = _rate(item["duplicates"], item["candidates"])
    item["resolved_public_rate"] = _rate(item["public"], resolved)
    return item


def _validation_summary(rows: list[ValidationRunMetric]) -> dict:
    summary = {
        "runs": len(rows),
        "selected": sum(row.selected for row in rows),
        "checked": sum(row.checked for row in rows),
        "public": sum(row.public for row in rows),
        "restricted": sum(row.restricted for row in rows),
        "dead": sum(row.dead for row in rows),
        "unknown": sum(row.unknown for row in rows),
        "errors": sum(row.errors for row in rows),
        "by_source": {},
    }
    resolved = summary["public"] + summary["restricted"] + summary["dead"]
    summary["public_rate"] = _rate(summary["public"], resolved)
    for row in rows:
        for source_name, values in (row.by_source_json or {}).items():
            target = summary["by_source"].setdefault(
                source_name,
                {"selected": 0, "checked": 0, "public": 0, "restricted": 0, "dead": 0, "unknown": 0, "errors": 0},
            )
            for key in target:
                target[key] += int(values.get(key, 0))
    return summary


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator
