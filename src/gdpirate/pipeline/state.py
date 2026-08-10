from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from gdpirate.core.models import CollectorState


async def load_collector_cursor(
    session: AsyncSession, collector_name: str, scope: str = "default"
) -> dict:
    state = await get_collector_state(session, collector_name, scope)
    return state.cursor_json or {} if state else {}


async def get_collector_state(
    session: AsyncSession, collector_name: str, scope: str
) -> CollectorState | None:
    result = await session.execute(
        select(CollectorState).where(
            CollectorState.collector_name == collector_name,
            CollectorState.scope == scope,
        )
    )
    return result.scalar_one_or_none()


async def record_collector_attempt(
    session: AsyncSession, collector_name: str, scope: str = "default"
) -> CollectorState:
    state = await get_collector_state(session, collector_name, scope)
    if state is None:
        state = CollectorState(
            collector_name=collector_name,
            scope=scope,
            cursor_json={},
            last_attempt_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        session.add(state)
    else:
        state.last_attempt_at = datetime.now(UTC)
    await session.flush()
    return state


async def record_collector_success(
    session: AsyncSession,
    collector_name: str,
    scope: str,
    cursor_json: dict,
) -> None:
    state = await get_collector_state(session, collector_name, scope)
    if state is None:
        state = CollectorState(
            collector_name=collector_name,
            scope=scope,
            cursor_json=cursor_json,
        )
        session.add(state)
    state.cursor_json = cursor_json
    state.last_success_at = datetime.now(UTC)
    state.last_attempt_at = state.last_attempt_at or datetime.now(UTC)
    state.last_error = None
    state.updated_at = datetime.now(UTC)
    await session.flush()


async def record_collector_error(
    session: AsyncSession, collector_name: str, scope: str, error: str
) -> None:
    state = await get_collector_state(session, collector_name, scope)
    if state is None:
        state = CollectorState(
            collector_name=collector_name,
            scope=scope,
            cursor_json={},
        )
        session.add(state)
    state.last_attempt_at = datetime.now(UTC)
    state.last_error = error[:4000]
    state.updated_at = datetime.now(UTC)
    await session.flush()
