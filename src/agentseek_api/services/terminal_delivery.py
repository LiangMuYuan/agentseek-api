"""Transactional terminal state and a durable two-envelope Redis outbox.

Redis deliberately has three phases: pending SQL result, stream delivery, final
SQL state. An end frame may precede the final GET status during phase 3 retry.
The stored result is immutable within an execution generation.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import asyncio
from datetime import UTC, datetime
import logging
from typing import Any

from sqlalchemy import select, update

from agentseek_api.core.database import db_manager
from agentseek_api.core.orm import Run, Thread
from agentseek_api.services.stream_persistence import (
    add_run_stream_event_to_session,
    add_thread_stream_event_to_session,
)
from agentseek_api.services.transaction_retry import retry_transaction
from agentseek_api.settings import settings

logger = logging.getLogger(__name__)
TERMINAL = {"success", "error", "interrupted"}


@dataclass(frozen=True)
class TerminalResult:
    status: str
    output: Any = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    thread_status: str | None = None


async def append_redis_envelope(**kwargs):
    # Lazy import avoids the protocol/persistence module import cycle.
    from agentseek_api.services.stream_persistence import append_redis_envelope as append
    return await append(**kwargs, retain=True)


def _envelopes(job, result: TerminalResult):
    from agentseek_api.services.thread_protocol import protocol_timestamp_ms
    lifecycle = {"success": "completed", "error": "failed", "interrupted": "interrupted"}[result.status]
    data = {"event": lifecycle, "graph_name": job.graph_id}
    if result.error is not None:
        data["error"] = result.error
    return [
        {"scope": "run", "stream_id": job.run_id,
         "operation_id": f"terminal:{job.run_id}:{job.execution_id}:run",
         "payload": {"event": "end", "status": result.status}},
        {"scope": "thread", "stream_id": job.thread_id,
         "operation_id": f"terminal:{job.run_id}:{job.execution_id}:thread",
         "payload": {"method": "lifecycle", "params": {
             "namespace": [], "timestamp": protocol_timestamp_ms(), "data": data}}},
    ]


async def _apply_result(session, run, result):
    run.status = result["status"]
    run.output_json = result["output"]
    run.last_error = result["error"]
    run.metadata_json = {**(run.metadata_json or {}), **result["metadata"]}
    run.execution_owner = None
    run.execution_lease_until = None
    run.dispatch_state = "finished"
    await session.execute(update(Thread).where(Thread.thread_id == run.thread_id).values(
        status=result.get("thread_status") or {"success": "idle", "error": "error", "interrupted": "interrupted"}[run.status],
        state_updated_at=datetime.now(UTC),
    ))


def _publish(envelopes, records):
    from agentseek_api.services import run_jobs
    for envelope, (seq, payload) in zip(envelopes, records, strict=True):
        try:
            if envelope["scope"] == "run":
                run_jobs.run_broker.publish(envelope["stream_id"], payload["event"], seq=seq,
                    **{k: v for k, v in payload.items() if k != "event"})
            else:
                run_jobs.thread_protocol_broker.publish(envelope["stream_id"], payload, seq=seq, persist=False)
        except Exception:
            # Durable replay, not re-appending, is the fallback for a failed notification.
            logger.exception("Terminal broker notification failed after durable commit")


async def finish_run(job, result: TerminalResult) -> None:
    if result.status not in TERMINAL:
        raise ValueError(f"Invalid terminal status: {result.status}")
    redis = settings.EXECUTOR_BACKEND.strip().lower() == "redis"
    envelopes = _envelopes(job, result)

    async def stage(session):
        run = await session.scalar(select(Run).where(Run.run_id == job.run_id).with_for_update())
        if run is None or run.execution_id != job.execution_id or run.status in TERMINAL:
            return None
        if run.status == "terminal_pending":
            return (run.terminal_result["envelopes"], []) if redis else None
        if run.execution_owner != job.owner_id:
            return None
        # Acquire a write lock on SQLite too, and fence a concurrently reclaimed
        # owner. Retrying the entire transaction rereads all predicates.
        locked = await session.execute(update(Run).where(
            Run.run_id == job.run_id, Run.execution_id == job.execution_id,
            Run.execution_owner == job.owner_id, Run.status == run.status,
        ).values(status="terminal_pending" if redis else run.status))
        if locked.rowcount != 1:
            return None
        stored = {**asdict(result), "envelopes": envelopes}
        if redis:
            run.status = "terminal_pending"
            run.terminal_result = stored
            run.execution_lease_until = None
            return envelopes, []
        await _apply_result(session, run, stored)
        end = await add_run_stream_event_to_session(session, job.run_id, payload=envelopes[0]["payload"])
        lifecycle = await add_thread_stream_event_to_session(session, job.thread_id, payload=envelopes[1]["payload"])
        return envelopes, [end, lifecycle]

    staged = await retry_transaction(stage)
    if staged is None:
        return
    if redis:
        await _deliver_pending(job.run_id, job.execution_id)
    else:
        _publish(*staged)


async def _deliver_pending(run_id: str, generation: str) -> bool:
    from agentseek_api.services.redis_delivery import reconcile_protocol_deliveries
    await reconcile_protocol_deliveries(run_id=run_id)
    # Hold the SQL row lock through the bounded external writes. This serializes
    # cancellation/deletion and multiple reconcilers with this terminal unit.
    async def deliver(session):
        run = await session.scalar(select(Run).where(Run.run_id == run_id).with_for_update())
        if run is None or run.execution_id != generation or run.status != "terminal_pending":
            return None
        locked = await session.execute(update(Run).where(
            Run.run_id == run_id, Run.execution_id == generation, Run.status == "terminal_pending",
        ).values(status="terminal_pending"))
        if locked.rowcount != 1:
            return None
        stored = run.terminal_result
        async with asyncio.timeout(5):
            records = [await append_redis_envelope(**envelope) for envelope in stored["envelopes"]]
        await _apply_result(session, run, stored)
        return stored["envelopes"], records
    delivered = await retry_transaction(deliver)
    if delivered is None:
        return False
    _publish(*delivered)
    await _cleanup_terminal_markers(run_id, generation)
    return True


async def _cleanup_terminal_markers(run_id: str, generation: str) -> None:
    from agentseek_api.services.stream_persistence import expire_redis_envelope
    try:
        async with db_manager.get_session_factory()() as session:
            run = await session.get(Run, run_id)
            if run is None or run.execution_id != generation or run.status not in TERMINAL or not run.terminal_result:
                return
            envelopes = run.terminal_result["envelopes"]
        async with asyncio.timeout(5):
            for envelope in envelopes:
                await expire_redis_envelope(**envelope)
        async def cleanup(session):
            await session.execute(update(Run).where(
                Run.run_id == run_id, Run.execution_id == generation, Run.status.in_(TERMINAL),
            ).values(terminal_result=None))
        await retry_transaction(cleanup)
    except Exception:
        logger.exception("Terminal marker cleanup remains pending", extra={"run_id": run_id})


async def reconcile_terminal_deliveries(*, limit: int = 100) -> int:
    async with db_manager.get_session_factory()() as session:
        pending = list((await session.execute(select(Run.run_id, Run.execution_id, Run.status).where(
            Run.terminal_result.is_not(None),
        ).order_by(Run.updated_at).limit(limit))).all())
    completed = 0
    for run_id, generation, status in pending:
        try:
            if status == "terminal_pending":
                completed += await _deliver_pending(run_id, generation)
            else:
                await _cleanup_terminal_markers(run_id, generation)
        except Exception:
            logger.exception("Terminal delivery remains pending", extra={"run_id": run_id})
    return completed
