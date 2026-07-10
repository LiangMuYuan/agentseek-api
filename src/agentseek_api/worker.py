from __future__ import annotations

import asyncio
import signal
from contextlib import suppress

from agentseek_api.core.database import db_manager
from agentseek_api.services.redis_queue import RedisRunQueue
from agentseek_api.services.run_jobs import execute_run_job
from agentseek_api.settings import settings


async def run_worker(
    *,
    queue: RedisRunQueue | None = None,
    stop_after_jobs: int | None = None,
    poll_timeout_seconds: int | None = None,
    shutdown_event: asyncio.Event | None = None,
) -> int:
    if settings.EXECUTOR_BACKEND.strip().lower() != "redis":
        raise RuntimeError("The worker requires EXECUTOR_BACKEND=redis.")

    await db_manager.initialize()
    run_queue = queue or RedisRunQueue()
    processed = 0
    processed_lock = asyncio.Lock()
    stop_requested = shutdown_event or asyncio.Event()
    registered_signals: list[signal.Signals] = []
    loop = asyncio.get_running_loop()
    concurrency = max(1, settings.WORKER_CONCURRENT_JOBS)
    semaphore = asyncio.Semaphore(concurrency)
    active_tasks: set[asyncio.Task] = set()

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, stop_requested.set)
            except (NotImplementedError, RuntimeError, ValueError):
                continue
            registered_signals.append(signum)
        # 只让第一个启动的 worker 做 requeue，防止多 worker 抢同一任务
        first_startup = await run_queue.acquire_startup_lock(ttl_seconds=30)
        if first_startup:
            await run_queue.requeue_inflight()
        timeout_seconds = poll_timeout_seconds if poll_timeout_seconds is not None else settings.REDIS_WORKER_POLL_TIMEOUT_SECONDS

        while not stop_requested.is_set():
            while len(active_tasks) < concurrency:
                if stop_after_jobs is not None:
                    async with processed_lock:
                        if processed >= stop_after_jobs:
                            break
                    if processed >= stop_after_jobs:
                        break

                reserved = await run_queue.reserve(timeout_seconds=timeout_seconds)
                if reserved is None:
                    break

                job, token = reserved

                async def _run_job(job: object, token: str) -> int:
                    async with semaphore:
                        await execute_run_job(job)
                        await run_queue.ack(token)
                    async with processed_lock:
                        nonlocal processed
                        processed += 1
                        return processed

                task = asyncio.create_task(_run_job(job, token))
                active_tasks.add(task)
                task.add_done_callback(active_tasks.discard)

            if stop_after_jobs is not None:
                async with processed_lock:
                    if processed >= stop_after_jobs:
                        break

            if not active_tasks:
                await asyncio.sleep(0.1)

    except asyncio.CancelledError:
        raise
    finally:
        if active_tasks:
            done, pending = await asyncio.wait(active_tasks, timeout=30)
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.wait(pending)

        for signum in registered_signals:
            loop.remove_signal_handler(signum)
        await run_queue.close()
        await db_manager.close()

    return processed


def main() -> int:
    return asyncio.run(run_worker())


if __name__ == "__main__":
    raise SystemExit(main())
