"""Лабораторна 4: ендпоінти черги задач, метрики черги, повернення прострочених задач у чергу."""
import asyncio
import json
import random
import time
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, status
from prometheus_client import Gauge

from app import database

router = APIRouter()

TASKS_QUEUED = Gauge("tasks_queued", "Кількість задач у статусі queued (зріз із БД кожні 5 с)")
TASKS_RUNNING = Gauge("tasks_running", "Кількість задач у статусі running (зріз із БД кожні 5 с)")
TASKS_DONE = Gauge("tasks_done", "Кількість задач у статусі done (зріз із БД кожні 5 с)")
TASKS_FAILED = Gauge("tasks_failed", "Кількість задач у статусі failed (зріз із БД кожні 5 с)")

TASK_COLUMNS = (
    "id, record_id, status, seed, attempts, max_attempts, token, lease_until, "
    "worker_id, created_at, started_at, finished_at"
)

# Повернення прострочених задач у чергу; після max_attempts спроб задача стає failed.
REQUEUE_SQL = """
UPDATE tasks
SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END,
    worker_id = NULL,
    finished_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END
WHERE status = 'running' AND lease_until < now()
"""


def _task_dict(row, result=None) -> dict:
    d = dict(row)
    d["result"] = result
    return d


@router.post("/appointments/{appointment_id}/tasks", status_code=status.HTTP_202_ACCEPTED)
async def create_task(appointment_id: UUID):
    if database.pool is None:
        await database.init_db_pool()
    seed = random.getrandbits(31)
    async with database.pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM appointments WHERE id = $1", appointment_id)
        if not exists:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Запис не знайдено")
        row = await conn.fetchrow(
            f"INSERT INTO tasks (record_id, seed) VALUES ($1, $2) RETURNING {TASK_COLUMNS}",
            appointment_id,
            seed,
        )
    return _task_dict(row)


@router.get("/tasks/{task_id}")
async def get_task(task_id: int):
    if database.pool is None:
        await database.init_db_pool()
    async with database.pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT {TASK_COLUMNS} FROM tasks WHERE id = $1", task_id)
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Задачу не знайдено")
        res = await conn.fetchval("SELECT result FROM task_results WHERE task_id = $1 LIMIT 1", task_id)
    return _task_dict(row, json.loads(res) if isinstance(res, str) else res)


@router.get("/tasks")
async def list_tasks(
    status_filter: Optional[str] = Query(None, alias="status", pattern="^(queued|running|done|failed)$"),
    limit: int = Query(20, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    if database.pool is None:
        await database.init_db_pool()
    async with database.pool.acquire() as conn:
        if status_filter:
            total = await conn.fetchval("SELECT COUNT(*) FROM tasks WHERE status = $1", status_filter)
            rows = await conn.fetch(
                f"SELECT {TASK_COLUMNS} FROM tasks WHERE status = $1 ORDER BY id LIMIT $2 OFFSET $3",
                status_filter, limit, offset,
            )
        else:
            total = await conn.fetchval("SELECT COUNT(*) FROM tasks")
            rows = await conn.fetch(
                f"SELECT {TASK_COLUMNS} FROM tasks ORDER BY id LIMIT $1 OFFSET $2", limit, offset
            )
    return {"items": [_task_dict(r) for r in rows], "total": total, "limit": limit, "offset": offset}


async def refresh_once() -> None:
    """Один цикл: повернути прострочені задачі в чергу і оновити gauge зі стану БД."""
    if database.pool is None:
        await database.init_db_pool()
    async with database.pool.acquire() as conn:
        res = await conn.execute(REQUEUE_SQL)   # 'UPDATE n'
        n_requeued = int(res.split()[-1])
        if n_requeued:
            print(f"[requeue] n={n_requeued} t={time.time():.3f}", flush=True)
        rows = await conn.fetch("SELECT status, COUNT(*) AS n FROM tasks GROUP BY status")
    counts = {r["status"]: r["n"] for r in rows}
    TASKS_QUEUED.set(counts.get("queued", 0))
    TASKS_RUNNING.set(counts.get("running", 0))
    TASKS_DONE.set(counts.get("done", 0))
    TASKS_FAILED.set(counts.get("failed", 0))


async def refresh_loop(period: float = 5.0) -> None:
    while True:
        try:
            await refresh_once()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # БД може бути недоступна (сценарій Ф4): просто чекаємо наступного кола
            print(f"[tasks] refresh failed: {e}", flush=True)
        await asyncio.sleep(period)
