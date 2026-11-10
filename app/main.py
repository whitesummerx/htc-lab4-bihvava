import asyncio
from contextlib import asynccontextmanager
from datetime import date
from typing import Optional
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app import database, tasks
from app.metrics import MetricsMiddleware
from app.models import (
    AppointmentCreate,
    AppointmentListResponse,
    AppointmentResponse,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await database.init_db_pool()
    except Exception as e:
        print(f"[Warning] Помилка підключення до БД під час старту: {e}")
    refresher = asyncio.create_task(tasks.refresh_loop())
    yield
    refresher.cancel()
    await database.close_db_pool()


app = FastAPI(title="Appointments Service", lifespan=lifespan)
app.add_middleware(MetricsMiddleware)
app.include_router(tasks.router)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc: RequestValidationError):
    errors = []
    for err in exc.errors():
        loc = " -> ".join([str(l) for l in err.get("loc", []) if l != "body"])
        errors.append(f"Поле '{loc}': {err.get('msg')}")
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": "; ".join(errors)},
    )


@app.get("/ping", include_in_schema=False)
async def ping():
    """Службовий ендпоінт без звернення до бази: baseline для перевірки генератора."""
    return {"pong": True}


@app.get("/metrics", include_in_schema=False)
async def metrics():
    """Метрики у текстовому форматі Prometheus."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/livez", status_code=status.HTTP_200_OK)
async def livez():
    """Процес живий; до бази не звертається."""
    return {"status": "alive"}


@app.get("/healthz", status_code=status.HTTP_200_OK)
async def healthz():
    """Перевірка доступності бази даних."""
    if database.pool is None:
        try:
            await database.init_db_pool()
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"База даних недоступна: {str(e)}",
            )
    try:
        async with database.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"База даних недоступна: {str(e)}",
        )


@app.post(
    "/appointments",
    response_model=AppointmentResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_appointment(appointment: AppointmentCreate):
    if database.pool is None:
        await database.init_db_pool()

    async with database.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO appointments (patient_code, doctor, room, start_time, duration_minutes, status)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, patient_code, doctor, room, start_time, duration_minutes, status, created_at
            """,
            appointment.patient_code,
            appointment.doctor,
            appointment.room,
            appointment.start_time,
            appointment.duration_minutes,
            appointment.status,
        )
        return dict(row)


@app.get("/appointments", response_model=AppointmentListResponse)
async def get_appointments(
    date_filter: Optional[date] = Query(
        None, alias="date", description="Фільтр за датою прийому YYYY-MM-DD"
    ),
    limit: int = Query(20, ge=1, le=100, description="Кількість записів"),
    offset: int = Query(0, ge=0, description="Зсув"),
):
    if database.pool is None:
        await database.init_db_pool()

    async with database.pool.acquire() as conn:
        if date_filter:
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM appointments WHERE (start_time AT TIME ZONE 'UTC')::date = $1",
                date_filter,
            )
            rows = await conn.fetch(
                """
                SELECT id, patient_code, doctor, room, start_time, duration_minutes, status, created_at
                FROM appointments
                WHERE (start_time AT TIME ZONE 'UTC')::date = $1
                ORDER BY start_time ASC
                LIMIT $2 OFFSET $3
                """,
                date_filter,
                limit,
                offset,
            )
        else:
            total = await conn.fetchval("SELECT COUNT(*) FROM appointments")
            rows = await conn.fetch(
                """
                SELECT id, patient_code, doctor, room, start_time, duration_minutes, status, created_at
                FROM appointments
                ORDER BY start_time ASC
                LIMIT $1 OFFSET $2
                """,
                limit,
                offset,
            )

        items = [dict(r) for r in rows]
        return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.get("/appointments/{appointment_id}", response_model=AppointmentResponse)
async def get_appointment(appointment_id: UUID):
    if database.pool is None:
        await database.init_db_pool()

    async with database.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, patient_code, doctor, room, start_time, duration_minutes, status, created_at
            FROM appointments
            WHERE id = $1
            """,
            appointment_id,
        )
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Запис не знайдено",
            )
        return dict(row)


@app.put("/appointments/{appointment_id}", response_model=AppointmentResponse)
async def update_appointment(
    appointment_id: UUID, appointment: AppointmentCreate
):
    if database.pool is None:
        await database.init_db_pool()

    async with database.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE appointments
            SET patient_code = $1,
                doctor = $2,
                room = $3,
                start_time = $4,
                duration_minutes = $5,
                status = $6
            WHERE id = $7
            RETURNING id, patient_code, doctor, room, start_time, duration_minutes, status, created_at
            """,
            appointment.patient_code,
            appointment.doctor,
            appointment.room,
            appointment.start_time,
            appointment.duration_minutes,
            appointment.status,
            appointment_id,
        )
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Запис не знайдено",
            )
        return dict(row)


@app.delete(
    "/appointments/{appointment_id}", status_code=status.HTTP_204_NO_CONTENT
)
async def delete_appointment(appointment_id: UUID):
    if database.pool is None:
        await database.init_db_pool()

    async with database.pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM appointments WHERE id = $1", appointment_id
        )
        deleted_count = int(result.split(" ")[1])
        if deleted_count == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Запис не знайдено",
            )
        return Response(status_code=status.HTTP_204_NO_CONTENT)