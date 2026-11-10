from typing import Optional
import asyncpg
from app.config import settings

pool: Optional[asyncpg.Pool] = None


async def init_db_pool() -> Optional[asyncpg.Pool]:
    """Ініціалізація пулу з'єднань з PostgreSQL."""
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=min(2, settings.DB_POOL_SIZE),
            max_size=settings.DB_POOL_SIZE,
        )
    return pool


async def close_db_pool() -> None:
    """Закриття пулу з'єднань при зупинці сервісу."""
    global pool
    if pool is not None:
        await pool.close()
        pool = None