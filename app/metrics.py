import socket
import time

from prometheus_client import Counter, Gauge, Histogram
from starlette.datastructures import MutableHeaders

from app.config import settings

# Ім'я екземпляра: POD_NAME, а якщо змінну не задано - ім'я хоста контейнера
POD_NAME = settings.POD_NAME or socket.gethostname()

# Межі бакетів у секундах (набір Г1: 5, 10, 25, 50, 100, 250, 500, 1000 мс).
# Бакет +Inf бібліотека додає сама.
DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)

# Метрики створюються один раз під час імпорту модуля, тобто один раз за запуск процесу
REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Кількість оброблених HTTP-запитів",
    ["method", "path", "code"],
)
REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "Тривалість обробки HTTP-запиту, секунди",
    ["method", "path"],
    buckets=DURATION_BUCKETS,
)
REQUESTS_IN_PROGRESS = Gauge(
    "http_requests_in_progress",
    "Кількість запитів, які зараз обробляються",
)


class MetricsMiddleware:
    """Проміжний обробник ASGI: оновлює метрики і додає заголовок X-Pod до кожної відповіді."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Сам збір метрик у статистику не входить
        measured = scope["path"] != "/metrics"
        status_code = 500

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                MutableHeaders(scope=message)["X-Pod"] = POD_NAME
            await send(message)

        if not measured:
            await self.app(scope, receive, send_wrapper)
            return

        REQUESTS_IN_PROGRESS.inc()
        started = time.perf_counter()
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            elapsed = time.perf_counter() - started
            REQUESTS_IN_PROGRESS.dec()
            # Шаблон шляху (/appointments/{appointment_id}), а не конкретний id:
            # інакше кожен id створював би окремий ряд. Маршрут кладе в scope FastAPI.
            route = scope.get("route")
            path = getattr(route, "path", None) or "unmatched"
            method = scope["method"]
            REQUESTS_TOTAL.labels(method, path, str(status_code)).inc()
            REQUEST_DURATION.labels(method, path).observe(elapsed)
