"""Воркер черги задач (Лабораторна 4). Один процес, одна задача за раз.

Режими (змінні середовища):
  CLAIM_MODE   naive | forupdate | skiplocked   (типово skiplocked)
  RESULT_MODE  plain | idempotent                (типово idempotent)
  EXCLUSIVE_RECORD=1  не більше однієї running-задачі на record_id (додаткова вимога Д3)
"""
import http.client
import json
import os
import random
import signal
import socket
import sys
import threading
import time
from urllib.parse import urlparse

import psycopg2
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from worker.processing import process


def env(name, default):
    v = os.environ.get(name)
    return default if v is None or v == "" else v


DB_HOST = env("DB_HOST", "db")
DB_PORT = int(env("DB_PORT", "5432"))
DB_USER = env("DB_USER", "postgres")
DB_PASSWORD = env("DB_PASSWORD", "")
DB_NAME = env("DB_NAME", "appointments_db")
WEB_URL = env("WEB_URL", "http://web:8000")
LEASE_SECONDS = float(env("LEASE_SECONDS", "15"))
T_POLL = float(env("T_POLL_MS", "50")) / 1000.0
FAIL_RATE = float(env("FAIL_RATE", "0"))
WORKER_ID = env("WORKER_ID", socket.gethostname())
CLAIM_MODE = env("CLAIM_MODE", "skiplocked")
RESULT_MODE = env("RESULT_MODE", "idempotent")
EXCLUSIVE_RECORD = env("EXCLUSIVE_RECORD", "0") == "1"
SLICE_SIZE = int(env("SLICE_SIZE", "256"))
Q_LO = float(env("Q_LO", "0.01"))
Q_HI = float(env("Q_HI", "0.99"))
GRACE_SECONDS = float(env("GRACE_SECONDS", "25"))
METRICS_PORT = int(env("METRICS_PORT", "9100"))
HEARTBEAT_SECONDS = LEASE_SECONDS / 3.0

BUCKETS = (0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
TASKS_TOTAL = Counter("worker_tasks_total", "Задачі за результатом", ["result"])
CLAIMS_TOTAL = Counter("worker_claims_total", "Спроби захоплення задачі", ["result"])
DURATION = Histogram("worker_task_duration_seconds", "Тривалість кроків задачі", ["step"], buckets=BUCKETS)
IN_FLIGHT = Gauge("worker_tasks_in_flight", "Задач у роботі зараз")
DB_ERRORS = Counter("worker_db_errors_total", "Помилки зʼєднання з БД")
for _r in ("done", "failed", "lost"):
    TASKS_TOTAL.labels(_r)
for _r in ("task", "empty"):
    CLAIMS_TOTAL.labels(_r)

stopping = threading.Event()
idle = threading.Event()
state_lock = threading.Lock()
current = {"id": None, "token": None, "lost": False}   # задача, яку обробляє воркер зараз


def log(ev, **kw):
    kw["ev"] = ev
    kw["w"] = WORKER_ID
    kw["t"] = round(time.time(), 3)
    print(json.dumps(kw, separators=(",", ":")), flush=True)


def connect():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME,
        connect_timeout=3, keepalives=1, keepalives_idle=5, keepalives_interval=2, keepalives_count=3,
        application_name=WORKER_ID,
    )
    conn.autocommit = True
    return conn


class Db:
    """Зʼєднання з автоматичним перепідключенням (потрібне для сценарію Ф4)."""

    def __init__(self):
        self.conn = None

    def get(self):
        if self.conn is None or self.conn.closed:
            self.conn = connect()
        return self.conn

    def drop(self):
        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            pass
        self.conn = None


# ---------------------------------------------------------------- захоплення задачі
CLAIM_UPDATE_RETURNING = (
    "status='running', worker_id=%(w)s, token=token+1, lease_until=now()+%(lease)s::interval, "
    "attempts=attempts+1, started_at=now()"
)
RET = "RETURNING id, record_id, seed, token, attempts"

SQL_SKIP = f"""
UPDATE tasks SET {CLAIM_UPDATE_RETURNING}
WHERE id = (SELECT id FROM tasks WHERE status='queued' ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED)
{RET}"""

SQL_FOR_UPDATE = f"""
UPDATE tasks SET {CLAIM_UPDATE_RETURNING}
WHERE id = (SELECT id FROM tasks WHERE status='queued' ORDER BY created_at, id LIMIT 1 FOR UPDATE)
{RET}"""

SQL_NAIVE_SELECT = "SELECT id FROM tasks WHERE status='queued' ORDER BY created_at, id LIMIT 1"
# наївне захоплення: окремий UPDATE без умови status='queued' і без блокувань
SQL_NAIVE_UPDATE = f"UPDATE tasks SET {CLAIM_UPDATE_RETURNING} WHERE id = %(id)s {RET}"

# Д3: кандидат лише серед записів без running-задач; далі advisory-блокування запису і повторна перевірка
SQL_EXCL_CANDIDATE = """
SELECT t.id, t.record_id FROM tasks t
WHERE t.status='queued'
  AND NOT EXISTS (SELECT 1 FROM tasks r WHERE r.record_id = t.record_id AND r.status='running')
ORDER BY t.created_at, t.id LIMIT 1 FOR UPDATE SKIP LOCKED"""
SQL_EXCL_RUNNING = "SELECT 1 FROM tasks WHERE record_id=%(rec)s AND status='running' LIMIT 1"
# clock_timestamp(): started_at не раніше фіксації попередньої задачі цього запису (для перевірки Д3 за інтервалами)
SQL_EXCL_UPDATE = (f"UPDATE tasks SET {CLAIM_UPDATE_RETURNING.replace('started_at=now()', 'started_at=clock_timestamp()')} "
                   f"WHERE id=%(id)s {RET}")


def row_to_task(row):
    return {"id": row[0], "record_id": str(row[1]), "seed": row[2], "token": row[3], "attempts": row[4]}


def claim(conn):
    params = {"w": WORKER_ID, "lease": f"{LEASE_SECONDS} seconds"}
    cur = conn.cursor()
    try:
        if EXCLUSIVE_RECORD:
            return claim_exclusive(conn, cur, params)
        if CLAIM_MODE == "naive":
            cur.execute(SQL_NAIVE_SELECT)
            r = cur.fetchone()
            if r is None:
                return None
            cur.execute(SQL_NAIVE_UPDATE, dict(params, id=r[0]))
            row = cur.fetchone()
            return row_to_task(row) if row else None
        cur.execute(SQL_FOR_UPDATE if CLAIM_MODE == "forupdate" else SQL_SKIP, params)
        row = cur.fetchone()
        return row_to_task(row) if row else None
    finally:
        cur.close()


def claim_exclusive(conn, cur, params):
    cur.execute("BEGIN")
    try:
        cur.execute(SQL_EXCL_CANDIDATE)
        cand = cur.fetchone()
        if cand is None:
            cur.execute("ROLLBACK")
            return None
        task_id, rec = cand
        cur.execute("SELECT pg_try_advisory_xact_lock(hashtext(%(rec)s::text))", {"rec": str(rec)})
        if not cur.fetchone()[0]:
            cur.execute("ROLLBACK")
            return None
        # новий оператор = новий знімок: бачимо всі вже зафіксовані running-задачі цього запису
        cur.execute(SQL_EXCL_RUNNING, {"rec": str(rec)})
        if cur.fetchone() is not None:
            cur.execute("ROLLBACK")
            return None
        cur.execute(SQL_EXCL_UPDATE, dict(params, id=task_id))
        row = cur.fetchone()
        cur.execute("COMMIT")
        return row_to_task(row) if row else None
    except Exception:
        try:
            cur.execute("ROLLBACK")
        except Exception:
            pass
        raise


# ---------------------------------------------------------------- крок 1: GET запису через web
class WebClient:
    def __init__(self, base):
        u = urlparse(base)
        self.host, self.port = u.hostname, u.port or 80
        self.conn = None

    def get(self, path):
        for attempt in (0, 1):
            try:
                if self.conn is None:
                    self.conn = http.client.HTTPConnection(self.host, self.port, timeout=5)
                self.conn.request("GET", path)
                resp = self.conn.getresponse()
                body = resp.read()
                if resp.status != 200:
                    raise RuntimeError(f"GET {path} -> {resp.status}")
                return json.loads(body)
            except (http.client.HTTPException, OSError):
                try:
                    self.conn.close()
                except Exception:
                    pass
                self.conn = None
                if attempt == 1:
                    raise


# ---------------------------------------------------------------- крок 3: запис результату
def store_result(conn, task, result):
    """Повертає True, якщо результат прийнято, і False, якщо lease втрачено (токен не збігся)."""
    payload = json.dumps(result)
    cur = conn.cursor()
    try:
        if RESULT_MODE == "plain":
            # без транзакції і без ON CONFLICT (крок 3 лабораторної)
            cur.execute(
                "INSERT INTO task_results (task_id, token, worker_id, result) VALUES (%s,%s,%s,%s::jsonb)",
                (task["id"], task["token"], WORKER_ID, payload),
            )
            cur.execute("UPDATE tasks SET status='done', finished_at=now() WHERE id=%s", (task["id"],))
            return True
        cur.execute("BEGIN")
        try:
            cur.execute(
                "UPDATE tasks SET status='done', finished_at=now() WHERE id=%s AND token=%s",
                (task["id"], task["token"]),
            )
            if cur.rowcount == 0:
                cur.execute("ROLLBACK")
                return False
            cur.execute(
                "INSERT INTO task_results (task_id, token, worker_id, result) VALUES (%s,%s,%s,%s::jsonb) "
                "ON CONFLICT (task_id) DO NOTHING",
                (task["id"], task["token"], WORKER_ID, payload),
            )
            cur.execute("COMMIT")
            return True
        except Exception:
            try:
                cur.execute("ROLLBACK")
            except Exception:
                pass
            raise
    finally:
        cur.close()


def release_task(conn, task_id, token):
    """Помилка обробки: повернути задачу в чергу (або failed, якщо спроби вичерпано)."""
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE tasks SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END, "
            "worker_id=NULL WHERE id=%s AND token=%s",
            (task_id, token),
        )
    finally:
        cur.close()


# ---------------------------------------------------------------- heartbeat
def heartbeat_loop():
    db = Db()
    while not stopping.is_set() or not idle.is_set():
        time.sleep(HEARTBEAT_SECONDS)
        with state_lock:
            tid, tok = current["id"], current["token"]
        if tid is None:
            continue
        try:
            cur = db.get().cursor()
            cur.execute(
                "UPDATE tasks SET lease_until = now() + %s::interval WHERE id=%s AND token=%s",
                (f"{LEASE_SECONDS} seconds", tid, tok),
            )
            n = cur.rowcount
            cur.close()
            if n == 0:
                with state_lock:
                    if current["id"] == tid and current["token"] == tok:
                        current["lost"] = True
                log("heartbeat_zero_rows", id=tid, token=tok)
            else:
                log("heartbeat", id=tid, token=tok)
        except Exception as e:
            DB_ERRORS.inc()
            db.drop()
            log("heartbeat_error", id=tid, err=str(e)[:120])


# ---------------------------------------------------------------- SIGTERM
def inflight_file(value):
    try:
        with open("/tmp/inflight", "w") as f:
            f.write(value)
    except OSError:
        pass


def watchdog():
    """Після SIGTERM чекає завершення поточної задачі; якщо не встигла за GRACE_SECONDS, повертає її в чергу."""
    if idle.wait(GRACE_SECONDS):
        return
    with state_lock:
        tid, tok = current["id"], current["token"]
    if tid is not None:
        try:
            conn = connect()
            cur = conn.cursor()
            cur.execute("UPDATE tasks SET status='queued' WHERE id=%s AND token=%s", (tid, tok))
            n = cur.rowcount
            conn.close()
            log("shutdown_requeue", id=tid, token=tok, updated=n)
        except Exception as e:
            log("shutdown_requeue_error", id=tid, err=str(e)[:120])
    os._exit(0)


def on_sigterm(signum, frame):
    if stopping.is_set():
        return
    log("sigterm")
    stopping.set()
    threading.Thread(target=watchdog, daemon=True).start()


# ---------------------------------------------------------------- головний цикл
def run_one(db, web, task):
    tid, tok = task["id"], task["token"]
    with state_lock:
        current.update(id=tid, token=tok, lost=False)
    inflight_file(str(tid))
    IN_FLIGHT.set(1)
    t_io = 0.0
    try:
        t = time.perf_counter()
        rec = web.get(f"/appointments/{task['record_id']}")
        d_fetch = time.perf_counter() - t
        DURATION.labels("fetch").observe(d_fetch)

        t = time.perf_counter()
        result = process(task["seed"], SLICE_SIZE, Q_LO, Q_HI)
        d_proc = time.perf_counter() - t
        DURATION.labels("process").observe(d_proc)
        result["record_id"] = rec["id"]

        if FAIL_RATE > 0 and random.random() < FAIL_RATE:
            log("fail_inject", id=tid, token=tok, attempts=task["attempts"])
            os._exit(1)

        t = time.perf_counter()
        ok = store_result(db.get(), task, result)
        d_store = time.perf_counter() - t
        DURATION.labels("store").observe(d_store)
        t_io = d_fetch + d_store
        if ok:
            TASKS_TOTAL.labels("done").inc()
            log("done", id=tid, token=tok, attempts=task["attempts"],
                fetch=round(d_fetch, 5), process=round(d_proc, 5), store=round(d_store, 5))
        else:
            TASKS_TOTAL.labels("lost").inc()
            log("lost", id=tid, token=tok, updated=0, note="UPDATE tasks ... AND token: 0 rows, result discarded")
    except psycopg2.Error as e:
        DB_ERRORS.inc()
        db.drop()
        TASKS_TOTAL.labels("failed").inc()
        log("db_error", id=tid, token=tok, err=str(e)[:160])
    except Exception as e:
        TASKS_TOTAL.labels("failed").inc()
        log("error", id=tid, token=tok, err=str(e)[:160])
        try:
            release_task(db.get(), tid, tok)
        except Exception:
            db.drop()
    finally:
        with state_lock:
            current.update(id=None, token=None, lost=False)
        inflight_file("")
        IN_FLIGHT.set(0)


def main():
    signal.signal(signal.SIGTERM, on_sigterm)
    signal.signal(signal.SIGINT, on_sigterm)
    try:
        with open("/tmp/worker.pid", "w") as f:
            f.write(str(os.getpid()))
    except OSError:
        pass
    start_http_server(METRICS_PORT)
    log("start", claim_mode=CLAIM_MODE, result_mode=RESULT_MODE, exclusive=EXCLUSIVE_RECORD,
        lease=LEASE_SECONDS, t_poll=T_POLL, fail_rate=FAIL_RATE)
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    db = Db()
    web = WebClient(WEB_URL)
    while not stopping.is_set():
        t0 = time.perf_counter()
        try:
            task = claim(db.get())
        except psycopg2.Error as e:
            DB_ERRORS.inc()
            db.drop()
            log("claim_error", err=str(e)[:120])
            stopping.wait(0.5)
            continue
        DURATION.labels("claim").observe(time.perf_counter() - t0)
        if task is None:
            CLAIMS_TOTAL.labels("empty").inc()
            stopping.wait(T_POLL)
            continue
        CLAIMS_TOTAL.labels("task").inc()
        log("claim", id=task["id"], token=task["token"], attempts=task["attempts"])
        run_one(db, web, task)
    idle.set()
    log("exit", code=0)
    sys.exit(0)


if __name__ == "__main__":
    main()
