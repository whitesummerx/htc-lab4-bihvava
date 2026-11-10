#!/usr/bin/env python3
"""
Оркестратор експериментів Лабораторної 4 (варіант 3). Запускає kubectl, генератор і збирає сирі дані в results/.

  python tools\\lab4.py check
  python tools\\lab4.py step2
  python tools\\lab4.py step3
  python tools\\lab4.py step4
  python tools\\lab4.py step5
  python tools\\lab4.py step6               (X(W) для W = 1, 2, 3)
  python tools\\lab4.py step7 --xr 18.5     (X(R) з кроку 6)
  python tools\\lab4.py step8 --xr 18.5
  python tools\\lab4.py step9 --xr 18.5
  python tools\\lab4.py step10
Кожна підкоманда приймає --only <мітка> (запустити лише один прогін) і --force (перезаписати результат).
Усі файли пишуться в UTF-8. Кластер видаляти не можна, доки не збережено results\\.
"""
import argparse
import csv
import json
import os
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request

try:
    sys.stdout.reconfigure(errors="replace")
    sys.stderr.reconfigure(errors="replace")
except Exception:
    pass

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
RESULTS = os.path.join(ROOT, "results")
GEN = os.path.join(ROOT, "load_generator", "task_generator.py")
BASE = "http://127.0.0.1:8080"
PROM = "http://127.0.0.1:9090"
R = 3
LEASE = 15
DB_POD = "db-0"
PSQL = ["kubectl", "exec", DB_POD, "--", "psql", "-U", "postgres", "-d", "appointments_db"]
DEFAULT_ENV = {"CLAIM_MODE": "skiplocked", "RESULT_MODE": "idempotent", "T_POLL_MS": "50",
               "FAIL_RATE": "0", "EXCLUSIVE_RECORD": "0"}


def say(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def run(cmd, check=True, timeout=None, input_bytes=None):
    p = subprocess.run(cmd, capture_output=True, timeout=timeout, input=input_bytes)
    out = p.stdout.decode("utf-8", errors="replace")
    err = p.stderr.decode("utf-8", errors="replace")
    if check and p.returncode != 0:
        raise SystemExit(f"Помилка: {' '.join(cmd)}\n{err or out}")
    return out, err, p.returncode


def kubectl(*args, check=True, timeout=None):
    return run(["kubectl", *args], check=check, timeout=timeout)[0]


def sql(query, check=True):
    out, err, rc = run(PSQL + ["-At", "-F", "|", "-c", query], check=check, timeout=60)
    return out.strip()


def sql_csv(query, path):
    out, err, rc = run(PSQL + ["-c", f"COPY ({query}) TO STDOUT WITH CSV HEADER"], timeout=120)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(out.replace("\r\n", "\n"))


def rpath(name):
    os.makedirs(RESULTS, exist_ok=True)
    return os.path.join(RESULTS, name)


def need_free(label, force):
    if os.path.exists(rpath(label + "_meta.json")) and not force:
        raise SystemExit(f"results\\{label}_meta.json вже існує (додайте --force, щоб перезаписати)")


# ------------------------------------------------------------------ стан кластера
def wait_web(timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with urllib.request.urlopen(BASE + "/healthz", timeout=3) as r:
                if r.status == 200:
                    return
        except Exception:
            pass
        time.sleep(1)
    raise SystemExit("web не відповідає на /healthz")


def configure(workers, env=None, settle=20):
    e = dict(DEFAULT_ENV)
    e.update(env or {})
    kubectl("set", "env", "deployment/worker", *[f"{k}={v}" for k, v in e.items()])
    kubectl("scale", "deployment/worker", f"--replicas={workers}")
    kubectl("rollout", "status", "deployment/worker", "--timeout=240s", timeout=260)
    # чекаємо готовності лише подів без deletionTimestamp (старі поди, що завершуються, ігноруємо)
    t0 = time.time()
    while True:
        d = json.loads(kubectl("get", "pods", "-l", "app=worker", "-o", "json"))
        live = [it for it in d["items"] if not it["metadata"].get("deletionTimestamp")]
        ready = [it for it in live if any(c.get("type") == "Ready" and c.get("status") == "True"
                                          for c in it["status"].get("conditions", []))]
        if len(live) == workers and len(ready) == workers:
            break
        if time.time() - t0 > 180:
            raise SystemExit(f"воркери не стали Ready: живих {len(live)}, готових {len(ready)}")
        time.sleep(1)
    say(f"воркерів: {workers}, змінні: {e}; чекаю {settle} с, поки Prometheus побачить поди")
    time.sleep(settle)


def reset_tasks():
    sql("TRUNCATE task_results, tasks RESTART IDENTITY")


def pg_plain_results():
    """Крок 3: на час наївного захоплення PRIMARY KEY у task_results замінено звичайним індексом."""
    has_pk = sql("SELECT count(*) FROM pg_constraint WHERE conname='task_results_pkey'")
    if has_pk == "1":
        sql("TRUNCATE task_results, tasks RESTART IDENTITY")
        sql("ALTER TABLE task_results DROP CONSTRAINT task_results_pkey")
        sql("CREATE INDEX IF NOT EXISTS idx_task_results_task ON task_results (task_id)")


def pg_restore_pk():
    has_pk = sql("SELECT count(*) FROM pg_constraint WHERE conname='task_results_pkey'")
    if has_pk == "0":
        sql("TRUNCATE task_results, tasks RESTART IDENTITY")
        sql("DROP INDEX IF EXISTS idx_task_results_task")
        sql("ALTER TABLE task_results ADD PRIMARY KEY (task_id)")


def open_tasks():
    out = sql("SELECT count(*) FROM tasks WHERE status IN ('queued','running')")
    return int(out or 0)


def wait_drain(timeout=600):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if open_tasks() == 0:
                return True
        except SystemExit:
            pass   # БД може бути недоступна (Ф4)
        time.sleep(1)
    say("УВАГА: черга не спорожніла за таймаут")
    return False


# ------------------------------------------------------------------ Prometheus
PROM_QUERIES = {
    "worker_duration_bucket": 'worker_task_duration_seconds_bucket',
    "worker_duration_count": 'worker_task_duration_seconds_count',
    "worker_duration_sum": 'worker_task_duration_seconds_sum',
    "worker_tasks_total": 'worker_tasks_total',
    "worker_claims_total": 'worker_claims_total',
    "worker_in_flight": 'worker_tasks_in_flight',
    "worker_db_errors": 'worker_db_errors_total',
    "tasks_queued": 'tasks_queued',
    "tasks_running": 'tasks_running',
    "tasks_done": 'tasks_done',
    "tasks_failed": 'tasks_failed',
    "pg_xact_commit": 'pg_stat_database_xact_commit{datname="appointments_db"}',
    "pg_xact_rollback": 'pg_stat_database_xact_rollback{datname="appointments_db"}',
    "pg_up": 'pg_up',
    "pg_connections": 'pg_stat_activity_count{datname="appointments_db"}',
    "cpu_db": 'container_cpu_usage_seconds_total{container="postgres",pod="db-0"}',
    "cpu_worker": 'container_cpu_usage_seconds_total{container="worker",pod=~"worker-.*"}',
    "cpu_web": 'container_cpu_usage_seconds_total{container="web",pod=~"web-.*"}',
    "throttled_worker": 'container_cpu_cfs_throttled_periods_total{container="worker",pod=~"worker-.*"}',
    "periods_worker": 'container_cpu_cfs_periods_total{container="worker",pod=~"worker-.*"}',
    "restarts_worker": 'kube_pod_container_status_restarts_total{pod=~"worker-.*"}',
    "up": 'up{job=~"worker|web"}',
}


def collect_prom(label, t0, t1):
    dur = int(t1 - t0) + 15
    out = {"t0": t0, "t1": t1}
    for name, expr in PROM_QUERIES.items():
        url = PROM + "/api/v1/query?" + urllib.parse.urlencode({"query": f"{expr}[{dur}s]", "time": f"{t1 + 5:.3f}"})
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                data = json.load(r)
            out[name] = [{"metric": x["metric"], "values": x["values"]} for x in data["data"]["result"]]
        except Exception as e:
            say(f"Prometheus: {name}: {e}")
            out[name] = []
    with open(rpath(label + "_prom.json"), "w", encoding="utf-8") as f:
        json.dump(out, f)


# ------------------------------------------------------------------ експорт з БД і логи
TASKS_SQL = ("SELECT t.id, t.record_id, t.status, t.attempts, t.token, t.worker_id, "
             "extract(epoch from t.created_at) AS created, extract(epoch from t.started_at) AS started, "
             "extract(epoch from t.finished_at) AS finished, "
             "(SELECT count(*) FROM task_results r WHERE r.task_id=t.id) AS n_results "
             "FROM tasks t ORDER BY t.id")
RESULTS_SQL = ("SELECT task_id, token, worker_id, extract(epoch from created_at) AS created, "
               "result->>'p_lo' AS p_lo, result->>'p_hi' AS p_hi, result->>'zeroed' AS zeroed, "
               "result->>'mean' AS mean, result->>'std' AS std FROM task_results ORDER BY task_id, created_at")


def export_db(label):
    sql_csv(TASKS_SQL, rpath(label + "_tasks.csv"))
    sql_csv(RESULTS_SQL, rpath(label + "_results.csv"))


def dump_logs(label, previous=False):
    out = kubectl("logs", "-l", "app=worker", "--prefix", "--tail=-1", "--max-log-requests=20", check=False, timeout=120)
    with open(rpath(label + "_worker_logs.txt"), "w", encoding="utf-8") as f:
        f.write(out)
    web = kubectl("logs", "-l", "app=web", "--prefix", "--tail=-1", check=False, timeout=60)
    with open(rpath(label + "_web_logs.txt"), "w", encoding="utf-8") as f:
        f.write(web)
    if previous:
        pods = kubectl("get", "pods", "-l", "app=worker", "-o", "jsonpath={.items[*].metadata.name}").split()
        with open(rpath(label + "_worker_logs_previous.txt"), "w", encoding="utf-8") as f:
            for p in pods:
                o, e, rc = run(["kubectl", "logs", p, "--previous", "--tail=-1"], check=False, timeout=60)
                if rc == 0:
                    for line in o.splitlines():
                        f.write(f"[pod/{p}] {line}\n")


def pods_snapshot(label):
    out = kubectl("get", "pods", "-o", "json")
    d = json.loads(out)
    rows = []
    for it in d["items"]:
        cs = (it["status"].get("containerStatuses") or [{}])[0]
        rows.append({"pod": it["metadata"]["name"], "restarts": cs.get("restartCount"),
                     "last_state": cs.get("lastState"), "phase": it["status"].get("phase")})
    with open(rpath(label + "_pods.json"), "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, default=str)
    return rows


class Sampler(threading.Thread):
    """Раз на period секунд виконує SQL у БД і запамʼятовує (час, значення)."""

    def __init__(self, query, period=1.0):
        super().__init__(daemon=True)
        self.query, self.period, self.samples, self._halt = query, period, [], threading.Event()

    def run(self):
        while not self._halt.is_set():
            t = time.time()
            try:
                v = sql(self.query, check=False)
                self.samples.append((t, v))
            except Exception:
                pass
            self._halt.wait(max(0.0, self.period - (time.time() - t)))

    def stop(self):
        self._halt.set()
        self.join(timeout=10)


class HealthProbe(threading.Thread):
    """Опитує /healthz (залежить від БД) 5 разів на секунду; для сценарію Ф4."""

    def __init__(self):
        super().__init__(daemon=True)
        self.samples, self._halt = [], threading.Event()

    def run(self):
        while not self._halt.is_set():
            t = time.time()
            try:
                with urllib.request.urlopen(BASE + "/healthz", timeout=1.5) as r:
                    code = r.status
            except urllib.error.HTTPError as e:
                code = e.code
            except Exception:
                code = 0
            self.samples.append((t, code))
            self._halt.wait(max(0.0, 0.2 - (time.time() - t)))

    def stop(self):
        self._halt.set()
        self.join(timeout=5)


# ------------------------------------------------------------------ один прогін
def run_case(label, workers, env=None, rate=None, count=None, duration=None, records=100,
             samplers=None, mid=None, drain=True, settle=20, args=None, extra_meta=None, previous_logs=False,
             drain_timeout=600):
    """mid = (секунди від старту генератора, функція) - подія посеред прогону (сценарій відмови)."""
    need_free(label, getattr(args, "force", False))
    say(f"===== {label} =====")
    configure(workers, env, settle=settle)
    wait_web()
    reset_tasks()
    time.sleep(3)
    smp = {k: Sampler(q, p) for k, (q, p) in (samplers or {}).items()}
    for s in smp.values():
        s.start()
    cmd = [sys.executable, GEN, "run", "--rate", str(rate), "--label", label, "--force"]
    if count:
        cmd += ["--count", str(count)]
    else:
        cmd += ["--duration", str(duration)]
    cmd += ["--records", str(records)]
    t_start = time.time()
    mid_info = {}
    if mid:
        def _mid():
            time.sleep(max(0.0, mid[0] - (time.time() - t_start)))
            mid_info.update(mid[1]() or {})
        mt = threading.Thread(target=_mid, daemon=True)
        mt.start()
    say("генератор: " + " ".join(cmd[2:]))
    p = subprocess.run(cmd, capture_output=True)
    print(p.stdout.decode("utf-8", errors="replace"))
    if p.returncode != 0:
        raise SystemExit(p.stderr.decode("utf-8", errors="replace"))
    t_gen_end = time.time()
    queued_at_gen_end = None
    try:
        queued_at_gen_end = int(sql("SELECT count(*) FROM tasks WHERE status='queued'"))
    except (Exception, SystemExit):
        pass
    if mid:
        mt.join(timeout=600)
    drained = wait_drain(drain_timeout) if drain else None
    t_end = time.time()
    time.sleep(12)
    for s in smp.values():
        s.stop()
    export_db(label)
    dump_logs(label, previous=previous_logs)
    snap = pods_snapshot(label)
    collect_prom(label, t_start - 10, t_end)
    meta = {"label": label, "workers": workers, "env": {**DEFAULT_ENV, **(env or {})}, "rate": rate, "count": count,
            "duration": duration, "records": records, "t_start": t_start, "t_gen_end": t_gen_end, "t_end": t_end,
            "drained": drained, "queued_at_gen_end": queued_at_gen_end, "mid": mid_info,
            "samples": {k: s.samples for k, s in smp.items()}}
    meta.update(extra_meta or {})
    with open(rpath(label + "_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False)
    say(f"готово: results\\{label}_*  (задач у БД: {sql('SELECT count(*) FROM tasks')})")
    return meta


# ------------------------------------------------------------------ кроки
def cmd_check(args):
    wait_web()
    out = subprocess.run([sys.executable, GEN, "preload"], capture_output=True)
    print(out.stdout.decode("utf-8", errors="replace"))
    print(kubectl("get", "pods", "-o", "wide"))
    print("tasks table:", sql("SELECT count(*) FROM tasks"))
    print("PK у task_results:", sql("SELECT count(*) FROM pg_constraint WHERE conname='task_results_pkey'"))
    with urllib.request.urlopen("http://127.0.0.1:9090/api/v1/targets", timeout=10) as r:
        t = json.load(r)
    jobs = {}
    for x in t["data"]["activeTargets"]:
        jobs.setdefault(x["labels"].get("job"), []).append(x["health"])
    print("Prometheus targets:", jobs)


def cmd_step2(args):
    """20 задач на одному воркері: t_io і t_cpu."""
    run_case("step2_1worker", 1, rate=4, count=20, args=args)


def cmd_step3(args):
    pg_plain_results()
    for tp in ("50", "1"):
        label = f"step3_naive_tpoll{tp}"
        if args.only and args.only != label:
            continue
        run_case(label, R, env={"CLAIM_MODE": "naive", "RESULT_MODE": "plain", "T_POLL_MS": tp},
                 rate=100, count=300, args=args)
    say("Після кроку 3 виконайте крок 4: він поверне PRIMARY KEY у task_results.")


LOCK_SQL = "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock'"


def cmd_step4(args):
    pg_restore_pk()
    for mode in ("skiplocked", "forupdate"):
        for tp in ("50", "1"):
            label = f"step4_{mode}_tpoll{tp}"
            if args.only and args.only != label:
                continue
            run_case(label, R, env={"CLAIM_MODE": mode, "T_POLL_MS": tp}, rate=100, count=args.n4,
                     samplers={"lock_waiters": (LOCK_SQL, 0.2)}, args=args)
    say("Повертаю SKIP LOCKED і T_poll = 50 мс")
    configure(1, {"CLAIM_MODE": "skiplocked", "T_POLL_MS": "50"}, settle=5)


def stop_worker_in_flight(pod):
    """Чекає, поки воркер візьме задачу, і ставить процес на паузу (SIGSTOP). Повертає id задачі."""
    for attempt in range(10):
        out, err, rc = run(["kubectl", "exec", pod, "--", "sh", "-c",
                            "while [ ! -s /tmp/inflight ]; do sleep 0.005; done; "
                            "kill -STOP $(cat /tmp/worker.pid); cat /tmp/inflight"],
                           check=False, timeout=120)
        tid = out.strip()
        t_stop = time.time()
        if rc == 0 and tid.isdigit():
            row = sql(f"SELECT id, status, attempts, token, worker_id FROM tasks WHERE id={tid}")
            if "|running|" in row:
                return {"task_id": int(tid), "t_stop": t_stop, "row_before": row}
        say(f"STOP потрапив поза задачею ({tid!r}), знімаю паузу і повторюю")
        run(["kubectl", "exec", pod, "--", "sh", "-c", "kill -CONT $(cat /tmp/worker.pid)"], check=False)
        time.sleep(1)
    raise SystemExit("не вдалося зупинити воркер під час задачі")


def cmd_step5(args):
    label = "step5_lease"
    need_free(label, args.force)
    configure(2, {}, settle=15)
    wait_web()
    reset_tasks()
    pods = sorted(kubectl("get", "pods", "-l", "app=worker", "-o", "jsonpath={.items[*].metadata.name}").split())
    victim = pods[0]
    say(f"жертва: {victim}")
    res = {}
    t_start = time.time()
    th = threading.Thread(target=lambda: res.update(stop_worker_in_flight(victim)), daemon=True)
    th.start()
    time.sleep(1)
    g = subprocess.run([sys.executable, GEN, "run", "--rate", "25", "--count", "100", "--label", label, "--force"],
                       capture_output=True)
    print(g.stdout.decode("utf-8", errors="replace"))
    th.join(timeout=150)
    if "task_id" not in res:
        raise SystemExit("зупинка не відбулась")
    tid = res["task_id"]
    say(f"воркер {victim} зупинено на задачі {tid}; пауза 2L = {2 * LEASE} с")
    time.sleep(2 * LEASE)
    mid_row = sql(f"SELECT id, status, attempts, token, worker_id FROM tasks WHERE id={tid}")
    run(["kubectl", "exec", victim, "--", "sh", "-c", "kill -CONT $(cat /tmp/worker.pid)"])
    t_cont = time.time()
    say("kill -CONT виконано, чекаю завершення черги")
    time.sleep(10)
    wait_drain(300)
    time.sleep(10)
    final_row = sql(f"SELECT id, status, attempts, token, worker_id FROM tasks WHERE id={tid}")
    n_res = sql(f"SELECT count(*) FROM task_results WHERE task_id={tid}")
    t_end = time.time()
    export_db(label)
    dump_logs(label)
    pods_snapshot(label)
    collect_prom(label, t_start - 10, t_end)
    meta = {"label": label, "victim": victim, "task_id": tid, "row_before_stop": res["row_before"],
            "row_after_lease_expiry": mid_row, "row_final": final_row, "rows_in_task_results": int(n_res),
            "t_start": t_start, "t_stop": res["t_stop"], "t_cont": t_cont, "t_end": t_end, "lease": LEASE}
    with open(rpath(label + "_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False)
    print(json.dumps(meta, ensure_ascii=False, indent=1))


def cmd_step6(args):
    counts = {1: args.count1, 2: args.count2, 3: args.count3}
    for w in (1, 2, 3):
        label = f"step6_w{w}"
        if args.only and args.only != label:
            continue
        run_case(label, w, rate=args.rate, count=counts[w],
                 samplers={}, args=args)


def cmd_step7(args):
    xr = args.xr
    run_case("step7_load07", R, rate=round(0.7 * xr, 3), duration=180, args=args, extra_meta={"xr": xr})
    run_case("step7_load13", R, rate=round(1.3 * xr, 3), duration=120, args=args, extra_meta={"xr": xr})


def cmd_step8(args):
    label = "step8_sigterm"
    xr = args.xr

    def restart():
        pods = kubectl("get", "pods", "-l", "app=worker", "-o", "jsonpath={.items[*].metadata.name}").split()
        info = {"pods_before": pods, "t_restart": time.time()}
        kubectl("rollout", "restart", "deployment/worker")
        kubectl("rollout", "status", "deployment/worker", "--timeout=240s", timeout=260)
        info["t_restart_done"] = time.time()
        return info

    # потоки логів і спостереження за подами стартують до restart
    need_free(label, args.force)
    configure(R, {}, settle=15)
    pods = kubectl("get", "pods", "-l", "app=worker", "-o", "jsonpath={.items[*].metadata.name}").split()
    procs, files = [], []
    for p in pods:
        f = open(rpath(f"{label}_stream_{p}.txt"), "wb")
        files.append(f)
        procs.append(subprocess.Popen(["kubectl", "logs", "-f", p, "--tail=-1"], stdout=f, stderr=subprocess.DEVNULL))
    wf = open(rpath(label + "_podwatch.json"), "wb")
    files.append(wf)
    watch = subprocess.Popen(["kubectl", "get", "pods", "-l", "app=worker", "--watch", "-o", "json"],
                             stdout=wf, stderr=subprocess.DEVNULL)
    time.sleep(3)
    try:
        run_case(label, R, rate=round(0.7 * xr, 3), duration=120, mid=(40, restart), args=args,
                 settle=0, extra_meta={"xr": xr, "pods_before": pods})
    finally:
        time.sleep(5)
        for pr in procs + [watch]:
            pr.terminate()
        for f in files:
            f.close()
    say("потоки логів старих подів у results\\step8_sigterm_stream_*.txt")


def cmd_step9(args):
    label = "step9_fail_f015"
    xr = args.xr
    t_marks = {}
    probe = HealthProbe()

    def kill_db():
        running = sql("SELECT string_agg(id::text, ',') FROM tasks WHERE status='running'")
        old_uid = kubectl("get", "pod", DB_POD, "-o", "jsonpath={.metadata.uid}").strip()
        t_marks["t_delete"] = time.time()
        t_marks["running_before_delete"] = running
        t_marks["old_uid"] = old_uid
        kubectl("delete", "pod", DB_POD, "--wait=false")
        # новий под (інший uid) і його Ready
        t0 = time.time()
        ready_at = None
        while time.time() - t0 < 240:
            o, e, rc = run(["kubectl", "get", "pod", DB_POD, "-o",
                            "jsonpath={.metadata.uid} {.status.containerStatuses[0].ready}"], check=False, timeout=20)
            parts = o.split()
            if rc == 0 and len(parts) == 2 and parts[0] != old_uid and parts[1] == "true":
                ready_at = time.time()
                break
            time.sleep(0.5)
        t_marks["db_pod_ready_at"] = ready_at
        return dict(t_marks)

    need_free(label, args.force)
    probe.start()
    try:
        meta = run_case(label, R, env={"FAIL_RATE": "0.15"}, rate=round(0.7 * xr, 3), duration=300,
                        mid=(150, kill_db), args=args, extra_meta={"xr": xr, "f": 0.15}, previous_logs=True,
                        drain_timeout=900)
    finally:
        probe.stop()
    with open(rpath(label + "_health_probe.json"), "w", encoding="utf-8") as f:
        json.dump(probe.samples, f)
    say("Ф4: зонд /healthz записано; недоступність БД обчислить аналіз")


def cmd_step9b(args):
    """Ф4 без аварій воркерів (FAIL_RATE=0): чисті значення для сценарію відмови БД."""
    label = "step9b_f4_clean"
    xr = args.xr
    t_marks = {}
    probe = HealthProbe()

    def kill_db():
        running = sql("SELECT string_agg(id::text, ',') FROM tasks WHERE status='running'")
        old_uid = kubectl("get", "pod", DB_POD, "-o", "jsonpath={.metadata.uid}").strip()
        t_marks["t_delete"] = time.time()
        t_marks["running_before_delete"] = running
        kubectl("delete", "pod", DB_POD, "--wait=false")
        t0 = time.time()
        ready_at = None
        while time.time() - t0 < 240:
            o, e, rc = run(["kubectl", "get", "pod", DB_POD, "-o",
                            "jsonpath={.metadata.uid} {.status.containerStatuses[0].ready}"], check=False, timeout=20)
            parts = o.split()
            if rc == 0 and len(parts) == 2 and parts[0] != old_uid and parts[1] == "true":
                ready_at = time.time()
                break
            time.sleep(0.5)
        t_marks["db_pod_ready_at"] = ready_at
        return dict(t_marks)

    need_free(label, args.force)
    probe.start()
    try:
        run_case(label, R, env={"FAIL_RATE": "0"}, rate=round(0.7 * xr, 3), duration=150, mid=(50, kill_db),
                 args=args, extra_meta={"xr": xr, "f": 0.0}, drain_timeout=600)
    finally:
        probe.stop()
    with open(rpath(label + "_health_probe.json"), "w", encoding="utf-8") as f:
        json.dump(probe.samples, f)


def cmd_step10(args):
    # 5 записів: без обмеження і з обмеженням Д3
    for excl in ("0", "1"):
        label = f"step10_d3_excl{excl}"
        if args.only and args.only != label:
            continue
        run_case(label, R, env={"EXCLUSIVE_RECORD": excl}, rate=args.rate, count=args.count, records=5,
                 samplers={"two_running": ("SELECT count(*) FROM (SELECT record_id FROM tasks WHERE status='running' "
                                           "GROUP BY record_id HAVING count(*) > 1) x", 0.5)}, args=args)
    configure(1, {"EXCLUSIVE_RECORD": "0"}, settle=5)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, fn in [("check", cmd_check), ("step2", cmd_step2), ("step3", cmd_step3), ("step4", cmd_step4),
                     ("step5", cmd_step5), ("step6", cmd_step6), ("step7", cmd_step7), ("step8", cmd_step8),
                     ("step9", cmd_step9), ("step9b", cmd_step9b), ("step10", cmd_step10)]:
        sp = sub.add_parser(name)
        sp.add_argument("--force", action="store_true")
        sp.add_argument("--only", default="")
        sp.add_argument("--xr", type=float, default=0.0, help="X(R), задач/с, з кроку 6")
        sp.add_argument("--rate", type=float, default=100.0)
        sp.add_argument("--count", type=int, default=400)
        sp.add_argument("--n4", type=int, default=300, help="кількість задач у прогонах кроку 4")
        sp.add_argument("--count1", type=int, default=400)
        sp.add_argument("--count2", type=int, default=400)
        sp.add_argument("--count3", type=int, default=400)
        sp.set_defaults(fn=fn)
    args = ap.parse_args()
    if args.cmd in ("step7", "step8", "step9", "step9b") and args.xr <= 0:
        raise SystemExit("Потрібен --xr <X(R) із кроку 6, задач/с>")
    args.fn(args)


if __name__ == "__main__":
    main()
