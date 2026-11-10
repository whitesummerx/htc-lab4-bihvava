#!/usr/bin/env python3
"""
Генератор створення задач (Лабораторна 4): POST /appointments/{id}/tasks з заданою інтенсивністю.
Розімкнений режим: момент відправки k-го запиту = start + k/rate, відповіді не гальмують розклад.
Лише стандартна бібліотека.

  python load_generator\\task_generator.py preload                       (гарантує 100 записів)
  python load_generator\\task_generator.py run --rate 100 --count 300 --label step3_naive
  python load_generator\\task_generator.py run --rate 12.5 --duration 180 --label step7_07
  python load_generator\\task_generator.py run --rate 20 --duration 60 --records 5 --label d3_5records
Результат: results/gen_<label>.json (підсумок) і results/gen_<label>.csv (кожен запит).
"""
import argparse
import csv
import http.client
import json
import os
import queue
import sys
import threading
import time
from urllib.parse import urlparse

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
RESULTS_DIR = os.path.join(REPO_ROOT, "results")
SEED_COUNT = 100


def http_json(base, method, path, body=None, timeout=10):
    u = urlparse(base)
    host = "127.0.0.1" if u.hostname == "localhost" else u.hostname
    conn = http.client.HTTPConnection(host, u.port or 80, timeout=timeout)
    try:
        headers = {"Content-Type": "application/json"} if body is not None else {}
        conn.request(method, path, json.dumps(body) if body is not None else None, headers)
        resp = conn.getresponse()
        data = resp.read()
        return resp.status, (json.loads(data) if data else None)
    finally:
        conn.close()


def ensure_seed(base, count=SEED_COUNT):
    """Гарантує щонайменше `count` записів у базі й повертає їхні ідентифікатори (стабільний порядок)."""
    try:
        status, data = http_json(base, "GET", "/appointments?limit=1")
    except OSError as e:
        raise SystemExit(f"Немає зʼєднання з {base}: {e}. Стенд піднято?")
    if status != 200:
        raise SystemExit(f"GET /appointments повернув {status}: стенд не готовий")
    total = data["total"]
    if total < count:
        print(f"Заношу {count - total} записів (у базі {total}, потрібно {count})...")
        for i in range(total, count):
            minutes = i * 30
            body = {
                "patient_code": f"SEED-{i:03d}",
                "doctor": "Dr. Seed",
                "room": str(100 + i % 20),
                "start_time": f"2026-11-{1 + minutes // 1440:02d}T{(minutes % 1440) // 60:02d}:{minutes % 60:02d}:00Z",
                "duration_minutes": 30,
                "status": "scheduled",
            }
            st, resp = http_json(base, "POST", "/appointments", body)
            if st != 201:
                raise SystemExit(f"Не вдалося створити запис {i}: {st} {resp}")
    status, data = http_json(base, "GET", f"/appointments?limit={count}")
    ids = [it["id"] for it in data["items"]]
    if len(ids) < count:
        raise SystemExit(f"Очікував {count} ідентифікаторів, отримав {len(ids)}")
    return ids[:count]


def cmd_preload(args):
    ids = ensure_seed(args.base, SEED_COUNT)
    print(f"Записів для задач: {len(ids)}")


def cmd_run(args):
    ids = ensure_seed(args.base, SEED_COUNT)[: args.records]
    if args.count:
        n = args.count
    else:
        n = int(round(args.rate * args.duration))
    u = urlparse(args.base)
    host = "127.0.0.1" if u.hostname == "localhost" else u.hostname
    port = u.port or 80
    q = queue.Queue()
    rows = [None] * n
    local = threading.local()

    def sender():
        while True:
            item = q.get()
            if item is None:
                return
            k, t_sched, rec = item
            t_send = time.time()
            status, task_id, err = 0, None, ""
            t0 = time.perf_counter()
            try:
                conn = getattr(local, "conn", None)
                if conn is None:
                    conn = http.client.HTTPConnection(host, port, timeout=args.timeout)
                    local.conn = conn
                conn.request("POST", f"/appointments/{rec}/tasks", b"", {"Content-Length": "0"})
                resp = conn.getresponse()
                body = resp.read()
                status = resp.status
                if status == 202:
                    task_id = json.loads(body)["id"]
            except (http.client.HTTPException, OSError, ValueError) as e:
                err = type(e).__name__
                try:
                    local.conn.close()
                except Exception:
                    pass
                local.conn = None
            rows[k] = (k, t_sched, t_send, time.perf_counter() - t0, status, task_id, rec, err)

    threads = [threading.Thread(target=sender, daemon=True) for _ in range(args.threads)]
    for t in threads:
        t.start()

    print(f"Старт: {n} запитів, {args.rate}/с, записів {len(ids)}")
    start = time.time() + 1.0
    t_begin_wall = start
    for k in range(n):
        due = start + k / args.rate
        while True:
            dt = due - time.time()
            if dt <= 0:
                break
            time.sleep(min(dt, 0.002) if dt < 0.01 else dt - 0.005)
        q.put((k, due, ids[k % len(ids)]))
    for _ in threads:
        q.put(None)
    for t in threads:
        t.join(timeout=args.timeout + 5)
    end = time.time()

    done = [r for r in rows if r is not None]
    ok = [r for r in done if 200 <= r[4] < 300]
    non2xx = [r for r in done if not (200 <= r[4] < 300)]
    codes = {}
    for r in non2xx:
        key = str(r[4]) if r[4] else (r[7] or "error")
        codes[key] = codes.get(key, 0) + 1
    lat = sorted(r[3] for r in ok)
    summary = {
        "label": args.label, "rate": args.rate, "planned": n, "records": len(ids),
        "start_epoch": t_begin_wall, "end_epoch": end,
        "sent": len(done), "ok_2xx": len(ok), "non_2xx": len(non2xx), "non_2xx_by_code": codes,
        "first_task_id": min((r[5] for r in ok), default=None),
        "last_task_id": max((r[5] for r in ok), default=None),
        "task_ids": [r[5] for r in ok],
        "latency_p50_ms": round(lat[len(lat) // 2] * 1000, 2) if lat else None,
        "latency_p99_ms": round(lat[min(len(lat) - 1, int(len(lat) * 0.99))] * 1000, 2) if lat else None,
        "max_send_lag_ms": round(max((r[2] - r[1]) for r in done) * 1000, 2) if done else None,
    }
    os.makedirs(RESULTS_DIR, exist_ok=True)
    base_name = os.path.join(RESULTS_DIR, f"gen_{args.label}")
    for ext in (".json", ".csv"):
        if os.path.exists(base_name + ext) and not args.force:
            raise SystemExit(f"{base_name + ext} уже існує (додайте --force)")
    with open(base_name + ".json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False)
    with open(base_name + ".csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["seq", "t_sched", "t_send", "latency_s", "status", "task_id", "record_id", "error"])
        for r in done:
            w.writerow(r)
    print(f"Надіслано {len(done)}, 2xx {len(ok)}, не-2xx {len(non2xx)} {codes}; "
          f"p99 {summary['latency_p99_ms']} мс; макс. відставання відправки {summary['max_send_lag_ms']} мс")
    print(f"Записано {base_name}.json")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("preload")
    sp.add_argument("--base", default="http://127.0.0.1:8080")
    sp.set_defaults(fn=cmd_preload)
    r = sub.add_parser("run")
    r.add_argument("--base", default="http://127.0.0.1:8080")
    r.add_argument("--rate", type=float, required=True, help="задач за секунду")
    r.add_argument("--count", type=int, default=0, help="кількість запитів (замість --duration)")
    r.add_argument("--duration", type=float, default=60.0, help="тривалість, с")
    r.add_argument("--records", type=int, default=SEED_COUNT, help="скільки записів використати (Д3: 5)")
    r.add_argument("--threads", type=int, default=64)
    r.add_argument("--timeout", type=float, default=5.0)
    r.add_argument("--label", required=True)
    r.add_argument("--force", action="store_true")
    r.set_defaults(fn=cmd_run)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
