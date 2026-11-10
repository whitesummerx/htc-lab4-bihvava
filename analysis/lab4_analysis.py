#!/usr/bin/env python3
"""
Обробка результатів Лабораторної 4: читає results\\<мітка>_*.csv|json|txt, пише
results\\analysis_lab4.md (таблиці) і графіки в analysis\\out\\.

  python analysis\\lab4_analysis.py            (усе, що є в results)
  python analysis\\lab4_analysis.py step6      (лише крок 6)
Кроки: step2 step3 step4 step5 step6 step7 step8 step9 step10
"""
import csv
import glob
import json
import math
import os
import re
import statistics
import sys
from collections import Counter, defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
RES = os.path.join(ROOT, "results")
OUT = os.path.join(ROOT, "analysis", "out")
os.makedirs(OUT, exist_ok=True)
R = 3
T_POLL = 0.050
MD = []   # рядки звіту


def md(*lines):
    for ln in lines:
        MD.append(ln)
        print(ln)


def table(header, rows):
    md("| " + " | ".join(header) + " |", "|" + "|".join(["---"] * len(header)) + "|")
    for r in rows:
        md("| " + " | ".join(str(x) for x in r) + " |")
    md("")


def exists(label, suffix="_meta.json"):
    return os.path.exists(os.path.join(RES, label + suffix))


def load_json(label, suffix):
    with open(os.path.join(RES, label + suffix), encoding="utf-8") as f:
        return json.load(f)


def load_csv(label, suffix):
    rows = []
    with open(os.path.join(RES, label + suffix), encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            rows.append({k: (None if v == "" else v) for k, v in r.items()})
    return rows


def load_tasks(label):
    out = []
    for r in load_csv(label, "_tasks.csv"):
        out.append({"id": int(r["id"]), "record": r["record_id"], "status": r["status"], "attempts": int(r["attempts"]),
                    "token": int(r["token"]), "worker": r["worker_id"],
                    "created": float(r["created"]), "started": float(r["started"]) if r["started"] else None,
                    "finished": float(r["finished"]) if r["finished"] else None, "n_results": int(r["n_results"])})
    return out


def parse_logs(path):
    """Рядки '[pod/worker-xxx] {json}' -> (pod, dict)."""
    out = []
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = re.match(r"\[pod/([^/\]]+)(?:/[^\]]+)?\]\s*(\{.*\})\s*$", line)
            if not m:
                m2 = re.match(r"\s*(\{.*\})\s*$", line)
                if not m2:
                    continue
                pod, js = None, m2.group(1)
            else:
                pod, js = m.group(1), m.group(2)
            try:
                out.append((pod, json.loads(js)))
            except ValueError:
                pass
    return out


def logs_of(label, extra=("_worker_logs.txt", "_worker_logs_previous.txt")):
    res = []
    for suf in extra:
        res += parse_logs(os.path.join(RES, label + suf))
    return res


# ------------------------------------------------------------------ Prometheus
def series(prom, name, **match):
    out = []
    for s in prom.get(name, []):
        if all(s["metric"].get(k) == v for k, v in match.items()):
            out.append(s)
    return out


def at(values, t):
    """Значення лічильника в останній відліку <= t (0, якщо відліків ще не було)."""
    v = 0.0
    for ts, x in values:
        if ts <= t:
            v = float(x)
        else:
            break
    return v


def delta(prom, name, a, b, **match):
    return sum(at(s["values"], b) - at(s["values"], a) for s in series(prom, name, **match))


def hist_quantile(prom, step, q, a, b):
    """Верхня і нижня межі бакета, що містить квантиль q (похибка = ширина бакета)."""
    buckets = {}
    for s in series(prom, "worker_duration_bucket", step=step):
        le = s["metric"]["le"]
        buckets[le] = buckets.get(le, 0.0) + at(s["values"], b) - at(s["values"], a)
    if not buckets:
        return None
    items = sorted(buckets.items(), key=lambda kv: float("inf") if kv[0] == "+Inf" else float(kv[0]))
    total = items[-1][1]
    if total <= 0:
        return None
    lower = 0.0
    for le, c in items:
        if c >= q * total:
            return lower, (float("inf") if le == "+Inf" else float(le)), total
        lower = float("inf") if le == "+Inf" else float(le)
    return None


def fmt_q(res):
    if res is None:
        return "-"
    lo, hi, n = res
    if math.isinf(hi):
        return f"> {lo * 1000:g} мс"
    return f"({lo * 1000:g}; {hi * 1000:g}] мс"


def gauge_samples(prom, name):
    ss = series(prom, name)
    if not ss:
        return []
    best = max(ss, key=lambda s: len(s["values"]))
    return [(ts, float(v)) for ts, v in best["values"]]


# ------------------------------------------------------------------ спільні обчислення
def throughput_window(tasks, window=30.0):
    """X = виконані задачі за секунду в останні `window` с до моменту спорожнення черги (останній claim)."""
    started = [t["started"] for t in tasks if t["started"]]
    t_empty = max(started)
    fin = sorted(t["finished"] for t in tasks if t["status"] == "done" and t["finished"])
    first_fin = fin[0]
    w = min(window, t_empty - first_fin)
    a = t_empty - w
    bins = Counter(int(f - a) for f in fin if a <= f < t_empty)
    per_sec = [bins.get(i, 0) for i in range(int(w))]
    x = sum(per_sec) / w if w > 0 else float("nan")
    sd = statistics.stdev(per_sec) if len(per_sec) > 1 else 0.0
    return {"X": x, "std": sd, "window": w, "a": a, "b": t_empty, "per_sec": per_sec}


def usl_fit(Ns, Cs):
    pts = [(n, c) for n, c in zip(Ns, Cs) if n > 1]
    rows = [dict(N=n, C=c, y=n / c - 1, u=n - 1, v=n * (n - 1)) for n, c in pts]
    S = dict(uu=sum(r["u"] ** 2 for r in rows), vv=sum(r["v"] ** 2 for r in rows),
             uv=sum(r["u"] * r["v"] for r in rows), uy=sum(r["u"] * r["y"] for r in rows),
             vy=sum(r["v"] * r["y"] for r in rows))
    det = S["uu"] * S["vv"] - S["uv"] ** 2
    if abs(det) < 1e-12:
        sg = S["uy"] / S["uu"]
        kp = 0.0
        det = 0.0
    else:
        sg = (S["uy"] * S["vv"] - S["vy"] * S["uv"]) / det
        kp = (S["uu"] * S["vy"] - S["uv"] * S["uy"]) / det
    note = "двопараметрична (обидві оцінки невід'ємні)"
    sg2, kp2 = sg, kp
    if kp < 0:
        kp2, note = 0.0, "κ<0 → κ=0, σ перераховано лише за u"
        sg2 = S["uy"] / S["uu"]
    if sg2 < 0:
        sg2, note = 0.0, note + "; σ<0 → σ=0"
        kp2 = max(S["vy"] / S["vv"], 0.0)
        if kp >= 0:
            note = "σ<0 → σ=0, κ перераховано лише за v"
    nstar = math.sqrt((1 - sg2) / kp2) if kp2 > 0 else math.inf
    return sg2, kp2, nstar, note, dict(rows=rows, S=S, det=det, sg_raw=sg, kp_raw=kp)


def usl(n, sg, kp):
    return n / (1 + sg * (n - 1) + kp * n * (n - 1))


# ------------------------------------------------------------------ крок 2
def step2():
    label = "step2_1worker"
    if not exists(label):
        return None
    md("", "## Крок 2: один воркер, 20 задач (t_io, t_cpu)", "")
    done = [e for _, e in logs_of(label) if e.get("ev") == "done"]
    if not done:
        md("У логах немає подій done."); return None
    io = [e["fetch"] + e["store"] for e in done]
    cpu = [e["process"] for e in done]
    rows = []
    for name, arr in (("t_io = fetch + store", io), ("t_cpu = process", cpu),
                      ("fetch", [e["fetch"] for e in done]), ("store", [e["store"] for e in done])):
        rows.append([name, len(arr), f"{statistics.mean(arr) * 1000:.2f}", f"{statistics.median(arr) * 1000:.2f}",
                     f"{statistics.stdev(arr) * 1000 if len(arr) > 1 else 0:.2f}", f"{min(arr) * 1000:.2f}", f"{max(arr) * 1000:.2f}"])
    table(["величина", "n", "середнє, мс", "медіана, мс", "σ, мс", "мін, мс", "макс, мс"], rows)
    tio, tcpu = statistics.mean(io), statistics.mean(cpu)
    md(f"t_io = {tio * 1000:.2f} мс, t_cpu = {tcpu * 1000:.2f} мс; 1/(t_io+t_cpu) = {1 / (tio + tcpu):.2f} задач/с; "
       f"1/(t_io+t_cpu+T_poll/2) = {1 / (tio + tcpu + T_POLL / 2):.2f} задач/с.", "")
    return {"t_io": tio, "t_cpu": tcpu}


# ------------------------------------------------------------------ кроки 3-4
def race_row(label, with_prom=True):
    meta = load_json(label, "_meta.json")
    tasks = load_tasks(label)
    results = load_csv(label, "_results.csv")
    n = len(tasks)
    per_task = Counter(int(r["task_id"]) for r in results)
    n_dup_tasks = sum(1 for c in per_task.values() if c > 1)
    extra_rows = sum(c - 1 for c in per_task.values() if c > 1)
    logs = [e for _, e in logs_of(label, ("_worker_logs.txt",))]
    claims = Counter(e["id"] for e in logs if e.get("ev") == "claim")
    dones = Counter(e["id"] for e in logs if e.get("ev") == "done")
    dup_claim_tasks = sum(1 for c in claims.values() if c > 1)
    dup_done_tasks = sum(1 for c in dones.values() if c > 1)
    t_fin = [t["finished"] for t in tasks if t["finished"]]
    t_cr = [t["created"] for t in tasks]
    elapsed = (max(t_fin) - min(t_cr)) if t_fin else float("nan")
    row = {"label": label, "n": n, "results_rows": len(results), "tasks_gt1_result": n_dup_tasks,
           "D": n_dup_tasks / n if n else float("nan"), "D_logs_claim": dup_claim_tasks / n if n else float("nan"),
           "D_logs_done": dup_done_tasks / n if n else float("nan"), "extra_rows": extra_rows,
           "elapsed": elapsed, "X": n / elapsed if elapsed == elapsed and elapsed > 0 else float("nan"),
           "not_done": sum(1 for t in tasks if t["status"] != "done")}
    if with_prom and exists(label, "_prom.json"):
        prom = load_json(label, "_prom.json")
        a, b = meta["t_start"], meta["t_end"] + 5
        row["p99_claim"] = fmt_q(hist_quantile(prom, "claim", 0.99, a, b))
        row["p50_claim"] = fmt_q(hist_quantile(prom, "claim", 0.50, a, b))
        row["empty_claims"] = int(delta(prom, "worker_claims_total", a, b, result="empty"))
        row["task_claims"] = int(delta(prom, "worker_claims_total", a, b, result="task"))
    lw = meta.get("samples", {}).get("lock_waiters")
    if lw:
        vals = [int(v) for _, v in lw if v not in ("", None) and str(v).strip().lstrip("-").isdigit()]
        row["lock_max"] = max(vals) if vals else None
        row["lock_samples"] = len(vals)
    return row


def step3():
    labels = [l for l in ("step3_naive_tpoll50", "step3_naive_tpoll1") if exists(l)]
    if not labels:
        return
    md("", "## Крок 3: наївне захоплення (SELECT, потім UPDATE; INSERT без ON CONFLICT)", "")
    rows = []
    for l in labels:
        r = race_row(l)
        rows.append([l, r["n"], r["results_rows"], r["tasks_gt1_result"], f"{r['D']:.4f}", f"{r['D_logs_claim']:.4f}",
                     f"{r['D_logs_done']:.4f}", r["extra_rows"]])
    table(["прогін", "n задач", "рядків у task_results", "задач з >1 результатом", "D (таблиця результатів)",
           "D (claim у логах)", "D (done у логах)", "зайвих рядків"], rows)


def step4():
    labels = [l for l in ("step4_skiplocked_tpoll50", "step4_skiplocked_tpoll1", "step4_forupdate_tpoll50",
                          "step4_forupdate_tpoll1") if exists(l)]
    if not labels:
        return
    md("", "## Крок 4: FOR UPDATE SKIP LOCKED і FOR UPDATE", "")
    rows = []
    for l in labels:
        r = race_row(l)
        rows.append([l, r["n"], r["not_done"], f"{r['D']:.4f}", f"{r['D_logs_claim']:.4f}", r.get("p50_claim", "-"),
                     r.get("p99_claim", "-"), r.get("empty_claims", "-"), r.get("task_claims", "-"),
                     r.get("lock_max", "-"), f"{r['elapsed']:.1f}", f"{r['X']:.1f}"])
    table(["прогін", "n", "не done", "D", "D (claim у логах)", "claim p50", "claim p99", "порожніх claim",
           "claim із задачею", "макс. очікувань Lock (вибірка 0.2 с)", "тривалість, с", "n/тривалість, задач/с"], rows)
    md("p99 оцінено за бакетами гістограми worker_task_duration_seconds{step=\"claim\"}: похибка дорівнює ширині бакета.", "")


# ------------------------------------------------------------------ крок 5
def step5():
    label = "step5_lease"
    if not exists(label):
        return
    m = load_json(label, "_meta.json")
    md("", "## Крок 5: втрата lease (kill -STOP на 2L, потім kill -CONT)", "")
    tid = m["task_id"]
    logs = logs_of(label, ("_worker_logs.txt",))
    mine = [(p, e) for p, e in logs if e.get("id") == tid]
    lost = [(p, e) for p, e in mine if e.get("ev") in ("lost", "heartbeat_zero_rows")]
    done = [(p, e) for p, e in mine if e.get("ev") == "done"]
    table(["величина", "значення"], [
        ["задача в роботі в момент SIGSTOP", tid],
        ["воркер, якого зупинено", m["victim"]],
        ["рядок задачі перед зупинкою (id|status|attempts|token|worker)", m["row_before_stop"]],
        ["рядок після спливання lease (до CONT)", m["row_after_lease_expiry"]],
        ["фінальний рядок", m["row_final"]],
        ["рядків у task_results для задачі", m["rows_in_task_results"]],
        ["lease L, c / пауза, c", f"{m['lease']} / {2 * m['lease']}"],
    ])
    md("Події задачі в логах воркерів:", "")
    for p, e in sorted(mine, key=lambda x: x[1].get("t", 0)):
        md(f"- `{p}` `{json.dumps(e, ensure_ascii=False)}`")
    md("", f"Рядків із нульовим оновленням у логах: {len(lost)}; завершень (done): {len(done)}.", "")
    tasks = load_tasks(label)
    md(f"Усього задач: {len(tasks)}, не done: {sum(1 for t in tasks if t['status'] != 'done')}, "
       f"задач з >1 результатом: {sum(1 for t in tasks if t['n_results'] > 1)}, "
       f"задач з attempts>1: {sum(1 for t in tasks if t['attempts'] > 1)}.", "")


# ------------------------------------------------------------------ крок 6
def step6(s2):
    info = {}
    for w in (1, 2, 3):
        l = f"step6_w{w}"
        if not exists(l):
            continue
        meta = load_json(l, "_meta.json")
        tasks = load_tasks(l)
        tw = throughput_window(tasks, 30.0)
        prom = load_json(l, "_prom.json") if exists(l, "_prom.json") else {}
        a, b = tw["a"], tw["b"]
        d = {"X": tw["X"], "std": tw["std"], "window": tw["window"], "n": len(tasks), "per_sec": tw["per_sec"]}
        d["p99"] = fmt_q(hist_quantile(prom, "claim", 0.99, a, b)) if prom else "-"
        if prom:
            tx = delta(prom, "pg_xact_commit", a, b) + delta(prom, "pg_xact_rollback", a, b)
            d["tps"] = tx / tw["window"]
            d["db_cpu"] = delta(prom, "cpu_db", a, b) / tw["window"]
            d["w_cpu"] = delta(prom, "cpu_worker", a, b) / tw["window"] / w
            thr = delta(prom, "throttled_worker", a, b)
            per = delta(prom, "periods_worker", a, b)
            d["throttle"] = thr / per if per else float("nan")
        info[w] = d
    if not info:
        return None
    md("", "## Крок 6: пропускна здатність пулу X(W) при непорожній черзі", "")
    rows = []
    for w, d in sorted(info.items()):
        rows.append([w, d["n"], f"{d['window']:.1f}", f"{d['X']:.2f}", f"{d['std']:.2f}", d["p99"],
                     f"{d.get('tps', float('nan')):.0f}", f"{d.get('db_cpu', float('nan')):.2f}",
                     f"{d.get('w_cpu', float('nan')):.2f}", f"{d.get('throttle', float('nan')):.2f}"])
    table(["W", "задач", "вікно, с", "X(W), задач/с", "розкид (σ за секундами)", "claim p99", "транзакцій БД/с",
           "CPU пода БД, ядер", "CPU воркера, ядер/під", "частка тротлінгу CFS"], rows)
    short = [w for w, d in info.items() if d["window"] < 29.5]
    if short:
        md(f"УВАГА: для W = {short} вікно менше за 30 с (черга спорожніла надто швидко): "
           f"повторіть прогін з більшою кількістю задач (--count{short[0]} ...).", "")
    if s2 and 1 in info:
        pred = 1 / (s2["t_io"] + s2["t_cpu"] + T_POLL / 2)
        pred0 = 1 / (s2["t_io"] + s2["t_cpu"])
        md(f"X(1) виміряне = {info[1]['X']:.2f} задач/с; 1/(t_io+t_cpu+T_poll/2) = {pred:.2f} "
           f"(відхилення {(info[1]['X'] - pred) / pred * 100:+.1f} %); 1/(t_io+t_cpu) = {pred0:.2f} "
           f"(відхилення {(info[1]['X'] - pred0) / pred0 * 100:+.1f} %). "
           "При непорожній черзі воркер не чекає T_poll, тому порівняння з 1/(t_io+t_cpu) очікувано точніше.", "")
    out = {"X": {w: d["X"] for w, d in info.items()}, "std": {w: d["std"] for w, d in info.items()}}
    if 1 in info and len(info) >= 3:
        x1 = info[1]["X"]
        Ns = sorted(info)
        Cs = [info[w]["X"] / x1 for w in Ns]
        sg, kp, nstar, note, det = usl_fit(Ns, Cs)
        md("C(W) = X(W)/X(1), USL: C = W / (1 + σ(W−1) + κ W(W−1)); оцінка МНК за y = σu + κv, "
           "y = W/C − 1, u = W − 1, v = W(W − 1).", "")
        table(["W", "C(W)", "y", "u", "v"], [[r["N"], f"{r['C']:.4f}", f"{r['y']:.4f}", r["u"], r["v"]] for r in det["rows"]])
        S = det["S"]
        md(f"Σu² = {S['uu']}, Σv² = {S['vv']}, Σuv = {S['uv']}, Σuy = {S['uy']:.4f}, Σvy = {S['vy']:.4f}, "
           f"det = {det['det']}; сирі оцінки σ = {det['sg_raw']:.5f}, κ = {det['kp_raw']:.5f}.",
           f"σ = {sg:.5f}, κ = {kp:.6f}; W* = {('∞' if math.isinf(nstar) else f'{nstar:.2f}')} ({note}).", "")
        out.update({"sigma": sg, "kappa": kp, "wstar": nstar})
        plot_usl(info, x1, sg, kp)
    return out


def plot_usl(info, x1, sg, kp):
    fig, ax = plt.subplots(figsize=(6.2, 3.8))
    Ws = sorted(info)
    ax.errorbar(Ws, [info[w]["X"] for w in Ws], yerr=[info[w]["std"] for w in Ws], fmt="o", color="#2563eb",
                capsize=4, label="X(W) ± розкид (σ за секундами)")
    xs = [1 + i * 0.1 for i in range(0, 61)]
    ax.plot(xs, [x1 * usl(x, sg, kp) for x in xs], color="#b45309", label=f"USL: σ={sg:.4f}, κ={kp:.5f}")
    ax.set_xlabel("кількість воркерів W")
    ax.set_ylabel("X(W), задач/с")
    ax.set_title("Пропускна здатність пулу воркерів")
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "lab4_usl.png"), dpi=150)
    plt.close(fig)


# ------------------------------------------------------------------ крок 7
def step7():
    l07, l13 = "step7_load07", "step7_load13"
    if exists(l07):
        meta = load_json(l07, "_meta.json")
        gen = load_json(l07, "_gen.json") if exists(l07, "_gen.json") else json.load(open(os.path.join(RES, f"gen_{l07}.json"), encoding="utf-8"))
        tasks = load_tasks(l07)
        prom = load_json(l07, "_prom.json")
        g0 = gen["start_epoch"]
        a, b = g0 + 60, g0 + 180
        inwin = [t for t in tasks if a <= t["created"] < b and t["started"]]
        lam = sum(1 for t in tasks if a <= t["created"] < b) / (b - a)
        wq = statistics.mean(t["started"] - t["created"] for t in inwin)
        gs = [v for ts, v in gauge_samples(prom, "tasks_queued") if a <= ts < b]
        lq_gauge = statistics.mean(gs) if gs else float("nan")
        # точне середнє: інтеграл кількості задач у черзі за часом
        ev = []
        for t in tasks:
            ev.append((t["created"], 1))
            if t["started"]:
                ev.append((t["started"], -1))
        ev.sort()
        cur, last, area = 0, a, 0.0
        for ts, dl in ev:
            if ts < a:
                cur += dl
                continue
            if ts > b:
                break
            area += cur * (ts - last)
            last = ts
            cur += dl
        area += cur * (b - last)
        lq_exact = area / (b - a)
        md("", "## Крок 7: закон Літтла, усталений режим 0.7·X(R), останні 120 с із 180", "")
        table(["величина", "значення"], [
            ["X(R), задач/с (з кроку 6)", meta.get("xr")], ["інтенсивність генератора, задач/с", meta["rate"]],
            ["λ (створено в вікні / 120 с), задач/с", f"{lam:.3f}"], ["W_q = середнє (started − created), с", f"{wq:.4f}"],
            ["λ·W_q", f"{lam * wq:.3f}"], ["L_q: середнє tasks_queued (gauge, зріз раз на 5 с)", f"{lq_gauge:.3f}"],
            ["розбіжність |λ·W_q − L_q| / L_q за gauge, %", f"{abs(lam * wq - lq_gauge) / lq_gauge * 100:.1f}" if lq_gauge else "-"],
            ["L_q: точне середнє з таблиці задач (за часом)", f"{lq_exact:.3f}"],
            ["розбіжність за точним L_q, %", f"{abs(lam * wq - lq_exact) / lq_exact * 100:.1f}" if lq_exact else "-"],
            ["задач у вікні (для W_q)", len(inwin)]])
    if exists(l13):
        meta = load_json(l13, "_meta.json")
        tasks = sorted(load_tasks(l13), key=lambda t: t["id"])
        md("", "## Крок 7: перевантаження 1.3·X(R), 120 с", "")
        w = [t["started"] - t["created"] for t in tasks if t["started"]]
        md_rows = [["інтенсивність, задач/с", meta["rate"]], ["довжина черги наприкінці генерації (queued)", meta["queued_at_gen_end"]],
                   ["W_q перших 100 задач, с", f"{statistics.mean(w[:100]):.3f}"],
                   ["W_q останніх 100 задач, с", f"{statistics.mean(w[-100:]):.3f}"]]
        table(["величина", "значення"], md_rows)
        prom = load_json(l13, "_prom.json")
        fig, ax = plt.subplots(figsize=(6.2, 3.6))
        gs = gauge_samples(prom, "tasks_queued")
        ax.plot([ts - meta["t_start"] for ts, _ in gs], [v for _, v in gs], color="#2563eb")
        ax.axvline(meta["t_gen_end"] - meta["t_start"], color="#9a3412", ls="--", label="кінець генерації")
        ax.set_xlabel("час від початку, с"); ax.set_ylabel("tasks_queued"); ax.set_title("Черга при 1.3·X(R)")
        ax.legend(frameon=False); fig.tight_layout(); fig.savefig(os.path.join(OUT, "lab4_overload.png"), dpi=150); plt.close(fig)


# ------------------------------------------------------------------ крок 8
def parse_concat_json(path):
    if not os.path.exists(path):
        return []
    txt = open(path, encoding="utf-8", errors="replace").read()
    dec = json.JSONDecoder()
    i, out = 0, []
    while i < len(txt):
        while i < len(txt) and txt[i] in " \r\n\t":
            i += 1
        if i >= len(txt):
            break
        try:
            obj, j = dec.raw_decode(txt, i)
        except ValueError:
            break
        out.append(obj)
        i = j
    return out


def step8():
    label = "step8_sigterm"
    if not exists(label):
        return
    md("", "## Крок 8: коректна зупинка при rollout restart", "")
    watch = parse_concat_json(os.path.join(RES, label + "_podwatch.json"))
    exits = {}
    for obj in watch:
        if obj.get("kind") != "Pod":
            continue
        name = obj["metadata"]["name"]
        for cs in obj.get("status", {}).get("containerStatuses", []) or []:
            term = (cs.get("state") or {}).get("terminated")
            if term:
                exits[name] = (term.get("exitCode"), term.get("reason"))
    meta = load_json(label, "_meta.json")
    before = meta.get("pods_before", [])
    table(["под (до restart)", "код виходу контейнера", "причина"],
          [[p, exits.get(p, ("не зафіксовано",))[0], exits.get(p, ("", "-"))[1] if p in exits else "-"] for p in before])
    ev = []
    for fn in glob.glob(os.path.join(RES, label + "_stream_*.txt")):
        pod = os.path.basename(fn)[len(label) + 8:-4]
        for _, e in parse_logs(fn):
            ev.append((pod, e))
    ev += logs_of(label, ("_worker_logs.txt",))
    sig = [(p, e) for p, e in ev if e.get("ev") == "sigterm"]
    ex = [(p, e) for p, e in ev if e.get("ev") == "exit"]
    rq = {(e.get("w"), e.get("id")): e for p, e in ev if e.get("ev") == "shutdown_requeue"}
    md(f"Подій sigterm у логах: {len({e.get('w') for _, e in sig})}; подій exit(code=0): {len({e.get('w') for _, e in ex})}; "
       f"задач, повернутих у чергу воркерами під час зупинки (shutdown_requeue): {len(rq)}.", "")
    tasks = load_tasks(label)
    table(["показник", "значення"], [
        ["задач усього", len(tasks)], ["у статусі failed", sum(1 for t in tasks if t["status"] == "failed")],
        ["не done", sum(1 for t in tasks if t["status"] != "done")],
        ["задач із >1 результатом", sum(1 for t in tasks if t["n_results"] > 1)],
        ["задач з attempts>1", sum(1 for t in tasks if t["attempts"] > 1)]])


# ------------------------------------------------------------------ крок 9 (Ф4)
def per_second_done(tasks, t0, t1):
    bins = Counter(int(t["finished"] - t0) for t in tasks if t["status"] == "done" and t["finished"] and t0 <= t["finished"] < t1)
    return [bins.get(i, 0) for i in range(int(t1 - t0))]


def step9(xr_by_step6, label="step9_fail_f015", title=None, dur=300):
    if not exists(label):
        return
    meta = load_json(label, "_meta.json")
    f = meta.get("f", 0.0)
    tasks = load_tasks(label)
    gen = json.load(open(os.path.join(RES, f"gen_{label}.json"), encoding="utf-8"))
    pods = load_json(label, "_pods.json")
    prom = load_json(label, "_prom.json")
    md("", f"## {title or label}", "")
    n = len(tasks)
    by_status = Counter(t["status"] for t in tasks)
    md(f"Створено задач: {n}; за статусами: {dict(by_status)}; ніколи не захоплено (attempts=0): "
       f"{sum(1 for t in tasks if t['attempts'] == 0)}; чергу спорожнено: {meta.get('drained')}.", "")
    if f > 0:
        restarts = {}
        for s_ in series(prom, "restarts_worker"):
            restarts[s_["metric"]["pod"]] = int(float(s_["values"][-1][1]))
        rows = [[p["pod"], p["restarts"]] for p in pods if p["pod"].startswith("worker")]
        table(["под worker", "RESTARTS (kubectl get pods)"], rows)
        md(f"Сума перезапусків за kubectl: {sum(r[1] or 0 for r in rows)}; за Prometheus: {sum(restarts.values())}.", "")
        fin = [t for t in tasks if t["status"] in ("done", "failed")]
        nf = len(fin)
        dist = Counter(t["attempts"] for t in fin)
        exp = {1: 1 - f, 2: f * (1 - f), 3: f * f}
        table(["спроб (серед завершених задач)", "задач", "частка", "очікувано за f"],
              [[k, dist.get(k, 0), f"{dist.get(k, 0) / nf:.4f}" if nf else "-", f"{exp.get(k, 0):.4f}"] for k in (1, 2, 3)])
        failed = sum(1 for t in fin if t["status"] == "failed")
        mean_att = statistics.mean(t["attempts"] for t in fin) if fin else float("nan")
        md(f"Завершених задач (done + failed): {nf}; частка failed = {failed / nf if nf else float('nan'):.4f} "
           f"(очікувано f³ = {f ** 3:.4f}); середня кількість спроб {mean_att:.3f} "
           f"(очікувано 1+f+f² = {1 + f + f * f:.3f}); задач із >1 результатом: {sum(1 for t in tasks if t['n_results'] > 1)}.", "")
    t0 = gen["start_epoch"]
    td = meta["mid"].get("t_delete")
    ps = per_second_done(tasks, t0, t0 + dur)
    pre = ps[min(60, int(td - t0) - 10):int(td - t0)] if td else ps[60:]
    xr = xr_by_step6.get("X", {}).get(3, float("nan"))
    md(f"X за {dur} с генерації: {sum(ps) / dur:.2f} задач/с; до видалення БД: {statistics.mean(pre):.2f} задач/с; "
       f"λ = {gen['rate']} задач/с; X(R) без аварій = {xr:.2f} (крок 6).", "")
    md("### Сценарій Ф4", "")
    probe_path = os.path.join(RES, label + "_health_probe.json")
    outage = None
    if td and os.path.exists(probe_path):
        samples = json.load(open(probe_path, encoding="utf-8"))
        after = [(t, c) for t, c in samples if t >= td - 0.5]
        t_fail = next((t for t, c in after if c != 200), None)
        t_rec = None
        if t_fail:
            t_rec = next((t for t, c in after if t > t_fail and c == 200), None)
        if t_fail and t_rec:
            outage = (t_fail - td, t_rec - t_fail)
    ready_at = meta["mid"].get("db_pod_ready_at")
    requeue_n = 0
    web_log = os.path.join(RES, label + "_web_logs.txt")
    if os.path.exists(web_log):
        for line in open(web_log, encoding="utf-8", errors="replace"):
            m = re.search(r"\[requeue\] n=(\d+) t=([\d.]+)", line)
            if m and td and td <= float(m.group(2)) <= td + 120:
                requeue_n += int(m.group(1))
    before_ids = [int(x) for x in (meta["mid"].get("running_before_delete") or "").split(",") if x.strip().isdigit()]
    tmap = {t["id"]: t for t in tasks}
    redo = sum(1 for i in before_ids if tmap.get(i) and tmap[i]["attempts"] > 1)
    pg_up = gauge_samples(prom, "pg_up")
    pg_down = [ts for ts, v in pg_up if v == 0 and td and ts >= td - 1]
    table(["показник Ф4", "значення"], [
        ["момент видалення пода БД, с від початку генерації", f"{td - t0:.1f}" if td else "-"],
        ["недоступність БД за зондом /healthz (5 запитів/с): тривалість відмов, с",
         f"{outage[1]:.1f} (перша відмова через {outage[0]:.1f} с після видалення)" if outage else "-"],
        ["новий под db-0 Ready (kubectl), с після видалення", f"{ready_at - td:.1f}" if ready_at and td else "-"],
        ["pg_up=0 у Prometheus: перший/останній відлік, с після видалення (крок 5 с)",
         f"{min(pg_down) - td:.0f} / {max(pg_down) - td:.0f}" if pg_down else "-"],
        ["задач повернуто в чергу після видалення (рядки [requeue] у логах web, вікно 120 с)", requeue_n],
        ["running у момент видалення: скільки з них отримали attempts>1", f"{redo} з {len(before_ids)}"],
        ["відповідей POST /tasks не 2xx", f"{gen['non_2xx']} {gen['non_2xx_by_code']}"],
        ["задач із >1 результатом", sum(1 for t in tasks if t["n_results"] > 1)]])
    plot_step9(meta, tasks, prom, t0, td, label, dur)


def plot_step9(meta, tasks, prom, t0, td, label="step9_fail_f015", dur=300):
    span = int(meta["t_end"] - t0)
    fig, axes = plt.subplots(3, 1, figsize=(7.2, 7.4), sharex=True)
    for ax, name, col in ((axes[0], "tasks_queued", "#2563eb"), (axes[1], "tasks_running", "#047857")):
        gs = [(ts - t0, v) for ts, v in gauge_samples(prom, name) if ts >= t0 - 5]
        ax.plot([x for x, _ in gs], [v for _, v in gs], color=col)
        ax.set_ylabel(name)
    ps = per_second_done(tasks, t0, t0 + span)
    ma = [statistics.mean(ps[max(0, i - 9):i + 1]) for i in range(len(ps))]
    axes[2].plot(range(len(ps)), ps, color="#f59e0b", lw=0.5, alpha=0.6, label="за секунду")
    axes[2].plot(range(len(ps)), ma, color="#b45309", lw=1.4, label="ковзне середнє 10 с")
    axes[2].set_ylabel("виконано задач/с")
    axes[2].set_xlabel("час від початку генерації, с")
    axes[2].legend(frameon=False, loc="upper right")
    axes[0].axvline(dur, color="#6b7280", ls=":", lw=1)
    axes[0].text(dur + 4, axes[0].get_ylim()[1] * 0.05, "кінець генерації", color="#6b7280", fontsize=8)
    if td:
        for ax in axes:
            ax.axvline(td - t0, color="#9a3412", ls="--")
        axes[1].text(td - t0 + 6, axes[1].get_ylim()[0] + (axes[1].get_ylim()[1] - axes[1].get_ylim()[0]) * 0.08,
                     "видалення пода БД", color="#9a3412", fontsize=8)
    axes[0].set_title(f"{label}: черга, running, виконано за секунду")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, f"lab4_{label}.png"), dpi=150)
    plt.close(fig)


# ------------------------------------------------------------------ крок 10 (Д3)
def overlaps(tasks):
    by = defaultdict(list)
    for t in tasks:
        if t["started"] and t["finished"]:
            by[t["record"]].append((t["started"], t["finished"], t["id"]))
    pairs, maxc = 0, 0
    for rec, iv in by.items():
        ev = []
        for s, e, i in iv:
            ev.append((s, 1)); ev.append((e, -1))
        ev.sort(key=lambda x: (x[0], x[1]))
        cur = 0
        for _, d in ev:
            cur += d
            maxc = max(maxc, cur)
        iv.sort()
        for i in range(len(iv)):
            for j in range(i + 1, len(iv)):
                if iv[j][0] < iv[i][1]:
                    pairs += 1
                else:
                    break
    return pairs, maxc


def step10():
    labs = [l for l in ("step10_d3_excl0", "step10_d3_excl1") if exists(l)]
    if not labs:
        return
    md("", "## Крок 10 (Д3): не більше однієї running-задачі на record_id, 5 записів", "")
    rows, xs = [], {}
    for l in labs:
        meta = load_json(l, "_meta.json")
        tasks = load_tasks(l)
        pairs, maxc = overlaps(tasks)
        tw = throughput_window(tasks, 30.0)
        xs[l] = tw["X"]
        sm = meta["samples"].get("two_running", [])
        vals = [int(v) for _, v in sm if str(v).strip().isdigit()]
        elapsed = max(t["finished"] for t in tasks if t["finished"]) - min(t["created"] for t in tasks)
        rows.append([l, len(tasks), f"{tw['X']:.2f}", f"{tw['std']:.2f}", f"{tw['window']:.1f}", pairs, maxc,
                     f"{sum(1 for v in vals if v > 0)} з {len(vals)}", f"{len(tasks) / elapsed:.2f}"])
    table(["прогін", "задач", "X(R), задач/с (вікно)", "розкид", "вікно, с", "пар перекритих інтервалів running одного record_id",
           "макс. одночасних running на record_id", "моментів із двома running (вибірка 0.5 с)", "n/тривалість"], rows)
    if len(xs) == 2:
        a, b = xs["step10_d3_excl0"], xs["step10_d3_excl1"]
        md(f"X(R) з обмеженням = {b:.2f} задач/с, без обмеження = {a:.2f} задач/с (відношення {b / a:.3f}).", "")


# ------------------------------------------------------------------ main
def main():
    which = set(sys.argv[1:]) or {"step2", "step3", "step4", "step5", "step6", "step7", "step8", "step9", "step9b", "step10"}
    md("# Лабораторна 4: результати обробки (автоматично згенеровано)")
    s2 = step2() if "step2" in which else None
    if s2 is None and exists("step2_1worker"):
        s2 = None
    if "step3" in which:
        step3()
    if "step4" in which:
        step4()
    if "step5" in which:
        step5()
    s6 = {}
    if "step6" in which or "step9" in which or "step9b" in which:
        if s2 is None:
            s2 = step2_quiet()
        s6 = step6(s2) or {}
    if "step7" in which:
        step7()
    if "step8" in which:
        step8()
    if "step9" in which:
        step9(s6, "step9_defaultbackoff", "Крок 9 (типовий CrashLoopBackOff до 300 с): FAIL_RATE = 0.15 і Ф4")
        step9(s6, "step9_fail_f015", "Крок 9: FAIL_RATE = 0.15 і сценарій Ф4 (видалення пода БД)")
    if "step9b" in which:
        step9(s6, "step9b_f4_clean", "Ф4 без аварій воркерів (FAIL_RATE = 0), 150 с", dur=150)
    if "step10" in which:
        step10()
    with open(os.path.join(RES, "analysis_lab4.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(MD) + "\n")
    print("\nЗаписано results\\analysis_lab4.md і графіки в analysis\\out\\")


def step2_quiet():
    label = "step2_1worker"
    if not exists(label):
        return None
    done = [e for _, e in logs_of(label) if e.get("ev") == "done"]
    if not done:
        return None
    return {"t_io": statistics.mean(e["fetch"] + e["store"] for e in done), "t_cpu": statistics.mean(e["process"] for e in done)}


if __name__ == "__main__":
    main()
