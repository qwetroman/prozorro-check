#!/usr/bin/env python3
"""Тіньова модель: вчиться на моїх рішеннях (✅ / ❌) і передбачає, чи схвалю я чернетку.

Бот її поки НЕ використовує. Кожен прохід:
  1. дописує в state/decisions.jsonl знімок ознак для кожного нового рішення
     (знімок потрібен, бо база тендерів чиститься через 45 днів);
  2. перенавчає логістичну регресію на всіх рішеннях і пише state/model.json;
  3. записує прогноз для кожної нової чернетки в state/shadow.jsonl,
     щоб потім перевірити, як часто модель вгадувала.

  4. веде історію якості (state/model_history.jsonl), зберігає «знімок» моделі щоразу, коли вона досягає
     нового найкращого результату (state/checkpoints/*.json), і порівнює найкращий знімок із поточною
     моделлю на рішеннях, яких жодна з них не бачила під час навчання (state/model_report.json).
     Якщо поточна модель помітно гірша, порядок показу визначає найкращий знімок, а вам приходить попередження.
Запуск: python prozorro_model.py --db state/prozorro.db
Без залежностей, крім стандартної бібліотеки.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import random
import sqlite3
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from prozorro.select import price_info  # noqa: E402

POS = {"approved", "published"}
NEG = {"rejected"}
MIN_FOR_METRICS = 20
CHECKPOINT_MIN_N = 30        # знімки - лише коли рішень достатньо, щоб метрика щось означала
CHECKPOINT_MARGIN = 0.005    # новий найкращий результат: AUC вище за попередній знімок щонайменше на стільки
COMPARE_MIN_N = 20           # порівнювати знімок із поточною моделлю лише на стількох нових рішеннях і більше
DEGRADE_MARGIN = 0.05        # поточна модель «зіпсувалася», якщо її AUC нижче за знімок більше ніж на стільки


def now():
    return datetime.now(timezone.utc).isoformat()


def log(msg):
    print(f"[модель] {msg}", file=sys.stderr)


def read_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return [json.loads(x) for x in fh if x.strip()]


def append_jsonl(path, rows):
    with open(path, "a", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n")


# ------------------------------------------------------------------ ознаки
def lg(x):
    return round(math.log1p(max(x or 0, 0)), 4)


def snapshot(con, tender_id):
    """Сирі ознаки випадку (без векторизації, щоб пізніше можна було перебудувати модель інакше)."""
    t = con.execute("SELECT * FROM tenders WHERE tender_id=?", (tender_id,)).fetchone()
    if not t:
        return None
    t = dict(t)
    fl = {r["code"]: (r["weight"], r["detail"]) for r in
          con.execute("SELECT code, weight, detail FROM flags WHERE tender_id=?", (t["id"],))}
    pi = price_info((fl.get("PRICE_ABOVE_PEERS") or (0, ""))[1]) or {}
    est, award = t.get("est") or 0, t.get("award_amt") or 0
    return {
        "flags": sorted(fl),
        "score": sum(w for w, _ in fl.values()),
        "log_est": lg(est),
        "log_award": lg(award),
        "award_ratio": round(award / est, 4) if est and award else None,
        "n_bids": t.get("n_bids"),
        "category": t.get("category"),
        "mtype": t.get("mtype"),
        "method": t.get("method"),
        "cpv2": (t.get("cpv") or "")[:2] or None,
        "region": t.get("region"),
        "buyer_kind": t.get("buyer_kind"),
        "military": t.get("military"),
        "n_complaints": t.get("n_complaints") or 0,
        "elim_gap_pct": t.get("elim_gap_pct"),
        "log_elim_gap": lg(t.get("elim_gap")),
        "price_ratio": pi.get("ratio"),
        "log_price_excess": lg(pi.get("excess")),
        "shared_contacts": 1 if t.get("shared_contacts") else 0,
    }


def vectorize(f):
    """Сирі ознаки -> розріджений вектор {назва: значення}."""
    if f.get("kind") == "pattern":
        v = {"bias": 1.0, "kind:pattern": 1.0, f"pattern:{f.get('pattern')}": 1.0,
             "log_total": (f.get("log_total") or 0) / 20}
        for k in ("share", "one_bid_share"):
            if f.get(k) is not None:
                v[k] = float(f[k])
        if f.get("n") is not None:
            v["pattern_n"] = min(f["n"], 50) / 50
        if f.get("over_threshold") is not None:
            v["over_threshold"] = min(f["over_threshold"], 10) / 10
        if f.get("category"):
            v[f"category:{f['category']}"] = 1.0
        return v
    v = {"bias": 1.0}
    est = math.expm1(f.get("log_est") or 0)
    big = est >= 1_000_000
    for lim, name in ((300_000, "est>300k"), (1_000_000, "est>1m"), (5_000_000, "est>5m"), (20_000_000, "est>20m")):
        if est >= lim:
            v[name] = 1.0
    for c in f.get("flags") or []:
        v[f"flag:{c}"] = 1.0
        if big:
            v[f"flag:{c}&est>1m"] = 1.0    # лінійна модель сама не бачить комбінацій «прапорець + велика сума»
    for k in ("category", "mtype", "method", "cpv2", "region", "buyer_kind", "military"):
        if f.get(k) is not None:
            v[f"{k}:{f[k]}"] = 1.0
    v["score"] = (f.get("score") or 0) / 10
    v["log_est"] = (f.get("log_est") or 0) / 20
    v["log_award"] = (f.get("log_award") or 0) / 20
    v["award_ratio"] = min(f.get("award_ratio") or 0, 2)
    v["one_bid"] = 1.0 if f.get("n_bids") == 1 else 0.0
    v["n_complaints"] = min(f.get("n_complaints") or 0, 5) / 5
    v["elim_gap_pct"] = min(f.get("elim_gap_pct") or 0, 5) / 5
    v["log_elim_gap"] = (f.get("log_elim_gap") or 0) / 20
    v["price_ratio"] = min(f.get("price_ratio") or 0, 15) / 15
    v["log_price_excess"] = (f.get("log_price_excess") or 0) / 20
    v["shared_contacts"] = float(f.get("shared_contacts") or 0)
    return v


# ------------------------------------------------------------------ логістична регресія (L2)
def sigmoid(z):
    return 1 / (1 + math.exp(-max(min(z, 30), -30)))


def predict(w, v):
    return sigmoid(sum(w.get(k, 0.0) * x for k, x in v.items()))


def train(X, y, l2=0.03, epochs=300, lr=0.5):
    w = {}
    n = len(X)
    for _ in range(epochs):
        grad = {}
        for v, t in zip(X, y):
            e = predict(w, v) - t
            for k, x in v.items():
                grad[k] = grad.get(k, 0.0) + e * x
        for k in set(grad) | set(w):
            reg = 0.0 if k == "bias" else l2 * w.get(k, 0.0)
            w[k] = w.get(k, 0.0) - lr * (grad.get(k, 0.0) / n + reg)
    return w


def auc(scores, labels):
    pos = [s for s, t in zip(scores, labels) if t == 1]
    neg = [s for s, t in zip(scores, labels) if t == 0]
    if not pos or not neg:
        return None
    wins = sum(1.0 if p > q else 0.5 if p == q else 0.0 for p in pos for q in neg)
    return round(wins / (len(pos) * len(neg)), 3)


def cross_val(X, y, k=5, seed=1):
    idx = list(range(len(X)))
    random.Random(seed).shuffle(idx)
    scores = [0.0] * len(X)
    for f in range(k):
        test = set(idx[f::k])
        w = train([X[i] for i in idx if i not in test], [y[i] for i in idx if i not in test])
        for i in test:
            scores[i] = predict(w, X[i])
    acc = sum((s >= 0.5) == (t == 1) for s, t in zip(scores, y)) / len(y)
    return round(acc, 3), auc(scores, y)


def logloss(scores, labels):
    eps = 1e-6
    return round(-sum(t * math.log(max(p, eps)) + (1 - t) * math.log(max(1 - p, eps))
                      for p, t in zip(scores, labels)) / len(labels), 3)


def quality(scores, labels):
    if not labels:
        return None
    acc = sum((p >= 0.5) == (t == 1) for p, t in zip(scores, labels)) / len(labels)
    return {"n": len(labels), "accuracy": round(acc, 3), "auc": auc(scores, labels), "logloss": logloss(scores, labels)}


# ------------------------------------------------------------------ прогрес, знімки, порівняння
def track(state_dir, model, decisions, shadow):
    """Історія якості, знімки на найкращих результатах, порівняння найкращого знімка з поточною моделлю."""
    hist_path = os.path.join(state_dir, "model_history.jsonl")
    ck_dir = os.path.join(state_dir, "checkpoints")
    rep_path = os.path.join(state_dir, "model_report.json")
    os.makedirs(ck_dir, exist_ok=True)
    try:
        prev = json.load(open(rep_path, encoding="utf-8"))
    except Exception:
        prev = {}

    # «жива» якість: прогнози, зроблені ДО рішення моделлю, що була чинна тоді (найчесніша оцінка)
    by_id = {d["tender_id"]: d for d in decisions}
    live = [(s["p_approve"], by_id[s["tender_id"]]["label"], by_id[s["tender_id"]]["decided_at"])
            for s in shadow if s["tender_id"] in by_id and s["scored_at"] < (by_id[s["tender_id"]]["decided_at"] or "9")]
    live_all = quality([p for p, _, _ in live], [t for _, t, _ in live])
    recent = sorted(live, key=lambda x: x[2])[-30:]
    live_recent = quality([p for p, _, _ in recent], [t for _, t, _ in recent])
    append_jsonl(hist_path, [{"trained_at": model["trained_at"], "n": model["n"], "cv": model["metrics"],
                              "live": live_all, "live_last30": live_recent}])

    # знімок, коли модель досягла нового найкращого результату (за AUC перехресної перевірки)
    cps = sorted(read_jsonl_dir(ck_dir), key=lambda c: c["metrics"]["cv_auc"] or 0)
    best = cps[-1] if cps else None
    m = model["metrics"]
    if m and m.get("cv_auc") and model["n"] >= CHECKPOINT_MIN_N and model["weights"] and (
            not best or m["cv_auc"] >= (best["metrics"]["cv_auc"] or 0) + CHECKPOINT_MARGIN):
        name = f"model-{model['trained_at'][:10]}-n{model['n']}-auc{m['cv_auc']:.3f}.json"
        best = {"file": name, "trained_at": model["trained_at"], "n": model["n"], "metrics": m, "weights": model["weights"]}
        with open(os.path.join(ck_dir, name), "w", encoding="utf-8") as fh:
            json.dump(best, fh, ensure_ascii=False, indent=1)
        log(f"новий найкращий результат, знімок: {name}")

    # порівняння: найкращий знімок vs поточна (жива) модель на рішеннях, ухвалених ПІСЛЯ знімка
    report = {"updated_at": now(), "n": model["n"], "cv": m, "live": live_all, "live_last30": live_recent,
              "best": None, "compare": None, "use": "current", "alerted": prev.get("alerted", False)}
    if best:
        report["best"] = {k: best[k] for k in ("file", "trained_at", "n", "metrics")}
        after = [(s["p_approve"], by_id[s["tender_id"]]) for s in shadow
                 if s["tender_id"] in by_id and s["scored_at"] < (by_id[s["tender_id"]]["decided_at"] or "9")
                 and (by_id[s["tender_id"]]["decided_at"] or "") > best["trained_at"]]
        if after:
            labels = [d["label"] for _, d in after]
            frozen = quality([predict(best["weights"], vectorize(d["features"])) for _, d in after], labels)
            current = quality([p for p, _ in after], labels)
            report["compare"] = {"n": len(after), "best": frozen, "current": current}
            if (len(after) >= COMPARE_MIN_N and frozen["auc"] is not None and current["auc"] is not None
                    and current["auc"] < frozen["auc"] - DEGRADE_MARGIN):
                report["use"] = best["file"]
    report["alert_now"] = report["use"] != "current" and not prev.get("alerted")
    report["alerted"] = report["use"] != "current"
    report["text"] = report_text(report)
    with open(rep_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=1)
    log(report["text"].replace("\n", " | "))
    return report


def read_jsonl_dir(path):
    out = []
    for f in sorted(os.listdir(path)) if os.path.isdir(path) else []:
        if f.endswith(".json"):
            try:
                c = json.load(open(os.path.join(path, f), encoding="utf-8"))
                c["file"] = f
                out.append(c)
            except Exception:
                pass
    return out


def report_text(r):
    pct = lambda q: f"{q['accuracy']:.0%}, AUC {q['auc']}" if q and q.get("auc") is not None else "—"  # noqa: E731
    lines = [f"🤖 Модель: рішень {r['n']}"]
    if r["cv"]:
        lines.append(f"Перехресна перевірка: {r['cv']['cv_accuracy']:.0%}, AUC {r['cv']['cv_auc']} (базова {r['cv']['baseline_accuracy']:.0%})")
    if r["live"]:
        lines.append(f"Прогнози до рішення: {pct(r['live'])} на {r['live']['n']}; останні {r['live_last30']['n']}: {pct(r['live_last30'])}")
    if r["best"]:
        b = r["best"]
        lines.append(f"Найкращий знімок: n={b['n']}, AUC {b['metrics']['cv_auc']} ({b['trained_at'][:10]})")
    c = r["compare"]
    if c:
        lines.append(f"Після знімка ({c['n']} рішень): знімок {pct(c['best'])} · поточна {pct(c['current'])}")
        if c["n"] < COMPARE_MIN_N:
            lines.append(f"Для висновку потрібно {COMPARE_MIN_N}+ нових рішень.")
    lines.append("⚠️ Поточна модель помітно гірша за найкращий знімок - порядок показу визначає знімок."
                 if r["use"] != "current" else "Порядок показу визначає поточна модель.")
    return "\n".join(lines)


# ------------------------------------------------------------------ прохід
def run(args):
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    os.makedirs(args.state, exist_ok=True)
    dec_path = os.path.join(args.state, "decisions.jsonl")
    shadow_path = os.path.join(args.state, "shadow.jsonl")
    model_path = os.path.join(args.state, "model.json")

    have_posts = con.execute("SELECT name FROM sqlite_master WHERE name='posts'").fetchone()
    cols = {r[1] for r in con.execute("PRAGMA table_info(posts)")} if have_posts else set()
    extra = ", kind, features" if {"kind", "features"} <= cols else ""
    posts = [dict(r) for r in con.execute(f"SELECT tender_id, status, approved_at{extra} FROM posts")] if have_posts else []

    def features_of(p):
        if p.get("kind") == "pattern":           # закономірність: ознаки збережені під час створення чернетки
            return json.loads(p["features"]) if p.get("features") else None
        return snapshot(con, p["tender_id"])

    # 1) нові рішення -> знімки ознак
    decisions = read_jsonl(dec_path)
    seen = {d["tender_id"] for d in decisions}
    new = []
    for p in posts:
        if p["tender_id"] in seen or p["status"] not in POS | NEG:
            continue
        f = features_of(p)
        if f is None:
            continue
        new.append({"tender_id": p["tender_id"], "label": 1 if p["status"] in POS else 0, "kind": p.get("kind") or "tender",
                    "decided_at": p.get("approved_at") or now(), "logged_at": now(), "features": f})
    append_jsonl(dec_path, new)
    decisions += new
    log(f"нових рішень: {len(new)}, усього: {len(decisions)}")

    # 2) навчання
    X = [vectorize(d["features"]) for d in decisions]
    y = [d["label"] for d in decisions]
    n_pos = sum(y)
    model = {"trained_at": now(), "n": len(y), "n_approved": n_pos, "n_rejected": len(y) - n_pos,
             "weights": {}, "metrics": None, "note": None}
    if len(y) >= 2 and 0 < n_pos < len(y):
        w = train(X, y)
        model["weights"] = {k: round(v, 4) for k, v in sorted(w.items(), key=lambda kv: -abs(kv[1]))}
        if len(y) >= MIN_FOR_METRICS:
            acc, a = cross_val(X, y)
            model["metrics"] = {"cv_accuracy": acc, "cv_auc": a,
                                "baseline_accuracy": round(max(n_pos, len(y) - n_pos) / len(y), 3)}
        else:
            model["note"] = f"замало даних для оцінки якості (потрібно {MIN_FOR_METRICS}+)"
    else:
        model["note"] = "потрібні і схвалені, і відхилені рішення"
    with open(model_path, "w", encoding="utf-8") as fh:
        json.dump(model, fh, ensure_ascii=False, indent=1)
    log(f"модель: n={model['n']}, метрики={model['metrics'] or model['note']}")

    # 3) тіньові прогнози для нових чернеток (бот їх не використовує)
    scored = {r["tender_id"] for r in read_jsonl(shadow_path)}
    rows = []
    for p in posts:
        if p["status"] != "in_review" or p["tender_id"] in scored or not model["weights"]:
            continue
        f = features_of(p)
        if f is None:
            continue
        rows.append({"tender_id": p["tender_id"], "p_approve": round(predict(model["weights"], vectorize(f)), 3),
                     "model_n": model["n"], "scored_at": now()})
    append_jsonl(shadow_path, rows)
    if not os.path.exists(shadow_path):
        open(shadow_path, "a").close()
    log(f"тіньових прогнозів: {len(rows)}")

    # 4) прогрес, знімки, порівняння
    if model["weights"]:
        track(args.state, model, decisions, read_jsonl(shadow_path))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="state/prozorro.db")
    ap.add_argument("--state", default="state", help="тека для decisions.jsonl, model.json, shadow.jsonl")
    run(ap.parse_args())


if __name__ == "__main__":
    main()
