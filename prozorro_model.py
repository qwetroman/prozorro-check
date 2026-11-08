#!/usr/bin/env python3
"""Тіньова модель: вчиться на моїх рішеннях (✅ / ❌) і передбачає, чи схвалю я чернетку.

Бот її поки НЕ використовує. Кожен прохід:
  1. дописує в state/decisions.jsonl знімок ознак для кожного нового рішення
     (знімок потрібен, бо база тендерів чиститься через 45 днів);
  2. перенавчає логістичну регресію на всіх рішеннях і пише state/model.json;
  3. записує прогноз для кожної нової чернетки в state/shadow.jsonl,
     щоб потім перевірити, як часто модель вгадувала.

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
from prozorro_publisher import price_info  # noqa: E402

POS = {"approved", "published"}
NEG = {"rejected"}
MIN_FOR_METRICS = 20


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


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="state/prozorro.db")
    ap.add_argument("--state", default="state", help="тека для decisions.jsonl, model.json, shadow.jsonl")
    run(ap.parse_args())


if __name__ == "__main__":
    main()
