#!/usr/bin/env python3
"""Аналітичний прохід «Прозорро Монітора» для GitHub Actions: з бази монітора - чернетки у Worker.

Telegram тут більше не використовується: натискання, черга перевірки й публікація - у Worker (worker/).

Команди (python -m prozorro.pipeline <команда>):
  sync      завантажити всі чернетки й рішення з Worker у локальну базу (таблиця posts - лише копія)
            і зберегти резервну копію state/backup.json
  draft     нові чернетки, прибирання застарілих і військових, свіжі тексти для черги на канал
  meta      надіслати у Worker підсумок тіньової моделі (для тижневої статистики)
  migrate   одноразово перенести старий журнал state/ledger.json у Worker
  preview   показати тексти для кількох найновіших кандидатів, нічого не надсилаючи
  health    стан Worker і вебхука
  events    журнал подій Worker за останні години (--hours)
Прапорець --dry-run: нічого не надсилати у Worker, лише показати.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import sqlite3
import sys

if __package__ in (None, ""):                        # запуск як файл, а не як модуль
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    __package__ = "prozorro"

from . import api  # noqa: E402
from .common import CFG, HERE, log  # noqa: E402
from .select import STRONG, candidates  # noqa: E402
from .texts import compose, compose_pattern, context, pattern_context  # noqa: E402

DRY = False
MIRROR = """CREATE TABLE posts (tender_id TEXT PRIMARY KEY, status TEXT, approved_at TEXT, kind TEXT, features TEXT,
  text_x TEXT, text_tg TEXT, edited INTEGER, channel_msg_id INTEGER, created TEXT, model_p REAL, priority REAL, money REAL)"""
WAITING = ("queued", "in_review", "awaiting_edit")


def db():
    con = sqlite3.connect(CFG["db_path"])
    con.row_factory = sqlite3.Row
    return con


def has_table(con, name):
    return bool(con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def send(path, body):
    if DRY:
        log(f"DRY /api/{path}: {json.dumps(body, ensure_ascii=False)[:400]}")
        return {}
    return api.call(path, body)


# ------------------------------------------------------------------ sync
def export_all(full=False):
    posts, meta, off = [], {}, 0
    while True:
        j = api.call(f"export?offset={off}&limit={100 if full else 300}" + ("&full=1" if full else ""))
        meta = j.get("meta") or meta
        posts += j["posts"]
        off += len(j["posts"])
        if not j.get("more"):
            return posts, meta


def cmd_sync(con, args):
    posts, meta = export_all(full=True)
    # локальна копія для вибору кандидатів (що вже пропонувалося) і для моделі (рішення);
    # у статусах моделі: усе, що чекає рішення, - in_review
    con.execute("DROP TABLE IF EXISTS posts")
    con.execute(MIRROR)
    con.executemany("INSERT INTO posts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", [
        (p["id"], "in_review" if p["status"] in WAITING else p["status"], p["decided_at"], p["kind"], p["features"],
         p["text_x"], p["text_tg"], p["edited"], p["channel_msg_id"], p["created_at"], p.get("model_p"), p.get("priority"), p.get("money"))
        for p in posts])
    con.commit()
    os.makedirs(os.path.dirname(args.backup) or ".", exist_ok=True)
    with open(args.backup, "w", encoding="utf-8") as fh:
        json.dump({"meta": meta, "posts": posts}, fh, ensure_ascii=False, indent=0)
    log(f"з Worker: записів {len(posts)}; резервна копія -> {args.backup}")


# ------------------------------------------------------------------ draft
def priority(money, p_model=None):
    """Порядок показу: за сумою під ризиком; з rank_by_model - спершу за прогнозом моделі."""
    base = round(math.log10(1 + max(money or 0, 0)), 3)
    return round(p_model * 10 + base / 10, 3) if p_model is not None else base


def load_model():
    """Чинна модель: поточна або найкращий знімок, якщо поточна помітно гірша (див. state/model_report.json)."""
    try:
        import prozorro_model as PM
        st = os.path.join(HERE, "state")
        use = "current"
        if os.path.exists(os.path.join(st, "model_report.json")):
            use = json.load(open(os.path.join(st, "model_report.json"), encoding="utf-8")).get("use", "current")
        path = os.path.join(st, "model.json") if use == "current" else os.path.join(st, "checkpoints", use)
        m = json.load(open(path, encoding="utf-8"))
        return (PM, m["weights"]) if m.get("weights") and m.get("metrics") else None
    except Exception as e:
        log(f"модель недоступна: {e}")
        return None


def model_p(con, model, tid, feats=None):
    if not model:
        return None
    PM, w = model
    if isinstance(feats, str):
        feats = json.loads(feats)
    f = feats or (PM.snapshot(con, tid) if has_table(con, "tenders") else None)
    return round(PM.predict(w, PM.vectorize(f)), 3) if f else None


def ranked(money, p):
    return priority(money, p if CFG.get("rank_by_model") else None)


def rescore(con, model):
    """Новий прогноз і порядок для чернеток, що чекають (зокрема старої черги): модель могла змінитися."""
    if not model:
        return []
    out = []
    tenders = has_table(con, "tenders")
    for p in con.execute("SELECT tender_id, kind, features, model_p, priority, money FROM posts WHERE status='in_review'").fetchall():
        money = p["money"]
        if money is None and p["kind"] != "pattern" and tenders:     # стара черга: суму беремо з бази тендерів
            rs = candidates(con, only=p["tender_id"])
            money = rs[0]["money"] if rs else None
        try:
            mp = model_p(con, model, p["tender_id"], p["features"] if p["kind"] == "pattern" else None)
        except Exception:
            mp = None
        if mp is None and money is None:
            continue
        pr = ranked(money or 0, mp)
        if (p["model_p"] is None) != (mp is None) or abs((mp or 0) - (p["model_p"] or 0)) >= 0.01 \
                or abs(pr - (p["priority"] or 0)) >= 0.01 or (p["money"] is None and money is not None):
            out.append({"id": p["tender_id"], "model_p": mp, "priority": pr, "money": money})
    return out


def stale_and_military(con):
    """Чернетки, які треба прибрати: військові/безпекові (коли вимкнено) і тендери, де сильних сигналів більше немає."""
    if not has_table(con, "tenders"):
        return []
    items = []
    off = [m for m, on in ((1, CFG["include_military"]), (2, CFG["include_security"])) if not on]
    if off:
        q = ",".join("?" * len(off))
        for r in con.execute(f"""SELECT p.tender_id, p.status FROM posts p JOIN tenders t ON t.tender_id = p.tender_id
                                 WHERE t.military IN ({q}) AND (p.status = 'in_review'
                                 OR (p.status = 'approved' AND p.channel_msg_id IS NULL))""", off):
            items.append({"id": r["tender_id"], "reason": "військові/безпекові закупівлі зараз вимкнено",
                          "channel": r["status"] == "approved"})
    seen = {i["id"] for i in items}
    for (tid,) in con.execute("SELECT tender_id FROM posts WHERE status='in_review' AND COALESCE(kind,'tender')='tender'"):
        if tid in seen or not con.execute("SELECT 1 FROM tenders WHERE tender_id=?", (tid,)).fetchone():
            continue                                   # тендера вже немає в базі - нічого не знаємо, не чіпаємо
        rs = candidates(con, only=tid)
        if not (rs and rs[0]["codes"] & STRONG):
            items.append({"id": tid, "reason": "сильних сигналів ризику більше немає (дані або правило змінилися)"})
    return items


def fresh_channel_texts(con):
    """Повні тексти для схвалених, що чекають на канал: прапорці могли змінитися після створення чернетки."""
    out = []
    for p in con.execute("SELECT * FROM posts WHERE status='approved' AND channel_msg_id IS NULL AND COALESCE(edited,0)=0 "
                         "AND COALESCE(kind,'tender') IN ('tender','explore')").fetchall():
        rs = candidates(con, only=p["tender_id"]) if has_table(con, "tenders") else []
        if not (rs and rs[0]["codes"]):
            continue
        m = re.search(r"\(\+(\d+) аналогічн", p["text_tg"] or p["text_x"] or "")
        if m:
            rs[0]["siblings"] = int(m.group(1))
        text = compose(rs[0], full=True)
        if text != (p["text_tg"] or p["text_x"]):
            out.append({"id": p["tender_id"], "text_tg": text})
    return out


def build_drafts(con):
    model = load_model()
    have = {r[0] for r in con.execute("SELECT tender_id FROM posts")}
    items = [("tender", r) for r in candidates(con)]
    try:
        import prozorro_patterns as PP
        PP.sync_awards(con)
        items += [("pattern", p) for p in PP.find_patterns(con)
                  if p["key"] not in have and p["pattern"] in CFG["pattern_types"]
                  and p["money"] >= CFG["pattern_min_money_uah"]]
    except Exception as e:
        log(f"помилка закономірностей: {e}")
    n_explore = min(CFG["explore_max_per_run"], math.ceil(len(items) * CFG["explore_share"])) if items else 0
    if n_explore:
        pool = candidates(con, explore=True)
        items += [("explore", r) for r in random.sample(pool, min(n_explore, len(pool)))]
    drafts = []
    for kind, r in items:
        if kind == "pattern":
            d = {"id": r["key"], "text_x": compose_pattern(r), "text_tg": compose_pattern(r, full=True),
                 "context": pattern_context(r), "features": r["features"],
                 "model_p": model_p(con, model, r["key"], r["features"]), "money": r["money"]}
            d["priority"] = ranked(r["money"], d["model_p"])
        else:
            ctx = context(r)
            if kind == "explore":
                ctx = "🧪 Пошуковий випадок: не пройшов звичайні фільтри, показую для навчання моделі.\n" + ctx
            d = {"id": r["tender_id"], "text_x": compose(r), "text_tg": compose(r, full=True), "context": ctx,
                 "features": None, "model_p": model_p(con, model, r["tender_id"]), "money": r["money"]}
            d["priority"] = ranked(r["money"], d["model_p"])
        if d["text_tg"] == d["text_x"]:
            d["text_tg"] = None
        d["kind"] = kind
        drafts.append(d)
    return drafts


def cmd_draft(con, args):
    if not has_table(con, "posts"):
        raise SystemExit("немає локальної копії чернеток: спершу sync")
    sup = stale_and_military(con)
    for part in api.chunks(sup, 15):                  # прибирання повідомлень у Telegram - до 2 запитів на запис
        send("suppress", {"items": part})
    texts = fresh_channel_texts(con)
    for part in api.chunks(texts):
        send("texts", {"items": part})
    scores = rescore(con, load_model())
    for part in api.chunks(scores, 100):
        send("scores", {"items": part})
    drafts = build_drafts(con)
    added = 0
    for part in api.chunks(drafts, 25):
        added += send("drafts", {"drafts": part}).get("added", 0)
    kinds = {k: sum(d["kind"] == k for d in drafts) for k in ("tender", "pattern", "explore")}
    log(f"прибрано: {len(sup)}; оновлено текстів для каналу: {len(texts)}; нових прогнозів: {len(scores)}; нових чернеток: {added} "
        f"(тендерів {kinds['tender']}, закономірностей {kinds['pattern']}, пошукових {kinds['explore']})")


# ------------------------------------------------------------------ інше
def cmd_meta(con, args):
    try:
        m = json.load(open(os.path.join(HERE, "state", "model.json"), encoding="utf-8"))
    except Exception:
        return
    try:
        rep = json.load(open(os.path.join(HERE, "state", "model_report.json"), encoding="utf-8"))
        send("meta", {"model_report": rep["text"]})
        if rep.get("alert_now"):
            send("notify", {"text": "⚠️ Модель: поточна версія помітно гірша за найкращий збережений знімок. "
                                    "Порядок показу тепер визначає знімок. Подробиці - /model\n\n" + rep["text"]})
    except Exception as e:
        log(f"звіт моделі: {e}")
    s = f"Тіньова модель: рішень {m.get('n', 0)}"
    met = m.get("metrics")
    s += (f", точність {met['cv_accuracy']:.0%} (базова {met['baseline_accuracy']:.0%}), AUC {met['cv_auc']}" if met
          else f" ({m.get('note') or 'ще вчиться'})")
    send("meta", {"model_summary": s})


def legacy_post(p):
    """Запис старого журналу (state/ledger.json) -> запис Worker."""
    st, ch = p.get("status"), p.get("tg_channel_msg_id")
    if st == "published":
        status = "published" if ch else "approved"          # посилання для X уже надіслано, канал ще ні
    elif st in ("draft", "in_review", "awaiting_edit"):
        status = "queued"
    else:
        status = st                                           # approved / rejected / suppressed / failed
    return {"id": p["tender_id"], "kind": p.get("kind") or "tender", "status": status, "text_x": p.get("text"),
            "text_tg": p.get("text_tg"), "context": None, "features": p.get("features"), "edited": int(p.get("note") == "edited"),
            "created_at": p.get("created"), "review_msg_id": p.get("review_msg_id") if status == "queued" else None,
            "decided_at": p.get("approved_at"), "decided_by": p.get("approved_by"),
            "x_sent_at": p.get("published_at") if st == "published" else None,
            "x_id": p.get("x_id") if p.get("x_id") not in (None, "manual") and not str(p.get("x_id")).startswith("dry") else None,
            "channel_msg_id": ch, "channel_at": p.get("published_at") if ch and ch > 0 else None}


def cmd_migrate(con, args):
    j = json.load(open(args.ledger, encoding="utf-8"))
    posts = [legacy_post(p) for p in j["posts"]]
    # контекст для перевірки був лише в базі (не в журналі): беремо з неї, якщо є
    if has_table(con, "posts") and "context" in {r[1] for r in con.execute("PRAGMA table_info(posts)")}:
        ctx = dict(con.execute("SELECT tender_id, context FROM posts").fetchall())
        for p in posts:
            p["context"] = ctx.get(p["id"])
    st = j.get("state", {})
    meta = {k: st.get(k) for k in ("schedule_msg_id", "last_channel_post", "last_digest", "paused")}
    added = 0
    for part in api.chunks(posts, 50):
        added += send("import", {"posts": part, "meta": meta}).get("added", 0)
    from collections import Counter
    log(f"перенесено у Worker: {added} з {len(posts)}; статуси: {dict(Counter(p['status'] for p in posts))}")


def cmd_preview(con, args):
    from .texts import weighted_len
    for r in candidates(con, limit=args.n):
        t = compose(r)
        print(f"--- {r['tender_id']} бали={r['score']} довжина={weighted_len(t)}\n{t}\n\n{compose(r, full=True)}\n")


def cmd_health(con, args):
    print(json.dumps(api.call("health"), ensure_ascii=False, indent=1))


def cmd_events(con, args):
    """Журнал подій Worker (натискання, повідомлення, помилки) без id користувачів - для публічних логів Actions."""
    from datetime import timedelta
    from .common import now
    since = (now() - timedelta(hours=args.hours)).isoformat()
    for e in api.call(f"events?since={since}")["events"]:
        print(f"{e['at'][:19]} {e['kind']:7s} {e.get('post_id') or '':28s} {(e.get('data') or '')[:300]}")


def main():
    global DRY
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", help="база монітора (типово з publisher.json)")
    p.add_argument("--dry-run", action="store_true", help="нічого не надсилати у Worker")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sync")
    s.add_argument("--backup", default=os.path.join("state", "backup.json"))
    s.set_defaults(fn=cmd_sync)
    for name, fn in (("draft", cmd_draft), ("meta", cmd_meta), ("health", cmd_health)):
        sub.add_parser(name).set_defaults(fn=fn)
    ev = sub.add_parser("events")
    ev.add_argument("--hours", type=int, default=6)
    ev.set_defaults(fn=cmd_events)
    m = sub.add_parser("migrate")
    m.add_argument("--ledger", default=os.path.join("state", "ledger.json"))
    m.set_defaults(fn=cmd_migrate)
    pv = sub.add_parser("preview")
    pv.add_argument("--n", type=int, default=10)
    pv.set_defaults(fn=cmd_preview)
    a = p.parse_args()
    DRY = a.dry_run
    if a.db:
        CFG["db_path"] = a.db
    con = db()
    a.fn(con, a)


if __name__ == "__main__":
    main()
