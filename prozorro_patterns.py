#!/usr/bin/env python3
"""Закономірності на рівні замовника / постачальника, а не окремого тендера.

Такі випадки не обов'язково означають переплату, але підозрілі самі по собі:
  dominance  - один постачальник забирає більшість конкурентних закупівель замовника
  split      - кілька допорогових закупівель одного предмета, які разом перевищують поріг
  buyer      - звички замовника: майже завжди одна пропозиція або неконкурентні процедури

Для цього ведеться окрема таблиця awards (хто в кого що виграв), яка живе ~400 днів,
хоча самі тендери з бази видаляються через 45.
"""
from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

PCFG = {
    "pattern_window_days": 180,       # за який період дивимося на пари замовник-постачальник
    "dominance_min_wins": 5,          # мінімум перемог одного постачальника в замовника
    "dominance_share": 0.6,           # частка вартості конкурентних закупівель замовника
    "dominance_min_total_uah": 2_000_000,
    "split_window_days": 30,
    "split_min_count": 3,             # поділ: щонайменше 3 допорогові закупівлі В ОДНОГО постачальника
    "split_max_count": 10,            # десятки договорів за місяць - звичайна робота (напр. обленерго), а не поділ
    "buyer_single_max_expected": 0.5,  # «майже завжди одна пропозиція»: лише якщо зазвичай таких не більше половини
    "keep_awards_days": 400,
}
# пороги, як у моніторі (ПЕРЕВІРТЕ за чинними правилами)
THRESHOLDS = {"goods": 200_000, "services": 200_000, "works": 1_500_000}
# замовники в окремих сферах господарювання (kind=special): вищі пороги
THRESHOLDS_SPECIAL = {"goods": 1_000_000, "services": 1_000_000, "works": 5_000_000}
MIN_VALUE = 100_000

AWARDS_SCHEMA = """CREATE TABLE IF NOT EXISTS awards (
  id TEXT PRIMARY KEY, tender_id TEXT, buyer_id TEXT, buyer TEXT, supplier_id TEXT, supplier TEXT,
  amount REAL, est REAL, category TEXT, cpv TEXT, method TEXT, mtype TEXT, n_bids INTEGER,
  military INTEGER, published TEXT, title TEXT, buyer_kind TEXT)"""
COLS = ("id, tender_id, buyer_id, buyer, supplier_id, supplier, amount, est, category, cpv, method, mtype, n_bids, "
        "military, published, title, buyer_kind")


def ensure_awards(con):
    con.execute(AWARDS_SCHEMA)
    if "buyer_kind" not in {r[1] for r in con.execute("PRAGMA table_info(awards)")}:
        con.execute("ALTER TABLE awards ADD COLUMN buyer_kind TEXT")


def thr_for(r):
    return (THRESHOLDS_SPECIAL if r.get("buyer_kind") == "special" else THRESHOLDS).get(r["category"])


def now():
    return datetime.now(timezone.utc)


def lg(x):
    return round(math.log1p(max(x or 0, 0)), 4)


def sync_awards(con):
    """Переносить завершені закупівлі з таблиці tenders у довгу пам'ять awards."""
    ensure_awards(con)
    if not con.execute("SELECT name FROM sqlite_master WHERE name='tenders'").fetchone():
        return
    con.execute(f"""INSERT OR REPLACE INTO awards ({COLS})
        SELECT id, tender_id, buyer_id, buyer, supplier_id, supplier, award_amt, est, category, cpv, method, mtype,
               n_bids, military, COALESCE(published, modified), title, buyer_kind
        FROM tenders
        WHERE status IN ('complete', 'active.awarded') AND supplier_id IS NOT NULL AND award_amt > 0
          AND currency = 'UAH'""")
    cut = (now() - timedelta(days=PCFG["keep_awards_days"])).isoformat()
    con.execute("DELETE FROM awards WHERE published < ?", (cut,))
    con.commit()


def _dominance(rows, days):
    out = []
    by_buyer = defaultdict(list)
    for r in rows:
        if r["method"] != "limited":
            by_buyer[r["buyer_id"]].append(r)
    for bid, g in by_buyer.items():
        total = sum(r["amount"] for r in g)
        per = defaultdict(list)
        for r in g:
            per[r["supplier_id"]].append(r)
        for sid, w in per.items():
            s = sum(r["amount"] for r in w)
            share = s / total if total else 0
            if (len(w) >= PCFG["dominance_min_wins"] and share >= PCFG["dominance_share"]
                    and s >= PCFG["dominance_min_total_uah"]):
                one_bid = sum(1 for r in w if r["n_bids"] == 1)
                ex = max(w, key=lambda r: r["amount"])
                out.append(dict(
                    key=f"PAT-DOM-{bid}-{sid}", pattern="dominance", buyer=g[0]["buyer"], buyer_id=bid,
                    supplier=w[0]["supplier"], supplier_id=sid, n=len(w), n_buyer=len(g), share=share,
                    total=s, one_bid=one_bid, days=days, example=ex["tender_id"], money=s,
                    features={"kind": "pattern", "pattern": "dominance", "n": len(w), "n_buyer": len(g),
                              "share": round(share, 3), "log_total": lg(s), "one_bid_share": round(one_bid / len(w), 3),
                              "category": Counter(r["category"] for r in w).most_common(1)[0][0]}))
    return out


def _split(rows):
    # класичний поділ: той самий замовник, той самий предмет, той самий постачальник, кожна закупівля
    # нижча за поріг, а разом за 30 днів - вище. Багато різних підрядників (напр. приєднання до мереж) не рахуємо.
    out = []
    groups = defaultdict(list)
    for r in rows:
        thr = thr_for(r)
        if thr and r["cpv"] and r["published"] and r["est"] and MIN_VALUE <= r["est"] < thr:
            groups[(r["buyer_id"], r["category"], r["cpv"][:5], r["supplier_id"])].append(r)
    win = timedelta(days=PCFG["split_window_days"])
    for (bid, cat, cpv5, sid), g in groups.items():
        thr = thr_for(g[0])
        g.sort(key=lambda r: r["published"])
        best, lo = None, 0
        for hi in range(len(g)):
            while datetime.fromisoformat(g[hi]["published"]) - datetime.fromisoformat(g[lo]["published"]) > win:
                lo += 1
            w = g[lo:hi + 1]
            s = sum(r["est"] for r in w)
            if len(w) > PCFG["split_max_count"]:     # десятки договорів за місяць: звичайна робота, групу пропускаємо
                best = None
                break
            if len(w) >= PCFG["split_min_count"] and s >= thr and (not best or len(w) > len(best)):
                best = list(w)
        if best:
            s = sum(r["est"] for r in best)
            d = max(1, (datetime.fromisoformat(best[-1]["published"]) - datetime.fromisoformat(best[0]["published"])).days)
            ex = max(best, key=lambda r: r["est"])
            out.append(dict(
                key=f"PAT-SPLIT-{bid}-{cpv5}-{sid}-{best[-1]['published'][:7]}", pattern="split", buyer=best[0]["buyer"],
                buyer_id=bid, supplier=best[0]["supplier"], supplier_id=sid, n=len(best), total=s, threshold=thr, days=d,
                title=ex["title"], example=ex["tender_id"], money=s,
                features={"kind": "pattern", "pattern": "split", "n": len(best), "log_total": lg(s),
                          "over_threshold": round(s / thr, 3), "category": cat}))
    return out


def _buyer_habits(con, rows):
    out = []
    military = {r["buyer_id"] for r in rows if r["military"]}  # військові й безпекові
    totals = defaultdict(float)
    names = {}
    for r in rows:
        totals[r["buyer_id"]] += r["amount"] or 0
        names[r["buyer_id"]] = r["buyer"]
    q = now().astimezone(timezone.utc)
    quarter = f"{q.year}Q{(q.month - 1) // 3 + 1}"
    for f in con.execute("SELECT tender_id, code, detail FROM flags WHERE tender_id LIKE 'buyer:%' "
                         "AND code IN ('BUYER_HIGH_SINGLE_BID_RATE', 'BUYER_HIGH_DIRECT_AWARD_SHARE')"):
        bid = f["tender_id"].split(":", 1)[1]
        if bid in military or bid not in names:
            continue
        name = names[bid]
        detail = f["detail"] or ""
        detail = detail[len(name) + 2:] if detail.startswith(name + ": ") else detail
        short = "single" if f["code"] == "BUYER_HIGH_SINGLE_BID_RATE" else "direct"
        if short == "single":   # 10 з 10 мало що значить, якщо для таких процедур це й так норма
            m = re.search(r"(\d+)/(\d+) конкурентних тендерів.*?очікувано близько (\d+)", detail)
            if not m or int(m.group(3)) > PCFG["buyer_single_max_expected"] * int(m.group(2)):
                continue
        out.append(dict(
            key=f"PAT-BUY-{short}-{bid}-{quarter}", pattern=f"buyer_{short}", buyer=name, buyer_id=bid,
            supplier=None, supplier_id=None, detail=detail, total=totals[bid], example=None, money=totals[bid],
            features={"kind": "pattern", "pattern": f"buyer_{short}", "log_total": lg(totals[bid])}))
    return out


def find_patterns(con):
    """Список закономірностей (словники) за останні pattern_window_days. Військових і безпекових замовників не чіпаємо."""
    ensure_awards(con)
    days = PCFG["pattern_window_days"]
    since = (now() - timedelta(days=days)).isoformat()
    rows = [dict(r) for r in con.execute("SELECT * FROM awards WHERE published >= ?", (since,))]
    civil = [r for r in rows if not r["military"] and r["buyer_id"] and r["supplier_id"]]
    out = _dominance(civil, days) + _split(civil)
    if con.execute("SELECT name FROM sqlite_master WHERE name='flags'").fetchone():
        out += _buyer_habits(con, rows)
    out.sort(key=lambda p: -p["money"])
    return out
