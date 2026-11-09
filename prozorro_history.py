#!/usr/bin/env python3
"""Історія постачальників: чи вигравав переможець щось у Prozorro раніше.

Червоний прапорець, коли великий договір отримує:
  SUPPLIER_NO_HISTORY      - постачальник без жодної перемоги за попередні 24 місяці
  SUPPLIER_NEW_IN_CATEGORY - постачальник, який раніше не вигравав закупівель цієї категорії (CPV, 4 цифри)

Історія зберігається компактно: лише «постачальник + категорія + місяць» (без сум і деталей).
Перші 24 місяці дозавантажуються у фоні (backfill, частинами в кожному проході), і поки це
не завершено, прапорці не ставляться, щоб не було хибних тривог.

  python prozorro_history.py --db state/prozorro.db flags            # прапорці для нових переможців
  python prozorro_history.py --db state/prozorro.db backfill --minutes 18
  python prozorro_history.py --db state/prozorro.db status
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from itertools import islice

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import prozorro_monitor as M  # noqa: E402

HCFG = {
    "history_months": 24,
    "min_award_uah": 1_000_000,       # для менших договорів прапорці не ставимо
    "lookback_days": 14,              # які нещодавні перемоги перевіряти
    "workers": 12,
}
DONE = ("complete", "active.awarded")

SCHEMA = """
CREATE TABLE IF NOT EXISTS supplier_hist (supplier_id TEXT, cpv4 TEXT, month TEXT,
  PRIMARY KEY (supplier_id, cpv4, month)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""


def now():
    return datetime.now(timezone.utc)


def log(msg):
    print(f"[історія] {msg}", file=sys.stderr)


def meta(con, k, v=None):
    if v is None:
        r = con.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return r[0] if r else None
    con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, str(v)))
    con.commit()


def db(path):
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    return con


def months_back(d, n):
    y, m = d.year, d.month - n
    while m <= 0:
        y, m = y - 1, m + 12
    return datetime(y, m, 1, tzinfo=timezone.utc)


def add_rows(con, recs):
    rows = {(r["supplier_id"], (r["cpv"] or "")[:4], (r["published"] or r["modified"] or "")[:7])
            for r in recs if r and r.get("status") in DONE and r.get("supplier_id")}
    con.executemany("INSERT OR IGNORE INTO supplier_hist VALUES (?,?,?)", [x for x in rows if x[2]])
    con.commit()
    return len(rows)


def sync_live(con):
    """Переможці, яких бот уже бачив у звичайному режимі, теж ідуть в історію."""
    if not con.execute("SELECT name FROM sqlite_master WHERE name='tenders'").fetchone():
        return
    add_rows(con, [dict(r) for r in con.execute(
        "SELECT status, supplier_id, cpv, published, modified FROM tenders WHERE supplier_id IS NOT NULL")])


# ------------------------------------------------------------------ фонове дозавантаження
def backfill(con, minutes):
    sync_live(con)
    if meta(con, "hist_complete") == "1":
        log("історію вже дозавантажено")
        return
    start = meta(con, "hist_from")
    if not start:
        start = months_back(now(), HCFG["history_months"]).isoformat()
        meta(con, "hist_from", start)
    until = meta(con, "hist_until")
    if not until:   # звичайний режим уже покриває все, що змінювалося після цієї дати
        first = None
        if con.execute("SELECT name FROM sqlite_master WHERE name='tenders'").fetchone():
            first = con.execute("SELECT MIN(modified) FROM tenders").fetchone()[0]
        until = first or now().isoformat()
        meta(con, "hist_until", until)
    cursor = M.ts(meta(con, "hist_cursor") or start)
    until_d = M.ts(until)
    deadline = time.time() + minutes * 60
    s = M.make_session()

    def feed():
        params = {"limit": 1000, "offset": cursor.isoformat(), "opt_fields": "status"}
        while True:
            r = s.get(f"{M.API}/tenders", params=params, timeout=60)
            r.raise_for_status()
            j = r.json()
            rows = j.get("data", [])
            if not rows:
                return
            for row in rows:
                yield row
            nxt = (j.get("next_page") or {}).get("offset")
            if not nxt or nxt == params["offset"]:
                return
            params = {"limit": 1000, "offset": nxt, "opt_fields": "status"}

    def work(row):
        try:
            t = M.fetch_tender(s, row["id"])
            return M.flatten(t) if t else None
        except Exception as e:
            log(f"пропущено {row['id']}: {e}")
            return None

    it, seen, added, done = feed(), 0, 0, False
    with ThreadPoolExecutor(HCFG["workers"]) as ex:
        while time.time() < deadline:
            batch = list(islice(it, 1000))
            if not batch:
                done = True
                break
            last = batch[-1]["dateModified"]
            if M.ts(last) >= until_d:
                batch = [b for b in batch if M.ts(b["dateModified"]) < until_d]
                done = True
            need = [b for b in batch if b.get("status") in DONE]
            added += add_rows(con, list(ex.map(work, need)))
            seen += len(batch)
            meta(con, "hist_cursor", last)
            if done:
                break
    if done:
        meta(con, "hist_complete", "1")
        log(f"історію дозавантажено повністю (з {start[:10]})")
    log(f"переглянуто {seen}, додано записів {added}, позиція {meta(con, 'hist_cursor')}")


# ------------------------------------------------------------------ прапорці
def flags(con):
    sync_live(con)
    if meta(con, "hist_complete") != "1":
        log(f"історія ще неповна (позиція {meta(con, 'hist_cursor') or '-'}), прапорці не ставлю")
        return
    since = (now() - timedelta(days=HCFG["lookback_days"])).isoformat()
    rows = [dict(r) for r in con.execute(
        """SELECT id, supplier_id, cpv, award_amt, published, modified, military FROM tenders
           WHERE status IN ('complete', 'active.awarded') AND supplier_id IS NOT NULL AND award_amt >= ?
             AND currency = 'UAH' AND COALESCE(modified, published) >= ?""", (HCFG["min_award_uah"], since))]
    stamp, new = now().isoformat(), 0
    for r in rows:
        month = (r["published"] or r["modified"] or "")[:7]
        cpv4 = (r["cpv"] or "")[:4]
        if not month:
            continue
        any_before = con.execute("SELECT 1 FROM supplier_hist WHERE supplier_id=? AND month<? LIMIT 1",
                                 (r["supplier_id"], month)).fetchone()
        if not any_before:
            code, w, detail = "SUPPLIER_NO_HISTORY", 3, f"Переможець не мав перемог у Prozorro за {HCFG['history_months']} міс. до цього"
        else:
            cat_before = con.execute("SELECT 1 FROM supplier_hist WHERE supplier_id=? AND cpv4=? AND month<? LIMIT 1",
                                     (r["supplier_id"], cpv4, month)).fetchone()
            if cat_before or not cpv4:
                continue
            code, w, detail = "SUPPLIER_NEW_IN_CATEGORY", 3, f"Переможець раніше не вигравав закупівель категорії CPV {cpv4}*"
        cur = con.execute("INSERT OR IGNORE INTO flags VALUES (?,?,?,?,?)", (r["id"], code, w, detail, stamp))
        new += cur.rowcount
    con.commit()
    log(f"перевірено перемог: {len(rows)}, нових прапорців: {new}")


def status(con):
    n = con.execute("SELECT COUNT(*) FROM supplier_hist").fetchone()[0]
    print(f"записів: {n}; з {meta(con, 'hist_from')}; позиція {meta(con, 'hist_cursor')}; "
          f"до {meta(con, 'hist_until')}; завершено: {meta(con, 'hist_complete') == '1'}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="state/prozorro.db")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("backfill")
    b.add_argument("--minutes", type=float, default=18)
    sub.add_parser("flags")
    sub.add_parser("status")
    a = ap.parse_args()
    con = db(a.db)
    if a.cmd == "backfill":
        backfill(con, a.minutes)
    elif a.cmd == "flags":
        flags(con)
    else:
        status(con)


if __name__ == "__main__":
    main()
