#!/usr/bin/env python3
"""Історія постачальників: чи вигравав переможець щось у Prozorro раніше.

Червоний прапорець, коли великий договір отримує:
  SUPPLIER_NO_HISTORY      - постачальник без жодної перемоги за попередні 24 місяці
  SUPPLIER_NEW_IN_CATEGORY - постачальник, який раніше не вигравав закупівель цієї групи CPV (3 цифри, напр. 452*)

Історія зберігається компактно в окремій базі (state/history.db): лише «постачальник + категорія + місяць».
24 місяці до початку живого моніторингу дозавантажуються окремим workflow (prozorro-history), який
перезапускає сам себе, доки не закінчить. Поки це не завершено, прапорці не ставляться.
Свіжі перемоги (від початку моніторингу) беруться з таблиці awards основної бази.

  python prozorro_history.py --hist state/history.db backfill --minutes 25 --until 2026-10-29T00:00:00+00:00
  python prozorro_history.py --db state/prozorro.db --hist state/history.db flags
  python prozorro_history.py --hist state/history.db status     # виводить також done=1, коли завершено
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
    "category_digits": 3,             # рівень CPV для «новачка»: 2 - розділ, 3 - група, 4 - клас
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


# ------------------------------------------------------------------ фонове дозавантаження
def backfill(con, minutes, until):
    # until: з цієї дати все покриває живий моніторинг (таблиця awards основної бази)
    if meta(con, "hist_complete") == "1":
        log("історію вже дозавантажено")
        return
    start = meta(con, "hist_from")
    if not start:
        start = months_back(M.ts(until), HCFG["history_months"]).isoformat()
        meta(con, "hist_from", start)
    if not meta(con, "hist_until"):
        meta(con, "hist_until", until)
    until = meta(con, "hist_until")
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
def flags(con, hist_path):
    import prozorro_patterns as PP
    PP.sync_awards(con)                       # свіжі перемоги з живого моніторингу
    if not os.path.exists(hist_path):
        log("бази історії ще немає, прапорці не ставлю")
        return
    con.execute("ATTACH DATABASE ? AS h", (hist_path,))
    h = con.execute("SELECT v FROM h.meta WHERE k='hist_complete'").fetchone()
    if not h or h[0] != "1":
        cur = con.execute("SELECT v FROM h.meta WHERE k='hist_cursor'").fetchone()
        log(f"історія ще неповна (позиція {cur[0] if cur else '-'}), прапорці не ставлю")
        return
    nd = HCFG["category_digits"]
    con.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")
    prev = con.execute("SELECT v FROM meta WHERE k='hist_category_digits'").fetchone()
    days = HCFG["lookback_days"] if prev and prev[0] == str(nd) else 60   # правило змінилося: переглянути все
    since = (now() - timedelta(days=days)).isoformat()
    rows = [dict(r) for r in con.execute(
        """SELECT id, supplier_id, cpv, award_amt, published, modified, military FROM tenders
           WHERE status IN ('complete', 'active.awarded') AND supplier_id IS NOT NULL AND award_amt >= ?
             AND currency = 'UAH' AND COALESCE(modified, published) >= ?""", (HCFG["min_award_uah"], since))]
    stamp, new, hit = now().isoformat(), 0, set()
    for r in rows:
        month = (r["published"] or r["modified"] or "")[:7]
        cpvn = (r["cpv"] or "")[:nd]
        if not month:
            continue
        any_before = con.execute(
            "SELECT 1 FROM h.supplier_hist WHERE supplier_id=? AND month<? UNION ALL "
            "SELECT 1 FROM awards WHERE supplier_id=? AND substr(published, 1, 7)<? LIMIT 1",
            (r["supplier_id"], month, r["supplier_id"], month)).fetchone()
        if not any_before:
            code, w, detail = "SUPPLIER_NO_HISTORY", 3, f"Переможець не мав перемог у Prozorro за {HCFG['history_months']} міс. до цього"
        else:
            cat_before = con.execute(
                "SELECT 1 FROM h.supplier_hist WHERE supplier_id=? AND substr(cpv4, 1, ?)=? AND month<? UNION ALL "
                "SELECT 1 FROM awards WHERE supplier_id=? AND substr(cpv, 1, ?)=? AND substr(published, 1, 7)<? LIMIT 1",
                (r["supplier_id"], nd, cpvn, month, r["supplier_id"], nd, cpvn, month)).fetchone()
            if cat_before or len(cpvn) < nd:
                continue
            code, w, detail = "SUPPLIER_NEW_IN_CATEGORY", 3, f"Переможець раніше не вигравав закупівель групи CPV {cpvn}*"
        hit.add((r["id"], code))
        cur = con.execute("INSERT OR IGNORE INTO flags VALUES (?,?,?,?,?)", (r["id"], code, w, detail, stamp))
        if not cur.rowcount:
            con.execute("UPDATE flags SET detail=? WHERE tender_id=? AND code=?", (detail, r["id"], code))
        new += cur.rowcount
    # прапорці, що вже не справджуються (напр. після зміни рівня CPV), прибираємо
    gone = 0
    for r in rows:
        for code in ("SUPPLIER_NO_HISTORY", "SUPPLIER_NEW_IN_CATEGORY"):
            if (r["id"], code) not in hit:
                gone += con.execute("DELETE FROM flags WHERE tender_id=? AND code=?", (r["id"], code)).rowcount
    con.execute("INSERT OR REPLACE INTO meta VALUES ('hist_category_digits', ?)", (str(nd),))
    con.commit()
    log(f"перевірено перемог: {len(rows)}, нових прапорців: {new}, прибрано застарілих: {gone}")


def status(con):
    n = con.execute("SELECT COUNT(*) FROM supplier_hist").fetchone()[0]
    done = meta(con, "hist_complete") == "1"
    print(f"записів: {n}; з {meta(con, 'hist_from')}; позиція {meta(con, 'hist_cursor')}; "
          f"до {meta(con, 'hist_until')}; завершено: {done}")
    print(f"done={1 if done else 0}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="state/prozorro.db", help="основна база бота")
    ap.add_argument("--hist", default="state/history.db", help="окрема база історії")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("backfill")
    b.add_argument("--minutes", type=float, default=25)
    b.add_argument("--until", default="2026-10-29T00:00:00+00:00", help="початок живого моніторингу")
    sub.add_parser("flags")
    sub.add_parser("status")
    a = ap.parse_args()
    if a.cmd == "backfill":
        backfill(db(a.hist), a.minutes, a.until)
    elif a.cmd == "flags":
        con = sqlite3.connect(a.db)
        con.row_factory = sqlite3.Row
        flags(con, a.hist)
    else:
        status(db(a.hist))


if __name__ == "__main__":
    main()
