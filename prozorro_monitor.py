#!/usr/bin/env python3
"""Монітор аномалій Prozorro: завантаження -> збереження (SQLite) -> пошук червоних прапорців -> звіт.

Встановлення:   pip install requests
Перший запуск:  python prozorro_monitor.py run --since 2026-11-01     (дозавантаження з дати)
За розкладом:   python prozorro_monitor.py run                        (інкрементально; cron / Планувальник завдань)
Повторний звіт: python prozorro_monitor.py report --days 7
Перевірка API:  python prozorro_monitor.py probe --n 30               (завантажує найновіші тендери, показує, які
                поля знайдено, пише probe_sample.json; запустіть ЦЕ першим на реальному API)
Пробний запуск: python prozorro_monitor.py --db trial.db run --since 2026-11-05 --limit 500

Результати (у ./reports): anomalies_<дата>.md і anomalies_<дата>.csv
Необов'язково:  задайте PROZORRO_WEBHOOK_URL (вхідний вебхук у стилі Slack/Mattermost) для короткого сповіщення.
Налаштування:   будь-який ключ із CFG нижче можна перевизначити у config.json.

Прапорці — це ІНДИКАТОРИ ризику для перевірки людиною, а не доказ порушень.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import statistics
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from itertools import islice

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API = "https://public-api.prozorro.gov.ua/api/2.5"
PORTAL = "https://prozorro.gov.ua/tender/"

CFG = {
    "db_path": "prozorro.db",
    "out_dir": "reports",
    "workers": 12,                      # паралельні запити деталей; не перевантажуйте API
    "max_tenders_per_run": 20000,
    "min_value_uah": 100_000,          # ігнорувати дрібні тендери
    "high_value_uah": 5_000_000,
    # мінімальний звичайний строк подання пропозицій (днів) за procurementMethodType; інші типи не перевіряються.
    # ПЕРЕВІРТЕ за чинним законом / правилами воєнного стану (скорочені процедури законно тривають 2-5 днів).
    "min_bid_period_days": {"aboveThreshold": 7, "aboveThresholdUA": 7, "aboveThresholdEU": 7,
                            "competitiveDialogueUA": 7, "competitiveDialogueEU": 7, "esco": 7,
                            "belowThreshold": 2, "priceQuotation": 2, "requestForProposal": 2},
    # ПЕРЕВІРТЕ за чинним законом про публічні закупівлі / правилами воєнного стану, перш ніж покладатися на це:
    "thresholds_uah": {"goods": 200_000, "services": 200_000, "works": 1_500_000},
    # замовники в окремих сферах господарювання (енергетика, транспорт, водопостачання...; kind=special)
    "thresholds_special_uah": {"goods": 1_000_000, "services": 1_000_000, "works": 5_000_000},
    "threshold_margin": 0.03,          # «трохи нижче» = у межах 3% під порогом
    "split_window_days": 30,
    "split_min_count": 3,
    "split_max_count": 10,             # десятки договорів за місяць - це вже звичайна робота, а не поділ
    "concentration_min_contracts": 5,
    "concentration_share": 0.60,       # один постачальник отримує >= 60% вартості конкурентних перемог замовника
    "ratio_min_group": 30,
    "ratio_z": 3.5,                    # поріг робастного z-показника для співвідношення ціна/очікувана вартість
    "buyer_min_tenders": 10,
    "report_min_score": 3,
    "price_min_peers": 3,               # перевірка ціни за одиницю: скільки потрібно порівнянних закупівель інших замовників
    "price_ratio": 1.8,                 # прапорець, коли ціна за одиницю >= 1,8× медіани ...
    "price_ratio_max": 15,              # ... але ігнорувати >15× (помилки в одиницях/фасуванні)
    "price_min_excess_uah": 200_000,
    "webhook_url": os.getenv("PROZORRO_WEBHOOK_URL", ""),
}
if os.path.exists("config.json"):
    with open("config.json", encoding="utf-8") as fh:
        CFG.update(json.load(fh))

DONE = {"complete", "active.awarded"}  # переможця визначено
DECIDED = DONE | {"active.qualification", "active.qualification.stand-still"}

COLS = ["id", "tender_id", "title", "status", "method", "mtype", "category", "currency", "est",
        "award_amt", "contract_amt", "n_bids", "n_unsuccessful", "buyer_id", "buyer", "region",
        "supplier_id", "supplier", "cpv", "period_start", "period_end", "period_days",
        "cause_desc", "published", "modified", "buyer_kind", "military", "mil_reason", "n_complaints",
        "complaints", "bidders", "elim", "elim_gap", "elim_gap_pct", "elim_kind", "shared_contacts", "unit_prices"]

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS tenders ({', '.join(c + (' TEXT PRIMARY KEY' if c == 'id' else '') for c in COLS)});
CREATE TABLE IF NOT EXISTS flags (tender_id TEXT, code TEXT, weight INTEGER, detail TEXT, first_seen TEXT,
                                  PRIMARY KEY (tender_id, code));
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""


# ---------------------------------------------------------------- допоміжні функції
def now() -> datetime:
    return datetime.now(timezone.utc)


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def threshold(r):
    t = CFG["thresholds_special_uah"] if r.get("buyer_kind") == "special" else CFG["thresholds_uah"]
    return t.get(r.get("category"))


def ts(x):
    if not x:
        return None
    try:
        d = datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def iso(x):
    d = ts(x)
    return d.isoformat() if d else None


def log(msg):
    print(f"[{now():%H:%M:%S}] {msg}", file=sys.stderr)


# ---------------------------------------------------------------- завантаження
def make_session():
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET",))
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=CFG["workers"] * 2))
    s.headers["User-Agent"] = "prozorro-anomaly-monitor/0.1"
    return s


def iter_feed_desc(s, stop_at: datetime, limit=100):
    """Іде стрічкою тендерів від найновіших до найстаріших, доки не дійде до `stop_at` (позначки попереднього запуску)."""
    params = {"descending": 1, "limit": limit}
    while True:
        r = s.get(f"{API}/tenders", params=params, timeout=60)
        r.raise_for_status()
        j = r.json()
        rows = j.get("data", [])
        if not rows:
            return
        for row in rows:
            d = ts(row.get("dateModified"))
            if d and d <= stop_at:
                return
            yield row
        nxt = (j.get("next_page") or {}).get("offset")
        if not nxt:
            return
        params = {"descending": 1, "limit": limit, "offset": nxt}


def iter_feed_asc(s, start: datetime, limit=100):
    """Іде стрічкою тендерів від найстаріших до найновіших з `start`, щоб обмежений чи перерваний запуск чисто продовжувався."""
    params = {"limit": limit, "offset": start.isoformat()}
    while True:
        r = s.get(f"{API}/tenders", params=params, timeout=60)
        r.raise_for_status()
        j = r.json()
        rows = j.get("data", [])
        if not rows:
            return
        for row in rows:
            d = ts(row.get("dateModified"))
            if d and d > start:
                yield row
        nxt = (j.get("next_page") or {}).get("offset")
        if not nxt or nxt == params["offset"]:
            return
        params = {"limit": limit, "offset": nxt}


def fetch_tender(s, tid):
    r = s.get(f"{API}/tenders/{tid}", timeout=60)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()["data"]



MIL_NAME = re.compile(r"військ|війск|в/ч|\bвч\b|\bзсу\b|збройних сил|національн\w* гвард|прикордонн|"
                      r"міністерств\w* оборони|генеральн\w* штаб|сил спеціальних операцій|командуванн|десантно|"
                      r"повітряних сил|військово-морськ|розвідк|тероборон|територіальн\w* центр\w* комплектування|\bтцк\b|"
                      r"спеціальн\w* служб\w* транспорту|квартирно-?\s?експлуатаційн|управління замовника робіт|"
                      r"оборонн|defen[cs]e|^\s*а\d{4}\b", re.I)
SEC_NAME = re.compile(r"поліці|служб\w* безпеки|\bсбу\b|спеціального зв|держспецзв|державної охорони|"
                      r"внутрішніх справ|\bмвс\b|прокуратур|бюро розслідувань|\bнабу\b", re.I)
MIL_CPV = ("353", "354", "355", "356", "357", "358")   # зброя/боєприпаси, військова техніка, кораблі, авіація, електроніка, спорядження
WITHDREW = re.compile(r"відмовився|не підписав|не уклав|відмов\w* від (укладення|підписання)", re.I)


def military_class(name, kind, cpvs):
    """1 = військові (ЗСУ, НГУ, прикордонники, органи Міноборони, військовий CPV); 2 = інші безпекові / правоохоронні."""
    name = (name or "").replace("\u00ad", "")
    if SEC_NAME.search(name) and not re.search(r"військов|національн\w* гвард", name, re.I):
        return 2, "безпековий/правоохоронний замовник"
    if MIL_NAME.search(name):
        return 1, "військовий замовник (назва)"
    hit = next((c for c in cpvs if c and c[:3] in MIL_CPV), None)
    if hit:
        return 1, f"військовий CPV {hit}"
    if kind == "defense":
        return 2, "безпековий замовник (kind=defense)"
    return 0, ""


def military_tag(t, cpvs):
    pe = t.get("procuringEntity") or {}
    return military_class(pe.get("name"), pe.get("kind"), cpvs)


def _party(p):
    return (p.get("identifier") or {}).get("id"), p.get("name")


def eliminations(t):
    """По кожному лоту: дешевші учасники, яких відхилили або які відмовилися, тоді як перемогла дорожча пропозиція."""
    lots = {l["id"]: l for l in (t.get("lots") or [])}
    by_lot = defaultdict(list)
    for a in t.get("awards") or []:
        by_lot[a.get("lotID")].append(a)
    out = []
    for lot, aw in by_lot.items():
        win = [a for a in aw if a.get("status") == "active"]
        if not win:
            continue
        w = win[-1]
        wid, wname = _party((w.get("suppliers") or [{}])[0])
        wp = num((w.get("value") or {}).get("amount"))
        est = num(((lots.get(lot) or t).get("value") or {}).get("amount"))
        for a in aw:
            if a.get("status") != "unsuccessful":
                continue
            rid, rname = _party((a.get("suppliers") or [{}])[0])
            rp = num((a.get("value") or {}).get("amount"))
            if not (rp and wp and rp < wp and rid != wid):
                continue
            why = " ".join(filter(None, [a.get("title"), a.get("description")]))
            out.append(dict(lot=lot, est=est, winner_id=wid, winner=wname, winner_price=wp, id=rid, name=rname,
                            price=rp, kind="withdrew" if WITHDREW.search(why) else "rejected", reason=why[:240]))
    return out


def shared_contacts(t):
    """Різні учасники (за ЄДРПОУ) одного тендера зі спільним телефоном, e-mail, контактною особою чи адресою."""
    seen = defaultdict(set)
    names = {}
    for b in t.get("bids") or []:
        for p in b.get("tenderers") or []:
            pid, nm = _party(p)
            if not pid:
                continue
            names[pid] = nm
            cp, ad = p.get("contactPoint") or {}, p.get("address") or {}
            ph = re.sub(r"\D", "", cp.get("telephone") or "")[-9:]
            keys = [("телефон", ph if len(ph) == 9 else ""), ("e-mail", (cp.get("email") or "").strip().lower()),
                    ("контактна особа", re.sub(r"\s+", " ", (cp.get("name") or "").strip().lower())),
                    ("адреса", re.sub(r"\W+", "", ((ad.get("streetAddress") or "") + (ad.get("locality") or "")).lower())
                                if re.search(r"\d", ad.get("streetAddress") or "") else "")]
            for k, v in keys:
                if v and len(v) > 5:
                    seen[(k, v)].add(pid)
    hits = []
    for (k, v), ids in seen.items():
        if len(ids) > 1:
            hits.append(dict(field=k, value=v, bidders=[[i, names.get(i)] for i in sorted(ids)]))
    return hits


def complaints_of(t):
    out = []
    for a in [t] + (t.get("awards") or []):
        on = "тендер" if a is t else f"рішення:{a.get('status')}:{_party((a.get('suppliers') or [{}])[0])[1]}"
        for c in a.get("complaints") or []:
            out.append(dict(type=c.get("type"), status=c.get("status"), on=on, date=c.get("dateDecision") or c.get("date")))
    return out


GENERIC = re.compile(r"асортимент|різн(і|их)\b|запчастин|запасн\w* частин|комплектуюч|матеріал\w* для|витратн", re.I)
NON_UNIT = re.compile(r"послуг|робот|лот|умовн|об.?єкт|місяц|рік|год|день|доба|комплекс|систем|пакет|набір|комплект", re.I)


def unit_prices(t):
    """Ціна за одиницю без ПДВ для кожного присудженого лоту з одним товаром (порівнянна між замовниками)."""
    out = []
    for a in t.get("awards") or []:
        if a.get("status") != "active":
            continue
        lot = a.get("lotID")
        its = [i for i in t.get("items") or [] if i.get("relatedLot") == lot or lot is None]
        descs = {re.sub(r"(розмір|size)[^,;]*", "", (i.get("description") or "").lower())[:40] for i in its}
        keys = {((i.get("classification") or {}).get("id"), (i.get("unit") or {}).get("code")) for i in its}
        if len(descs) != 1 or len(keys) != 1:
            continue
        unit_name = (its[0].get("unit") or {}).get("name") or ""
        if NON_UNIT.search(unit_name) or GENERIC.search(its[0].get("description") or ""):
            continue
        qty = sum(num(i.get("quantity")) or 0 for i in its)
        av = a.get("value") or {}
        amt = num(av.get("amount"))
        if not qty or not amt:
            continue
        cpv, unit = list(keys)[0]
        vat = (1.07 if (cpv or "").startswith(("336", "3314")) else 1.2) if av.get("valueAddedTaxIncluded") else 1.0
        out.append(dict(cpv=cpv, unit=unit, unit_name=unit_name, descr=(its[0].get("description") or "")[:160],
                        qty=qty, price=amt / qty / vat))
    return out


def flatten(t: dict) -> dict:
    """Зводить повний документ тендера до плоских полів, потрібних детекторам."""
    pe = t.get("procuringEntity") or {}
    bids = t.get("bids") or []
    nb = t.get("numberOfBids")
    n_bids = int(nb) if str(nb).isdigit() else (len(bids) if bids else None)
    awards = t.get("awards") or []
    active = [a for a in awards if a.get("status") == "active"]
    win = active[-1] if active else {}
    sup = (win.get("suppliers") or [{}])[0]
    # сума за всіма активними рішеннями (багатолотові тендери); договори порівнюються на тій самій базі ПДВ, що й рішення
    a_amt = sum(num((a.get("value") or {}).get("amount")) or 0 for a in active) or None
    award_vat = any((a.get("value") or {}).get("valueAddedTaxIncluded") for a in active)
    contracts = [c for c in (t.get("contracts") or []) if c.get("status") in ("active", "pending")]

    def c_val(c):
        v = c.get("value") or {}
        if v.get("valueAddedTaxIncluded") and not award_vat and v.get("amountNet") is not None:
            return num(v.get("amountNet")) or 0
        return num(v.get("amount")) or 0
    c_amt = sum(c_val(c) for c in contracts) or None
    items = t.get("items") or []
    cpv = ((items[0].get("classification") or {}).get("id")) if items else None
    tp = t.get("tenderPeriod") or {}
    s, e = ts(tp.get("startDate")), ts(tp.get("endDate"))
    val = t.get("value") or {}
    cpvs = [((i.get("classification") or {}).get("id")) for i in items]
    el = eliminations(t)
    gaps = {}
    for x in el:
        k = x["lot"]
        gaps[k] = max(gaps.get(k, (0, 0)), (x["winner_price"] - x["price"], (x["winner_price"] - x["price"]) / x["price"]))
    kinds = {x["kind"] for x in el}
    cpl = complaints_of(t)
    bidders = sorted({_party(p) for b in bids for p in (b.get("tenderers") or []) if _party(p)[0]}, key=str)
    shc = shared_contacts(t)
    up = unit_prices(t)
    return dict(
        id=t["id"], tender_id=t.get("tenderID"), title=(t.get("title") or "")[:200],
        status=t.get("status"), method=t.get("procurementMethod"), mtype=t.get("procurementMethodType"),
        category=t.get("mainProcurementCategory"), currency=val.get("currency"), est=num(val.get("amount")),
        award_amt=a_amt, contract_amt=c_amt, n_bids=n_bids,
        n_unsuccessful=sum(1 for a in awards if a.get("status") == "unsuccessful"),
        buyer_id=(pe.get("identifier") or {}).get("id"), buyer=pe.get("name"),
        region=(pe.get("address") or {}).get("region"),
        supplier_id=(sup.get("identifier") or {}).get("id"), supplier=sup.get("name"),
        cpv=cpv, period_start=iso(tp.get("startDate")), period_end=iso(tp.get("endDate")),
        period_days=(e - s).total_seconds() / 86400 if s and e else None,
        cause_desc=(t.get("causeDescription") or (t.get("causeDetails") or {}).get("description")
                    or (t.get("causeDetails") or {}).get("code") or "").strip(),
        published=iso(t.get("datePublished") or t.get("date") or tp.get("startDate")),
        modified=iso(t.get("dateModified")),
        buyer_kind=pe.get("kind"), military=military_tag(t, cpvs)[0], mil_reason=military_tag(t, cpvs)[1],
        n_complaints=sum(c["type"] == "complaint" for c in cpl),
        complaints=json.dumps(cpl, ensure_ascii=False) if cpl else None,
        bidders=json.dumps(bidders, ensure_ascii=False) if bidders else None,
        elim=json.dumps(el, ensure_ascii=False) if el else None,
        elim_gap=sum(g for g, _ in gaps.values()) or None,
        elim_gap_pct=max((p for _, p in gaps.values()), default=None),
        elim_kind=("withdrew" if kinds == {"withdrew"} else "rejected" if kinds == {"rejected"} else "mixed") if kinds else None,
        shared_contacts=json.dumps(shc, ensure_ascii=False) if shc else None,
        unit_prices=json.dumps(up, ensure_ascii=False) if up else None,
    )


# ---------------------------------------------------------------- зберігання
def db():
    con = sqlite3.connect(CFG["db_path"])
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    have = {r[1] for r in con.execute("PRAGMA table_info(tenders)")}
    for c in COLS:
        if c not in have:
            con.execute(f"ALTER TABLE tenders ADD COLUMN {c}")
    return con


def upsert(con, recs):
    q = f"INSERT OR REPLACE INTO tenders ({','.join(COLS)}) VALUES ({','.join(':' + c for c in COLS)})"
    con.executemany(q, recs)
    con.commit()


def meta_get(con, k):
    r = con.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    return r["v"] if r else None


def meta_set(con, k, v):
    con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
    con.commit()


def save_flags(con, flags, stamp):
    """Зберігає прапорці; повертає лише нові (щоб сповіщення не повторювалися)."""
    new = []
    for tid, code, w, detail in flags:
        cur = con.execute("INSERT OR IGNORE INTO flags VALUES (?,?,?,?,?)", (tid, code, w, detail, stamp))
        if cur.rowcount:
            new.append((tid, code, w, detail))
    con.commit()
    return new


# ---------------------------------------------------------------- детектори: окремий тендер
def tender_flags(r: dict):
    out = []
    est = r["est"] if r["currency"] == "UAH" else None
    if not est or est < CFG["min_value_uah"]:
        return out
    competitive = r["method"] != "limited"
    nb, big = r["n_bids"], est >= CFG["high_value_uah"]

    def add(code, w, detail):
        out.append((r["id"], code, w, detail))

    if competitive and nb == 1 and r["status"] in DECIDED:
        add("SINGLE_BIDDER", 3 if big else 2, f"Лише 1 пропозиція в конкурентному тендері на {est:,.0f} грн")

    d, mind = r["period_days"], CFG["min_bid_period_days"].get(r["mtype"])
    if competitive and d is not None and mind and d < mind - 0.05:
        add("SHORT_BID_PERIOD", 2, f"Строк подання лише {d:.1f} дн. (звичайний мінімум {mind} для {r['mtype']})")

    thr = threshold(r)
    if thr and thr * (1 - CFG["threshold_margin"]) <= est < thr:
        add("JUST_BELOW_THRESHOLD", 1, f"Очікувана вартість {est:,.0f} трохи нижча за поріг {thr:,.0f} грн")

    if r["award_amt"]:
        ratio = r["award_amt"] / est
        if competitive and 0.9999 <= ratio <= 1.0:
            add("BID_AT_ESTIMATE_EXACT", 2, f"Ціна переможця {r['award_amt']:,.0f} лише на {est - r['award_amt']:,.0f} грн "
                                            f"нижча за очікувану {est:,.0f} ({ratio:.3%})")
        elif competitive and ratio >= 0.99 and nb is not None and nb <= 2:
            add("AWARD_AT_ESTIMATE", 1, f"Сума рішення — {ratio:.1%} очікуваної вартості, пропозицій лише {nb}")
        if ratio <= 0.5:
            add("VERY_LOW_AWARD", 1, f"Сума рішення лише {ratio:.0%} очікуваної вартості")
        if r["contract_amt"] and r["contract_amt"] > r["award_amt"] * 1.005:
            add("CONTRACT_ABOVE_AWARD", 3,
                f"Договір {r['contract_amt']:,.0f} перевищує суму рішення {r['award_amt']:,.0f} грн")

    gap, gp = r.get("elim_gap"), r.get("elim_gap_pct")
    if competitive and gap:
        w = 2 + (1 if (gp or 0) >= 0.2 or gap >= 1_000_000 else 0)
        what = {"withdrew": "відмовилися / не підписали договір", "rejected": "були відхилені",
                "mixed": "були відхилені або відмовилися"}[r["elim_kind"]]
        add("CHEAPER_BIDS_WITHDREW" if r["elim_kind"] == "withdrew" else "CHEAPER_BIDS_REJECTED", w,
            f"Дешевші учасники {what}; переможець дорожчий на {gap:,.0f} грн (+{gp:.0%}) за найдешевшого з них")
    elif competitive and nb and nb >= 2 and r["n_unsuccessful"] >= nb - 1 and r["supplier_id"]:
        add("COMPETITORS_ELIMINATED", 1, f"Відхилено {r['n_unsuccessful']} з {nb} учасників; переможець залишився методом виключення")
    if r.get("shared_contacts"):
        sc = json.loads(r["shared_contacts"])
        add("SHARED_BIDDER_CONTACTS", 3, "; ".join(
            f"збіг ({h['field']}): " + " & ".join(n or i for i, n in h["bidders"]) for h in sc)[:400])
    if r.get("complaints"):
        sat = [c for c in json.loads(r["complaints"]) if c["type"] == "complaint" and c["status"] == "satisfied"]
        if sat:
            add("AMCU_COMPLAINT_UPHELD", 2, f"АМКУ задовольнив скарг: {len(sat)} ({sat[0]['on']})")

    if not competitive and big:
        add("DIRECT_AWARD_HIGH_VALUE", 1, f"Неконкурентна процедура на {est:,.0f} грн")
    if (r["mtype"] or "").startswith("negotiation") and not r["cause_desc"]:
        add("NEGOTIATION_NO_JUSTIFICATION", 2, "Переговорна процедура без обґрунтування")
    return out


# ---------------------------------------------------------------- детектори: закономірності за період
def aggregate_flags(con, days: int):
    since = (now() - timedelta(days=days)).isoformat()
    rows = [dict(r) for r in con.execute(
        "SELECT * FROM tenders WHERE COALESCE(published, modified) >= ?", (since,))]
    flags = {}

    def add(tid, code, w, detail):
        flags.setdefault((tid, code), (tid, code, w, detail))

    # 1) можливий поділ закупівлі: кілька допорогових тендерів одного замовника/групи CPV, що разом перевищують поріг
    groups = defaultdict(list)
    for r in rows:
        thr = threshold(r)
        if r.get("buyer_kind") == "special":    # окремі сфери (обленерго тощо): поділ не перевіряємо
            continue
        if (thr and r["currency"] == "UAH" and r["est"] and r["buyer_id"] and r["cpv"] and r["published"]
                and CFG["min_value_uah"] <= r["est"] < thr):
            groups[(r["buyer_id"], r["category"], r["cpv"][:5], thr)].append(r)
    win = timedelta(days=CFG["split_window_days"])
    for (_, cat, cpv5, thr), g in groups.items():
        g.sort(key=lambda x: x["published"])
        wins, lo = [], 0
        for hi in range(len(g)):
            while ts(g[hi]["published"]) - ts(g[lo]["published"]) > win:
                lo += 1
            wins.append(g[lo:hi + 1])
        if max(len(w) for w in wins) > CFG["split_max_count"]:   # десятки за місяць: звичайна робота, не поділ
            continue
        for w in wins:
            if len(w) >= CFG["split_min_count"] and sum(x["est"] for x in w) >= thr:
                for x in w:
                    add(x["id"], "POSSIBLE_SPLIT_PURCHASE", 2,
                        f"{len(w)} допорогових тендерів за CPV {cpv5}* протягом {CFG['split_window_days']} дн., "
                        f"разом {sum(y['est'] for y in w):,.0f} грн (поріг {thr:,.0f})")

    competitive_done = [r for r in rows if r["method"] != "limited" and r["status"] in DONE]

    # 2) один постачальник домінує в конкурентних перемогах замовника
    by_buyer = defaultdict(list)
    for r in competitive_done:
        if r["supplier_id"] and r["buyer_id"] and r["award_amt"]:
            by_buyer[r["buyer_id"]].append(r)
    for g in by_buyer.values():
        if len(g) < CFG["concentration_min_contracts"]:
            continue
        per = defaultdict(float)
        for x in g:
            per[x["supplier_id"]] += x["award_amt"]
        sid, val = max(per.items(), key=lambda kv: kv[1])
        share = val / sum(per.values())
        if share >= CFG["concentration_share"]:
            for x in g:
                if x["supplier_id"] == sid:
                    add(x["id"], "SUPPLIER_CONCENTRATION", 2,
                        f"{x['supplier']} має {share:.0%} вартості конкурентних перемог цього замовника ({len(g)} договорів)")

    # 3) співвідношення ціна/очікувана вартість далеке від типового для розділу CPV (робастний z-показник)
    by_cpv = defaultdict(list)
    for r in competitive_done:
        if r["currency"] == "UAH" and r["est"] and r["award_amt"] and r["cpv"]:
            by_cpv[r["cpv"][:2]].append((r["award_amt"] / r["est"], r))
    for div, g in by_cpv.items():
        if len(g) < CFG["ratio_min_group"]:
            continue
        xs = [x for x, _ in g]
        med = statistics.median(xs)
        mad = statistics.median(abs(x - med) for x in xs)
        if mad == 0:
            continue
        for x, r in g:
            z = 0.6745 * (x - med) / mad
            if abs(z) >= CFG["ratio_z"]:
                add(r["id"], "RATIO_OUTLIER", 1,
                    f"Ціна/очікувана {x:.0%} проти медіани {med:.0%} для CPV {div}* (z={z:+.1f})")

    # 3a) ціна за одиницю значно вища, ніж у інших замовників за той самий товар (той самий CPV + одиниця + схожий опис)
    stop = {"або", "еквівалент", "код", "дк", "021", "2015", "для", "згідно", "шт", "товар"}

    def toks(x):
        return {w for w in re.findall(r"[a-zа-яіїєґ0-9]+", (x or "").lower().replace("’", "").replace("'", ""))
                if (len(w) >= 3 or w.isdigit()) and w not in stop}
    pg = defaultdict(list)
    for r in rows:
        if r.get("status") not in DONE or r.get("currency") != "UAH":
            continue
        for u in json.loads(r.get("unit_prices") or "[]"):
            pg[(u["cpv"], u["unit"])].append((r, u, toks(u["descr"])))
    for lst in pg.values():
        if len(lst) < 4:
            continue
        for r, u, tk in lst:
            if len(tk) < 2:
                continue
            peers = [p for p in lst if p[0]["buyer_id"] != r["buyer_id"] and p[2]
                     and len(tk & p[2]) / len(tk | p[2]) >= 0.5]
            if len(peers) < CFG["price_min_peers"]:
                continue
            med = statistics.median(p[1]["price"] for p in peers)
            ratio, excess = u["price"] / med, (u["price"] - med) * u["qty"]
            # дуже великі коефіцієнти — майже завжди помилки в одиницях/фасуванні, а не завищення ціни
            if CFG["price_ratio"] <= ratio <= CFG["price_ratio_max"] and excess >= CFG["price_min_excess_uah"]:
                add(r["id"], "PRICE_ABOVE_PEERS", 3,
                    f"{u['descr'][:60]}: {u['price']:,.2f} грн/{u['unit_name']} проти медіани {med:,.2f} у "
                    f"{len(peers)} інших замовників (x{ratio:.1f}); ~{excess:,.0f} грн понад медіану")

    # 3b) той самий дешевший учасник неодноразово відмовляється на користь того самого переможця
    pairs = defaultdict(list)
    for r in rows:
        for e in json.loads(r.get("elim") or "[]"):
            if e["kind"] == "withdrew" and e["id"] and e["winner_id"]:
                pairs[(e["id"], e["winner_id"])].append((r, e))
    for (rid, wid), lst in pairs.items():
        tids = {x["id"] for x, _ in lst}
        buyers = defaultdict(set)
        for x, _ in lst:
            buyers[x["buyer_id"]].add(x["id"])
        if len(tids) >= 3 or max(len(v) for v in buyers.values()) >= 2:
            e0 = lst[0][1]
            extra = sum(e["winner_price"] - e["price"] for _, e in lst)
            for x, _ in lst:
                add(x["id"], "REPEAT_WITHDRAWAL_PAIR", 3,
                    f"{e0['name']} ({rid}) поступився {e0['winner']} ({wid}) у {len(tids)} тендерах у "
                    f"{len(buyers)} замовника(ів); переплата {extra:,.0f} грн")

    # 4) звички замовника (частка тендерів з однією пропозицією порівняно з загальною для тих самих типів процедур)
    pop = [r for r in competitive_done if r["n_bids"] is not None]
    base = defaultdict(lambda: [0, 0])
    for r in pop:
        base[r["mtype"]][0] += r["n_bids"] == 1
        base[r["mtype"]][1] += 1
    rate = {k: s / n for k, (s, n) in base.items()}
    pop_rate = (sum(r["n_bids"] == 1 for r in pop) / len(pop)) if pop else 0.5
    stats = defaultdict(lambda: dict(n=0, single=0, lim_val=0.0, all_val=0.0, lim_n=0, all_n=0, name=""))
    for r in rows:
        if not r["buyer_id"] or r["status"] not in DECIDED | {"unsuccessful"}:
            continue
        b = stats[r["buyer_id"]]
        b["name"] = r["buyer"]
        v = r["est"] or 0
        b["all_n"] += 1
        b["all_val"] += v
        if r["method"] == "limited":
            b["lim_n"] += 1
            b["lim_val"] += v
        elif r["n_bids"] is not None:
            b["n"] += 1
            b["single"] += r["n_bids"] == 1
            b["expected"] = b.get("expected", 0) + rate.get(r["mtype"], pop_rate)
    for bid, b in stats.items():
        m = CFG["buyer_min_tenders"]
        exp = b.get("expected", 0)
        if b["n"] >= m and b["single"] / b["n"] >= 0.8 and b["single"] >= 1.3 * exp:
            add(f"buyer:{bid}", "BUYER_HIGH_SINGLE_BID_RATE", 2,
                f"{b['name']}: {b['single']}/{b['n']} конкурентних тендерів мали одну пропозицію "
                f"(очікувано близько {exp:.0f} для такого набору процедур)")
        if b["all_n"] >= m and b["all_val"] and b["lim_val"] / b["all_val"] >= 0.5:
            add(f"buyer:{bid}", "BUYER_HIGH_DIRECT_AWARD_SHARE", 1,
                f"{b['name']}: {b['lim_val'] / b['all_val']:.0%} вартості тендерів — через неконкурентні процедури")
    return list(flags.values())


# ---------------------------------------------------------------- звіти
def cell(x, n=60):
    return (str(x or "-").replace("|", "/").replace("\n", " "))[:n]


def build_report(con, since_iso: str):
    rows = con.execute("SELECT * FROM flags WHERE first_seen >= ?", (since_iso,)).fetchall()
    per, buyer_rows = defaultdict(list), []
    for f in rows:
        (buyer_rows if f["tender_id"].startswith("buyer:") else per[f["tender_id"]]).append(f)
    items = []
    for tid, fl in per.items():
        t = con.execute("SELECT * FROM tenders WHERE id=?", (tid,)).fetchone()
        if t:
            items.append((sum(f["weight"] for f in fl), dict(t), fl))
    items.sort(key=lambda x: (-x[0], -(x[1]["est"] or 0)))
    top = [i for i in items if i[0] >= CFG["report_min_score"]]

    os.makedirs(CFG["out_dir"], exist_ok=True)
    day = f"{now():%Y-%m-%d_%H%M%S}"
    csv_path = os.path.join(CFG["out_dir"], f"anomalies_{day}.csv")
    md_path = os.path.join(CFG["out_dir"], f"anomalies_{day}.md")

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(["бали", "id_тендера", "посилання", "замовник", "єдрпоу_замовника", "постачальник", "єдрпоу_постачальника", "очікувана_грн",
                    "сума_рішення_грн", "пропозицій", "процедура", "статус", "регіон", "військовий", "чому_військовий", "прапорці", "деталі"])
        for score, t, fl in items:
            w.writerow([score, t["tender_id"], PORTAL + str(t["tender_id"]), t["buyer"], t["buyer_id"],
                        t["supplier"], t["supplier_id"], t["est"], t["award_amt"], t["n_bids"], t["mtype"],
                        t["status"], t["region"], t.get("military"), t.get("mil_reason"), ";".join(f["code"] for f in fl), " | ".join(f["detail"] for f in fl)])

    lines = [f"# Звіт про аномалії Prozorro, {day}", "",
             f"Тендерів з новими прапорцями: {len(items)}; з балом >= {CFG['report_min_score']}: {len(top)}. "
             "Прапорці — індикатори ризику для перевірки, а не доказ порушень.", "",
             "## Тендери з найвищим ризиком", "",
             "| Бали | Військ. | Тендер | Замовник | Постачальник | Очікувана (грн) | Прапорці |", "|---|---|---|---|---|---|---|"]
    for score, t, fl in top[:50]:
        lines.append(f"| {score} | {'ВІЙСЬК' if t.get('military') == 1 else 'БЕЗП' if t.get('military') == 2 else ''} | [{t['tender_id']}]({PORTAL}{t['tender_id']}) | {cell(t['buyer'])} | "
                     f"{cell(t['supplier'])} | {(t['est'] or 0):,.0f} | {', '.join(f['code'] for f in fl)} |")
    mil = [i for i in top if i[1].get("military") == 1]
    if mil:
        lines += ["", f"## Військові / оборонні закупівлі ({len(mil)} з балом >= {CFG['report_min_score']})", "",
                  "| Бали | Тендер | Замовник | Чому військова | Очікувана (грн) | Прапорці |", "|---|---|---|---|---|---|"]
        for score, t, fl in mil[:30]:
            lines.append(f"| {score} | [{t['tender_id']}]({PORTAL}{t['tender_id']}) | {cell(t['buyer'])} | "
                         f"{t['mil_reason']} | {(t['est'] or 0):,.0f} | {', '.join(f['code'] for f in fl)} |")
    if buyer_rows:
        lines += ["", "## Закономірності на рівні замовника", ""]
        lines += [f"- **{f['code']}**: {f['detail']}" for f in buyer_rows]
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return top, buyer_rows, md_path, csv_path


def notify(top, buyer_rows, md_path):
    if not CFG["webhook_url"] or not (top or buyer_rows):
        return
    text = [f"Prozorro: тендерів високого ризику — {len(top)}, закономірностей замовників — {len(buyer_rows)} ({md_path})"]
    for score, t, fl in top[:10]:
        text.append(f"- [{score}] {PORTAL}{t['tender_id']} {cell(t['buyer'], 40)}: {', '.join(f['code'] for f in fl)}")
    try:
        requests.post(CFG["webhook_url"], json={"text": "\n".join(text)}, timeout=20)
    except requests.RequestException as e:
        log(f"помилка вебхука: {e}")


# ---------------------------------------------------------------- команди
def cmd_run(args):
    con = db()
    s = make_session()
    started = now()
    stamp = started.isoformat()
    wm = ts(meta_get(con, "watermark")) or ts(args.since) or (started - timedelta(days=1))
    log(f"збираю тендери, змінені після {wm.isoformat()}")

    def work(row):
        try:
            t = fetch_tender(s, row["id"])
            return flatten(t) if t else None
        except Exception as e:  # продовжуємо; тендер знову з'явиться в стрічці, якщо його змінять
            log(f"пропущено {row['id']}: {e}")
            return None

    feed = islice(iter_feed_asc(s, wm), CFG["max_tenders_per_run"])
    total, new = 0, []
    with ThreadPoolExecutor(CFG["workers"]) as ex:
        while True:
            batch = list(islice(feed, 500))
            if not batch:
                break
            recs = [r for r in ex.map(work, batch) if r]
            upsert(con, recs)
            new += save_flags(con, [f for r in recs for f in tender_flags(r)], stamp)
            meta_set(con, "watermark", batch[-1]["dateModified"])  # прогрес зберігається після кожної партії
            total += len(recs)
            log(f"збережено тендерів: {total} (до {batch[-1]['dateModified']})")
    if not total:
        log("нічого нового")
        return
    if total >= CFG["max_tenders_per_run"]:
        log("досягнуто max_tenders_per_run; наступний запуск продовжить звідси")

    new += save_flags(con, aggregate_flags(con, days=args.window), stamp)
    log(f"нових прапорців: {len(new)}")
    if not new:
        return
    top, buyers, md, cs = build_report(con, stamp)
    notify(top, buyers, md)
    log(f"звіт: {md} | {cs}")


def cmd_probe(args):
    """Завантажує N найновіших тендерів і показує, які поля парсер справді знайшов."""
    s = make_session()
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    feed = list(islice(iter_feed_desc(s, epoch, limit=min(args.n, 100)), args.n))
    raw, recs = [], []

    def work(row):
        try:
            t = fetch_tender(s, row["id"])
            return (t, flatten(t)) if t else None
        except Exception as e:
            log(f"пропущено {row['id']}: {e}")
            return None

    with ThreadPoolExecutor(CFG["workers"]) as ex:
        for res in ex.map(work, feed):
            if res:
                raw.append(res[0])
                recs.append(res[1])
    if not recs:
        log("не завантажено жодного тендера; перевірте доступ до public-api.prozorro.gov.ua")
        return
    with open("probe_sample.json", "w", encoding="utf-8") as fh:
        json.dump(raw[:3], fh, ensure_ascii=False, indent=1)
    print(f"\nЗавантажено тендерів: {len(recs)} (найновіші першими). Заповненість полів:")
    for k in ("status", "method", "mtype", "category", "currency", "est", "n_bids", "buyer_id", "cpv",
              "period_days", "published", "supplier_id", "award_amt", "contract_amt"):
        n = sum(r[k] not in (None, "") for r in recs)
        print(f"  {k:13} {n:>4}/{len(recs)}")
    print("\nСтатуси:", dict(sorted(((st, sum(r['status'] == st for r in recs)) for st in {r['status'] for r in recs}))))
    flagged = [(r, tender_flags(r)) for r in recs]
    flagged = [(r, f) for r, f in flagged if f]
    print(f"\n{len(flagged)} з {len(recs)} тендерів мають щонайменше один прапорець окремого тендера:")
    for r, f in flagged[:15]:
        print(f"  {PORTAL}{r['tender_id']}  " + ", ".join(c for _, c, _, _ in f))
    print("\nПоле з нульовою заповненістю означає, що API називає його інакше. Надішліть probe_sample.json, щоб підлаштувати парсер.")


def cmd_prune(args):
    """Тримає БД малою для кешу CI: видаляє тендери, не змінені N днів, якщо їх нещодавно не позначено."""
    con = db()
    cut = (now() - timedelta(days=args.days)).isoformat()
    keep = (now() - timedelta(days=args.keep_flagged_days)).isoformat()
    n = con.execute("""DELETE FROM tenders WHERE COALESCE(modified, published) < ?
                       AND id NOT IN (SELECT tender_id FROM flags WHERE first_seen >= ?)""", (cut, keep)).rowcount
    m = con.execute("DELETE FROM flags WHERE first_seen < ? AND tender_id NOT IN (SELECT id FROM tenders)",
                    (cut,)).rowcount
    con.commit()
    con.execute("VACUUM")
    log(f"видалено тендерів: {n}, прапорців: {m} (старші за {args.days} дн.)")


def cmd_report(args):
    con = db()
    since = (now() - timedelta(days=args.days)).isoformat()
    _, _, md, cs = build_report(con, since)
    log(f"звіт: {md} | {cs}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", help="шлях до SQLite (типово prozorro.db); для проб використовуйте тимчасовий файл")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="завантажити нові тендери, знайти аномалії, записати звіт")
    r.add_argument("--since", help="ISO-дата, з якої дозавантажити під час першого запуску (типово: 1 день тому)")
    r.add_argument("--window", type=int, default=90, help="скільки днів історії використовують детектори закономірностей")
    r.add_argument("--limit", type=int, help="максимум тендерів за цей запуск")
    r.set_defaults(fn=cmd_run)
    pr = sub.add_parser("probe", help="завантажити найновіші тендери й перевірити заповненість полів")
    pr.add_argument("--n", type=int, default=30)
    pr.set_defaults(fn=cmd_probe)
    pz = sub.add_parser("prune", help="видалити старі тендери, щоб база лишалася малою")
    pz.add_argument("--days", type=int, default=45)
    pz.add_argument("--keep-flagged-days", type=int, default=30)
    pz.set_defaults(fn=cmd_prune)
    q = sub.add_parser("report", help="перебудувати звіт із прапорців, що вже є в базі")
    q.add_argument("--days", type=int, default=7)
    q.set_defaults(fn=cmd_report)
    args = p.parse_args()
    if args.db:
        CFG["db_path"] = args.db
    if getattr(args, "limit", None):
        CFG["max_tenders_per_run"] = args.limit
    args.fn(args)


if __name__ == "__main__":
    main()
