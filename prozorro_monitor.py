#!/usr/bin/env python3
"""Prozorro anomaly monitor: fetch -> store (SQLite) -> detect red flags -> report.

Setup:      pip install requests
First run:  python prozorro_monitor.py run --since 2026-11-01     (back-fill from a date)
Scheduled:  python prozorro_monitor.py run                        (incremental; cron / Task Scheduler)
Re-report:  python prozorro_monitor.py report --days 7
Live check: python prozorro_monitor.py probe --n 30               (fetches the newest tenders, shows which
            fields were found, writes probe_sample.json; run this FIRST against the real API)
Trial run:  python prozorro_monitor.py --db trial.db run --since 2026-11-05 --limit 500

Outputs (in ./reports): anomalies_<date>.md and anomalies_<date>.csv
Optional:   set PROZORRO_WEBHOOK_URL (Slack/Mattermost-style incoming webhook) for a short alert.
Tuning:     put any key from CFG below into config.json to override it.

Flags are risk INDICATORS for human review, not proof of wrongdoing.
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
    "workers": 12,                      # parallel detail requests; be polite
    "max_tenders_per_run": 20000,
    "min_value_uah": 100_000,          # ignore tiny tenders
    "high_value_uah": 5_000_000,
    # minimum normal bidding window (days) per procurementMethodType; types not listed are not checked.
    # VERIFY against current law / martial-law rules (short-form procedures legitimately run 2-5 days).
    "min_bid_period_days": {"aboveThreshold": 7, "aboveThresholdUA": 7, "aboveThresholdEU": 7,
                            "competitiveDialogueUA": 7, "competitiveDialogueEU": 7, "esco": 7,
                            "belowThreshold": 2, "priceQuotation": 2, "requestForProposal": 2},
    # VERIFY against current Ukrainian procurement law / martial-law rules before relying on these:
    "thresholds_uah": {"goods": 200_000, "services": 200_000, "works": 1_500_000},
    "threshold_margin": 0.03,          # "just below" = within 3% under a threshold
    "split_window_days": 30,
    "split_min_count": 3,
    "concentration_min_contracts": 5,
    "concentration_share": 0.60,       # one supplier takes >= 60% of a buyer's competitive award value
    "ratio_min_group": 30,
    "ratio_z": 3.5,                    # robust z-score cut-off for award/estimate ratio
    "buyer_min_tenders": 10,
    "report_min_score": 3,
    "price_min_peers": 3,               # unit-price check: need this many comparable purchases by other buyers
    "price_ratio": 1.8,                 # flag when unit price >= 1.8x the peer median ...
    "price_ratio_max": 15,              # ... but ignore >15x (unit/packaging errors)
    "price_min_excess_uah": 200_000,
    "webhook_url": os.getenv("PROZORRO_WEBHOOK_URL", ""),
}
if os.path.exists("config.json"):
    with open("config.json", encoding="utf-8") as fh:
        CFG.update(json.load(fh))

DONE = {"complete", "active.awarded"}  # a winner exists
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


# ---------------------------------------------------------------- helpers
def now() -> datetime:
    return datetime.now(timezone.utc)


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


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


# ---------------------------------------------------------------- fetching
def make_session():
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET",))
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=CFG["workers"] * 2))
    s.headers["User-Agent"] = "prozorro-anomaly-monitor/0.1"
    return s


def iter_feed_desc(s, stop_at: datetime, limit=100):
    """Walk the tender feed newest -> oldest until we reach `stop_at` (the previous run's watermark)."""
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
    """Walk the tender feed oldest -> newest from `start`, so a capped or interrupted run resumes cleanly."""
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
MIL_CPV = ("353", "354", "355", "356", "357", "358")   # weapons/ammo, military vehicles, ships, aircraft, electronics, kit
WITHDREW = re.compile(r"відмовився|не підписав|не уклав|відмов\w* від (укладення|підписання)", re.I)


def military_class(name, kind, cpvs):
    """1 = military (armed forces, NGU, border guard, MoD bodies, military CPV); 2 = other security / law enforcement."""
    name = (name or "").replace("­", "")
    if SEC_NAME.search(name) and not re.search(r"військов|національн\w* гвард", name, re.I):
        return 2, "security/law-enforcement buyer"
    if MIL_NAME.search(name):
        return 1, "military buyer (name)"
    hit = next((c for c in cpvs if c and c[:3] in MIL_CPV), None)
    if hit:
        return 1, f"military CPV {hit}"
    if kind == "defense":
        return 2, "security buyer (kind=defense)"
    return 0, ""


def military_tag(t, cpvs):
    pe = t.get("procuringEntity") or {}
    return military_class(pe.get("name"), pe.get("kind"), cpvs)


def _party(p):
    return (p.get("identifier") or {}).get("id"), p.get("name")


def eliminations(t):
    """Per lot: cheaper bidders that were rejected or walked away while a pricier bid won."""
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
    """Different bidders (by EDRPOU) in one tender sharing a phone, e-mail, contact person or street address."""
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
            keys = [("phone", ph if len(ph) == 9 else ""), ("email", (cp.get("email") or "").strip().lower()),
                    ("person", re.sub(r"\s+", " ", (cp.get("name") or "").strip().lower())),
                    ("address", re.sub(r"\W+", "", ((ad.get("streetAddress") or "") + (ad.get("locality") or "")).lower())
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
        on = "tender" if a is t else f"award:{a.get('status')}:{_party((a.get('suppliers') or [{}])[0])[1]}"
        for c in a.get("complaints") or []:
            out.append(dict(type=c.get("type"), status=c.get("status"), on=on, date=c.get("dateDecision") or c.get("date")))
    return out


GENERIC = re.compile(r"асортимент|різн(і|их)\b|запчастин|запасн\w* частин|комплектуюч|матеріал\w* для|витратн", re.I)
NON_UNIT = re.compile(r"послуг|робот|лот|умовн|об.?єкт|місяц|рік|год|день|доба|комплекс|систем|пакет|набір|комплект", re.I)


def unit_prices(t):
    """Net unit price per awarded lot that holds a single product (comparable across buyers)."""
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
    """Reduce a full tender document to the flat fields the detectors need."""
    pe = t.get("procuringEntity") or {}
    bids = t.get("bids") or []
    nb = t.get("numberOfBids")
    n_bids = int(nb) if str(nb).isdigit() else (len(bids) if bids else None)
    awards = t.get("awards") or []
    active = [a for a in awards if a.get("status") == "active"]
    win = active[-1] if active else {}
    sup = (win.get("suppliers") or [{}])[0]
    # sum over all active awards (multi-lot tenders) and compare contracts on the same VAT basis as awards
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


# ---------------------------------------------------------------- storage
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
    """Store flags; return only those never seen before (so alerts don't repeat)."""
    new = []
    for tid, code, w, detail in flags:
        cur = con.execute("INSERT OR IGNORE INTO flags VALUES (?,?,?,?,?)", (tid, code, w, detail, stamp))
        if cur.rowcount:
            new.append((tid, code, w, detail))
    con.commit()
    return new


# ---------------------------------------------------------------- detectors: single tender
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
        add("SINGLE_BIDDER", 3 if big else 2, f"Only 1 bid on a {est:,.0f} UAH competitive tender")

    d, mind = r["period_days"], CFG["min_bid_period_days"].get(r["mtype"])
    if competitive and d is not None and mind and d < mind - 0.05:
        add("SHORT_BID_PERIOD", 2, f"Bidding window only {d:.1f} days (normal minimum {mind} for {r['mtype']})")

    thr = CFG["thresholds_uah"].get(r["category"])
    if thr and thr * (1 - CFG["threshold_margin"]) <= est < thr:
        add("JUST_BELOW_THRESHOLD", 1, f"Estimate {est:,.0f} sits just under the {thr:,.0f} UAH threshold")

    if r["award_amt"]:
        ratio = r["award_amt"] / est
        if competitive and 0.9999 <= ratio <= 1.0:
            add("BID_AT_ESTIMATE_EXACT", 2, f"Winning price {r['award_amt']:,.0f} is {est - r['award_amt']:,.0f} UAH "
                                            f"under the {est:,.0f} estimate ({ratio:.3%})")
        elif competitive and ratio >= 0.99 and nb is not None and nb <= 2:
            add("AWARD_AT_ESTIMATE", 1, f"Award is {ratio:.1%} of estimate with only {nb} bid(s)")
        if ratio <= 0.5:
            add("VERY_LOW_AWARD", 1, f"Award is only {ratio:.0%} of estimate")
        if r["contract_amt"] and r["contract_amt"] > r["award_amt"] * 1.005:
            add("CONTRACT_ABOVE_AWARD", 3,
                f"Contract {r['contract_amt']:,.0f} exceeds award {r['award_amt']:,.0f} UAH")

    gap, gp = r.get("elim_gap"), r.get("elim_gap_pct")
    if competitive and gap:
        w = 2 + (1 if (gp or 0) >= 0.2 or gap >= 1_000_000 else 0)
        what = {"withdrew": "walked away / refused to sign", "rejected": "were disqualified",
                "mixed": "were disqualified or walked away"}[r["elim_kind"]]
        add("CHEAPER_BIDS_WITHDREW" if r["elim_kind"] == "withdrew" else "CHEAPER_BIDS_REJECTED", w,
            f"Cheaper bidders {what}; winner paid {gap:,.0f} UAH (+{gp:.0%}) over the cheapest of them")
    elif competitive and nb and nb >= 2 and r["n_unsuccessful"] >= nb - 1 and r["supplier_id"]:
        add("COMPETITORS_ELIMINATED", 1, f"{r['n_unsuccessful']} of {nb} bidders disqualified; winner left by elimination")
    if r.get("shared_contacts"):
        sc = json.loads(r["shared_contacts"])
        add("SHARED_BIDDER_CONTACTS", 3, "; ".join(
            f"{h['field']} shared by " + " & ".join(n or i for i, n in h["bidders"]) for h in sc)[:400])
    if r.get("complaints"):
        sat = [c for c in json.loads(r["complaints"]) if c["type"] == "complaint" and c["status"] == "satisfied"]
        if sat:
            add("AMCU_COMPLAINT_UPHELD", 2, f"{len(sat)} AMCU complaint(s) upheld ({sat[0]['on']})")

    if not competitive and big:
        add("DIRECT_AWARD_HIGH_VALUE", 1, f"Non-competitive procedure for {est:,.0f} UAH")
    if (r["mtype"] or "").startswith("negotiation") and not r["cause_desc"]:
        add("NEGOTIATION_NO_JUSTIFICATION", 2, "Negotiated procedure without a cause description")
    return out


# ---------------------------------------------------------------- detectors: patterns over a window
def aggregate_flags(con, days: int):
    since = (now() - timedelta(days=days)).isoformat()
    rows = [dict(r) for r in con.execute(
        "SELECT * FROM tenders WHERE COALESCE(published, modified) >= ?", (since,))]
    flags = {}

    def add(tid, code, w, detail):
        flags.setdefault((tid, code), (tid, code, w, detail))

    # 1) possible split purchases: several sub-threshold tenders by one buyer/CPV group that sum past the threshold
    groups = defaultdict(list)
    for r in rows:
        thr = CFG["thresholds_uah"].get(r["category"])
        if (thr and r["currency"] == "UAH" and r["est"] and r["buyer_id"] and r["cpv"] and r["published"]
                and CFG["min_value_uah"] <= r["est"] < thr):
            groups[(r["buyer_id"], r["category"], r["cpv"][:5])].append(r)
    win = timedelta(days=CFG["split_window_days"])
    for (_, cat, cpv5), g in groups.items():
        thr = CFG["thresholds_uah"][cat]
        g.sort(key=lambda x: x["published"])
        lo = 0
        for hi in range(len(g)):
            while ts(g[hi]["published"]) - ts(g[lo]["published"]) > win:
                lo += 1
            w = g[lo:hi + 1]
            if len(w) >= CFG["split_min_count"] and sum(x["est"] for x in w) >= thr:
                for x in w:
                    add(x["id"], "POSSIBLE_SPLIT_PURCHASE", 2,
                        f"{len(w)} sub-threshold tenders in CPV {cpv5}* within {CFG['split_window_days']}d "
                        f"total {sum(y['est'] for y in w):,.0f} UAH (threshold {thr:,.0f})")

    competitive_done = [r for r in rows if r["method"] != "limited" and r["status"] in DONE]

    # 2) one supplier dominating a buyer's competitive awards
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
                        f"{x['supplier']} holds {share:.0%} of this buyer's competitive award value ({len(g)} contracts)")

    # 3) award/estimate ratio far from what is normal for the CPV division (robust z-score)
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
                    f"Award/estimate {x:.0%} vs CPV {div}* median {med:.0%} (z={z:+.1f})")

    # 3a) unit price far above what other buyers paid for the same product (same CPV + unit + similar description)
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
            # very large ratios are almost always unit/packaging mistakes, not overpricing
            if CFG["price_ratio"] <= ratio <= CFG["price_ratio_max"] and excess >= CFG["price_min_excess_uah"]:
                add(r["id"], "PRICE_ABOVE_PEERS", 3,
                    f"{u['descr'][:60]}: {u['price']:,.2f} UAH/{u['unit_name']} vs median {med:,.2f} at "
                    f"{len(peers)} other buyers (x{ratio:.1f}); ~{excess:,.0f} UAH above median")

    # 3b) the same cheaper bidder repeatedly walking away in favour of the same winner
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
                    f"{e0['name']} ({rid}) stepped aside for {e0['winner']} ({wid}) in {len(tids)} tenders at "
                    f"{len(buyers)} buyer(s); extra cost {extra:,.0f} UAH")

    # 4) buyer-level habits (single-bid rate compared with the national rate for the same procedure types)
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
                f"{b['name']}: {b['single']}/{b['n']} competitive tenders had a single bid "
                f"(expected about {exp:.0f} for this procedure mix)")
        if b["all_n"] >= m and b["all_val"] and b["lim_val"] / b["all_val"] >= 0.5:
            add(f"buyer:{bid}", "BUYER_HIGH_DIRECT_AWARD_SHARE", 1,
                f"{b['name']}: {b['lim_val'] / b['all_val']:.0%} of tender value via non-competitive procedures")
    return list(flags.values())


# ---------------------------------------------------------------- reporting
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
        w.writerow(["score", "tender_id", "url", "buyer", "buyer_id", "supplier", "supplier_id", "estimate_uah",
                    "award_uah", "bids", "method", "status", "region", "military", "military_reason", "flags", "details"])
        for score, t, fl in items:
            w.writerow([score, t["tender_id"], PORTAL + str(t["tender_id"]), t["buyer"], t["buyer_id"],
                        t["supplier"], t["supplier_id"], t["est"], t["award_amt"], t["n_bids"], t["mtype"],
                        t["status"], t["region"], t.get("military"), t.get("mil_reason"), ";".join(f["code"] for f in fl), " | ".join(f["detail"] for f in fl)])

    lines = [f"# Prozorro anomaly report, {day}", "",
             f"{len(items)} tenders with new flags; {len(top)} at score >= {CFG['report_min_score']}. "
             "Flags are risk indicators for review, not proof of wrongdoing.", "",
             "## Highest-risk tenders", "",
             "| Score | Mil | Tender | Buyer | Supplier | Estimate (UAH) | Flags |", "|---|---|---|---|---|---|---|"]
    for score, t, fl in top[:50]:
        lines.append(f"| {score} | {'MIL' if t.get('military') == 1 else 'SEC' if t.get('military') == 2 else ''} | [{t['tender_id']}]({PORTAL}{t['tender_id']}) | {cell(t['buyer'])} | "
                     f"{cell(t['supplier'])} | {(t['est'] or 0):,.0f} | {', '.join(f['code'] for f in fl)} |")
    mil = [i for i in top if i[1].get("military") == 1]
    if mil:
        lines += ["", f"## Military / defence purchases ({len(mil)} at score >= {CFG['report_min_score']})", "",
                  "| Score | Tender | Buyer | Why military | Estimate (UAH) | Flags |", "|---|---|---|---|---|---|"]
        for score, t, fl in mil[:30]:
            lines.append(f"| {score} | [{t['tender_id']}]({PORTAL}{t['tender_id']}) | {cell(t['buyer'])} | "
                         f"{t['mil_reason']} | {(t['est'] or 0):,.0f} | {', '.join(f['code'] for f in fl)} |")
    if buyer_rows:
        lines += ["", "## Buyer-level patterns", ""]
        lines += [f"- **{f['code']}**: {f['detail']}" for f in buyer_rows]
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return top, buyer_rows, md_path, csv_path


def notify(top, buyer_rows, md_path):
    if not CFG["webhook_url"] or not (top or buyer_rows):
        return
    text = [f"Prozorro: {len(top)} high-risk tenders, {len(buyer_rows)} buyer patterns ({md_path})"]
    for score, t, fl in top[:10]:
        text.append(f"- [{score}] {PORTAL}{t['tender_id']} {cell(t['buyer'], 40)}: {', '.join(f['code'] for f in fl)}")
    try:
        requests.post(CFG["webhook_url"], json={"text": "\n".join(text)}, timeout=20)
    except requests.RequestException as e:
        log(f"webhook failed: {e}")


# ---------------------------------------------------------------- commands
def cmd_run(args):
    con = db()
    s = make_session()
    started = now()
    stamp = started.isoformat()
    wm = ts(meta_get(con, "watermark")) or ts(args.since) or (started - timedelta(days=1))
    log(f"collecting tenders modified after {wm.isoformat()}")

    def work(row):
        try:
            t = fetch_tender(s, row["id"])
            return flatten(t) if t else None
        except Exception as e:  # keep going; the tender reappears in the feed if modified again
            log(f"skip {row['id']}: {e}")
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
            meta_set(con, "watermark", batch[-1]["dateModified"])  # progress is saved per batch
            total += len(recs)
            log(f"stored {total} tenders (up to {batch[-1]['dateModified']})")
    if not total:
        log("nothing new")
        return
    if total >= CFG["max_tenders_per_run"]:
        log("hit max_tenders_per_run; the next run continues from here")

    new += save_flags(con, aggregate_flags(con, days=args.window), stamp)
    log(f"{len(new)} new flags")
    if not new:
        return
    top, buyers, md, cs = build_report(con, stamp)
    notify(top, buyers, md)
    log(f"report: {md} | {cs}")


def cmd_probe(args):
    """Fetch the newest N tenders and show which fields the parser actually found."""
    s = make_session()
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    feed = list(islice(iter_feed_desc(s, epoch, limit=min(args.n, 100)), args.n))
    raw, recs = [], []

    def work(row):
        try:
            t = fetch_tender(s, row["id"])
            return (t, flatten(t)) if t else None
        except Exception as e:
            log(f"skip {row['id']}: {e}")
            return None

    with ThreadPoolExecutor(CFG["workers"]) as ex:
        for res in ex.map(work, feed):
            if res:
                raw.append(res[0])
                recs.append(res[1])
    if not recs:
        log("no tenders fetched; check network access to public-api.prozorro.gov.ua")
        return
    with open("probe_sample.json", "w", encoding="utf-8") as fh:
        json.dump(raw[:3], fh, ensure_ascii=False, indent=1)
    print(f"\nFetched {len(recs)} tenders (newest first). Field coverage:")
    for k in ("status", "method", "mtype", "category", "currency", "est", "n_bids", "buyer_id", "cpv",
              "period_days", "published", "supplier_id", "award_amt", "contract_amt"):
        n = sum(r[k] not in (None, "") for r in recs)
        print(f"  {k:13} {n:>4}/{len(recs)}")
    print("\nStatuses:", dict(sorted(((st, sum(r['status'] == st for r in recs)) for st in {r['status'] for r in recs}))))
    flagged = [(r, tender_flags(r)) for r in recs]
    flagged = [(r, f) for r, f in flagged if f]
    print(f"\n{len(flagged)} of {len(recs)} tenders trip at least one single-tender flag:")
    for r, f in flagged[:15]:
        print(f"  {PORTAL}{r['tender_id']}  " + ", ".join(c for _, c, _, _ in f))
    print("\nA field at 0 coverage means the API names it differently. Share probe_sample.json to adjust the parser.")


def cmd_prune(args):
    """Keep the DB small enough for CI caches: drop tenders not modified for N days unless recently flagged."""
    con = db()
    cut = (now() - timedelta(days=args.days)).isoformat()
    keep = (now() - timedelta(days=args.keep_flagged_days)).isoformat()
    n = con.execute("""DELETE FROM tenders WHERE COALESCE(modified, published) < ?
                       AND id NOT IN (SELECT tender_id FROM flags WHERE first_seen >= ?)""", (cut, keep)).rowcount
    m = con.execute("DELETE FROM flags WHERE first_seen < ? AND tender_id NOT IN (SELECT id FROM tenders)",
                    (cut,)).rowcount
    con.commit()
    con.execute("VACUUM")
    log(f"pruned {n} tenders and {m} flags older than {args.days} days")


def cmd_report(args):
    con = db()
    since = (now() - timedelta(days=args.days)).isoformat()
    _, _, md, cs = build_report(con, since)
    log(f"report: {md} | {cs}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", help="SQLite path (default prozorro.db); use a throwaway file for trials")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="fetch new tenders, detect anomalies, write report")
    r.add_argument("--since", help="ISO date to back-fill from on the very first run (default: 1 day ago)")
    r.add_argument("--window", type=int, default=90, help="days of history used for pattern detectors")
    r.add_argument("--limit", type=int, help="cap tenders fetched this run (trial runs only)")
    r.set_defaults(fn=cmd_run)
    pr = sub.add_parser("probe", help="fetch the newest tenders and check parser field coverage")
    pr.add_argument("--n", type=int, default=30)
    pr.set_defaults(fn=cmd_probe)
    pz = sub.add_parser("prune", help="delete old tenders to keep the database small")
    pz.add_argument("--days", type=int, default=45)
    pz.add_argument("--keep-flagged-days", type=int, default=30)
    pz.set_defaults(fn=cmd_prune)
    q = sub.add_parser("report", help="rebuild the report from flags already in the database")
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
