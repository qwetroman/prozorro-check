"""Вибір кандидатів у чернетки з бази монітора."""
from __future__ import annotations

import re
from datetime import datetime, timedelta

from .common import CFG, now
from .texts import subject

STRONG = {"PRICE_ABOVE_PEERS", "CHEAPER_BIDS_WITHDREW", "CHEAPER_BIDS_REJECTED", "REPEAT_WITHDRAWAL_PAIR",
          "SHARED_BIDDER_CONTACTS", "AMCU_COMPLAINT_UPHELD", "BID_AT_ESTIMATE_EXACT",
          "SUPPLIER_NO_HISTORY", "SUPPLIER_NEW_IN_DIVISION"}   # останні два ставить prozorro_history.py
# SUPPLIER_NEW_IN_CATEGORY (нова лише група CPV) - допоміжний: сам по собі чернетку не створює


def price_info(detail):
    m = re.search(r": ([\d,\.]+) грн/(.+?) проти медіани ([\d,\.]+) у (\d+) інших замовників \(x([\d\.]+)\); ~([\d,]+) грн",
                  detail or "")
    if not m:
        return None
    f = lambda x: float(x.replace(",", ""))  # noqa: E731
    return dict(price=f(m.group(1)), unit=m.group(2), med=f(m.group(3)), n=int(m.group(4)),
                ratio=float(m.group(5)), excess=f(m.group(6)))


def candidates(con, limit=None, explore=False, only=None):
    # explore=True: навпаки, випадки з прапорцями, які НЕ пройшли звичайні фільтри
    # only=<tender_id>: поточний стан одного вже обраного тендера (щоб оновити текст перед публікацією)
    since = (now() - timedelta(days=CFG["lookback_days"])).isoformat()
    sel = """SELECT t.*, SUM(f.weight) AS score, GROUP_CONCAT(f.code, '|') AS codes, GROUP_CONCAT(f.detail, '||') AS details
        FROM flags f JOIN tenders t ON t.id = f.tender_id"""
    if only:
        rows = con.execute(sel + " WHERE t.tender_id = ? GROUP BY t.id", (only,)).fetchall()
    else:
        rows = con.execute(sel + """
        WHERE f.first_seen >= ? AND t.status NOT IN ('cancelled', 'unsuccessful')
          AND t.tender_id NOT IN (SELECT tender_id FROM posts)
        GROUP BY t.id HAVING score >= ?""", (since, 2 if explore else 3)).fetchall()
    out, rest = [], []
    for r in rows:
        r = dict(r)
        codes = set((r["codes"] or "").split("|"))
        flags = dict(zip((r["codes"] or "").split("|"), (r["details"] or "").split("||")))
        strong = codes & STRONG
        weak_only = not strong or (strong == {"BID_AT_ESTIMATE_EXACT"}
                                   and not ("SINGLE_BIDDER" in codes and (r["est"] or 0) >= 5e6))
        if (r.get("elim_gap_pct") or 0) > 5:            # різниця >500% = майже завжди помилка учасника в одиницях
            codes -= {"CHEAPER_BIDS_WITHDREW", "CHEAPER_BIDS_REJECTED"}
        pi = price_info(flags.get("PRICE_ABOVE_PEERS"))
        # «сума під ризиком»: виміряна переплата, якщо є; інакше невелика частка вартості
        money = max((r.get("elim_gap") or 0) if codes & {"CHEAPER_BIDS_WITHDREW", "CHEAPER_BIDS_REJECTED"} else 0,
                    pi["excess"] if pi else 0,
                    0.05 * (r["est"] or 0) if strong & {"AMCU_COMPLAINT_UPHELD", "SHARED_BIDDER_CONTACTS",
                                                        "REPEAT_WITHDRAWAL_PAIR"} else 0,
                    0.02 * (r["est"] or 0) if "BID_AT_ESTIMATE_EXACT" in strong else 0,
                    # переможець без історії (взагалі або в цій категорії): під ризиком увесь договір
                    (r.get("award_amt") or 0) if strong & {"SUPPLIER_NO_HISTORY", "SUPPLIER_NEW_IN_DIVISION"} else 0)
        if re.search(r"газове паливо|природн\w* газ|електричн\w* енергі|теплов\w* енергі", r["title"] or "", re.I) and not pi:
            money *= 0.2   # регульовані комунальні послуги: ціна на рівні очікуваної — норма
        if only:
            r.update(codes=codes, flags=flags, price=pi, money=money)
            out.append(r)
            continue
        if r.get("military") == 2 and (not CFG["include_security"] or explore):
            continue
        if r.get("military") == 1:
            if not CFG["include_military"] or explore:
                continue
            pub = r.get("period_start") or r.get("published") or r.get("modified")  # дата оголошення
            if pub and datetime.fromisoformat(pub) > now() - timedelta(days=CFG["military_delay_days"]):
                continue
        r.update(codes=codes, flags=flags, price=pi, money=money)
        ok = (not weak_only and money >= CFG["min_money_uah"]
              and not (r["score"] < CFG["min_score"] and money < CFG["big_money_uah"]))
        (out if ok else rest).append(r)
    if only:
        return sorted(out, key=lambda r: -r["money"])[:1]
    pool = rest if explore else out
    pool.sort(key=lambda r: (-r["money"], -r["score"]))
    seen, uniq = {}, []
    for r in pool:                                        # один допис на замовника + предмет (лоти однієї закупівлі)
        k = (r["buyer_id"], subject(r)[:40])
        if k in seen:
            seen[k]["siblings"] = seen[k].get("siblings", 0) + 1
            continue
        seen[k] = r
        uniq.append(r)
    return uniq if limit is None else uniq[:limit]
