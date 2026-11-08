#!/usr/bin/env python3
"""Prozorro -> X (Twitter) publisher with human approval via Telegram.

Flow:  monitor DB (prozorro_monitor.py)  ->  draft  ->  Telegram review (Publish / Edit / Reject)
       ->  approved queue  ->  X (+ optional Telegram channel), respecting daily cap and spacing.
Nothing is ever posted without an explicit "Publish" tap from an allow-listed Telegram user.

Commands
  draft      pick new strong cases from the monitor DB and send them for review
  poll       process Telegram button taps / edits once, then publish due approved posts
  serve      run forever: long-poll Telegram; draft every `draft_every_min` minutes
  queue      print the queue                     (CLI fallback)
  approve ID / reject ID                         (CLI fallback, same effect as the buttons)
  preview    print drafts for the newest candidates without saving or sending anything

Secrets come from environment variables (or a .env file next to this script):
  TG_BOT_TOKEN, TG_ADMIN_IDS (comma-separated Telegram user ids allowed to approve),
  TG_REVIEW_CHAT_ID (where drafts are sent; usually your own user id),
  TG_CHANNEL_ID (optional, e.g. @my_channel -> also publish there),
  X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET (OAuth 1.0a user context, Read+Write app)
Use --dry-run to print every outgoing request instead of sending it.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import html
import json
import os
import re
import secrets
import sqlite3
import sys
import time
import urllib.parse
from datetime import datetime, timedelta, timezone

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
PORTAL = "https://prozorro.gov.ua/tender/"

CFG = {
    "db_path": "prozorro.db",
    "min_score": 5,                     # total flag weight a tender needs
    "min_money_uah": 300_000,           # overpayment / gap / value at stake needed to be "worth a post"
    "big_money_uah": 2_000_000,         # ...and with this much at stake a lower flag score is enough
    "lookback_days": 14,                # only consider tenders flagged in this window
    "max_drafts_per_run": 5,
    "max_posts_per_day": 5,
    "min_minutes_between_posts": 90,
    "quiet_hours_kyiv": [23, 8],        # don't publish between 23:00 and 08:00 Kyiv time
    "military_delay_days": 14,          # military cases wait this long after publication on Prozorro
    "include_military": True,           # if False, military cases are never drafted
    "draft_every_min": 60,
    "hashtags": "#Prozorro",
}
if os.path.exists(os.path.join(HERE, "publisher.json")):
    with open(os.path.join(HERE, "publisher.json"), encoding="utf-8") as fh:
        CFG.update(json.load(fh))


def load_env():
    p = os.path.join(HERE, ".env")
    if os.path.exists(p):
        for line in open(p, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env()
ENV = lambda k, d="": os.environ.get(k, d)  # noqa: E731
DRY = False


def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", file=sys.stderr)


def now():
    return datetime.now(timezone.utc)


# ------------------------------------------------------------------ storage
SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
  tender_id TEXT PRIMARY KEY, created TEXT, status TEXT, text TEXT, context TEXT,
  review_msg_id INTEGER, approved_by TEXT, approved_at TEXT,
  x_id TEXT, tg_channel_msg_id INTEGER, published_at TEXT, note TEXT);
CREATE TABLE IF NOT EXISTS pub_meta (k TEXT PRIMARY KEY, v TEXT);
"""


def db():
    con = sqlite3.connect(CFG["db_path"])
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    return con


def meta(con, k, v=None):
    if v is None:
        r = con.execute("SELECT v FROM pub_meta WHERE k=?", (k,)).fetchone()
        return r["v"] if r else None
    con.execute("INSERT OR REPLACE INTO pub_meta VALUES (?,?)", (k, str(v)))
    con.commit()


# ------------------------------------------------------------------ candidate selection
STRONG = {"PRICE_ABOVE_PEERS", "CHEAPER_BIDS_WITHDREW", "CHEAPER_BIDS_REJECTED", "REPEAT_WITHDRAWAL_PAIR",
          "SHARED_BIDDER_CONTACTS", "AMCU_COMPLAINT_UPHELD", "BID_AT_ESTIMATE_EXACT"}


def price_info(detail):
    m = re.search(r": ([\d,\.]+) UAH/(.+?) vs median ([\d,\.]+) at (\d+) other buyers \(x([\d\.]+)\); ~([\d,]+) UAH",
                  detail or "")
    if not m:
        return None
    f = lambda x: float(x.replace(",", ""))  # noqa: E731
    return dict(price=f(m.group(1)), unit=m.group(2), med=f(m.group(3)), n=int(m.group(4)),
                ratio=float(m.group(5)), excess=f(m.group(6)))


def candidates(con, limit=50):
    since = (now() - timedelta(days=CFG["lookback_days"])).isoformat()
    rows = con.execute(f"""
        SELECT t.*, SUM(f.weight) AS score, GROUP_CONCAT(f.code, '|') AS codes, GROUP_CONCAT(f.detail, '||') AS details
        FROM flags f JOIN tenders t ON t.id = f.tender_id
        WHERE f.first_seen >= ? AND t.status NOT IN ('cancelled', 'unsuccessful')
          AND t.tender_id NOT IN (SELECT tender_id FROM posts)
        GROUP BY t.id HAVING score >= 3""", (since,)).fetchall()
    out = []
    for r in rows:
        r = dict(r)
        codes = set((r["codes"] or "").split("|"))
        flags = dict(zip((r["codes"] or "").split("|"), (r["details"] or "").split("||")))
        strong = codes & STRONG
        if not strong:
            continue
        if strong == {"BID_AT_ESTIMATE_EXACT"} and not ("SINGLE_BIDDER" in codes and (r["est"] or 0) >= 5e6):
            continue
        if (r.get("elim_gap_pct") or 0) > 5:            # >500% gap = almost always a bidder's unit error
            codes -= {"CHEAPER_BIDS_WITHDREW", "CHEAPER_BIDS_REJECTED"}
        pi = price_info(flags.get("PRICE_ABOVE_PEERS"))
        # "money at stake": measured overpayment where we have it; otherwise a small share of the value
        money = max((r.get("elim_gap") or 0) if codes & {"CHEAPER_BIDS_WITHDREW", "CHEAPER_BIDS_REJECTED"} else 0,
                    pi["excess"] if pi else 0,
                    0.05 * (r["est"] or 0) if strong & {"AMCU_COMPLAINT_UPHELD", "SHARED_BIDDER_CONTACTS",
                                                        "REPEAT_WITHDRAWAL_PAIR"} else 0,
                    0.02 * (r["est"] or 0) if "BID_AT_ESTIMATE_EXACT" in strong else 0)
        if re.search(r"газове паливо|природн\w* газ|електричн\w* енергі|теплов\w* енергі", r["title"] or "", re.I) and not pi:
            money *= 0.2   # regulated utilities: price at estimate is normal
        if money < CFG["min_money_uah"]:
            continue
        if r["score"] < CFG["min_score"] and money < CFG["big_money_uah"]:
            continue
        if r.get("military") == 1:
            if not CFG["include_military"]:
                continue
            pub = r.get("period_start") or r.get("published") or r.get("modified")  # announcement date
            if pub and datetime.fromisoformat(pub) > now() - timedelta(days=CFG["military_delay_days"]):
                continue
        r.update(codes=codes, flags=flags, price=pi, money=money)
        out.append(r)
    out.sort(key=lambda r: (-r["money"], -r["score"]))
    seen, uniq = {}, []
    for r in out:                                        # one post per buyer + subject (lots of the same purchase)
        k = (r["buyer_id"], subject(r)[:40])
        if k in seen:
            seen[k]["siblings"] = seen[k].get("siblings", 0) + 1
            continue
        seen[k] = r
        uniq.append(r)
    return uniq[:limit]


# ------------------------------------------------------------------ text generation (Ukrainian)
ABBR = [(r"товариство з обмеженою відповідальністю", "ТОВ"), (r"комунальне некомерційне підприємство", "КНП"),
        (r"комунальне некомерційне товариство", "КНТ"), (r"комунальне підприємство", "КП"),
        (r"державне підприємство", "ДП"), (r"приватне підприємство", "ПП"), (r"акціонерне товариство", "АТ"),
        (r"приватне акціонерне товариство", "ПрАТ"), (r"фізична особа[- ]підприємець", "ФОП"),
        (r"обласної державної адміністрації", "ОДА"), (r"міської ради", "міськради"),
        (r"сільської ради", "сільради"), (r"селищної ради", "селищради"), (r"виконавчого комітету", "виконкому")]


def short_name(name, n=70):
    s = re.sub(r"\s+", " ", (name or "").replace("­", "")).strip()
    for pat, rep in ABBR:
        s = re.sub(pat, rep, s, flags=re.I)
    def fix(w):
        letters = re.sub(r"[^A-Za-zА-Яа-яІіЇїЄєҐґ]", "", w)
        if len(letters) > 4 and letters.isupper():
            i = next((k for k, ch in enumerate(w) if ch.isalpha()), 0)
            return w[:i] + w[i] + w[i + 1:].lower()
        return w
    s = " ".join(fix(w) for w in s.split())
    return s if len(s) <= n else s[: n - 1].rstrip(" ,") + "…"


def money(x):
    if x is None:
        return "—"
    if x >= 1e9:
        return f"{x / 1e9:.2f} млрд грн".replace(".", ",")
    if x >= 1e6:
        return f"{x / 1e6:.1f} млн грн".replace(".", ",")
    return f"{x:,.0f} грн".replace(",", " ")


def num_ua(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ").replace(".", ",")


MIL_UNIT = re.compile(r"(в/ч|військов\w+ частин\w+)\s*[№#]?\s*[А-ЯA-Z]?\s*-?\s*\d{3,5}|\b[АA]\s?\d{4}\b", re.I)


def subject(r):
    t = re.sub(r"(код\s*)?(за\s*)?(ДК\s*021:?\s*2015[:\s–-]*)?\d{8}-\d\s*[–-]?\s*", "", r["title"] or "", flags=re.I)
    t = re.sub(r"^\s*\d{2}[А-ЯA-Z]-\d+_", "", t)            # internal prefixes like 26Т-953_
    t = re.sub(r"^\s*код\s+", "", t, flags=re.I).strip(" -–:«»\"")
    t = re.sub(r"\.\s*:\s*", ": ", t)
    t = re.sub(r"\s+", " ", t).strip(" -–—:«»\"")
    if r.get("military") == 1:
        t = MIL_UNIT.sub("", t)
    t = short_name(t, 200)
    return t[:60].rstrip(" ,(") + ("…" if len(t) > 60 else "")


def signal_lines(r):
    c, lines = r["codes"], []
    pi = r.get("price")
    if pi:
        lines.append(f"Ціна {num_ua(pi['price'], 2 if pi['price'] < 100 else 0)} грн/{pi['unit']} — у {pi['ratio']:.1f} раза "
                     f"вища за медіану ({num_ua(pi['med'], 2 if pi['med'] < 100 else 0)} грн) у {pi['n']} інших замовників.")
    if c & {"CHEAPER_BIDS_WITHDREW", "CHEAPER_BIDS_REJECTED"} and r.get("elim_gap"):
        how = "відмовились від договору" if "CHEAPER_BIDS_WITHDREW" in c else "були відхилені"
        lines.append(f"Усі дешевші пропозиції {how}; переможець дорожчий на {money(r['elim_gap'])} "
                     f"(+{r['elim_gap_pct'] * 100:.0f}%).")
    if "BID_AT_ESTIMATE_EXACT" in c and r.get("award_amt") and r.get("est"):
        diff = r["est"] - r["award_amt"]
        who = "Єдиний учасник" if "SINGLE_BIDDER" in c else "Переможець"
        if diff < 1:
            lines.append(f"{who} запропонував ціну, що до гривні дорівнює очікуваній вартості.")
        else:
            lines.append(f"{who} запропонував ціну лише на {num_ua(diff)} грн нижчу за очікувану вартість.")
    if "REPEAT_WITHDRAWAL_PAIR" in c:
        m = re.search(r"in (\d+) tenders at (\d+) buyer", r["flags"].get("REPEAT_WITHDRAWAL_PAIR", ""))
        if m:
            lines.append(f"Той самий дешевший учасник уже {m.group(1)} раз(и) «поступався» цьому переможцю.")
    if "SHARED_BIDDER_CONTACTS" in c:
        f = r["flags"].get("SHARED_BIDDER_CONTACTS", "")
        what = [w for k, w in (("phone", "телефон"), ("email", "e-mail"), ("person", "контактну особу"),
                               ("address", "адресу")) if k in f]
        lines.append(f"Конкуруючі учасники мають спільні {', '.join(what) or 'контакти'}.")
    if "AMCU_COMPLAINT_UPHELD" in c:
        lines.append("АМКУ задовольнив скаргу в цій закупівлі.")
    return lines


def weighted_len(text):
    """X counts every URL as 23 chars; Latin/Cyrillic chars count 1, CJK/emoji 2."""
    t = re.sub(r"https?://\S+", "x" * 23, text)
    return sum(1 if ord(ch) <= 0x10FF or 0x2000 <= ord(ch) <= 0x200D or 0x2010 <= ord(ch) <= 0x201F
               or 0x2032 <= ord(ch) <= 0x2037 else 2 for ch in t)


def compose(r):
    buyer = "Військовий замовник" if r.get("military") == 1 else short_name(r["buyer"], 50)
    more = f" (+{r['siblings']} аналогічн. лот.)" if r.get("siblings") else ""
    head = f"{buyer}: {subject(r)}, {money(r.get('award_amt') or r.get('est'))}{more}."
    tail = f"Сигнал ризику, не доказ порушення.\n{PORTAL}{r['tender_id']} {CFG['hashtags']}".rstrip()
    sigs = signal_lines(r)
    for k in range(len(sigs), 0, -1):                      # drop weaker signals until it fits 280
        text = "\n".join([head] + sigs[:k] + [tail])
        if weighted_len(text) <= 280:
            return text
    text = "\n".join([head, sigs[0] if sigs else "", tail])
    while weighted_len(text) > 280 and len(buyer) > 20:   # last resort: shorten the buyer name
        buyer = buyer[:-8].rstrip() + "…"
        head = f"{buyer}: {subject(r)}, {money(r.get('award_amt') or r.get('est'))}{more}."
        text = "\n".join([head, sigs[0] if sigs else "", tail])
    return text


def context(r):
    """Extra facts for the reviewer only (never published)."""
    lines = [f"Сума/оцінка: {money(r.get('award_amt'))} / {money(r.get('est'))}; учасників: {r.get('n_bids') or '—'}; "
             f"процедура: {r.get('mtype')}; статус: {r.get('status')}",
             f"Замовник: {r.get('buyer')} ({r.get('buyer_id')})",
             f"Переможець: {r.get('supplier') or '—'} ({r.get('supplier_id') or '—'})",
             f"Бали: {r['score']}; сектор: {['цивільний', 'ВІЙСЬКОВИЙ', 'безпековий'][r.get('military') or 0]}"]
    for code, det in r["flags"].items():
        lines.append(f"• {code}: {det[:220]}")
    return "\n".join(lines)


# ------------------------------------------------------------------ Telegram
def tg(method, **params):
    if DRY:
        log(f"DRY telegram.{method} {json.dumps(params, ensure_ascii=False)[:600]}")
        return {"message_id": int(time.time()) % 100000, "result": []}
    r = requests.post(f"https://api.telegram.org/bot{ENV('TG_BOT_TOKEN')}/{method}", json=params, timeout=70)
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(f"telegram {method}: {j}")
    return j["result"]


def admin_ids():
    return {x.strip() for x in ENV("TG_ADMIN_IDS").split(",") if x.strip()}


def buttons(tid):
    return {"inline_keyboard": [[{"text": "✅ Опублікувати", "callback_data": f"ok:{tid}"},
                                 {"text": "✏️ Редагувати", "callback_data": f"ed:{tid}"},
                                 {"text": "❌ Відхилити", "callback_data": f"no:{tid}"}]]}


def review_html(tid, text, ctx, status=None):
    s = (f"<b>Чернетка для X</b> ({weighted_len(text)}/280)\n<pre>{html.escape(text)}</pre>\n"
         f"<b>Для перевірки:</b>\n{html.escape(ctx)}\n{PORTAL}{tid}")
    if status:
        s += f"\n\n<b>{html.escape(status)}</b>"
    return s[:4090]


def send_for_review(con, tid, text, ctx):
    res = tg("sendMessage", chat_id=ENV("TG_REVIEW_CHAT_ID"), text=review_html(tid, text, ctx), parse_mode="HTML",
             disable_web_page_preview=True, reply_markup=buttons(tid))
    con.execute("UPDATE posts SET status='in_review', review_msg_id=? WHERE tender_id=?", (res["message_id"], tid))
    con.commit()


def set_review_status(con, tid, status, keep_buttons=False):
    p = con.execute("SELECT * FROM posts WHERE tender_id=?", (tid,)).fetchone()
    if not p or not p["review_msg_id"]:
        return
    try:
        kw = {"reply_markup": buttons(tid)} if keep_buttons else {"reply_markup": {"inline_keyboard": []}}
        tg("editMessageText", chat_id=ENV("TG_REVIEW_CHAT_ID"), message_id=p["review_msg_id"],
           text=review_html(tid, p["text"], p["context"] or "", status), parse_mode="HTML",
           disable_web_page_preview=True, **kw)
    except Exception as e:  # message too old to edit etc.
        log(f"could not edit review message: {e}")


def handle_update(con, u):
    if "callback_query" in u:
        q = u["callback_query"]
        uid = str(q["from"]["id"])
        if uid not in admin_ids():
            tg("answerCallbackQuery", callback_query_id=q["id"], text="Немає прав")
            log(f"ignored callback from non-admin {uid}")
            return
        action, tid = q.get("data", ":").split(":", 1)
        p = con.execute("SELECT * FROM posts WHERE tender_id=?", (tid,)).fetchone()
        if not p or p["status"] not in ("in_review", "awaiting_edit"):
            tg("answerCallbackQuery", callback_query_id=q["id"], text="Вже оброблено")
            return
        if action == "ok":
            con.execute("UPDATE posts SET status='approved', approved_by=?, approved_at=? WHERE tender_id=?",
                        (uid, now().isoformat(), tid))
            con.commit()
            tg("answerCallbackQuery", callback_query_id=q["id"], text="Схвалено — стане в чергу")
            set_review_status(con, tid, "✅ Схвалено, у черзі на публікацію")
        elif action == "no":
            con.execute("UPDATE posts SET status='rejected', approved_by=? WHERE tender_id=?", (uid, tid))
            con.commit()
            tg("answerCallbackQuery", callback_query_id=q["id"], text="Відхилено")
            set_review_status(con, tid, "❌ Відхилено")
        elif action == "ed":
            res = tg("sendMessage", chat_id=ENV("TG_REVIEW_CHAT_ID"),
                     text=f"Надішліть новий текст для {tid} відповіддю (reply) на це повідомлення. "
                          f"Посилання на тендер додайте самі або напишіть +link наприкінці.",
                     reply_markup={"force_reply": True})
            con.execute("UPDATE posts SET status='awaiting_edit', note=? WHERE tender_id=?",
                        (str(res["message_id"]), tid))
            con.commit()
            tg("answerCallbackQuery", callback_query_id=q["id"], text="Чекаю новий текст")
        return
    msg = u.get("message") or {}
    uid = str((msg.get("from") or {}).get("id", ""))
    if uid not in admin_ids():
        return
    text = (msg.get("text") or "").strip()
    reply_to = (msg.get("reply_to_message") or {}).get("message_id")
    if reply_to:
        p = con.execute("SELECT * FROM posts WHERE status='awaiting_edit' AND note=?", (str(reply_to),)).fetchone()
        if p:
            if text.endswith("+link"):
                text = text[:-5].rstrip() + f"\n{PORTAL}{p['tender_id']}"
            if weighted_len(text) > 280:
                tg("sendMessage", chat_id=ENV("TG_REVIEW_CHAT_ID"),
                   text=f"Задовго: {weighted_len(text)}/280. Спробуйте ще раз (reply на попереднє прохання).")
                return
            set_review_status(con, p["tender_id"], "✏️ Замінено новою версією нижче")
            con.execute("UPDATE posts SET text=?, status='draft', note=NULL WHERE tender_id=?", (text, p["tender_id"]))
            con.commit()
            send_for_review(con, p["tender_id"], text, p["context"] or "")
        return
    if text in ("/pause", "/resume"):
        meta(con, "paused", "1" if text == "/pause" else "0")
        tg("sendMessage", chat_id=ENV("TG_REVIEW_CHAT_ID"),
           text="Публікацію призупинено" if text == "/pause" else "Публікацію відновлено")
    elif text == "/queue":
        rows = con.execute("SELECT tender_id, status FROM posts WHERE status IN ('in_review','approved','awaiting_edit') "
                           "ORDER BY created").fetchall()
        tg("sendMessage", chat_id=ENV("TG_REVIEW_CHAT_ID"),
           text="\n".join(f"{r['tender_id']} — {r['status']}" for r in rows) or "Черга порожня")
    elif text in ("/start", "/help"):
        tg("sendMessage", chat_id=ENV("TG_REVIEW_CHAT_ID"),
           text="Кнопки під чернеткою: опублікувати / редагувати / відхилити.\n/queue — черга, /pause, /resume")


def poll_telegram(con, timeout=0):
    off = int(meta(con, "tg_offset") or 0)
    if DRY:
        return 0
    ups = tg("getUpdates", offset=off, timeout=timeout, allowed_updates=["message", "callback_query"])
    for u in ups:
        try:
            handle_update(con, u)
        except Exception as e:
            log(f"update {u.get('update_id')} failed: {e}")
        meta(con, "tg_offset", u["update_id"] + 1)
    return len(ups)


# ------------------------------------------------------------------ X (OAuth 1.0a user context)
def _pct(s):
    return urllib.parse.quote(str(s), safe="~")


def x_post(text):
    url = "https://api.x.com/2/tweets"
    if DRY:
        log(f"DRY X post ({weighted_len(text)} chars):\n{text}")
        return "dry-" + secrets.token_hex(4)
    oauth = {"oauth_consumer_key": ENV("X_API_KEY"), "oauth_nonce": secrets.token_hex(16),
             "oauth_signature_method": "HMAC-SHA1", "oauth_timestamp": str(int(time.time())),
             "oauth_token": ENV("X_ACCESS_TOKEN"), "oauth_version": "1.0"}
    base = "&".join(["POST", _pct(url), _pct("&".join(f"{_pct(k)}={_pct(v)}" for k, v in sorted(oauth.items())))])
    key = f"{_pct(ENV('X_API_SECRET'))}&{_pct(ENV('X_ACCESS_SECRET'))}"
    oauth["oauth_signature"] = base64.b64encode(hmac.new(key.encode(), base.encode(), hashlib.sha1).digest()).decode()
    auth = "OAuth " + ", ".join(f'{_pct(k)}="{_pct(v)}"' for k, v in sorted(oauth.items()))
    r = requests.post(url, json={"text": text}, headers={"Authorization": auth}, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"X API {r.status_code}: {r.text[:300]}")
    return r.json()["data"]["id"]


def publish_due(con):
    if meta(con, "paused") == "1":
        return
    kyiv = now() + timedelta(hours=2)  # Kyiv winter time; good enough for quiet hours
    a, b = CFG["quiet_hours_kyiv"]
    if (kyiv.hour >= a or kyiv.hour < b) if a > b else (a <= kyiv.hour < b):
        return
    day0 = now().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    n_today = con.execute("SELECT COUNT(*) FROM posts WHERE status='published' AND published_at>=?", (day0,)).fetchone()[0]
    last = con.execute("SELECT MAX(published_at) FROM posts WHERE status='published'").fetchone()[0]
    if n_today >= CFG["max_posts_per_day"]:
        return
    if last and datetime.fromisoformat(last) > now() - timedelta(minutes=CFG["min_minutes_between_posts"]):
        return
    p = con.execute("SELECT * FROM posts WHERE status='approved' ORDER BY approved_at LIMIT 1").fetchone()
    if not p:
        return
    try:
        xid = x_post(p["text"]) if ENV("X_API_KEY") or DRY else None
        chan = None
        if ENV("TG_CHANNEL_ID"):
            chan = tg("sendMessage", chat_id=ENV("TG_CHANNEL_ID"), text=p["text"])["message_id"]
        con.execute("UPDATE posts SET status='published', x_id=?, tg_channel_msg_id=?, published_at=? WHERE tender_id=?",
                    (xid, chan, now().isoformat(), p["tender_id"]))
        con.commit()
        link = f"https://x.com/i/web/status/{xid}" if xid and not str(xid).startswith("dry") else xid
        set_review_status(con, p["tender_id"], f"📤 Опубліковано {link or ''}")
        log(f"published {p['tender_id']} -> X {xid}")
    except Exception as e:
        con.execute("UPDATE posts SET status='failed', note=? WHERE tender_id=?", (str(e)[:500], p["tender_id"]))
        con.commit()
        set_review_status(con, p["tender_id"], f"⚠️ Помилка публікації: {e}")
        log(f"publish failed {p['tender_id']}: {e}")


# ------------------------------------------------------------------ commands
def cmd_draft(con, _args):
    made = 0
    for r in candidates(con):
        if made >= CFG["max_drafts_per_run"]:
            break
        text, ctx = compose(r), context(r)
        con.execute("INSERT OR IGNORE INTO posts (tender_id, created, status, text, context) VALUES (?,?,?,?,?)",
                    (r["tender_id"], now().isoformat(), "draft", text, ctx))
        con.commit()
        send_for_review(con, r["tender_id"], text, ctx)
        made += 1
    meta(con, "last_draft", now().isoformat())
    log(f"{made} drafts sent for review")


def cmd_poll(con, _args):
    n = poll_telegram(con)
    publish_due(con)
    log(f"{n} telegram updates processed")


def cmd_serve(con, _args):
    log("serving; Ctrl+C to stop")
    while True:
        try:
            last = meta(con, "last_draft")
            if not last or datetime.fromisoformat(last) < now() - timedelta(minutes=CFG["draft_every_min"]):
                cmd_draft(con, None)
            poll_telegram(con, timeout=50)
            publish_due(con)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            log(f"loop error: {e}")
            time.sleep(30)


def cmd_ci(con, args):
    """One scheduled CI pass: restore ledger, process taps/edits, draft if due, publish if due, save ledger."""
    cmd_ledger_import(con, args)
    poll_telegram(con)
    last = meta(con, "last_draft")
    if not last or datetime.fromisoformat(last) < now() - timedelta(minutes=CFG["draft_every_min"]):
        cmd_draft(con, None)
    publish_due(con)
    cmd_ledger_export(con, args)


def cmd_preview(con, args):
    for r in candidates(con, limit=args.n):
        t = compose(r)
        print(f"--- {r['tender_id']} score={r['score']} money={money(r['money'])} len={weighted_len(t)}\n{t}\n")


def cmd_queue(con, _args):
    for r in con.execute("SELECT tender_id, status, created, published_at, x_id FROM posts ORDER BY created DESC LIMIT 50"):
        print(dict(r))


def cmd_set(status):
    def f(con, args):
        con.execute("UPDATE posts SET status=?, approved_by='cli', approved_at=? WHERE tender_id=?",
                    (status, now().isoformat(), args.tender_id))
        con.commit()
        set_review_status(con, args.tender_id, "✅ Схвалено (CLI)" if status == "approved" else "❌ Відхилено (CLI)")
        print(f"{args.tender_id} -> {status}")
    return f


def cmd_ledger_export(con, args):
    """Small, durable state for CI: post decisions + Telegram offset + monitor watermark (committed to git)."""
    posts = [dict(r) for r in con.execute(
        "SELECT tender_id, created, status, text, approved_by, approved_at, x_id, published_at FROM posts")]
    st = {k: meta(con, k) for k in ("tg_offset", "paused", "last_draft")}
    try:
        st["watermark"] = con.execute("SELECT v FROM meta WHERE k='watermark'").fetchone()[0]
    except Exception:
        st["watermark"] = None
    os.makedirs(os.path.dirname(args.path) or ".", exist_ok=True)
    with open(args.path, "w", encoding="utf-8") as fh:
        json.dump({"state": st, "posts": sorted(posts, key=lambda p: p["tender_id"])}, fh, ensure_ascii=False, indent=1)
    log(f"ledger: {len(posts)} posts -> {args.path}")


def cmd_ledger_import(con, args):
    """Restore decisions after a lost CI cache, so nothing is re-offered and published posts are not repeated."""
    if not os.path.exists(args.path):
        log("no ledger yet")
        return
    j = json.load(open(args.path, encoding="utf-8"))
    for p in j.get("posts", []):
        con.execute("""INSERT OR IGNORE INTO posts (tender_id, created, status, text, approved_by, approved_at, x_id,
                       published_at) VALUES (:tender_id,:created,:status,:text,:approved_by,:approved_at,:x_id,
                       :published_at)""", p)
    st = j.get("state", {})
    for k in ("tg_offset", "paused", "last_draft"):
        if st.get(k) and not meta(con, k):
            meta(con, k, st[k])
    if st.get("watermark"):
        con.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")
        if not con.execute("SELECT v FROM meta WHERE k='watermark'").fetchone():
            con.execute("INSERT INTO meta VALUES ('watermark', ?)", (st["watermark"],))
    con.commit()
    log(f"ledger: restored {len(j.get('posts', []))} posts")


def main():
    global DRY
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", help="monitor SQLite DB (default from publisher.json or prozorro.db)")
    p.add_argument("--dry-run", action="store_true", help="print Telegram/X requests instead of sending")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name, fn in (("draft", cmd_draft), ("poll", cmd_poll), ("serve", cmd_serve), ("queue", cmd_queue)):
        sub.add_parser(name).set_defaults(fn=fn)
    pv = sub.add_parser("preview")
    pv.add_argument("--n", type=int, default=10)
    pv.set_defaults(fn=cmd_preview)
    for name, fn in (("ledger-export", cmd_ledger_export), ("ledger-import", cmd_ledger_import), ("ci", cmd_ci)):
        lg = sub.add_parser(name)
        lg.add_argument("--path", default="state/ledger.json")
        lg.set_defaults(fn=fn)
    for name, st in (("approve", "approved"), ("reject", "rejected")):
        s = sub.add_parser(name)
        s.add_argument("tender_id")
        s.set_defaults(fn=cmd_set(st))
    args = p.parse_args()
    DRY = args.dry_run
    if args.db:
        CFG["db_path"] = args.db
    if not DRY and args.cmd in ("draft", "poll", "serve", "ci"):
        missing = [k for k in ("TG_BOT_TOKEN", "TG_ADMIN_IDS", "TG_REVIEW_CHAT_ID") if not ENV(k)]
        if missing:
            sys.exit(f"missing settings: {', '.join(missing)} (put them in .env or use --dry-run)")
    args.fn(db(), args)


if __name__ == "__main__":
    main()
