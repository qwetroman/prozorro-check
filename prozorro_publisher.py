#!/usr/bin/env python3
"""Публікатор Prozorro -> X (Twitter) зі схваленням людиною через Telegram.

Схема: БД монітора (prozorro_monitor.py)  ->  чернетка  ->  перевірка в Telegram (Опублікувати / Редагувати / Відхилити)
       ->  черга схвалених  ->  X (+ необов'язково Telegram-канал), з денним лімітом та інтервалами.
Нічого не публікується без явного натискання «Опублікувати» користувачем із дозволеного списку Telegram.

Команди
  draft      вибрати нові сильні випадки з БД монітора й надіслати на перевірку
  poll       один раз обробити натискання / правки в Telegram, потім опублікувати схвалене, якщо настав час
  serve      працювати безперервно: long-poll Telegram; чернетки кожні `draft_every_min` хвилин
  queue      показати чергу                      (резервний варіант через CLI)
  approve ID / reject ID                         (резервний варіант через CLI, те саме, що кнопки)
  preview    показати чернетки для найновіших кандидатів, нічого не зберігаючи й не надсилаючи

Секрети беруться зі змінних середовища (або з файлу .env поруч зі скриптом):
  TG_BOT_TOKEN, TG_ADMIN_IDS (id користувачів Telegram через кому, яким дозволено схвалювати),
  TG_REVIEW_CHAT_ID (куди надсилати чернетки; зазвичай ваш власний id),
  TG_CHANNEL_ID (необов'язково, напр. @my_channel -> публікувати й там),
  X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET (OAuth 1.0a від імені користувача, застосунок Read+Write)
  Без ключів X схвалені дописи надходять вам у Telegram як посилання, що відкриває X із готовим текстом.
Використовуйте --dry-run, щоб друкувати всі вихідні запити замість надсилання.
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
    "min_score": 5,                     # потрібна сумарна вага прапорців тендера
    "min_money_uah": 300_000,           # переплата / різниця / сума під ризиком, щоб випадок «вартував допису»
    "big_money_uah": 2_000_000,         # ...а за такої суми під ризиком достатньо меншого балу
    "lookback_days": 14,                # розглядати лише тендери, позначені за цей період
    "max_drafts_per_run": 5,
    "max_posts_per_day": 5,
    "min_minutes_between_posts": 90,
    "quiet_hours_kyiv": [23, 8],        # не публікувати з 23:00 до 08:00 за київським часом
    "military_delay_days": 14,          # військові випадки чекають стільки днів після оголошення на Prozorro
    "include_military": True,           # якщо False, військові випадки ніколи не потрапляють у чернетки
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


# ------------------------------------------------------------------ зберігання
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


# ------------------------------------------------------------------ вибір кандидатів
STRONG = {"PRICE_ABOVE_PEERS", "CHEAPER_BIDS_WITHDREW", "CHEAPER_BIDS_REJECTED", "REPEAT_WITHDRAWAL_PAIR",
          "SHARED_BIDDER_CONTACTS", "AMCU_COMPLAINT_UPHELD", "BID_AT_ESTIMATE_EXACT"}


def price_info(detail):
    m = re.search(r": ([\d,\.]+) грн/(.+?) проти медіани ([\d,\.]+) у (\d+) інших замовників \(x([\d\.]+)\); ~([\d,]+) грн",
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
        if (r.get("elim_gap_pct") or 0) > 5:            # різниця >500% = майже завжди помилка учасника в одиницях
            codes -= {"CHEAPER_BIDS_WITHDREW", "CHEAPER_BIDS_REJECTED"}
        pi = price_info(flags.get("PRICE_ABOVE_PEERS"))
        # «сума під ризиком»: виміряна переплата, якщо є; інакше невелика частка вартості
        money = max((r.get("elim_gap") or 0) if codes & {"CHEAPER_BIDS_WITHDREW", "CHEAPER_BIDS_REJECTED"} else 0,
                    pi["excess"] if pi else 0,
                    0.05 * (r["est"] or 0) if strong & {"AMCU_COMPLAINT_UPHELD", "SHARED_BIDDER_CONTACTS",
                                                        "REPEAT_WITHDRAWAL_PAIR"} else 0,
                    0.02 * (r["est"] or 0) if "BID_AT_ESTIMATE_EXACT" in strong else 0)
        if re.search(r"газове паливо|природн\w* газ|електричн\w* енергі|теплов\w* енергі", r["title"] or "", re.I) and not pi:
            money *= 0.2   # регульовані комунальні послуги: ціна на рівні очікуваної — норма
        if money < CFG["min_money_uah"]:
            continue
        if r["score"] < CFG["min_score"] and money < CFG["big_money_uah"]:
            continue
        if r.get("military") == 1:
            if not CFG["include_military"]:
                continue
            pub = r.get("period_start") or r.get("published") or r.get("modified")  # дата оголошення
            if pub and datetime.fromisoformat(pub) > now() - timedelta(days=CFG["military_delay_days"]):
                continue
        r.update(codes=codes, flags=flags, price=pi, money=money)
        out.append(r)
    out.sort(key=lambda r: (-r["money"], -r["score"]))
    seen, uniq = {}, []
    for r in out:                                        # один допис на замовника + предмет (лоти однієї закупівлі)
        k = (r["buyer_id"], subject(r)[:40])
        if k in seen:
            seen[k]["siblings"] = seen[k].get("siblings", 0) + 1
            continue
        seen[k] = r
        uniq.append(r)
    return uniq[:limit]


# ------------------------------------------------------------------ генерування тексту
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
    t = re.sub(r"^\s*\d{2}[А-ЯA-Z]-\d+_", "", t)            # внутрішні префікси на кшталт 26Т-953_
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
        lines.append(f"Ціна {num_ua(pi['price'], 2 if pi['price'] < 100 else 0)} грн/{pi['unit']} — у {num_ua(pi['ratio'], 1)} раза "
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
        m = re.search(r"у (\d+) тендерах у (\d+) замовник", r["flags"].get("REPEAT_WITHDRAWAL_PAIR", ""))
        if m:
            lines.append(f"Той самий дешевший учасник уже {m.group(1)} раз(и) «поступався» цьому переможцю.")
    if "SHARED_BIDDER_CONTACTS" in c:
        f = r["flags"].get("SHARED_BIDDER_CONTACTS", "")
        what = [w for k, w in (("(телефон)", "телефон"), ("(e-mail)", "e-mail"), ("(контактна особа)", "контактна особа"),
                               ("(адреса)", "адреса")) if k in f]
        lines.append(f"Конкуруючі учасники мають спільні контакти{' (' + ', '.join(what) + ')' if what else ''}.")
    if "AMCU_COMPLAINT_UPHELD" in c:
        lines.append("АМКУ задовольнив скаргу в цій закупівлі.")
    return lines


def weighted_len(text):
    """X рахує кожне посилання як 23 символи; латиниця/кирилиця — 1, CJK/емодзі — 2."""
    t = re.sub(r"https?://\S+", "x" * 23, text)
    return sum(1 if ord(ch) <= 0x10FF or 0x2000 <= ord(ch) <= 0x200D or 0x2010 <= ord(ch) <= 0x201F
               or 0x2032 <= ord(ch) <= 0x2037 else 2 for ch in t)


def compose(r):
    buyer = "Військовий замовник" if r.get("military") == 1 else short_name(r["buyer"], 50)
    more = f" (+{r['siblings']} аналогічн. лот.)" if r.get("siblings") else ""
    head = f"{buyer}: {subject(r)}, {money(r.get('award_amt') or r.get('est'))}{more}."
    tail = f"Сигнал ризику, не доказ порушення.\n{PORTAL}{r['tender_id']} {CFG['hashtags']}".rstrip()
    sigs = signal_lines(r)
    for k in range(len(sigs), 0, -1):                      # відкидаємо слабші сигнали, доки не вміститься в 280
        text = "\n".join([head] + sigs[:k] + [tail])
        if weighted_len(text) <= 280:
            return text
    text = "\n".join([head, sigs[0] if sigs else "", tail])
    while weighted_len(text) > 280 and len(buyer) > 20:   # крайній захід: скорочуємо назву замовника
        buyer = buyer[:-8].rstrip() + "…"
        head = f"{buyer}: {subject(r)}, {money(r.get('award_amt') or r.get('est'))}{more}."
        text = "\n".join([head, sigs[0] if sigs else "", tail])
    return text


def context(r):
    """Додаткові факти лише для того, хто перевіряє (ніколи не публікуються)."""
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
    except Exception as e:  # повідомлення застаре для редагування тощо
        log(f"не вдалося змінити повідомлення для перевірки: {e}")


def handle_update(con, u):
    if "callback_query" in u:
        q = u["callback_query"]
        uid = str(q["from"]["id"])
        if uid not in admin_ids():
            tg("answerCallbackQuery", callback_query_id=q["id"], text="Немає прав")
            log(f"проігноровано натискання від не-адміністратора {uid}")
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
            log(f"помилка обробки оновлення {u.get('update_id')}: {e}")
        meta(con, "tg_offset", u["update_id"] + 1)
    return len(ups)


# ------------------------------------------------------------------ X (OAuth 1.0a від імені користувача)
def _pct(s):
    return urllib.parse.quote(str(s), safe="~")


def x_post(text):
    url = "https://api.x.com/2/tweets"
    if DRY:
        log(f"DRY допис у X ({weighted_len(text)} симв.):\n{text}")
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


def x_intent_url(text):
    return "https://x.com/intent/post?text=" + urllib.parse.quote(text, safe="")


def send_manual_post(tid, text):
    """Безкоштовний режим (без ключів X API): надіслати посилання, що відкриває X із готовим текстом допису."""
    url = x_intent_url(text)
    body = (f"<b>Готово до публікації в X</b> ({tid})\nНатисніть кнопку, перевірте текст і натисніть «Опублікувати» (Post).\n"
            f"<pre>{html.escape(text)}</pre>")
    try:
        tg("sendMessage", chat_id=ENV("TG_REVIEW_CHAT_ID"), text=body, parse_mode="HTML",
           disable_web_page_preview=True, reply_markup={"inline_keyboard": [[{"text": "Відкрити в X", "url": url}]]})
    except Exception as e:  # напр. URL задовгий для кнопки: надсилаємо звичайне посилання
        log(f"кнопка не спрацювала ({e}); надсилаю звичайне посилання")
        tg("sendMessage", chat_id=ENV("TG_REVIEW_CHAT_ID"), parse_mode="HTML", disable_web_page_preview=True,
           text=body + f'\n<a href="{html.escape(url)}">Відкрити в X</a>')


def publish_due(con):
    if meta(con, "paused") == "1":
        return
    kyiv = now() + timedelta(hours=2)  # київський зимовий час; для тихих годин цього достатньо
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
        if ENV("X_API_KEY") or DRY:
            xid = x_post(p["text"])
        else:
            send_manual_post(p["tender_id"], p["text"])
            xid = "manual"
        chan = None
        if ENV("TG_CHANNEL_ID"):
            chan = tg("sendMessage", chat_id=ENV("TG_CHANNEL_ID"), text=p["text"])["message_id"]
        con.execute("UPDATE posts SET status='published', x_id=?, tg_channel_msg_id=?, published_at=? WHERE tender_id=?",
                    (xid, chan, now().isoformat(), p["tender_id"]))
        con.commit()
        link = f"https://x.com/i/web/status/{xid}" if xid and not str(xid).startswith(("dry", "manual")) else xid
        set_review_status(con, p["tender_id"], "📤 Надіслано вам для публікації в X" if xid == "manual"
                          else f"📤 Опубліковано {link or ''}")
        log(f"опубліковано {p['tender_id']} -> X {xid}")
    except Exception as e:
        con.execute("UPDATE posts SET status='failed', note=? WHERE tender_id=?", (str(e)[:500], p["tender_id"]))
        con.commit()
        set_review_status(con, p["tender_id"], f"⚠️ Помилка публікації: {e}")
        log(f"помилка публікації {p['tender_id']}: {e}")


# ------------------------------------------------------------------ команди
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
    log(f"надіслано на перевірку чернеток: {made}")


def cmd_poll(con, _args):
    n = poll_telegram(con)
    publish_due(con)
    log(f"оброблено оновлень Telegram: {n}")


def cmd_serve(con, _args):
    log("працюю; Ctrl+C — зупинити")
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
            log(f"помилка циклу: {e}")
            time.sleep(30)


def cmd_ci(con, args):
    """Один прохід CI за розкладом: відновити журнал, обробити натискання/правки, створити чернетки й опублікувати, якщо настав час, зберегти журнал."""
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
        print(f"--- {r['tender_id']} бали={r['score']} сума={money(r['money'])} довжина={weighted_len(t)}\n{t}\n")


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
    """Малий постійний стан для CI: рішення щодо дописів + зсув Telegram + позначка монітора (комітиться в git)."""
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
    log(f"журнал: дописів {len(posts)} -> {args.path}")


def cmd_ledger_import(con, args):
    """Відновлює рішення після втрати кешу CI, щоб нічого не пропонувалося вдруге й опубліковане не повторювалося."""
    if not os.path.exists(args.path):
        log("журналу ще немає")
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
    log(f"журнал: відновлено дописів {len(j.get('posts', []))}")


def main():
    global DRY
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", help="SQLite-база монітора (типово з publisher.json або prozorro.db)")
    p.add_argument("--dry-run", action="store_true", help="друкувати запити до Telegram/X замість надсилання")
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
            sys.exit(f"бракує налаштувань: {', '.join(missing)} (додайте їх у .env або використайте --dry-run)")
    args.fn(db(), args)


if __name__ == "__main__":
    main()
