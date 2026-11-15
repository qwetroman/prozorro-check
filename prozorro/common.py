"""Спільне: налаштування (publisher.json), секрети (.env / змінні середовища), час, журнал."""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # корінь репозиторію
PORTAL = "https://prozorro.gov.ua/tender/"

CFG = {
    "db_path": "state/prozorro.db",
    "min_score": 5,                     # потрібна сумарна вага прапорців тендера
    "min_money_uah": 300_000,           # переплата / різниця / сума під ризиком, щоб випадок «вартував допису»
    "big_money_uah": 2_000_000,         # ...а за такої суми під ризиком достатньо меншого балу
    "lookback_days": 14,                # розглядати лише тендери, позначені за цей період
    "pattern_min_money_uah": 2_000_000,  # закономірності (замовник/постачальник): мінімальна сума
    "explore_share": 0.25,              # пошукові випадки: ~1 на 4 чернетки, для навчання моделі
    "explore_max_per_run": 3,
    # типи закономірностей для чернеток; buyer_direct (частка неконкурентних процедур) за замовчуванням вимкнено:
    # для енергетики й комунальних монополій прямі договори - норма, тому сигналів забагато
    "pattern_types": ["dominance", "split", "buyer_single"],
    "military_delay_days": 14,          # військові випадки чекають стільки днів після оголошення на Prozorro
    "include_military": False,          # військові замовники / військові CPV: поки повністю вимкнено
    "include_security": False,          # безпекові й правоохоронні замовники (поліція, СБУ, kind=defense): теж вимкнено
    "hashtags": "#Prozorro",
    "rank_by_model": False,             # True = порядок показу чернеток за прогнозом моделі, а не за сумою під ризиком
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


def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", file=sys.stderr)


def now():
    return datetime.now(timezone.utc)


def _kyiv_tz():
    try:
        from zoneinfo import ZoneInfo
        for name in ("Europe/Kyiv", "Europe/Kiev"):
            try:
                return ZoneInfo(name)
            except Exception:
                pass
    except ImportError:
        pass
    return timezone(timedelta(hours=2))


KYIV = _kyiv_tz()  # з переходом на літній час
