"""Тексти дописів: коротка версія для X (до 280 символів) і повна для Telegram-каналу, контекст для перевірки."""
from __future__ import annotations

import re

from .common import CFG, PORTAL


ABBR = [(r"товариство з обмеженою відповідальністю", "ТОВ"), (r"комунальне некомерційне підприємство", "КНП"),
        (r"комунальне некомерційне товариство", "КНТ"), (r"комунальне підприємство", "КП"),
        (r"державне підприємство", "ДП"), (r"приватне підприємство", "ПП"), (r"акціонерне товариство", "АТ"),
        (r"приватне акціонерне товариство", "ПрАТ"), (r"фізична особа[- ]підприємець", "ФОП"),
        (r"обласної державної адміністрації", "ОДА"), (r"міської ради", "міськради"),
        (r"сільської ради", "сільради"), (r"селищної ради", "селищради"), (r"виконавчого комітету", "виконкому")]


# службові слова, на яких не можна обривати скорочений текст
DANGLING = {"в", "у", "з", "із", "зі", "на", "для", "та", "і", "й", "або", "чи", "до", "від", "по", "за", "при", "про",
            "щодо", "а", "без", "під", "над", "через", "між", "к-сть", "м.", "вул.", "№"}


ADJ_END = re.compile(r"[а-яіїєґ'’](ого|ього|ої|ьої|их|іх|ому|ьому|ий|ій)$")


def fit(s, n):
    """Скорочує текст до n символів по межі слова, без «…»: не обриває на прийменнику чи сполучнику,
    прибирає або закриває незакриту дужку/лапки. «…» лише якщо слово одне й дуже довге."""
    s = (s or "").strip()
    if n is None or len(s) <= n:
        return s
    cut = s[:n + 1]
    i = cut.rfind(" ")
    if i < n * 0.5:
        return s[:n - 1].rstrip(" ,(") + "…"
    t = cut[:i]

    def tidy(t):
        while True:
            t2 = t.rstrip(" ,;:/-–—+&")
            words = t2.split()
            last = words[-1].lower() if words else ""
            # прийменник/сполучник або прикметник без іменника («…і технічного») наприкінці - прибираємо
            if len(words) > 1 and (last in DANGLING or (ADJ_END.search(last) and len(t2) - len(last) >= n * 0.35)):
                t2 = t2[: len(t2) - len(words[-1])]
            if t2 == t:
                return t
            t = t2
    t = tidy(t)
    # краще обірвати на межі частини речення (кома, закрита лапка/дужка), якщо втрачаємо небагато
    b = max(t.rfind(", "), max(t.rfind(c) + 1 for c in ")»\"“") - 1)
    if b >= len(t) * 0.6 and b >= n * 0.45:
        t = tidy(t[: b + 1])
    for o, c in (("(", ")"), ("«", "»"), ("„", "“")):
        if t.count(o) > t.count(c):
            j = t.rfind(o)
            t = tidy(t[:j]) if j >= n * 0.5 else t + c
    if t.count('"') % 2:
        j = t.rfind('"')
        t = tidy(t[:j]) if j >= n * 0.5 else t + '"'
    return t


def short_name(name, n=None):
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
    if n is not None and len(s) > n:
        # довга назва: організаційну форму перед лапками скорочуємо до ініціалів, щоб лишилася власне назва
        m = re.match(r"^([^\"«]+?)\s*([\"«].+)$", s)
        words = m.group(1).split() if m else []
        if len(words) >= 2 and all(w[0].isalpha() for w in words):
            s = "".join(w[0].upper() for w in words) + " " + m.group(2)
    if n is not None and len(s) > n:
        # усе ще довга: обриваємо одразу після назви в лапках, якщо вона вміщається
        q = max(s.rfind(c, 0, n) for c in ("»", '"'))
        if q >= 8 and s[:q].count('"') % 2 == 1 or q >= 8 and "«" in s[:q]:
            s = s[: q + 1]
    return fit(s, n)


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


def subject(r, n=None):
    t = re.sub(r"(код\s*)?(за\s*)?(ДК\s*021:?\s*2015[:\s–-]*)?\d{8}-\d\s*[–-]?\s*", "", r["title"] or "", flags=re.I)
    t = re.sub(r"^\s*\d{2}[А-ЯA-Z]-\d+_", "", t)            # внутрішні префікси на кшталт 26Т-953_
    t = re.sub(r"^\s*код\s+", "", t, flags=re.I).strip(" -–:«»\"")
    t = re.sub(r"\.\s*:\s*", ": ", t)
    t = re.sub(r"\s+", " ", t).strip(" -–—:«»\"")
    if r.get("military") == 1:
        t = MIL_UNIT.sub("", t)
    t = short_name(t).rstrip(" .")
    return fit(t, n)


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
    if "SUPPLIER_NO_HISTORY" in c:
        lines.append("Переможець не мав жодної перемоги в Prozorro за попередні 24 місяці.")
    m = re.search(r"CPV (\d+)\*", r["flags"].get("SUPPLIER_NEW_IN_DIVISION", ""))
    if "SUPPLIER_NEW_IN_DIVISION" in c and m:
        lines.append(f"Переможець раніше взагалі не працював у цій сфері (розділ CPV {m.group(1)}) за 24 місяці.")
    m = re.search(r"CPV (\d+)\*", r["flags"].get("SUPPLIER_NEW_IN_CATEGORY", ""))
    if "SUPPLIER_NEW_IN_CATEGORY" in c and m:
        lines.append(f"Переможець раніше не вигравав закупівель групи CPV {m.group(1)} за 24 місяці.")
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
    m = re.search(r"скаржилися в (\d+) закупівлях \(АМКУ задовольнив: (\d+)\)", r["flags"].get("SUPPLIER_COMPLAINT_HISTORY", ""))
    if "SUPPLIER_COMPLAINT_HISTORY" in c and m:
        lines.append(f"На попередні перемоги цього постачальника скаржилися ще в {m.group(1)} "
                     f"{'закупівлі' if int(m.group(1)) % 10 == 1 and int(m.group(1)) % 100 != 11 else 'закупівлях'}"
                     + (f", у {m.group(2)} АМКУ скаргу задовольнив." if int(m.group(2)) else "."))
    if "BUYER_FIXED_AFTER_CLAIM" in c:
        lines.append("Замовник визнав вимогу учасника і змінив своє рішення чи умови.")
    m = re.search(r"від (\d+) різних учасників", r["flags"].get("MULTIPLE_COMPLAINANTS", ""))
    if "MULTIPLE_COMPLAINANTS" in c and m:
        lines.append(f"Скаржилися {m.group(1)} різні учасники.")
    # решта прапорців, від сильніших до слабших (у X слабші відкидаються, якщо не влазить у 280)
    fl, num = r["flags"], lambda x: float(x.replace(",", ""))
    if "CONTRACT_ABOVE_AWARD" in c and r.get("contract_amt") and r.get("award_amt"):
        lines.append(f"Договір ({money(r['contract_amt'])}) дорожчий за суму, з якою учасник переміг ({money(r['award_amt'])}).")
    m = re.search(r"має (\d+)% вартості.*\((\d+) договорів\)", fl.get("SUPPLIER_CONCENTRATION", ""))
    if "SUPPLIER_CONCENTRATION" in c and m:
        lines.append(f"Цей переможець отримав {m.group(1)}% вартості конкурентних закупівель замовника "
                     f"({m.group(2)} договорів за 30 днів).")
    m = re.search(r"^(\d+) допорогових тендерів .*? протягом (\d+) дн\., разом ([\d,]+) грн \(поріг ([\d,]+)\)",
                  fl.get("POSSIBLE_SPLIT_PURCHASE", ""))
    if "POSSIBLE_SPLIT_PURCHASE" in c and m:
        lines.append(f"{m.group(1)} схожих допорогових закупівель за {m.group(2)} дн. на {money(num(m.group(3)))} "
                     f"(поріг {money(num(m.group(4)))}): можливий поділ закупівлі.")
    if "SINGLE_BIDDER" in c and "BID_AT_ESTIMATE_EXACT" not in c:
        lines.append("У конкурентній процедурі була лише одна пропозиція.")
    if "SHORT_BID_PERIOD" in c and r.get("period_days") is not None:
        lines.append(f"На подання пропозицій дали лише {num_ua(r['period_days'], 1)} дн.")
    if "COMPETITORS_ELIMINATED" in c and not c & {"CHEAPER_BIDS_WITHDREW", "CHEAPER_BIDS_REJECTED"}:
        lines.append(f"Відхилено {r.get('n_unsuccessful')} з {r.get('n_bids')} учасників, переможець залишився сам.")
    if "NEGOTIATION_NO_JUSTIFICATION" in c:
        lines.append("Переговорна процедура без обґрунтування.")
    m = re.search(r"поріг ([\d,]+) грн", fl.get("JUST_BELOW_THRESHOLD", ""))
    if "JUST_BELOW_THRESHOLD" in c and m:
        lines.append(f"Очікувана вартість трохи нижча за поріг {money(num(m.group(1)))}, від якого потрібна складніша процедура.")
    if "AWARD_AT_ESTIMATE" in c and "BID_AT_ESTIMATE_EXACT" not in c and r.get("award_amt") and r.get("est"):
        lines.append(f"Ціна переможця — {num_ua(r['award_amt'] / r['est'] * 100, 1)}% очікуваної вартості, "
                     f"пропозицій лише {r.get('n_bids')}.")
    if "DIRECT_AWARD_HIGH_VALUE" in c and r.get("est"):
        lines.append(f"Неконкурентна процедура на {money(r['est'])}.")
    cur = r["award_amt"] / r["est"] * 100 if r.get("award_amt") and r.get("est") else None
    m = re.search(r"Ціна/очікувана (\d+)% проти медіани (\d+)%", fl.get("RATIO_OUTLIER", ""))
    # перевіряємо з поточною сумою: прапорець міг лишитися від попереднього переможця
    if ("RATIO_OUTLIER" in c and m and abs(int(m.group(1)) - int(m.group(2))) >= 15
            and cur is not None and abs(cur - int(m.group(1))) <= 2):
        lines.append(f"Ціна рішення — {m.group(1)}% очікуваної, тоді як зазвичай у цій категорії {m.group(2)}%.")
    if "VERY_LOW_AWARD" in c and cur is not None and cur <= 50:
        lines.append(f"Ціна рішення лише {cur:.0f}% очікуваної вартості.")
    return lines


def weighted_len(text):
    """X рахує кожне посилання як 23 символи; латиниця/кирилиця — 1, CJK/емодзі — 2."""
    t = re.sub(r"https?://\S+", "x" * 23, text)
    return sum(1 if ord(ch) <= 0x10FF or 0x2000 <= ord(ch) <= 0x200D or 0x2010 <= ord(ch) <= 0x201F
               or 0x2032 <= ord(ch) <= 0x2037 else 2 for ch in t)


def is_person(name, ident=None):
    # ФОП: за назвою або за 10-значним ІПН (у юросіб 8-значний ЄДРПОУ); ФОП у постах не називаємо
    return bool(re.fullmatch(r"\d{10}", str(ident or ""))
                or re.search(r"фізична особа|\bФОП\b", name or "", re.I))


def n_lots(n):
    if n % 10 == 1 and n % 100 != 11:
        return f"{n} аналогічний лот"
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return f"{n} аналогічні лоти"
    return f"{n} аналогічних лотів"


def n_zakupivel(n):
    if n % 10 == 1 and n % 100 != 11:
        return f"{n} закупівлю"
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return f"{n} закупівлі"
    return f"{n} закупівель"


def edrpou_line(r):
    # тільки 8-значні коди юросіб; у ФОП 10-значний ІПН, це персональні дані. Для військових не показуємо.
    if r.get("military") == 1:
        return None
    parts = []
    if re.fullmatch(r"\d{8}", str(r.get("buyer_id") or "")):
        parts.append(f"замовник {r['buyer_id']}")
    if re.fullmatch(r"\d{8}", str(r.get("supplier_id") or "")):
        parts.append(f"переможець {r['supplier_id']}")
    return "ЄДРПОУ: " + ", ".join(parts) if parts else None


def compose(r, full=False):
    # full=True: повний текст для Telegram-каналу (без обмеження 280 і без скорочень назв)
    if r.get("military") == 1:
        buyer = "Військовий замовник"
    elif is_person(r.get("buyer"), r.get("buyer_id")):
        buyer = "Замовник (ФОП)"
    else:
        buyer = None                                      # звичайна назва: скорочуємо через short_name
    more = f" (+{n_lots(r['siblings'])})" if r.get("siblings") else ""
    subj = subject(r)
    amount = money(r.get("award_amt") or r.get("est"))

    def head(bl=None, sl=None, b=None, t=None):          # bl, sl: максимальна довжина назви замовника і предмета
        bn = (fit(buyer, bl) if buyer else short_name(r["buyer"], bl)) if b is None else b
        return f"{bn}: {fit(subj, sl) if t is None else t}, {amount}{more}."
    tail = f"Сигнал ризику, не доказ порушення.\n{PORTAL}{r['tender_id']} {CFG['hashtags']}".rstrip()
    sigs = signal_lines(r)
    ids = edrpou_line(r)
    if full:
        return "\n".join([head()] + sigs + ([ids] if ids else []) + [tail])
    # X: більше сигналів важливіше за повну назву; ЄДРПОУ - лише якщо назви лишаються майже повними.
    # Вільне місце ділимо між замовником і предметом, скорочуємо по межі слова.
    lb, ls = len(buyer or short_name(r["buyer"])), len(subj)
    for k in (range(len(sigs), 0, -1) if sigs else [0]):
        for extra, mb, ms in (((ids, 35, 40),) if ids else ()) + ((None, 20, 25),):
            rest = sigs[:k] + ([extra] if extra else []) + [tail]
            budget = 280 - weighted_len("\n".join([head(b="", t="")] + rest))
            mb, ms = min(lb, mb), min(ls, ms)
            if budget < mb + ms:
                continue
            sl = min(ls, max(ms, int(budget * 0.6)))
            bl = min(lb, budget - sl)
            sl = min(ls, budget - bl)
            while True:
                text = "\n".join([head(bl, sl)] + rest)
                if weighted_len(text) <= 280:
                    return text
                if sl > ms:
                    sl -= 2
                elif bl > mb:
                    bl -= 2
                else:
                    break
    text = "\n".join([head(20, 25)] + sigs[:1] + [tail])
    # крайній випадок: у X без сигналу (повний текст однаково піде в Telegram-канал)
    return text if weighted_len(text) <= 280 else "\n".join([head(35, 40)] + [tail])


def compose_pattern(p, full=False):
    """Текст для закономірності (замовник/постачальник), до 280 символів."""
    def head(n=None):
        n5 = None if n is None else n - 5
        buyer = "Замовник (ФОП)" if is_person(p["buyer"], p.get("buyer_id")) else short_name(p["buyer"], n)
        if p["pattern"] == "dominance":
            sup = "переможець-ФОП" if is_person(p["supplier"], p.get("supplier_id")) else short_name(p["supplier"], n5)
            return (f"{buyer}: {sup} отримав {p['n']} з {p['n_buyer']} конкурентних закупівель за {p['days']} дн., "
                    f"{p['share']:.0%} їхньої вартості ({money(p['total'])}).")
        if p["pattern"] == "split":
            subj = subject({"title": p.get("title"), "military": 0}, n)
            sup = "переможець-ФОП" if is_person(p.get("supplier"), p.get("supplier_id")) else short_name(p.get("supplier"), n5)
            return (f"{buyer}: {sup} отримав {n_zakupivel(p['n'])} «{subj}» за {p['days']} дн. на {money(p['total'])}, "
                    f"кожна нижче порогу {money(p['threshold'])}. Можливий поділ закупівлі.")
        return f"{buyer}: за останні 30 днів {p['detail'].rstrip('.')}."
    lines = [f"У {p['one_bid']} з {p['n']} була лише одна пропозиція."] if p.get("one_bid") else []
    ids = edrpou_line({"buyer_id": p["buyer_id"], "supplier_id": p.get("supplier_id"), "military": 0})
    ex = f"Приклад: {PORTAL}{p['example']} " if p.get("example") else ""
    tail = f"Сигнал ризику, не доказ порушення.\n{ex}{CFG['hashtags']}".rstrip()
    if full:
        return "\n".join([head()] + lines + ([ids] if ids else []) + [tail])
    # рядок про одну пропозицію важливіший за повні назви; ЄДРПОУ - лише якщо назви лишаються довгими
    for extra, nmin in ((lines + ([ids] if ids else []), 40), (lines, 20), ([], 20)):
        for n in (70, 60, 50, 45, 40, 35, 30, 25, 20):
            if n < nmin:
                break
            text = "\n".join([head(n)] + extra + [tail])
            if weighted_len(text) <= 280:
                return text
    return text


def pattern_context(p):
    tag = TAGS.get(p.get("pattern"))
    lines = ([f"🏷 #{tag}"] if tag else []) + ["🔗 Закономірність (замовник/постачальник), а не окремий тендер.",
             f"Замовник: {p['buyer']} ({p['buyer_id']})"]
    if p.get("supplier"):
        lines.append(f"Постачальник: {p['supplier']} ({p['supplier_id']})")
    lines.append(f"Сума: {money(p['total'])}")
    if p.get("example"):
        lines.append(f"Найбільша закупівля: {PORTAL}{p['example']}")
    return "\n".join(lines)


# хештег головного прапорця - лише в чаті перевірки, щоб шукати/фільтрувати випадки за типом
TAGS = {
    "PRICE_ABOVE_PEERS": "завищена_ціна", "CHEAPER_BIDS_WITHDREW": "дешевші_відмовились",
    "CHEAPER_BIDS_REJECTED": "дешевших_відхилено", "REPEAT_WITHDRAWAL_PAIR": "повторне_поступання",
    "SHARED_BIDDER_CONTACTS": "спільні_контакти", "AMCU_COMPLAINT_UPHELD": "скарга_АМКУ",
    "SUPPLIER_NO_HISTORY": "постачальник_без_історії", "SUPPLIER_NEW_IN_CATEGORY": "новачок_у_групі",
    "SUPPLIER_NEW_IN_DIVISION": "новачок_у_сфері",
    "BID_AT_ESTIMATE_EXACT": "ціна_як_очікувана", "CONTRACT_ABOVE_AWARD": "договір_більший_за_рішення",
    "COMPETITORS_ELIMINATED": "усунення_конкурентів", "SUPPLIER_CONCENTRATION": "домінування",
    "POSSIBLE_SPLIT_PURCHASE": "поділ_закупівлі", "DIRECT_AWARD_HIGH_VALUE": "без_конкуренції",
    "NEGOTIATION_NO_JUSTIFICATION": "переговори_без_підстав", "SINGLE_BIDDER": "один_учасник",
    "AWARD_AT_ESTIMATE": "майже_очікувана_ціна", "SHORT_BID_PERIOD": "короткий_строк",
    "JUST_BELOW_THRESHOLD": "трохи_нижче_порогу", "RATIO_OUTLIER": "нетипове_співвідношення",
    "VERY_LOW_AWARD": "дуже_низька_ціна", "SUPPLIER_COMPLAINT_HISTORY": "скарги_на_постачальника",
    "BUYER_FIXED_AFTER_CLAIM": "замовник_визнав_вимогу", "MULTIPLE_COMPLAINANTS": "кілька_скаржників",
    "dominance": "домінування", "split": "поділ_закупівлі", "buyer_single": "постійно_одна_пропозиція",
    "buyer_direct": "постійно_без_конкуренції",
}
TAG_ORDER = list(TAGS)   # якщо сума під ризиком не визначає головного, - за цим порядком (від сильніших)


def main_tag(r):
    """Головний прапорець випадку: той, що дає найбільшу суму під ризиком; інакше найсильніший за TAG_ORDER."""
    codes, est = r.get("codes") or set(), r.get("est") or 0
    pi = r.get("price") or {}
    contrib = {"CHEAPER_BIDS_WITHDREW": r.get("elim_gap") or 0, "CHEAPER_BIDS_REJECTED": r.get("elim_gap") or 0,
               "PRICE_ABOVE_PEERS": pi.get("excess") or 0, "AMCU_COMPLAINT_UPHELD": 0.05 * est,
               "SHARED_BIDDER_CONTACTS": 0.05 * est, "REPEAT_WITHDRAWAL_PAIR": 0.05 * est,
               "BID_AT_ESTIMATE_EXACT": 0.02 * est, "SUPPLIER_NO_HISTORY": r.get("award_amt") or 0,
               "SUPPLIER_NEW_IN_DIVISION": r.get("award_amt") or 0}
    known = [c for c in codes if c in TAGS]
    if not known:
        return None
    best = max(known, key=lambda c: (contrib.get(c, 0), -TAG_ORDER.index(c)))
    return "#" + TAGS[best]


def context(r):
    """Додаткові факти лише для того, хто перевіряє (ніколи не публікуються)."""
    tag = main_tag(r)
    lines = ([f"🏷 {tag}"] if tag else []) + [f"Сума/оцінка: {money(r.get('award_amt'))} / {money(r.get('est'))}; учасників: {r.get('n_bids') or '—'}; "
             f"процедура: {r.get('mtype')}; статус: {r.get('status')}",
             f"Замовник: {r.get('buyer')} ({r.get('buyer_id')})",
             f"Переможець: {r.get('supplier') or '—'} ({r.get('supplier_id') or '—'})",
             f"Бали: {r['score']}; сектор: {['цивільний', 'ВІЙСЬКОВИЙ', 'безпековий'][r.get('military') or 0]}"]
    for code, det in r["flags"].items():
        lines.append(f"• {code}: {det[:220]}")
    return "\n".join(lines)
