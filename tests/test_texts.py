"""Тексти дописів: скорочення для X без «…», повні назви для Telegram."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prozorro.texts import compose, compose_pattern, fit, short_name, weighted_len  # noqa: E402

BASE = dict(military=0, buyer_id="41798218", supplier_id="42306487", award_amt=14.9e6, est=15e6,
            tender_id="UA-2026-11-13-016066-a", flags={}, elim_gap=None,
            buyer='ДЕРЖАВНЕ НЕКОМЕРЦІЙНЕ ТОВАРИСТВО "ЦЕНТР ЗАХИСТУ І ПІДТРИМКИ КУЛЬТУРНОЇ СПАДЩИНИ УКРАЇНИ"',
            title="92310000-7 Виробництво (створення) культурно-мистецького проекту, спрямованого на популяризацію "
                  "української культури серед молоді",
            siblings=2, codes={"SUPPLIER_NO_HISTORY", "SINGLE_BIDDER"},
            price={"price": 1250.0, "unit": "шт", "ratio": 3.4, "med": 370.0, "n": 12})


def test_fit_word_boundary_no_ellipsis():
    assert fit("Послуги з ремонту і технічного обслуговування транспортних засобів", 31) == "Послуги з ремонту"
    assert fit("Діазепам, розчин для ін’єкцій 5 мг/мл по 2 мл в ампулах", 40) == "Діазепам, розчин для ін’єкцій 5 мг/мл"
    assert fit("Коротко", 40) == "Коротко"


def test_short_name_keeps_quoted_name():
    assert short_name('Комунальне підприємство "Київпастранс" виконавчого органу Київської міської ради', 40) == 'КП "Київпастранс"'
    assert short_name(BASE["buyer"], 40) == 'ДНТ "Центр Захисту І Підтримки"'


def test_x_version_fits_and_has_no_ellipsis():
    t = compose(BASE)
    assert weighted_len(t) <= 280 and "…" not in t
    assert "(+2 аналогічні лоти)" in t


def test_telegram_version_is_full():
    t = compose(BASE, full=True)
    assert "Центр Захисту І Підтримки Культурної Спадщини України" in t
    assert "популяризацію української культури серед молоді" in t
    assert "ЄДРПОУ: замовник 41798218, переможець 42306487" in t
    assert "…" not in t


def test_person_buyer_is_never_named():
    r = {**BASE, "buyer": "ФОП Іваненко Іван Іванович", "buyer_id": "1234567890"}
    for full in (False, True):
        t = compose(r, full=full)
        assert "Іваненко" not in t and "Замовник (ФОП)" in t


def test_military_buyer_is_anonymous():
    t = compose({**BASE, "military": 1}, full=True)
    assert "Військовий замовник" in t and "41798218" not in t


def test_pattern_keeps_one_bid_line():
    p = dict(pattern="dominance", buyer='Комунальне підприємство "Київпастранс" виконавчого органу Київської міської ради',
             buyer_id="31725604", supplier='ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ "ТЕХНОСЕРВІС ПЛЮС УКРАЇНА"',
             supplier_id="12345678", n=14, n_buyer=17, days=180, share=0.82, total=48e6, one_bid=9,
             example="UA-2026-10-01-000001-a")
    t = compose_pattern(p)
    assert weighted_len(t) <= 280 and "У 9 з 14 була лише одна пропозиція." in t
    assert "виконавчого органу Київської міськради" in compose_pattern(p, full=True)
