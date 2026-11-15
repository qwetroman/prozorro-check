"""Перенесення старого журналу та порядок показу."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prozorro.pipeline import legacy_post, priority  # noqa: E402


def test_legacy_statuses():
    mk = lambda **k: {"tender_id": "UA-1", "created": "2026-11-10T00:00:00+00:00", **k}  # noqa: E731
    assert legacy_post(mk(status="in_review", review_msg_id=5))["status"] == "queued"
    assert legacy_post(mk(status="published", tg_channel_msg_id=None))["status"] == "approved"   # чекає на канал
    assert legacy_post(mk(status="published", tg_channel_msg_id=77))["status"] == "published"
    assert legacy_post(mk(status="published", tg_channel_msg_id=-1))["status"] == "published"    # канал пропущено
    assert legacy_post(mk(status="rejected"))["status"] == "rejected"
    assert legacy_post(mk(status="in_review", note="edited"))["edited"] == 1


def test_priority_by_money_and_model():
    assert priority(10_000_000) > priority(500_000) > priority(0) == 0
    assert priority(1, p_model=0.9) > priority(50_000_000, p_model=0.2)
