"""Знімки моделі на найкращих результатах і порівняння з поточною моделлю."""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import prozorro_model as PM  # noqa: E402


def dec(i, label, day):
    return {"tender_id": f"T{i}", "label": label, "decided_at": f"2026-11-{day:02d}T12:00:00+00:00",
            "features": {"kind": "pattern", "pattern": "dominance" if label else "split"}}


def model(n, auc, at="2026-11-10T00:00:00+00:00"):
    return {"trained_at": at, "n": n, "weights": {"bias": 0.0, "pattern:dominance": 3.0, "pattern:split": -3.0},
            "metrics": {"cv_accuracy": 0.8, "cv_auc": auc, "baseline_accuracy": 0.5}}


def test_checkpoint_only_on_new_best(tmp_path):
    D = [dec(i, i % 2, 5) for i in range(40)]
    PM.track(str(tmp_path), model(40, 0.80), D, [])
    PM.track(str(tmp_path), model(41, 0.802), D, [])          # не краще на 0.005 - без нового знімка
    PM.track(str(tmp_path), model(50, 0.85), D, [])
    assert sorted(os.listdir(tmp_path / "checkpoints")) == ["model-2026-11-10-n40-auc0.800.json",
                                                            "model-2026-11-10-n50-auc0.850.json"]
    assert len(PM.read_jsonl(str(tmp_path / "model_history.jsonl"))) == 3


def test_switch_to_checkpoint_when_live_model_degrades(tmp_path):
    D = [dec(i, i % 2, 5) for i in range(40)]
    r = PM.track(str(tmp_path), model(40, 0.80), D, [])
    assert r["use"] == "current"
    # 30 нових рішень після знімка; поточна модель тоді вгадувала навпаки, знімок - правильно
    new = [dec(100 + i, i % 2, 12) for i in range(30)]
    shadow = [{"tender_id": d["tender_id"], "p_approve": 0.2 if d["label"] else 0.8, "scored_at": "2026-11-11T00:00:00+00:00"}
              for d in new]
    r = PM.track(str(tmp_path), model(70, 0.70, "2026-11-12T13:00:00+00:00"), D + new, shadow)
    assert r["compare"]["n"] == 30 and r["compare"]["best"]["auc"] == 1.0 and r["compare"]["current"]["auc"] == 0.0
    assert r["use"].startswith("model-") and r["alert_now"] is True
    r = PM.track(str(tmp_path), model(70, 0.70, "2026-11-12T14:00:00+00:00"), D + new, shadow)
    assert r["alert_now"] is False and r["alerted"] is True                 # попередження - лише раз
    assert json.load(open(tmp_path / "model_report.json"))["use"] == r["use"]
