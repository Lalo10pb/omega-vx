import csv
import sys
from types import SimpleNamespace, ModuleType

import pytest

stub = ModuleType("ib_insync")
stub.IB = object
stub.Stock = object
stub.MarketOrder = object
stub.LimitOrder = object
stub.StopOrder = object
sys.modules.setdefault("ib_insync", stub)

import omega_vx_bot as bot


def _write_trade_log(path, statuses):
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "status"])
        writer.writeheader()
        for idx, status in enumerate(statuses):
            writer.writerow({"timestamp": f"2025-01-0{idx+1} 09:30:0{idx}", "status": status})


@pytest.mark.parametrize(
    "equity,max_equity,expected",
    [
        (95000, 100000, True),  # ≥5% drawdown
        (100000, 100000, False),
    ],
)
def test_is_ai_mood_bad_drawdown(tmp_path, monkeypatch, equity, max_equity, expected):
    trade_log = tmp_path / "trade_log.csv"
    _write_trade_log(trade_log, ["executed"] * 5)

    monkeypatch.setattr(bot, "TRADE_LOG_PATH", str(trade_log))
    monkeypatch.setattr(
        bot,
        "trading_client",
        SimpleNamespace(get_account=lambda: SimpleNamespace(equity=equity)),
    )
    monkeypatch.setattr(bot, "get_max_equity", lambda: max_equity)

    assert bot.is_ai_mood_bad() is expected


def test_is_ai_mood_bad_loss_streak(tmp_path, monkeypatch):
    statuses = ["loss", "loss", "loss", "skipped", "error"]
    trade_log = tmp_path / "trade_log.csv"
    _write_trade_log(trade_log, statuses)

    monkeypatch.setattr(bot, "TRADE_LOG_PATH", str(trade_log))
    monkeypatch.setattr(
        bot,
        "trading_client",
        SimpleNamespace(get_account=lambda: SimpleNamespace(equity=100000)),
    )
    monkeypatch.setattr(bot, "get_max_equity", lambda: 100000)

    assert bot.is_ai_mood_bad() is True


def test_mood_guard_blocks_alert_throttling(monkeypatch):
    calls = []

    def fake_alert(msg):
        calls.append(msg)

    monkeypatch.setattr(bot, "MOOD_GUARD_ENABLED", True)
    monkeypatch.setattr(bot, "MOOD_GUARD_ALERT_COOLDOWN", 3600)
    monkeypatch.setattr(bot, "send_telegram_alert", fake_alert)
    monkeypatch.setattr(bot, "is_ai_mood_bad", lambda: True)
    bot._MOOD_GUARD_LAST_ALERT = None

    assert bot._mood_guard_blocks("test-webhook") is True
    assert len(calls) == 1

    # Second call within cooldown should still block but not alert again
    assert bot._mood_guard_blocks("test-webhook") is True
    assert len(calls) == 1


def test_mood_guard_no_block_when_disabled(monkeypatch):
    monkeypatch.setattr(bot, "MOOD_GUARD_ENABLED", False)
    monkeypatch.setattr(bot, "is_ai_mood_bad", lambda: True)
    bot._MOOD_GUARD_LAST_ALERT = None

    assert bot._mood_guard_blocks("autoscan") is False
