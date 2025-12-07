from __future__ import annotations

import importlib
import sys
from datetime import datetime, timedelta
import csv


class FakePosition:
    def __init__(self, symbol, qty, current_price, avg_entry_price):
        self.symbol = symbol
        self.qty = qty
        self.current_price = current_price
        self.avg_entry_price = avg_entry_price


class FakeActivity:
    def __init__(self, side, symbol, price, qty, transaction_time, net_amount=0, activity_id=""):
        self.side = side
        self.symbol = symbol
        self.price = price
        self.qty = qty
        self.transaction_time = transaction_time
        self.net_amount = net_amount
        self.id = activity_id


class FakeClient:
    def __init__(self, positions=None, activities=None):
        self.positions = positions or []
        self.activities = activities or []
        self.closed_symbols = []

    def get_all_positions(self):
        return self.positions

    def close_position(self, symbol):
        self.closed_symbols.append(symbol)

    def get_activities(self, activity_type=None):
        return self.activities


def _reload_monitor(monkeypatch, tmp_path, max_hold_minutes=0):
    monkeypatch.setenv("APCA_API_KEY_ID", "test-key")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "test-secret")
    monkeypatch.setenv("POSITION_MONITOR_LOG", str(tmp_path / "decision_log.csv"))
    monkeypatch.setenv("POSITION_MONITOR_ACTIVITY_CACHE", str(tmp_path / "activity_cache.txt"))
    if max_hold_minutes:
        monkeypatch.setenv("MAX_HOLD_MINUTES", str(max_hold_minutes))
    sys.modules.pop("position_monitor", None)
    return importlib.import_module("position_monitor")


def test_monitor_positions_handles_tp_sl_and_max_hold(monkeypatch, tmp_path):
    pm = _reload_monitor(monkeypatch, tmp_path, max_hold_minutes=60)
    now = datetime.utcnow()
    positions = [
        FakePosition("WIN", 1, 10.5, 10.0),  # +5% take profit
        FakePosition("LOSE", 2, 9.5, 10.0),  # -5% stop loss
        FakePosition("STALE", 1, 10.0, 10.0),  # exceeds max hold
    ]
    activities = [
        FakeActivity("buy", "STALE", 10.0, 1, now - timedelta(minutes=90), net_amount=-10, activity_id="a1")
    ]
    client = FakeClient(positions=positions, activities=activities)
    monkeypatch.setattr(pm, "client", client)

    pm.monitor_positions()

    assert set(client.closed_symbols) == {"WIN", "LOSE", "STALE"}
    with (tmp_path / "decision_log.csv").open() as f:
        rows = list(csv.DictReader(f))
    actions = {row["symbol"]: row["action"] for row in rows}
    assert actions["WIN"] == "take_profit"
    assert actions["LOSE"] == "stop_loss"
    assert actions["STALE"] == "max_hold"


def test_log_closed_trades_dedupes(monkeypatch, tmp_path, capsys):
    pm = _reload_monitor(monkeypatch, tmp_path)
    now = datetime.utcnow()
    activities = [
        FakeActivity("sell", "FAST", 9.0, 1, now - timedelta(seconds=10), net_amount="5", activity_id="abc"),
        FakeActivity("sell", "OLD", 8.0, 1, now - timedelta(minutes=10), net_amount="1", activity_id="old"),
    ]
    client = FakeClient(activities=activities)
    monkeypatch.setattr(pm, "client", client)

    pm.log_closed_trades()
    first = capsys.readouterr().out
    assert "FAST" in first
    assert "OLD" not in first  # filtered by staleness window

    pm.log_closed_trades()
    second = capsys.readouterr().out
    assert "FAST" not in second  # deduped by cache

