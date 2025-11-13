import csv
import threading
import sys
from types import ModuleType

import pytest

stub = ModuleType("ib_insync")
stub.IB = object
stub.Stock = object
stub.MarketOrder = object
stub.LimitOrder = object
stub.StopOrder = object
sys.modules.setdefault("ib_insync", stub)

import omega_vx_bot as bot
import backfill_filled_trades as backfill


def test_append_filled_trade_entry_computes_pnl(tmp_path, monkeypatch):
    filled_path = tmp_path / "filled_trades.csv"
    monkeypatch.setattr(bot, "FILLED_TRADES_PATH", str(filled_path))
    monkeypatch.setattr(bot, "_FILLED_TRADES_LOCK", threading.Lock())

    bot._append_filled_trade_entry(
        timestamp="2025-01-01 10:00:00",
        symbol="AAPL",
        qty=2,
        entry_price=100,
        exit_price=102,
        realized_pnl=None,
        exit_reason="unit-test",
        activity_id="abc123",
    )

    with open(filled_path) as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "AAPL"
    assert float(row["P/L $"]) == pytest.approx(4.0)
    assert row["activity_id"] == "abc123"


def test_backfill_skips_duplicates(tmp_path, monkeypatch):
    trade_log = tmp_path / "trade_log.csv"
    filled = tmp_path / "filled_trades.csv"
    monkeypatch.setattr(backfill, "TRADE_LOG", trade_log)
    monkeypatch.setattr(backfill, "FILLED_TRADES", filled)

    with open(trade_log, "w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "timestamp",
                "symbol",
                "qty",
                "entry",
                "take_profit",
                "status",
                "action",
                "fill_price",
                "realized_pnl",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "timestamp": "2025-01-01 10:00:00",
                "symbol": "MSFT",
                "qty": "5",
                "entry": "300",
                "take_profit": "305",
                "status": "closed",
                "action": "SELL",
                "fill_price": "304",
                "realized_pnl": "",
            }
        )

    added_first = backfill.backfill()
    added_second = backfill.backfill()

    assert added_first == 1
    assert added_second == 0

    with open(filled) as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["symbol"] == "MSFT"
