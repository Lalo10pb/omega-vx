import types

import pytest

import omega_vx_bot as bot


def _stub_account(equity: float) -> types.SimpleNamespace:
    return types.SimpleNamespace(equity=str(equity))


@pytest.fixture(autouse=True)
def reset_equity_guard_defaults(monkeypatch, tmp_path):
    tmp_file = tmp_path / "max_equity.txt"
    tmp_file.write_text("0")
    monkeypatch.setattr(bot, "MAX_EQUITY_FILE", str(tmp_file), raising=False)
    monkeypatch.setattr(bot, "EQUITY_GUARD_MAX_EQUITY_FLOOR", 0.0, raising=False)
    monkeypatch.setattr(bot, "EQUITY_GUARD_STALE_RATIO", 0.0, raising=False)
    monkeypatch.setattr(bot, "_update_day_trade_status_from_account", lambda account: (None, None), raising=False)
    yield


def test_equity_guard_triggers_when_drawdown_exceeds_threshold(monkeypatch):
    alerts = []
    monkeypatch.setattr(bot, "send_telegram_alert", lambda msg: alerts.append(("tg", msg)), raising=False)
    monkeypatch.setattr(bot, "send_email", lambda subject, body: alerts.append(("email", subject)), raising=False)
    monkeypatch.setattr(bot, "EQUITY_GUARD_MIN_DRAWDOWN", 0.05, raising=False)
    monkeypatch.setattr(bot, "EQUITY_DRAWDOWN_MAX_PCT", 0.0, raising=False)
    monkeypatch.setattr(bot, "_safe_get_account", lambda timeout=6.0: _stub_account(900.0), raising=False)

    # Seed previous max equity higher than current equity
    bot._write_max_equity(1000.0)

    assert bot.should_block_trading_due_to_equity() is True
    assert any(tag == "tg" for tag, _ in alerts)
    assert any(tag == "email" for tag, _ in alerts)


def test_equity_guard_updates_baseline_when_equity_improves(monkeypatch):
    alerts = []
    monkeypatch.setattr(bot, "send_telegram_alert", lambda msg: alerts.append(("tg", msg)), raising=False)
    monkeypatch.setattr(bot, "send_email", lambda subject, body: alerts.append(("email", subject)), raising=False)
    monkeypatch.setattr(bot, "EQUITY_GUARD_MIN_DRAWDOWN", 0.10, raising=False)
    monkeypatch.setattr(bot, "EQUITY_DRAWDOWN_MAX_PCT", 0.0, raising=False)
    monkeypatch.setattr(bot, "_safe_get_account", lambda timeout=6.0: _stub_account(1200.0), raising=False)

    bot._write_max_equity(1100.0)

    assert bot.should_block_trading_due_to_equity() is False
    # Guard should update the stored max equity to the new higher value.
    assert pytest.approx(bot.get_max_equity(), rel=1e-6) == 1200.0
    assert alerts == []
