import csv
from pathlib import Path

import account_logging as al


class DummyAccount:
    def __init__(self, equity: float, cash: float, portfolio_value: float):
        self.equity = equity
        self.cash = cash
        self.portfolio_value = portfolio_value


class DummyClient:
    def __init__(self, account: DummyAccount):
        self._account = account

    def get_account(self) -> DummyAccount:
        return self._account


def _prime_tmp_paths(tmp_path, monkeypatch):
    portfolio = tmp_path / "portfolio_log.csv"
    equity = tmp_path / "equity_curve.log"
    monkeypatch.setattr(al, "PORTFOLIO_LOG_PATH", portfolio, raising=False)
    monkeypatch.setattr(al, "EQUITY_CURVE_LOG_PATH", equity, raising=False)
    return portfolio, equity


def test_record_account_snapshot_writes_both_logs(tmp_path, monkeypatch):
    portfolio_path, equity_path = _prime_tmp_paths(tmp_path, monkeypatch)
    account = DummyAccount(equity=10234.56, cash=5000.0, portfolio_value=15234.56)
    monkeypatch.setattr(al, "get_trading_client", lambda: DummyClient(account), raising=False)

    snapshot = al.record_account_snapshot()

    assert portfolio_path.exists()
    assert equity_path.exists()
    assert snapshot.equity == account.equity

    with portfolio_path.open() as handle:
        rows = list(csv.reader(handle))
    assert rows[0] == al.PORTFOLIO_LOG_HEADER
    assert rows[1][1:4] == [str(account.equity), str(account.cash), str(account.portfolio_value)]

    contents = equity_path.read_text().strip()
    assert "EQUITY: $10234.56" in contents


def test_append_equity_curve_with_override_values(tmp_path, monkeypatch):
    _, equity_path = _prime_tmp_paths(tmp_path, monkeypatch)
    al.append_equity_curve(timestamp="2025-11-15 12:00:00", equity=12345.67)
    assert "2025-11-15 12:00:00" in equity_path.read_text()
