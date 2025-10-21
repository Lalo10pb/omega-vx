from __future__ import annotations

import logging
import time
from types import SimpleNamespace
from typing import Dict, Optional

from ib_insync import IB, Stock, MarketOrder, LimitOrder, StopOrder


class IBKRAdapter:
    """
    Lightweight wrapper around ib_insync so Omega can talk to IBKR without
    touching the raw API everywhere. Supports the handful of account and order
    calls Omega needs (market BUY/SELL, optional TP/SL attachments, closes).
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 7497, client_id: int = 1):
        self.ib = IB()
        self.host = host
        self.port = port
        self.client_id = client_id
        self.logger = logging.getLogger("IBKR")
        self.connected = False

    # ------------------------------------------------------------------ connect
    def connect(self) -> bool:
        if self.connected:
            return True
        try:
            self.ib.connect(self.host, self.port, clientId=self.client_id)
            self.connected = True
            self.logger.info("✅ Connected to IBKR Gateway/TWS.")
            return True
        except Exception as exc:  # pragma: no cover - network/permission errors
            self.logger.error(f"❌ IBKR connection failed: {exc}")
            self.connected = False
            return False

    # -------------------------------------------------------- account snapshot
    def get_account_summary(self) -> Dict[str, float]:
        """
        Return a dict containing equity, cash, buying power, etc.
        Mirrors the fields Omega expects from the Alpaca account object.
        """
        if not self.connect():
            return {}
        try:
            summary = self.ib.accountSummary()
            data = {item.tag: item.value for item in summary}
            buying_power = float(data.get("BuyingPower", 0) or 0)
            cash = float(data.get("AvailableFunds", data.get("TotalCashValue", 0) or 0))
            equity = float(data.get("NetLiquidation", 0) or 0)
            return {
                "equity": equity,
                "cash": cash,
                "buying_power": buying_power or cash,
                "multiplier": 1.0,  # Cash / IBKR Lite accounts = 1x
                "broker": "IBKR",
            }
        except Exception as exc:
            self.logger.error(f"⚠️ Failed to fetch IBKR account summary: {exc}")
            return {}

    # -------------------------------------------------------------- place order
    def place_order(
        self,
        symbol: str,
        qty: float,
        side: str = "buy",
        order_type: str = "market",
        limit_price: Optional[float] = None,
        tp: Optional[float] = None,
        sl: Optional[float] = None,
    ):
        """
        Submit a market/limit order. Optional tp/sl are implemented as separate
        child orders (good enough for cash accounts which don't support server
        brackets).
        """
        if not self.connect():
            return None
        try:
            contract = Stock(symbol.upper(), "SMART", "USD")
            action = "BUY" if side.lower() == "buy" else "SELL"
            if order_type.lower() == "limit" and limit_price is not None:
                order = LimitOrder(action, qty, limit_price)
            else:
                order = MarketOrder(action, qty)
            trade = self.ib.placeOrder(contract, order)
            self.logger.info(f"🚀 Submitted {action} {symbol} x{qty}")
            self._sleep_until_filled(trade)
            if tp:
                tp_order = LimitOrder("SELL", qty, tp)
                self.ib.placeOrder(contract, tp_order)
            if sl:
                sl_order = StopOrder("SELL", qty, sl)
                self.ib.placeOrder(contract, sl_order)
            if tp or sl:
                self.logger.info(f"🛡️ TP/SL placed for {symbol} (tp={tp}, sl={sl})")
            return trade
        except Exception as exc:
            self.logger.error(f"❌ Order failed for {symbol}: {exc}")
            return None

    # -------------------------------------------------------------- close order
    def close_position(self, symbol: str) -> bool:
        """
        Market-close any long position on the symbol.
        """
        if not self.connect():
            return False
        try:
            symbol = symbol.upper()
            for pos in self.ib.positions():
                if pos.contract.symbol.upper() == symbol:
                    qty = pos.position
                    if qty > 0:
                        order = MarketOrder("SELL", qty)
                        self.ib.placeOrder(pos.contract, order)
                        self.logger.info(f"🧹 Closed {symbol} position ({qty}).")
                        return True
            return False
        except Exception as exc:
            self.logger.error(f"⚠️ Close position failed for {symbol}: {exc}")
            return False

    # ---------------------------------------------------------- utility helpers
    def positions(self):
        if not self.connect():
            return []
        return self.ib.positions()

    def open_orders(self):
        if not self.connect():
            return []
        return self.ib.openOrders()

    def build_position_namespace(self, position) -> SimpleNamespace:
        """
        Convert ib_insync Position to a namespace Omega can reason about.
        """
        contract = position.contract
        qty = float(position.position)
        avg_cost = float(position.avgCost or 0)
        return SimpleNamespace(
            symbol=contract.symbol.upper(),
            qty=qty,
            qty_available=qty,
            avg_entry_price=avg_cost if avg_cost else 0.0,
            current_price=avg_cost,
            unrealized_plpc=0.0,
        )

    # -------------------------------------------------------------- private API
    def _sleep_until_filled(self, trade, timeout: float = 5.0):
        """
        Basic wait loop so we have a fill before attaching TP/SL.
        """
        start = time.time()
        while time.time() - start < timeout:
            self.ib.sleep(0.25)
            if trade.orderStatus.status in {"Filled", "Cancelled"}:
                return
