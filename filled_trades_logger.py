import csv
import os
import time
from datetime import datetime

from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest
from alpaca.data.enums import DataFeed

load_dotenv()

API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
PAPER_MODE = str(os.getenv("ALPACA_PAPER", "true")).strip().lower() in ("1", "true", "yes")

trading_client = TradingClient(API_KEY, API_SECRET, paper=PAPER_MODE)
data_client = StockHistoricalDataClient(API_KEY, API_SECRET)
DATA_FEED = DataFeed.IEX

LOG_PATH = os.path.join(os.path.dirname(__file__), "filled_trades.csv")


def log_filled_trade(symbol, qty, entry_price, exit_price, pl_dollars, pl_percent, reason):
    file_exists = os.path.exists(LOG_PATH)
    with open(LOG_PATH, "a", newline="") as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow([
                "timestamp",
                "symbol",
                "qty",
                "entry_price",
                "exit_price",
                "P/L $",
                "P/L %",
                "exit_reason",
            ])
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            symbol,
            qty,
            entry_price,
            exit_price,
            round(pl_dollars, 2),
            round(pl_percent, 2),
            reason,
        ])


def latest_trade_price(symbol: str) -> float:
    try:
        req = StockLatestTradeRequest(symbol_or_symbols=symbol, feed=DATA_FEED)
        trade = data_client.get_stock_latest_trade(req)
        info = trade[symbol]
        return float(getattr(info, "price", 0) or 0)
    except Exception as e:
        print(f"⚠️ Failed to fetch latest trade for {symbol}: {e}")
        return 0.0


def monitor_and_log_closed_trades():
    print("📉 Monitoring for closed positions with realized P/L...")
    tracked: dict[str, dict] = {}

    while True:
        try:
            positions = trading_client.get_all_positions()
            current = {p.symbol: p for p in positions if float(p.qty) > 0}

            closed_symbols = [symbol for symbol in tracked if symbol not in current]

            for symbol in closed_symbols:
                trade = tracked.pop(symbol, None)
                if not trade:
                    continue

                qty = float(trade["qty"])
                entry_price = float(trade["entry"])
                side = trade["side"]
                exit_price = latest_trade_price(symbol)
                if exit_price == 0:
                    continue

                if side == "long":
                    pl_dollars = (exit_price - entry_price) * qty
                else:
                    pl_dollars = (entry_price - exit_price) * qty
                pl_percent = (pl_dollars / (entry_price * qty)) * 100 if entry_price and qty else 0

                reason = "manual/unknown"
                target = trade.get("target")
                stop = trade.get("stop")
                if target and abs(exit_price - float(target)) < 0.05:
                    reason = "TP hit"
                elif stop and abs(exit_price - float(stop)) < 0.05:
                    reason = "SL hit"

                log_filled_trade(symbol, qty, entry_price, exit_price, pl_dollars, pl_percent, reason)
                print(f"✅ {symbol} closed. P/L: ${pl_dollars:.2f} ({pl_percent:.2f}%) — {reason}")

            for symbol, pos in current.items():
                if symbol not in tracked:
                    tracked[symbol] = {
                        "qty": pos.qty,
                        "entry": pos.avg_entry_price,
                        "side": pos.side,
                        "target": None,
                        "stop": None,
                    }

            time.sleep(10)
        except Exception as e:
            print("⚠️ Error in closed trade monitor:", e)
            time.sleep(10)


if __name__ == "__main__":
    monitor_and_log_closed_trades()
