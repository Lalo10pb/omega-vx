import csv
from datetime import datetime
import alpaca_trade_api as tradeapi
import time

API_KEY = 'YOUR_API_KEY'
API_SECRET = 'YOUR_API_SECRET'
BASE_URL = 'https://paper-api.alpaca.markets'

api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version='v2')

def log_exit(symbol, qty, entry_price, exit_price, stop_loss, take_profit, reason):
    pnl = (exit_price - entry_price) * qty
    pnl_percent = ((exit_price - entry_price) / entry_price) * 100
    with open('performance_log.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        if file.tell() == 0:
            writer.writerow(['timestamp', 'symbol', 'qty', 'entry_price', 'exit_price', 'pnl', 'pnl_%', 'stop_loss', 'take_profit', 'reason'])
        writer.writerow([
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            symbol,
            qty,
            entry_price,
            exit_price,
            round(pnl, 2),
            f"{pnl_percent:.2f}%",
            stop_loss,
            take_profit,
            reason
        ])

def monitor_closed_positions():
    print("🔍 Tracking closed positions...")
    recorded = set()

    while True:
        try:
            closed = api.list_positions()
            for pos in closed:
                if pos.unrealized_pl == '0.00' and pos.symbol not in recorded:
                    entry_price = float(pos.avg_entry_price)
                    exit_price = float(pos.current_price)
                    qty = float(pos.qty)
                    symbol = pos.symbol
                    reason = 'manual or unknown exit'
                    log_exit(symbol, qty, entry_price, exit_price, 'N/A', 'N/A', reason)
                    recorded.add(symbol)
                    print(f"✅ Logged exit for {symbol}")
        except Exception as e:
            print("Error:", e)

        time.sleep(15)
