import time
import csv
from datetime import datetime
from alpaca_trade_api.rest import REST

API_KEY = "PKSJWB3PTLU4VIUEBMAR"
API_SECRET = "0k0o4OAZsdjLaKK1mUBfQ5AzEV5Vh9gbMR46lb6N"
BASE_URL = "https://paper-api.alpaca.markets"

api = REST(API_KEY, API_SECRET, BASE_URL)

# ✅ Log completed trades to CSV
def log_filled_trade(symbol, qty, entry_price, exit_price, pl_dollars, pl_percent, reason):
    file_exists = False
    try:
        with open('filled_trades.csv', 'r'): file_exists = True
    except FileNotFoundError:
        pass

    with open('filled_trades.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(['timestamp', 'symbol', 'qty', 'entry_price', 'exit_price', 'P/L $', 'P/L %', 'exit_reason'])
        writer.writerow([
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            symbol,
            qty,
            entry_price,
            exit_price,
            round(pl_dollars, 2),
            round(pl_percent, 2),
            reason
        ])

# ✅ Monitor positions for closure
def monitor_and_log_closed_trades():
    print("📉 Monitoring for closed positions with realized P/L...")
    tracked = {}

    while True:
        try:
            positions = api.list_positions()
            current = {p.symbol: p for p in positions if float(p.qty) > 0}

            # Check for symbols that disappeared (i.e. closed)
            closed_symbols = [s for s in tracked if s not in current]

            for symbol in closed_symbols:
                trade = tracked[symbol]
                qty = float(trade['qty'])
                entry_price = float(trade['entry'])
                exit_price = float(api.get_latest_trade(symbol).price)
                pl_dollars = (exit_price - entry_price) * qty if trade['side'] == 'long' else (entry_price - exit_price) * qty
                pl_percent = (pl_dollars / (entry_price * qty)) * 100

                # Determine reason
                reason = "manual/unknown"
                if trade['target'] and abs(exit_price - float(trade['target'])) < 0.05:
                    reason = "TP hit"
                elif trade['stop'] and abs(exit_price - float(trade['stop'])) < 0.05:
                    reason = "SL hit"

                log_filled_trade(symbol, qty, entry_price, exit_price, pl_dollars, pl_percent, reason)
                print(f"✅ {symbol} closed. P/L: ${pl_dollars:.2f} ({pl_percent:.2f}%) — {reason}")

                del tracked[symbol]

            # Update tracked
            for symbol, pos in current.items():
                if symbol not in tracked:
                    tracked[symbol] = {
                        'qty': pos.qty,
                        'entry': pos.avg_entry_price,
                        'side': pos.side,
                        'target': None,
                        'stop': None
                    }

            time.sleep(10)
        except Exception as e:
            print("⚠️ Error in closed trade monitor:", e)
            time.sleep(10)


if __name__ == '__main__':
    monitor_and_log_closed_trades()
