import time
import os
from datetime import datetime
import pandas as pd

TRADE_LOG = 'trade_log.csv'

def clear_screen():
    os.system('clear' if os.name == 'posix' else 'cls')

def display_dashboard():
    clear_screen()
    print("📊 OMEGA-VX DASHBOARD —", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    if not os.path.isfile(TRADE_LOG):
        print("No trades logged yet.")
        return

    df = pd.read_csv(TRADE_LOG)
    if df.empty:
        print("Trade log is empty.")
        return

    df = df.tail(10)  # Show only last 10 trades
    print("\nLast Trades:")
    print(df.to_string(index=False))

while True:
    display_dashboard()
    time.sleep(10)
