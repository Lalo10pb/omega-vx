import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
import requests

# ✅ Load environment variables
load_dotenv()
LOG_FILE = "trade_log.csv"

# ✅ Telegram details
bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
chat_id = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_report(message):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = {"chat_id": chat_id, "text": message}
    try:
        requests.post(url, data=data)
    except Exception as e:
        print("⚠️ Failed to send Telegram report:", e)

def generate_daily_report():
    try:
        df = pd.read_csv(LOG_FILE)

        # 📅 Filter today's trades
        today = datetime.now().strftime("%Y-%m-%d")
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df_today = df[df["timestamp"].dt.strftime("%Y-%m-%d") == today]

        if df_today.empty:
            print("📭 No trades today.")
            send_telegram_report("📭 No trades today.")
            return

        executed = df_today[df_today["status"].str.contains("executed|error|closed", case=False)]

        message = f"📊 *Daily Report — {today}*\n\n"
        for _, row in executed.iterrows():
            line = f"{row['timestamp'].split()[1]} | {row['symbol']} x{row['qty']} @ ${row['entry']} → {row['status']}"
            message += line + "\n"

        message += f"\nTotal Executed Trades: {len(executed)}"
        print(message)
        send_telegram_report(message)

    except Exception as e:
        print("⚠️ Error generating report:", e)

# ✅ Run report
generate_daily_report()