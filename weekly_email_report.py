from omega_vx_bot import send_email
import csv
from datetime import datetime, timedelta

def analyze_weekly_trades():
    filename = "trade_log.csv"
    today = datetime.now().date()
    week_ago = today - timedelta(days=7)

    total_trades = 0
    wins = 0
    losses = 0
    gain = 0.0

    try:
        with open(filename, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                timestamp = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S").date()
                if timestamp >= week_ago:
                    total_trades += 1
                    status = row["status"]
                    entry = float(row["entry"])
                    take_profit = float(row["take_profit"])
                    stop_loss = float(row["stop_loss"])

                    if status == "executed":
                        # Approximate win if TP > SL by ratio
                        rr_ratio = (take_profit - entry) / abs(entry - stop_loss)
                        if rr_ratio >= 1:
                            wins += 1
                            gain += rr_ratio
                        else:
                            losses += 1
                            gain -= 1
        return total_trades, wins, losses, gain
    except Exception as e:
        print("⚠️ Failed to analyze weekly trades:", e)
        return 0, 0, 0, 0

def send_weekly_report():
    total, wins, losses, gain = analyze_weekly_trades()
    win_rate = (wins / total) * 100 if total > 0 else 0
    subject = "📊 OMEGA-VX Weekly Summary"
    body = f"""
📆 Weekly Trade Summary

🔢 Total Trades: {total}
✅ Wins: {wins}
❌ Losses: {losses}
🏆 Win Rate: {win_rate:.2f}%
📈 Estimated Gain Score: {gain:.2f}

– OMEGA-VX
"""
    send_email(subject, body)

if __name__ == "__main__":
    send_weekly_report()
