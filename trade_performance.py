import pandas as pd
from datetime import datetime

def analyze_trade_performance(log_path="trade_log.csv"):
    try:
        df = pd.read_csv(log_path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        executed_trades = df[df['status'] == 'executed']
        skipped_trades = df[df['status'].str.contains("skipped", na=False)]

        total = len(df)
        executed = len(executed_trades)
        skipped = len(skipped_trades)

        win_count = 0
        loss_count = 0
        rr_ratios = []

        for _, row in executed_trades.iterrows():
            entry = float(row['entry'])
            sl = float(row['stop_loss'])
            tp = float(row['take_profit'])

            if abs(entry - sl) == 0:
                continue

            rr = abs(tp - entry) / abs(entry - sl)
            rr_ratios.append(rr)

            if rr >= 1.5:
                win_count += 1
            else:
                loss_count += 1

        avg_rr = sum(rr_ratios) / len(rr_ratios) if rr_ratios else 0
        win_rate = (win_count / executed) * 100 if executed > 0 else 0

        summary = [
            "🔍 TRADE PERFORMANCE ANALYSIS",
            f"📅 Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"📊 Total trades: {total}",
            f"✅ Executed trades: {executed}",
            f"❌ Skipped trades: {skipped}",
            f"🏆 Wins (RR ≥ 1.5): {win_count}",
            f"💥 Losses (RR < 1.5): {loss_count}",
            f"🎯 Win rate: {win_rate:.2f}%",
            f"📈 Avg Risk/Reward: {avg_rr:.2f}",
        ]

        print("\n".join(summary))

        with open("performance_summary.txt", "w") as f:
            f.write("\n".join(summary))

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    analyze_trade_performance()
