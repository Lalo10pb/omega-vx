from trade_performance import analyze_trade_performance

from omega_vx.notifications import send_email

# Generate report and read summary
analyze_trade_performance()
with open("performance_summary.txt", "r") as f:
    content = f.read()

send_email("📊 Weekly Trade Performance", content)
