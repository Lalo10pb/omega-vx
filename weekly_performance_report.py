from omega_vx_bot import send_email
from trade_performance import analyze_trade_performance

# Generate report and read summary
analyze_trade_performance()
with open("performance_summary.txt", "r") as f:
    content = f.read()

send_email("📊 Weekly Trade Performance", content)
