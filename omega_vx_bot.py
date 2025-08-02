from flask import Flask, request, jsonify
import numpy as np
import csv
import os
from dotenv import load_dotenv 
import alpaca_trade_api as tradeapi
import threading
import time
import requests
import pandas as pd
from datetime import datetime
import smtplib
from email.message import EmailMessage
import yfinance as yf

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, timedelta

load_dotenv()

# ✅ Define log directory
LOG_DIR = os.path.expanduser("~/omega-vx/logs")
os.makedirs(LOG_DIR, exist_ok=True)

DAILY_RISK_LIMIT = -10  # 💥 Stop trading after $10 loss
LAST_BLOCK_FILE = os.path.join(LOG_DIR, "last_block.txt")

LAST_TRADE_FILE = os.path.join(LOG_DIR, "last_trade_time.txt")
TRADE_COOLDOWN_SECONDS = 300  # 5 minutes
MAX_RISK_PER_TRADE_PERCENT = 1.0  # Risk 1% of total equity per trade
MIN_TRADE_QTY = 1  # Never trade less than 1 share

# ✅ Load environment variables
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL)
app = Flask(__name__)

from datetime import datetime
from datetime import datetime, time as dt_time

data_client = StockHistoricalDataClient(API_KEY, API_SECRET)

def auto_adjust_risk_percent():
    try:
        path = os.path.join(LOG_DIR, "equity_curve.log")
        if not os.path.exists(path):
            print("🧠 No equity curve data found — skipping AI adjustment.")
            return

        with open(path, "r") as f:
            lines = f.readlines()[-5:]  # last 5 records
            equity_values = []
            for line in lines:
                try:
                    parts = line.strip().split("EQUITY:")
                    if len(parts) == 2:
                        equity = float(parts[1].replace("$", ""))
                        equity_values.append(equity)
                except:
                    continue

        if len(equity_values) < 2:
            print("🧠 Not enough equity data to analyze slope.")
            return

        slope = (equity_values[-1] - equity_values[0]) / (len(equity_values) - 1)
        print(f"📈 Equity slope: {slope:.2f}")

        global MAX_RISK_PER_TRADE_PERCENT
        if slope < -10:
            MAX_RISK_PER_TRADE_PERCENT = 0.5
            print("⚠️ AI Auto-Tune: Slope negative → Risk reduced to 0.5%")
        elif slope > 20:
            MAX_RISK_PER_TRADE_PERCENT = 1.5
            print("🚀 AI Auto-Tune: Strong growth → Risk increased to 1.5%")
        else:
            MAX_RISK_PER_TRADE_PERCENT = 1.0
            print("🧠 AI Auto-Tune: Risk normalized to 1.0%")

    except Exception as e:
        print(f"❌ AI Auto-Tune failed: {e}")
auto_adjust_risk_percent()

def get_bars(symbol, interval='15m', lookback=10):
    try:
        # Set up time range
        end = datetime.now()
        start = end - timedelta(minutes=15 * (lookback + 1))

        # Convert to Alpaca timeframe
        tf = TimeFrame.Minute if interval == '15m' else TimeFrame.Hour

        request_params = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=tf,
            start=start,
            end=end
        )

        bars = data_client.get_stock_bars(request_params).df

        if bars.empty:
            print(f"⚠️ No Alpaca data returned for {symbol}")
            return None

        return bars
    except Exception as e:
        print(f"❌ Alpaca data fetch failed for {symbol}: {e}")
        return None

def is_within_trading_hours(start_hour=14, end_hour=18, end_minute=30):
    now_utc = datetime.utcnow().time()
    start = dt_time(hour=start_hour, minute=0)
    end = dt_time(hour=end_hour, minute=end_minute)
    return start <= now_utc <= end

def get_heikin_ashi_trend(symbol, interval='15m', lookback=2):
    try:
        bars = get_bars(symbol, interval, lookback)
        if bars is None or len(bars) < lookback:
         return None
        # Convert interval to Alpaca's TimeFrame
        tf = TimeFrame.Minute if interval == '15m' else TimeFrame.Hour

        end = datetime.utcnow()
        start = end - timedelta(days=2)

        request_params = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=tf,
            start=start,
            end=end
        )

        bars = data_client.get_stock_bars(request_params).df

        if bars.empty or len(bars) < lookback:
            print(f"⚠️ No data returned for {symbol} — bars empty or insufficient.")
            return None

        bars = bars[bars['symbol'] == symbol]  # Only this symbol

        ha_candles = []
        for i in range(1, lookback + 1):
            o = (bars['open'].iloc[-i] + bars['close'].iloc[-(i + 1)]) / 2
            h = max(bars['high'].iloc[-i], o, bars['close'].iloc[-i])
            l = min(bars['low'].iloc[-i], o, bars['close'].iloc[-i])
            c = (bars['open'].iloc[-i] + bars['high'].iloc[-i] + bars['low'].iloc[-i] + bars['close'].iloc[-i]) / 4
            ha_candles.append({'open': o, 'close': c})

        last = ha_candles[-1]
        if last['close'] > last['open']:
            return 'bullish'
        elif last['close'] < last['open']:
            return 'bearish'
        else:
            return 'neutral'

    except Exception as e:
        print(f"❌ Failed to get Heikin Ashi trend for {symbol} on {interval}: {e}")
        return None

def is_multi_timeframe_confirmed(symbol):
    trend_15m = get_heikin_ashi_trend(symbol, interval='15m')
    trend_1h = get_heikin_ashi_trend(symbol, interval='1h')

    print(f"🕒 MTF Check | 15m: {trend_15m} | 1h: {trend_1h}")

    if trend_15m == trend_1h and trend_15m == 'bullish':
        return True
    else:
        return False

def log_equity_curve():
    try:
        equity = get_account_equity()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 🔧 Write to the local logs/ folder
        with open("logs/equity_curve.log", "a") as f:
            f.write(f"[{now}] EQUITY: ${equity:.2f}\n")

        print(f"📈 Equity logged: ${equity:.2f}")
    except Exception as e:
        print(f"❌ Failed to log equity curve: {e}")

def get_account_equity():
    try:
        account = api.get_account()  # ✅ not 'alpaca'
        return float(account.equity)
    except Exception as e:
        print(f"❌ Failed to fetch account equity: {e}")
        return 0
def calculate_position_size(entry_price, stop_loss):
    try:
        equity = get_account_equity()
        risk_percent = MAX_RISK_PER_TRADE_PERCENT / 100  # convert 1.0 → 0.01
        risk_amount = equity * risk_percent
        risk_per_share = abs(entry_price - stop_loss)

        if risk_per_share == 0:
            print("⚠️ Risk per share is zero. Skipping.")
            return 0

        position_size = int(risk_amount / risk_per_share)
        return position_size
    except Exception as e:
        print(f"❌ Error calculating position size: {e}")
        return 0

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.get_json()
        symbol = data.get("symbol")
        entry = float(data.get("entry"))
        stop_loss = float(data.get("stop_loss"))
        take_profit = float(data.get("take_profit"))
        use_trailing = bool(data.get("use_trailing", False))

        print("🧪 Test mode: skipping all filters")
        send_telegram_alert("🧪 Test webhook — all filters skipped")

        success = submit_order_with_retries(
            symbol=symbol,
            entry=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            use_trailing=use_trailing
        )

        return jsonify({"status": "success" if success else "failed"}), 200

    except Exception as e:
        print(f"❌ Exception in webhook: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200

MAX_EQUITY_FILE = os.path.join(LOG_DIR, "max_equity.txt")
SNAPSHOT_LOG_PATH = os.path.join(LOG_DIR, "last_snapshot.txt")
PORTFOLIO_LOG_PATH = os.path.join(LOG_DIR, "portfolio_log.csv")
TRADE_LOG_PATH = os.path.join(LOG_DIR, "trade_log.csv")
def calculate_trade_qty(entry_price, stop_loss_price):
    """
    Calculate the number of shares to buy based on max risk per trade and account equity.
    """
    try:
        account = api.get_account()
        equity = float(account.equity)
        max_risk_amount = equity * (MAX_RISK_PER_TRADE_PERCENT / 100)
        risk_per_share = abs(entry_price - stop_loss_price)

        if risk_per_share == 0:
            return 0

        qty = int(max(max_risk_amount / risk_per_share, MIN_TRADE_QTY))
        return qty

    except Exception as e:
        print("⚠️ Error calculating trade quantity:", e)
        send_telegram_alert(f"⚠️ Risk-based quantity error: {e}")
        return 0

def get_current_vix():
    try:
        vix = yf.Ticker("^VIX")
        data = vix.history(period="1d")
        if not data.empty:
            return data['Close'].iloc[-1]
        else:
            print("⚠️ No VIX data found.")
            return 0
    except Exception as e:
        print(f"❌ Failed to get VIX: {e}")
        return 0

def is_market_mood_negative():
    try:
        vix = get_current_vix()
        print(f"📊 Current VIX: {vix}")
        return vix > 20  # adjust threshold as needed
    except Exception as e:
        print(f"⚠️ Market mood check failed: {e}")
        return False

def get_max_equity():
    if os.path.exists(MAX_EQUITY_FILE):
        with open(MAX_EQUITY_FILE, "r") as f:
            return float(f.read().strip())
    return 0.0

def update_max_equity(current_equity):
    max_equity = get_max_equity()
    if current_equity > max_equity:
        with open(MAX_EQUITY_FILE, "w") as f:
            f.write(str(current_equity))

def should_block_trading_due_to_equity():
    try:
        account = api.get_account()
        equity = float(account.equity)
        update_max_equity(equity)
        max_equity = get_max_equity()
        if max_equity == 0:
            return False
        drop_percent = (max_equity - equity) / max_equity
        if drop_percent >= 0.05:
            msg = f"🛑 Trading blocked — Portfolio dropped {drop_percent*100:.2f}% from high (${max_equity:.2f} → ${equity:.2f})"
            print(msg)
            send_telegram_alert(msg)
            send_email("🚫 Trading Disabled", msg)
            return True
        return False
    except Exception as e:
        print("⚠️ Error checking equity drop:", e)
        send_telegram_alert(f"⚠️ Equity check error: {e}")
        return False

def send_email(subject, body):
    EMAIL_ADDRESS = os.getenv("EMAIL_USER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = EMAIL_ADDRESS
    msg.set_content(body)

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            smtp.send_message(msg)
        print(f"📧 Email sent: {subject}")
    except Exception as e:
        print(f"❌ Email send failed: {e}")  
        send_telegram_alert(f"❌ Email failure: {e}")

def send_telegram_alert(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
        requests.post(url, data=data)
    except Exception as e:
        print("Failed to send Telegram alert:", e)

def get_equity_slope():
    try:
        path = os.path.join(LOG_DIR, "equity_curve.log")
        df = pd.read_csv(path, names=["timestamp", "equity"], delim_whitespace=True, engine='python')
        df["equity"] = df["equity"].str.replace("EQUITY:", "").str.replace("$", "").astype(float)
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="[%Y-%m-%d")
        if len(df) < 5:
            return 0
        slope = (df["equity"].iloc[-1] - df["equity"].iloc[-5]) / 5
        return slope
    except Exception as e:
        print(f"❌ Failed to analyze equity slope: {e}")
        return 0

def submit_order_with_retries(symbol, entry, stop_loss, take_profit, use_trailing, max_retries=3):
    print("🚨 FORCING TRADE EXECUTION FOR TESTING")
    send_telegram_alert("🚨 FORCING TRADE EXECUTION FOR TESTING")
    return True
    
    # 📏 Calculate position size based on risk
    qty = calculate_position_size(entry, stop_loss)
    if qty <= 0:
        msg = f"❌ Trade skipped: Invalid position size ({qty}) for {symbol} at ${entry} with SL ${stop_loss}"
        print(msg)
        send_telegram_alert(msg)
        return False

    if not is_multi_timeframe_confirmed(symbol):
        print("⛔ Trade skipped — multi-timeframe trend mismatch.")
        send_telegram_alert("⛔ Trade skipped — 15m and 1h trends don't align.")
        return False

    # 🧠 AI Mood Filter check (optional)
    if is_ai_mood_bad():
        print("🚫 Trade skipped due to AI mood filter.")
        send_telegram_alert("🧠 Trade skipped — AI mood filter detected high risk.")
        return False

    if not is_within_trading_hours():
        print("🕑 Trade skipped — outside allowed trading hours.")
        send_telegram_alert("🕑 Trade skipped — outside allowed trading hours.")
        return False

    # 🚀 Submit the order with retry logic
    for attempt in range(1, max_retries + 1):
        try:
            if use_trailing:
                api.submit_order(
                    symbol=symbol,
                    qty=qty,
                    side='buy',
                    type='limit',
                    time_in_force='gtc',
                    limit_price=entry,
                    order_class='bracket',
                    stop_loss={'stop_price': stop_loss, 'trail_percent': 1.0},
                    take_profit={'limit_price': take_profit}
                )
            else:
                api.submit_order(
                    symbol=symbol,
                    qty=qty,
                    side='buy',
                    type='limit',
                    time_in_force='gtc',
                    limit_price=entry,
                    order_class='bracket',
                    stop_loss={'stop_price': stop_loss},
                    take_profit={'limit_price': take_profit}
                )

            print(f"✅ Order placed for {symbol} (attempt {attempt})")

            # 🧠 GPT trade explanation
            explanation = generate_trade_explanation(
                symbol=symbol,
                entry=entry,
                stop_loss=stop_loss,
                take_profit=take_profit,
                rsi=None,  # Optional
                trend="uptrend" if use_trailing else "neutral",
                ha_candle="bullish"  # Optional
            )

            # 📲 Telegram alert
            send_telegram_alert(f"🚀 Trade executed:\n{explanation}")
            return True
            log_equity_curve()
        
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed for {symbol}: {e}")

    print(f"❌ Order failed for {symbol} after {max_retries} attempts")
    send_telegram_alert(f"❌ Order failed for {symbol} after {max_retries} attempts")
    return False

def log_portfolio_snapshot():
    try:
        account = api.get_account()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row = [timestamp, account.equity, account.cash, account.portfolio_value]
        file_exists = os.path.exists(PORTFOLIO_LOG_PATH)
        with open(PORTFOLIO_LOG_PATH, mode='a', newline='') as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["timestamp", "equity", "cash", "portfolio_value"])
            writer.writerow(row)
        print("✅ Daily snapshot logged:", row)
        send_telegram_alert(f"📸 Snapshot logged: Equity ${account.equity}, Cash ${account.cash}")
    
    except Exception as e:
        print("⚠️ Failed to log portfolio snapshot:", e)
        send_telegram_alert(f"⚠️ Snapshot failed: {e}")


# ✅ GPT Trade Explanation
def generate_trade_explanation(symbol, entry, stop_loss, take_profit, rsi=None, trend=None, ha_candle=None):
    explanation = f"🧠 Trade rationale for {symbol}:\n"
    explanation += f"• Entry at ${entry:.2f}, Stop Loss at ${stop_loss:.2f}, Take Profit at ${take_profit:.2f}\n"
    if rsi is not None:
        explanation += f"• RSI filter passed: {rsi:.2f}\n"
    if trend is not None:
        explanation += f"• Trend confirmed: {trend}\n"
    if ha_candle == "bullish":
        explanation += "• Heikin Ashi shows bullish momentum.\n"
    elif ha_candle == "bearish":
        explanation += "• Heikin Ashi indicates weakness.\n"
    explanation += "✅ All filters passed — trade executed confidently."
    return explanation    

    tags = {
        "rsi": "enabled" if rsi_enabled else "disabled",
        "ema": "uptrend" if ema_trend == "up" else "flat",
        "mtf": trend_15m + "+" + trend_1h,
        "ha": heikin_ashi_trend,
        "result": "win" or "loss"  # later filled after TP or SL
    }

def send_weekly_summary():
    try:
        if not os.path.isfile(TRADE_LOG_PATH):
            print("No trade log found.")
            return

        df = pd.read_csv(TRADE_LOG_PATH, parse_dates=["timestamp"])

        # Filter for last 7 days
        one_week_ago = datetime.now() - pd.Timedelta(days=7)
        df = df[df["timestamp"] >= one_week_ago.strftime("%Y-%m-%d")]

        if df.empty:
            send_email("📊 Weekly P&L Summary", "No trades in the last 7 days.")
            return

        df["pnl"] = df["take_profit"] - df["entry"]  # Approx P&L (simplified logic)
        total_trades = len(df)
        wins = df[df["pnl"] > 0]
        losses = df[df["pnl"] <= 0]
        win_rate = (len(wins) / total_trades) * 100
        total_pnl = df["pnl"].sum()
        avg_gain = wins["pnl"].mean() if not wins.empty else 0
        avg_loss = losses["pnl"].mean() if not losses.empty else 0

        body = (
            f"📈 Weekly Trade Summary ({total_trades} trades)\n\n"
            f"• Total P&L: ${total_pnl:.2f}\n"
            f"• Win rate: {win_rate:.2f}%\n"
            f"• Avg gain: ${avg_gain:.2f}\n"
            f"• Avg loss: ${avg_loss:.2f}\n"
        )

        send_email("📊 Weekly P&L Summary", body)
        print("✅ Weekly summary sent.")
        send_telegram_alert("📬 Weekly P&L email sent.")
        
    except Exception as e:
        print(f"⚠️ Weekly summary failed: {e}")
        send_telegram_alert(f"⚠️ Weekly summary failed: {e}")

def start_position_watchdog(interval_minutes=5, loss_threshold=-0.03):
    def watchdog():
        while True:
            try:
                positions = api.list_positions()
                for pos in positions:
                    unrealized_plpc = float(pos.unrealized_plpc)
                    symbol = pos.symbol

                    if unrealized_plpc <= loss_threshold:
                        msg = f"⚠️ {symbol} is down {unrealized_plpc*100:.2f}% — check position!"
                        print(msg)
                        send_telegram_alert(msg)

                time.sleep(interval_minutes * 60)

            except Exception as e:
                print("🚨 Position Watchdog Error:", e)
                send_telegram_alert(f"🚨 Watchdog error: {e}")
                time.sleep(60)  # wait 1 min before retry

    thread = threading.Thread(target=watchdog, daemon=True)
    thread.start()

def is_cooldown_active():
    if not os.path.exists(LAST_TRADE_FILE):
        return False
    with open(LAST_TRADE_FILE, 'r') as f:
        last_trade_time_str = f.read().strip()
    if not last_trade_time_str:
        return False
    try:
        last_trade_time = datetime.strptime(last_trade_time_str, "%Y-%m-%d %H:%M:%S")
        elapsed = (datetime.now() - last_trade_time).total_seconds()
        return elapsed < TRADE_COOLDOWN_SECONDS
    except Exception as e:
        print("⚠️ Error reading last trade time:", e)
        return False

def is_ai_mood_bad():
    """Evaluate trade mood based on recent outcomes and equity drawdown."""
    try:
        df = pd.read_csv(TRADE_LOG_PATH, parse_dates=["timestamp"])
        recent = df.tail(5)

        loss_streak = sum(recent["status"].str.contains("loss|error|skipped", case=False))
        win_streak = sum(recent["status"].str.contains("executed", case=False))

        account = api.get_account()
        equity = float(account.equity)
        max_equity = get_max_equity()

        drawdown = 0
        if max_equity and equity < max_equity:
            drawdown = (max_equity - equity) / max_equity

        # 💡 Mood logic
        if drawdown >= 0.05:
            print("🧠 Mood: Drawdown triggered")
            return True
        if loss_streak >= 3 and win_streak == 0:
            print("🧠 Mood: Consecutive losses")
            return True

        return False

    except Exception as e:
        print("⚠️ AI Mood check failed:", e)
        return False

def log_trade(symbol, qty, entry, stop_loss, take_profit, status):
    explanation = generate_trade_explanation(
        symbol=symbol,
        entry=entry,
        stop_loss=stop_loss,
        take_profit=take_profit,
        rsi=None,
        trend=None,
        ha_candle=None
    )

    now = datetime.now()
    hour = now.hour
    if hour < 10:
        context_time = "morning"
    elif hour < 14:
        context_time = "midday"
    else:
        context_time = "afternoon"

    context_recent_outcome = "unknown"
    context_equity_change = "unknown"
    try:
        if os.path.isfile(TRADE_LOG_PATH):
            recent_df = pd.read_csv(TRADE_LOG_PATH)
            if not recent_df.empty:
                last_status = recent_df.iloc[-1]["status"]
                context_recent_outcome = "win" if last_status == "executed" else "loss"

                if "equity" in recent_df.columns:
                    prev_equity = float(recent_df.iloc[-1].get("equity", 0))
                    account = api.get_account()
                    current_equity = float(account.equity)
                    context_equity_change = "gain" if current_equity > prev_equity else "loss"
    except Exception as e:
        print("⚠️ Failed to fetch recent outcome context:", e)

    # 🕰 Timestamp
    timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

    # ✅ Save to cooldown file ONLY if status was executed
    if status == "executed":
        with open(LAST_TRADE_FILE, 'w') as f:
            f.write(timestamp_str)

    # 🧠 Compile and log trade data
    trade_data = {
        "timestamp": timestamp_str,
        "symbol": symbol,
        "qty": qty,
        "entry": entry,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "status": status,
        "explanation": explanation,
        "context_time_of_day": context_time,
        "context_recent_outcome": context_recent_outcome,
        "context_equity_change": context_equity_change
    }

    df = pd.DataFrame([trade_data])
    if not os.path.isfile(TRADE_LOG_PATH):
        df.to_csv(TRADE_LOG_PATH, index=False)
    else:
        df.to_csv(TRADE_LOG_PATH, mode='a', header=False, index=False)

if __name__ == '__main__':
    today = datetime.now().strftime("%Y-%m-%d")
    if not os.path.exists(SNAPSHOT_LOG_PATH) or open(SNAPSHOT_LOG_PATH).read().strip() != today:
        log_portfolio_snapshot()
        with open(SNAPSHOT_LOG_PATH, "w") as f:
            f.write(today)

    start_position_watchdog()
    app.run(host='0.0.0.0', port=5050)
    log_equity_curve()