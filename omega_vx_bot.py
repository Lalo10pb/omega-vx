from flask import Flask, request, jsonify
import numpy as np
import csv
import os
from dotenv import load_dotenv 
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, StopLossRequest, TakeProfitRequest
from alpaca.trading.enums import OrderSide, TimeInForce
import threading
import time
import requests
import pandas as pd
from datetime import datetime
import smtplib
from email.message import EmailMessage
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, timedelta
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType
import functools
print = functools.partial(print, flush=True)
load_dotenv()

# ✅ Define log directory
LOG_DIR = os.path.expanduser("~/omega-vx/logs")
os.makedirs(LOG_DIR, exist_ok=True)
CRASH_LOG_FILE = os.path.join(LOG_DIR, "last_boot.txt")


DAILY_RISK_LIMIT = -10  # 💥 Stop trading after $10 loss
LAST_BLOCK_FILE = os.path.join(LOG_DIR, "last_block.txt")

WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN")

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

trading_client = TradingClient(API_KEY, API_SECRET, paper=True)
app = Flask(__name__)

from datetime import datetime
from datetime import datetime, time as dt_time

data_client = StockHistoricalDataClient(API_KEY, API_SECRET)

def handle_restart_notification():
    now = datetime.now()
    try:
        if os.path.exists(CRASH_LOG_FILE):
            with open(CRASH_LOG_FILE, 'r') as f:
                last_boot = f.read().strip()
                if last_boot:
                    last_time = datetime.strptime(last_boot, "%Y-%m-%d %H:%M:%S")
                    diff = (now - last_time).total_seconds() / 60
                    msg = f"♻️ OMEGA-VX restarted — last boot was {diff:.1f} minutes ago."
                    print(msg)
                    send_telegram_alert(msg)
        else:
            print("🆕 First boot — no prior crash log found.")

    except Exception as e:
        print(f"⚠️ Crash log check failed: {e}")

    # ✅ Update the file with the current boot time
    try:
        with open(CRASH_LOG_FILE, 'w') as f:
            f.write(now.strftime("%Y-%m-%d %H:%M:%S"))
    except Exception as e:
        print(f"⚠️ Failed to write crash log: {e}")

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
        # 🕒 Set correct timeframe for Alpaca
        tf = TimeFrame.Minute if interval == '15m' else TimeFrame.Hour

        end = datetime.utcnow()
        start = end - timedelta(days=2)

        # 📈 Fetch historical bars from Alpaca
        request_params = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=tf,
            start=start,
            end=end
        )

        bars = data_client.get_stock_bars(request_params).df

        if bars.empty or symbol not in bars.index.get_level_values(0):
            print(f"⚠️ No data returned for {symbol}.")
            return None

        # 🧼 Filter and reset index
        bars = bars.loc[symbol].reset_index()

        if len(bars) < lookback + 1:
            print(f"⚠️ Not enough data to compute Heikin Ashi for {symbol}.")
            return None

        # 🔥 Calculate Heikin Ashi candles
        ha_candles = []
        for i in range(1, lookback + 1):
            prev_close = bars['close'].iloc[-(i + 1)]
            curr_open = bars['open'].iloc[-i]
            ha_open = (curr_open + prev_close) / 2
            ha_close = (bars['open'].iloc[-i] + bars['high'].iloc[-i] +
                        bars['low'].iloc[-i] + bars['close'].iloc[-i]) / 4

            ha_candles.append({'open': ha_open, 'close': ha_close})

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
        account = trading_client.get_account()  # ✅ from alpaca-py
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
        print(f"📥 Webhook received: {data}", flush=True)

        # 🔐 Step 1: Validate the secret token
        if not data or 'token' not in data or data['token'] != WEBHOOK_SECRET_TOKEN:
            print("🚫 Unauthorized webhook attempt blocked.", flush=True)
            send_telegram_alert("🚫 Unauthorized webhook attempt blocked.")
            return jsonify({"status": "unauthorized"}), 403

        # 🔄 Step 2: Extract and process trade data
        symbol = data.get("symbol")
        entry = float(data.get("entry"))
        stop_loss = float(data.get("stop_loss"))
        take_profit = float(data.get("take_profit"))
        use_trailing = bool(data.get("use_trailing", False))

        print(f"🔄 Calling submit_order_with_retries for {symbol}", flush=True)

        try:
            success = submit_order_with_retries(
                symbol=symbol,
                entry=entry,
                stop_loss=stop_loss,
                take_profit=take_profit,
                use_trailing=use_trailing
            )
        except Exception as trade_error:
            print(f"💥 Exception during submit_order_with_retries: {trade_error}", flush=True)
            send_telegram_alert(f"💥 submit_order_with_retries error: {trade_error}")
            return jsonify({"status": "error", "message": str(trade_error)}), 500

        print(f"✅ Trade result: {'Success' if success else 'Failed'}", flush=True)
        return jsonify({"status": "success" if success else "failed"}), 200

    except Exception as e:
        print(f"❌ Exception in webhook: {e}", flush=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200

MAX_EQUITY_FILE = os.path.join(LOG_DIR, "max_equity.txt")
SNAPSHOT_LOG_PATH = os.path.join(LOG_DIR, "last_snapshot.txt")
PORTFOLIO_LOG_PATH = os.path.join(LOG_DIR, "portfolio_log.csv")
TRADE_LOG_PATH = os.path.join(LOG_DIR, "trade_log.csv")

def calculate_trade_qty(entry_price, stop_loss_price):
    try:
        account = trading_client.get_account()  # ✅ Updated
        equity = float(account.equity)
        max_risk_amount = equity * (MAX_RISK_PER_TRADE_PERCENT / 100)
        risk_per_share = abs(entry_price - stop_loss_price)

        print(f"🧮 DEBUG:")
        print(f"  • Account equity: {equity}")
        print(f"  • Max risk per trade: {MAX_RISK_PER_TRADE_PERCENT}% → {max_risk_amount}")
        print(f"  • Risk per share: {risk_per_share}")

        if risk_per_share == 0:
            print("⚠️ Risk per share is 0 — invalid stop loss?")
            return 0

        qty = int(max(max_risk_amount / risk_per_share, MIN_TRADE_QTY))
        print(f"  • Final calculated qty: {qty}")
        return qty

    except Exception as e:
        print("⚠️ Error calculating trade quantity:", e)
        send_telegram_alert(f"⚠️ Risk-based quantity error: {e}")
        return 0

def get_current_vix():
    try:
        client = StockHistoricalDataClient(API_KEY, API_SECRET)
        request = StockLatestQuoteRequest(symbol_or_symbols=["VIXY"])  # fallback ETF if ^VIX is not supported
        response = client.get_stock_latest_quote(request)
        quote = response["VIXY"]

        vix_price = quote.ask_price or quote.bid_price
        if vix_price:
            return float(vix_price)
        else:
            print("⚠️ No VIXY quote data available.")
            return 0
    except Exception as e:
        print(f"❌ Failed to get VIXY data: {e}")
        return 0

def get_current_vix():
    try:
        request_params = StockBarsRequest(
            symbol_or_symbols="^VIX",
            timeframe=TimeFrame.Day,
            start=datetime.utcnow() - timedelta(days=5),
            end=datetime.utcnow()
        )

        bars = data_client.get_stock_bars(request_params).df
        bars = bars[bars['symbol'] == "^VIX"]

        if not bars.empty:
            vix_value = bars['close'].iloc[-1]
            print(f"📊 VIX fetched from Alpaca: {vix_value}")
            return vix_value
        else:
            print("⚠️ No VIX data found.")
            return 0
    except Exception as e:
        print(f"❌ Failed to get VIX: {e}")
        return 0

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
        account = trading_client.get_account()  # ✅ Updated
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

def get_rsi_value(symbol, interval='15m', period=14):
    try:
        # Convert interval to Alpaca's TimeFrame
        tf = TimeFrame.Minute if interval == '15m' else TimeFrame.Hour

        end = datetime.utcnow()
        start = end - timedelta(days=5)  # Enough data for RSI

        bars_request = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=tf,
            start=start,
            end=end
        )

        bars = data_client.get_stock_bars(bars_request).df

        if bars.empty or len(bars) < period:
            print(f"⚠️ Not enough data to calculate RSI for {symbol}")
            return None

        bars = bars[bars['symbol'] == symbol]

        delta = bars['close'].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        latest_rsi = round(rsi.iloc[-1], 2)
        print(f"📈 RSI ({interval}) for {symbol}: {latest_rsi}")
        return latest_rsi

    except Exception as e:
        print(f"❌ Failed to calculate RSI for {symbol}: {e}")
        return None

def submit_order_with_retries(symbol, entry, stop_loss, take_profit, use_trailing, max_retries=3):
    print("📌 About to calculate quantity...")
    qty = calculate_trade_qty(entry, stop_loss)
    if qty == 0:
        print("❌ Qty is 0 — skipping order.")
        send_telegram_alert("❌ Trade aborted — calculated qty was 0.")
        return False

    print(f"📌 Quantity calculated: {qty}")
    print('🔍 Checking equity guard...')

    if should_block_trading_due_to_equity():
        print('🛑 BLOCKED: Equity drop filter triggered.')
        msg = "🛑 Webhook blocked: Equity protection triggered."
        print(msg)
        send_telegram_alert(msg)
        return False
    else:
        print("✅ Equity check passed — trading allowed.")

    print("⚙️ Starting trade submission process for:", symbol)

    if not is_within_trading_hours():
        print('🕑 Reason: Outside trading hours')
        send_telegram_alert("🕑 Trade skipped — outside allowed trading hours.")
        return False
    else:
        print("✅ Within allowed trading hours.")

    for attempt in range(1, max_retries + 1):
        try:
            print(f"🚀 Attempting order for {symbol} (try {attempt})")

            order_request = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.GTC,
                order_class='bracket',
                stop_loss=StopLossRequest(
                    stop_price=stop_loss,
                    trail_percent=1.0 if use_trailing else None
                ),
                take_profit=TakeProfitRequest(limit_price=take_profit)
            )

            trading_client.submit_order(order_request)
            print(f"✅ Order placed for {symbol} (attempt {attempt})")

            explanation = generate_trade_explanation(
                symbol=symbol,
                entry=entry,
                stop_loss=stop_loss,
                take_profit=take_profit,
                rsi=None,
                trend="uptrend" if use_trailing else "neutral"
            )

            send_telegram_alert(f"🚀 Trade executed:\n{explanation}")
            log_equity_curve()
            return True

        except Exception as e:
            print("🚨 ERROR while submitting the order to Alpaca API")
            print(f"📉 Symbol: {symbol}")
            print(f"📊 Entry: {entry}, Stop Loss: {stop_loss}, Take Profit: {take_profit}")
            print(f"🔁 Retry attempt: {attempt}")
            print(f"🧨 Error message: {str(e)}")

    print(f"❌ Order failed for {symbol} after {max_retries} attempts")
    send_telegram_alert(f"❌ Order failed for {symbol} after {max_retries} attempts")
    return False

def log_portfolio_snapshot():
    try:
        account = trading_client.get_account()
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

# ✅ GPT Trade Explanation (Heikin Ashi disabled)
def generate_trade_explanation(symbol, entry, stop_loss, take_profit, rsi=None, trend=None):
    explanation = f"🧠 Trade rationale for {symbol}:\n"
    explanation += f"• Entry at ${entry:.2f}, Stop Loss at ${stop_loss:.2f}, Take Profit at ${take_profit:.2f}\n"
    if rsi is not None:
        explanation += f"• RSI filter passed: {rsi:.2f}\n"
    if trend is not None:
        explanation += f"• Trend confirmed: {trend}\n"
    explanation += "✅ All filters passed — trade executed confidently."
    return explanation

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
                positions = trading_client.get_all_positions()  # ✅ alpaca-py method

                for pos in positions:
                    unrealized_plpc = float(pos.unrealized_plpc)
                    symbol = pos.symbol

                    if unrealized_plpc <= loss_threshold:
                        msg = f"⚠️ {symbol} is down {unrealized_plpc * 100:.2f}% — check position!"
                        print(msg)
                        send_telegram_alert(msg)

                time.sleep(interval_minutes * 60)

            except Exception as e:
                print("🚨 Position Watchdog Error:", e)
                send_telegram_alert(f"🚨 Watchdog error: {e}")
                time.sleep(60)

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

        account = trading_client.get_account()  # ✅ New SDK method
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
                    account = trading_client.get_account()  # ✅ Updated
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

def run_position_watchdog():
    def check_positions():
        while True:
            try:
                positions = trading_client.get_all_positions()
                for position in positions:
                    symbol = position.symbol
                    qty = int(float(position.qty))
                    entry_price = float(position.avg_entry_price)
                    current_price = float(position.current_price)
                    unrealized_plpc = float(position.unrealized_plpc)

                    percent_change = unrealized_plpc * 100
                    print(f"🐶 [{symbol}] Checking: {percent_change:.2f}% P/L")

                    if percent_change <= -3.0:
                        msg = f"🔻 Auto-exit triggered for {symbol}: unrealized P/L = {percent_change:.2f}%"
                        print(msg)
                        send_telegram_alert(msg)

                        # ✅ Correct way to submit market order with alpaca-py
                        market_order = MarketOrderRequest(
                            symbol=symbol,
                            qty=qty,
                            side=OrderSide.SELL,
                            time_in_force=TimeInForce.GTC
                        )
                        trading_client.submit_order(order_data=market_order)

                        send_telegram_alert(f"✅ {symbol} closed at market due to -3% drop.")
                        print(f"✅ {symbol} closed at market.")

                time.sleep(60)

            except Exception as e:
                print(f"⚠️ Watchdog error: {e}")
                send_telegram_alert(f"⚠️ Watchdog error: {e}")
                time.sleep(60)

    threading.Thread(target=check_positions, daemon=True).start()


# ✅ Entry point
if __name__ == '__main__':
    today = datetime.now().strftime("%Y-%m-%d")
    if not os.path.exists(SNAPSHOT_LOG_PATH) or open(SNAPSHOT_LOG_PATH).read().strip() != today:
        log_portfolio_snapshot()
        with open(SNAPSHOT_LOG_PATH, "w") as f:
            f.write(today)

    handle_restart_notification()  # 🧠 Step 20 – Notify if bot restarted unexpectedly

    run_position_watchdog()
    log_equity_curve()
    app.run(host='0.0.0.0', port=5050)