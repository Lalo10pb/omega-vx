# === Flask and Webhook Handling ===
from flask import Flask, request, jsonify

# === Core Packages ===
import os
import csv
import time
import threading
import requests
import subprocess
import sys
import functools
from datetime import datetime, timedelta, time as dt_time
import numpy as np
import pandas as pd
from dotenv import load_dotenv

# === Alpaca Trading and Data ===
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    StopLossRequest,
    TakeProfitRequest,
    GetOrdersRequest,
)
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType, QueryOrderStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame

# === Email Reporting ===
import smtplib
from email.message import EmailMessage

# === Google Sheets ===
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# === Load .env and Set Environment Vars ===
from pathlib import Path
ENV_PATH = Path(__file__).parent / ".env"
if not load_dotenv(ENV_PATH):
    print(f"⚠️ Could not load .env at {ENV_PATH} — using environment vars only.", flush=True)
    
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL")
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_FILE = "google_credentials.json"
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# === Trading Configuration ===
DAILY_RISK_LIMIT = -10  # 💥 Stop trading after $10 loss
TRADE_COOLDOWN_SECONDS = 300  # ⏱️ 5 minutes cooldown
MAX_RISK_PER_TRADE_PERCENT = 1.0
MIN_TRADE_QTY = 1

# === Logging ===
print = functools.partial(print, flush=True)
LOG_DIR = os.path.expanduser("~/omega-vx/logs")
os.makedirs(LOG_DIR, exist_ok=True)
CRASH_LOG_FILE = os.path.join(LOG_DIR, "last_boot.txt")
LAST_BLOCK_FILE = os.path.join(LOG_DIR, "last_block.txt")
LAST_TRADE_FILE = os.path.join(LOG_DIR, "last_trade_time.txt")

# === Alpaca Clients ===
trading_client = TradingClient(API_KEY, API_SECRET, paper=True)
data_client = StockHistoricalDataClient(API_KEY, API_SECRET)

# === Flask App ===
app = Flask(__name__)

# === Watchdog cooldown ===
from time import monotonic
_last_close_attempt = {}
_CLOSE_COOLDOWN_SEC = 20  # consider 60–120 during market hours

# === Dev flags ===
FORCE_WEBHOOK_TEST = str(os.getenv("FORCE_WEBHOOK_TEST", "0")).strip().lower() in ("1", "true", "yes")

# 🧠 Generate AI-based trade explanation
def generate_trade_explanation(symbol, entry, stop_loss, take_profit, rsi=None, trend=None, ha_candle=None):
    """
    Generate a human-readable explanation for why the trade was taken.
    """

    reasons = []

    if rsi is not None:
        if rsi < 30:
            reasons.append(f"RSI is oversold ({rsi}), suggesting a potential upward reversal.")
        elif rsi > 70:
            reasons.append(f"RSI is overbought ({rsi}), suggesting a potential downward reversal.")

    if trend is not None:
        if trend == "uptrend":
            reasons.append("EMA trend filter indicates a bullish market.")
        elif trend == "downtrend":
            reasons.append("EMA trend filter indicates a bearish market.")

    if ha_candle is not None:
        if ha_candle == "bullish":
            reasons.append("Heikin Ashi candle shows bullish momentum.")
        elif ha_candle == "bearish":
            reasons.append("Heikin Ashi candle shows bearish momentum.")

    explanation = f"Trade setup for {symbol} at {entry}, Stop Loss={stop_loss}, Take Profit={take_profit}."
    if reasons:
        explanation += " Reasons: " + " ".join(reasons)

    return explanation

def send_telegram_alert(message: str):
    import requests  # keep local to guarantee availability
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram not configured (missing token/chat id).")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}

    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            print(f"⚠️ Telegram send failed: {r.status_code} {r.text}")
    except Exception as e:
        print(f"❌ Telegram alert error: {e}")

def get_watchlist_from_google_sheet(sheet_name="OMEGA-VX LOGS", tab_name="watchlist"):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        
        sheet = client.open(sheet_name).worksheet(tab_name)
        symbols = sheet.col_values(1)  # Get column A
        return [s.strip().upper() for s in symbols if s.strip()]
    
    except Exception as e:
        print(f"❌ Failed to fetch watchlist: {e}")
        return []

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

def is_within_trading_hours(start_hour=13, start_minute=30, end_hour=20):
    if FORCE_WEBHOOK_TEST:
        return True
    now_utc = datetime.utcnow().time()
    start = dt_time(hour=start_hour, minute=start_minute)
    end = dt_time(hour=end_hour, minute=0)
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
        data = request.get_json(silent=True) or {}
        print(f"📥 Webhook received: {data}", flush=True)

        # 🔐 Validate the secret from HEADER
        header_secret = request.headers.get("X-OMEGA-SECRET")
        env_secret = os.getenv("WEBHOOK_SECRET_TOKEN")  # be explicit
        if header_secret != env_secret:
            print("🚫 Unauthorized webhook attempt blocked.", flush=True)
            try:
                send_telegram_alert("🚫 Unauthorized webhook attempt blocked.")
            except Exception:
                pass
            return jsonify({"status": "failed", "reason": "auth"}), 200

        # 🔎 Basic validation
        required = ("symbol", "entry", "stop_loss", "take_profit")
        missing = [k for k in required if k not in data]
        if missing:
            return jsonify({"status": "failed", "reason": f"missing_fields:{','.join(missing)}"}), 200

        symbol = str(data.get("symbol", "")).upper().strip()

        # 🧮 Safe numeric parsing
        try:
            entry = float(data.get("entry"))
            stop_loss = float(data.get("stop_loss"))
            take_profit = float(data.get("take_profit"))
        except Exception as conv_err:
            return jsonify({"status": "failed", "reason": f"bad_numbers:{conv_err}"}), 200

        # ✅ Proper boolean handling (strings "true"/"false" etc.)
        ut_raw = data.get("use_trailing", False)
        if isinstance(ut_raw, str):
            use_trailing = ut_raw.strip().lower() in ("1", "true", "yes", "y", "on")
        else:
            use_trailing = bool(ut_raw)
        # --- optional dry-run guard (header, query, or default from env) ---
        def _as_bool(x):
            return str(x).strip().lower() in ("1", "true", "yes", "y", "on")

        dry_run = (
            _as_bool(request.headers.get("X-OMEGA-DRYRUN", "0"))
            or _as_bool(request.args.get("dryrun", "0"))
            or _as_bool(os.getenv("OMEGA_WEBHOOK_DRYRUN", "0"))
        )

        if dry_run:
            msg = (
                f"🧪 Webhook DRY-RUN for {symbol} "
                f"(entry {entry}, sl {stop_loss}, tp {take_profit}, trailing {use_trailing})"
            )
            print(msg, flush=True)
            try:
                send_telegram_alert(msg)
            except Exception:
                pass
            return jsonify({
                "status": "ok",
                "trade_placed": False,
                "reason": "dry_run"
            }), 200
        # --- end dry-run guard ---
        print(f"🔄 Calling submit_order_with_retries for {symbol}", flush=True)

        try:
            success = submit_order_with_retries(
                symbol=symbol,
                entry=entry,
                 stop_loss=stop_loss,
                take_profit=take_profit,
                use_trailing=use_trailing,
                dry_run=dry_run
            )
            
        except Exception as trade_error:
            msg = f"💥 submit_order_with_retries error: {trade_error}"
            print(msg, flush=True)
            try:
                send_telegram_alert(msg)
            except Exception:
                pass
            return jsonify({"status": "failed", "reason": "exception", "detail": str(trade_error)}), 200

        placed = bool(success)
        print(f"✅ Webhook processed (trade_placed={placed})", flush=True)
        resp = {
            "status": "ok",                      # webhook processed fine
            "trade_placed": placed               # whether an order actually went in
        }
        if not placed:
           resp["reason"] = "submit_returned_false"  # e.g., qty=0 / buying power / weekend

        return jsonify(resp), 200

    except Exception as e:
        print(f"❌ Exception in webhook: {e}", flush=True)
        return jsonify({"status": "failed", "reason": "handler_exception", "detail": str(e)}), 200

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

def submit_order_with_retries(symbol, entry, stop_loss, take_profit, use_trailing, max_retries=3, dry_run=False):
    print("📌 About to calculate quantity...")
    qty = calculate_trade_qty(entry, stop_loss)
    if qty == 0:
        print("❌ Qty is 0 — skipping order.")
        send_telegram_alert("❌ Trade aborted — calculated qty was 0.")
        return False

    # ✅ DRY-RUN SHORT-CIRCUIT (no real orders sent)
    if dry_run:
        print(f"🧪 [DRY-RUN] Would place order for {symbol} | qty={qty} | entry={entry} | SL={stop_loss} | TP={take_profit} | trailing={use_trailing}")
        try:
            send_telegram_alert(f"🧪 [DRY-RUN] {symbol} qty={qty} @ {entry} (SL {stop_loss} / TP {take_profit}, trailing={use_trailing})")
        except Exception:
            pass
        return False  # signal "no order placed" to the caller

    # ✅ START OF PATCH (buying power guard)
    account = trading_client.get_account()
    buying_power = float(account.buying_power)
    estimated_cost = entry * qty
    if estimated_cost > buying_power:
        qty = int(buying_power // entry)
        print(f"⚠️ Adjusted qty due to buying power: {qty}")
        if qty == 0:
            print("❌ Not enough buying power for even 1 share.")
            send_telegram_alert("❌ Trade aborted — not enough buying power.")
            return False
    # ✅ END OF PATCH

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
                take_profit=TakeProfitRequest(
                    limit_price=max(round(take_profit, 2), round(entry + 0.01, 2))
                )
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
        equity = float(account.equity)
        cash = float(account.cash)
        portfolio_value = float(account.portfolio_value)

        row = [timestamp, equity, cash, portfolio_value]

        # ✅ Log to local CSV file
        file_exists = os.path.exists(PORTFOLIO_LOG_PATH)
        with open(PORTFOLIO_LOG_PATH, mode='a', newline='') as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["timestamp", "equity", "cash", "portfolio_value"])
            writer.writerow(row)

        print("✅ Daily snapshot logged (CSV):", row)
        send_telegram_alert(f"📸 Snapshot logged: Equity ${equity:.2f}, Cash ${cash:.2f}")

        # ✅ Also log to Google Sheet
        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name('google_credentials.json', scope)
            client = gspread.authorize(creds)

            sheet_id = os.getenv("GOOGLE_SHEET_ID")
            sheet_name = os.getenv("PORTFOLIO_SHEET_NAME", "Portfolio Log")  # Default fallback

            if not sheet_id or not sheet_name:
                raise ValueError("Missing GOOGLE_SHEET_ID or PORTFOLIO_SHEET_NAME environment variable.")

            sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
            sheet.append_row(row)
            print("✅ Snapshot logged to Google Sheet.")

        except Exception as gs_error:
            print("⚠️ Failed to log snapshot to Google Sheet:", gs_error)

    except Exception as e:
        print("⚠️ Failed to log portfolio snapshot:", e)
        send_telegram_alert(f"⚠️ Snapshot failed: {e}")

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

def start_auto_sell_monitor():
    def monitor():
        while True:
            try:
                # Add a timeout to the API call
                positions = trading_client.get_all_positions()

                for position in positions:
                    symbol = position.symbol
                    qty = float(position.qty)
                    entry_price = float(position.avg_entry_price)
                    current_price = float(position.current_price)
                    percent_change = float(position.unrealized_plpc) * 100

                    print(f"📊 {symbol}: Qty={qty} Entry=${entry_price:.2f} Now=${current_price:.2f} PnL={percent_change:.2f}%")

                    hit_trailing = percent_change <= -2.0
                    hit_take_profit = percent_change >= 5.0
                    hit_stop_loss = percent_change <= -3.5

                    if hit_trailing or hit_take_profit or hit_stop_loss:
                        reason = "❗"
                        if hit_take_profit:
                            reason += "Take Profit Hit"
                        elif hit_trailing:
                            reason += "Trailing Stop Hit"
                        elif hit_stop_loss:
                            reason += "Hard Stop Hit"

                        print(f"💥 {symbol} closing position — {reason}")

                        try:
                            close_order = trading_client.close_position(symbol)
                            print(f"✅ Position closed for {symbol}")
                            send_telegram_alert(f"💥 {symbol} auto-closed: {reason}")
                        except Exception as e:
                            print(f"❌ Failed to close {symbol}: {e}")
                            send_telegram_alert(f"❌ Failed to close {symbol}: {e}")

            except Exception as monitor_error:
                print(f"❌ Watchdog error: {monitor_error}")
                send_telegram_alert(f"⚠️ Watchdog error: {monitor_error}")

            time.sleep(30)

    t = threading.Thread(target=monitor, daemon=True)
    t.start()

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
                    account = trading_client.get_account()
                    current_equity = float(account.equity)
                    context_equity_change = "gain" if current_equity > prev_equity else "loss"
    except Exception as e:
        print("⚠️ Failed to fetch recent outcome context:", e)

    # 🕰 Timestamp
    timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

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

    # ✅ Google Sheet Logging
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('google_credentials.json', scope)
        client = gspread.authorize(creds)

        sheet_id = os.getenv("GOOGLE_SHEET_ID")
        sheet_name = os.getenv("SHEET_NAME")

        if not sheet_id or not sheet_name:
            raise ValueError("Missing GOOGLE_SHEET_ID or SHEET_NAME environment variable.")

        sheet = client.open_by_key(sheet_id).worksheet(sheet_name)

        sheet.append_row([
            timestamp_str, symbol, qty, entry, stop_loss, take_profit,
            status, explanation, context_time, context_recent_outcome, context_equity_change
        ])
        print("✅ Trade logged to Google Sheet.")

    except Exception as e:
        print("⚠️ Failed to log trade to Google Sheet:", e)

from time import monotonic

_last_close_attempt = {}
_CLOSE_COOLDOWN_SEC = 20  # seconds between close attempts per symbol

def _get_available_qty(symbol: str) -> float:
    """Return qty_available for an open position, or 0 if none."""
    try:
        positions = trading_client.get_all_positions()
        for p in positions:
            if p.symbol.upper() == symbol.upper():
                return float(getattr(p, "qty_available", getattr(p, "qty", "0")))
        return 0.0
    except Exception as e:
        print(f"⚠️ Could not fetch positions: {e}")
        return 0.0

def close_position_if_needed(symbol: str, reason: str) -> bool:
    """Safely close a position if qty is available. Returns True if order was submitted."""
    now = monotonic()
    last = _last_close_attempt.get(symbol)
    if last is not None and (now - last) < _CLOSE_COOLDOWN_SEC:
        print(f"⏳ Skipping close for {symbol} (cooldown {int(_CLOSE_COOLDOWN_SEC - (now - last))}s).")
        return False

    qty_available = _get_available_qty(symbol)
    if qty_available <= 0:
        print(f"⚠️ No available qty for {symbol}, skipping close.")
        return False

    print(f"💥 {symbol} closing position — {reason} (qty_available={qty_available})")
    try:
        market_order = MarketOrderRequest(
            symbol=symbol,
            qty=qty_available,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC
        )
        trading_client.submit_order(order_data=market_order)
        _last_close_attempt[symbol] = now
        send_telegram_alert(f"✅ {symbol} closed at market. Reason: {reason}")
        print(f"✅ {symbol} position close submitted.")
        return True
    except Exception as e:
        _last_close_attempt[symbol] = now
        print(f"❌ Failed to close {symbol}: {e}")
        send_telegram_alert(f"❌ Failed to close {symbol}: {e}")
        return False

def run_position_watchdog():
    def check_positions():
        while True:
            try:
                positions = trading_client.get_all_positions()
                for p in positions:
                    symbol = p.symbol
                    qty = int(float(p.qty))
                    entry_price = float(p.avg_entry_price)
                    current_price = float(p.current_price)
                    unrealized_plpc = float(p.unrealized_plpc)  # decimal (e.g., 0.0123)
                    percent_change = unrealized_plpc * 100

                    print(f"🐶 [{symbol}] Checking: {percent_change:.2f}% P/L")

                    # ---- cooldown guard (per symbol) ----
                    now_t = monotonic()
                    last_t = _last_close_attempt.get(symbol, 0.0)
                    if now_t - last_t < _CLOSE_COOLDOWN_SEC:
                        # Skip this symbol this cycle to avoid rapid re-submits
                        continue

                    # ---- auto-exit rule ----
                    if percent_change <= -3.0:
                        _last_close_attempt[symbol] = now_t

                        msg = f"🔻 Auto-exit triggered for {symbol}: unrealized P/L = {percent_change:.2f}%"
                        print(msg)
                        try:
                            send_telegram_alert(msg)
                        except Exception as e:
                            print(f"⚠️ Telegram send failed: {e}")

                        ok = safe_close_position(symbol, qty)
                        try:
                            if ok:
                                send_telegram_alert(f"✅ {symbol} close submitted at market.")
                            else:
                                send_telegram_alert(f"⚠️ {symbol} close failed; will retry after cooldown.")
                        except Exception as e:
                            print(f"⚠️ Telegram send failed: {e}")

                time.sleep(60)  # poll interval

            except Exception as e:
                print(f"⚠️ Watchdog error: {e}")
                try:
                    send_telegram_alert(f"⚠️ Watchdog error: {e}")
                except Exception as _:
                    pass
                time.sleep(60)

    threading.Thread(target=check_positions, daemon=True).start()

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

def place_market_buy(symbol, qty):
    if DRY_RUN:
        print(f"🧪 DRY-RUN buy {symbol} x{qty} (no order sent)")
        return {"id": "dryrun", "symbol": symbol, "qty": qty}
    order = MarketOrderRequest(
        symbol=symbol, qty=qty,
        side=OrderSide.BUY, time_in_force=TimeInForce.GTC
    )
    return trading_client.submit_order(order_data=order)

def place_market_sell(symbol, qty):
    if DRY_RUN:
        print(f"🧪 DRY-RUN sell {symbol} x{qty} (no order sent)")
        return {"id": "dryrun", "symbol": symbol, "qty": qty}
    order = MarketOrderRequest(
        symbol=symbol, qty=qty,
        side=OrderSide.SELL, time_in_force=TimeInForce.GTC
    )
    return trading_client.submit_order(order_data=order)

def handle_critical_error(e):
    crash_message = f"🧨 OMEGA-VX crashed: {e}"
    print(crash_message)
    send_telegram_alert(crash_message)

    with open(CRASH_LOG_FILE, 'a') as f:
        f.write(f"{datetime.now()} - {e}\n")

    print("♻️ Restarting OMEGA-VX...")
    time.sleep(3)  # slight pause
    subprocess.Popen([sys.executable] + sys.argv)
    sys.exit(1)

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
    start_auto_sell_monitor()
try:
    app.run(host="0.0.0.0", port=5050)
except Exception as e:
    handle_critical_error(e)
  