from omega_vx_bot import generate_trade_explanation

# Test Case 1 – Bearish Heikin Ashi and high RSI
print("🧪 Test 1:\n", generate_trade_explanation(
    symbol="TSLA",
    entry=254.10,
    stop_loss=248.00,
    take_profit=270.00,
    rsi=74.2,
    trend="downtrend",
    ha_candle="bearish"
), "\n")

# Test Case 2 – No trend but good RSI
print("🧪 Test 2:\n", generate_trade_explanation(
    symbol="AAPL",
    entry=189.50,
    stop_loss=183.00,
    take_profit=205.00,
    rsi=52.1,
    trend=None,
    ha_candle=None
), "\n")

# Test Case 3 – All indicators aligned
print("🧪 Test 3:\n", generate_trade_explanation(
    symbol="NVDA",
    entry=456.80,
    stop_loss=441.00,
    take_profit=500.00,
    rsi=29.4,
    trend="uptrend",
    ha_candle="bullish"
), "\n")
