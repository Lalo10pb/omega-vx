import pytest

from omega_vx_bot import generate_trade_explanation


@pytest.mark.parametrize(
    ("kwargs", "expected_snippets"),
    [
        (
            {
                "symbol": "TSLA",
                "entry": 254.10,
                "stop_loss": 248.00,
                "take_profit": 270.00,
                "rsi": 74.2,
                "trend": "downtrend",
                "ha_candle": "bearish",
            },
            ["RSI is overbought (74.2)", "EMA trend filter indicates a bearish market."],
        ),
        (
            {
                "symbol": "AAPL",
                "entry": 189.50,
                "stop_loss": 183.00,
                "take_profit": 205.00,
                "rsi": 52.1,
                "trend": None,
                "ha_candle": None,
            },
            ["Trade setup for AAPL", "Stop Loss=183.0"],
        ),
        (
            {
                "symbol": "NVDA",
                "entry": 456.80,
                "stop_loss": 441.00,
                "take_profit": 500.00,
                "rsi": 29.4,
                "trend": "uptrend",
                "ha_candle": "bullish",
            },
            ["RSI is oversold (29.4)", "EMA trend filter indicates a bullish market."],
        ),
    ],
)
def test_generate_trade_explanation_includes_key_reasons(kwargs, expected_snippets):
    explanation = generate_trade_explanation(**kwargs)
    assert kwargs["symbol"] in explanation
    for snippet in expected_snippets:
        assert snippet in explanation
