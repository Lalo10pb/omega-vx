from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple

import pandas as pd
import numpy as np
from pandas import DataFrame

# Configure logger
logger = logging.getLogger("omega_vx.strategy")

class Strategy(ABC):
    """
    Abstract base class for Omega-VX trading strategies.
    """

    @abstractmethod
    def analyze(self, symbol: str, data: DataFrame) -> Dict:
        pass

class OmegaStrategy(Strategy):
    """
    The core Omega-VX strategy.
    
    Logic:
    1. Trend Filter: EMA Short > EMA Long
    2. Momentum: RSI (Customizable bands)
    3. Volatility: Bollinger Bands (Price near lower band is bullish in uptrend)
    4. Confirmation: MACD Crossover
    """

    def __init__(self, 
                 ema_short: int = 9, 
                 ema_long: int = 21, 
                 rsi_period: int = 14,
                 rsi_min: int = 30,
                 rsi_max: int = 70):
        self.ema_short_period = ema_short
        self.ema_long_period = ema_long
        self.rsi_period = rsi_period
        self.rsi_min = rsi_min
        self.rsi_max = rsi_max

    def _calculate_rsi(self, series, period):
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    def _calculate_macd(self, series, fast=12, slow=26, signal=9):
        ema_fast = series.ewm(span=fast, adjust=False).mean()
        ema_slow = series.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    def _calculate_bbands(self, series, length=20, std=2):
        sma = series.rolling(window=length).mean()
        rstd = series.rolling(window=length).std()
        upper = sma + (rstd * std)
        lower = sma - (rstd * std)
        return upper, lower

    def _calculate_atr(self, df, length=14):
        high = df['high']
        low = df['low']
        close = df['close']
        
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=length).mean()
        return atr

    def analyze(self, symbol: str, df: DataFrame) -> Dict:
        """
        Expects a DataFrame with columns: ['open', 'high', 'low', 'close', 'volume']
        """
        if df.empty or len(df) < 50:
            return {"signal": "skip", "reason": "insufficient_data", "score": 0}

        # Ensure numeric type
        cols = ['open', 'high', 'low', 'close', 'volume']
        for c in cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')

        # --- Calculate Indicators (Manual Pandas) ---
        # 1. EMA
        df['ema_short'] = df['close'].ewm(span=self.ema_short_period, adjust=False).mean()
        df['ema_long'] = df['close'].ewm(span=self.ema_long_period, adjust=False).mean()
        
        # 2. RSI
        df['rsi'] = self._calculate_rsi(df['close'], self.rsi_period)
        
        # 3. MACD
        df['macd'], df['macd_signal'], df['macd_hist'] = self._calculate_macd(df['close'])

        # 4. Bollinger Bands
        df['bb_upper'], df['bb_lower'] = self._calculate_bbands(df['close'])

        # 5. ATR
        df['atr'] = self._calculate_atr(df)

        # --- Logic Checks (on the latest bar) ---
        last = df.iloc[-1]
        
        score = 0
        reasons = []

        # A. Trend Filter (EMA Cross)
        if last['ema_short'] > last['ema_long']:
            score += 2
            reasons.append("uptrend_ema")
        else:
            score -= 2
            reasons.append("downtrend_ema")

        # B. RSI Filter
        rsi_val = last['rsi']
        if pd.isna(rsi_val):
             pass
        elif self.rsi_min < rsi_val < self.rsi_max:
            score += 1 
        elif rsi_val <= self.rsi_min:
            score += 2 
            reasons.append("rsi_oversold")
        elif rsi_val >= self.rsi_max:
            score -= 1 
            reasons.append("rsi_overbought")

        # C. MACD Confirmation
        if last['macd_hist'] > 0:
             score += 1
             reasons.append("macd_green")

        # D. Bollinger Value
        if last['close'] <= last['bb_lower'] * 1.01:
            if "uptrend_ema" in reasons:
                score += 2
                reasons.append("bb_lower_bounce")

        signal = "neutral"
        if score >= 4:
            signal = "buy"
        
        return {
            "symbol": symbol,
            "signal": signal,
            "score": score,
            "metrics": {
                "price": last['close'],
                "rsi": last['rsi'],
                "ema_short": last['ema_short'],
                "ema_long": last['ema_long'],
                "atr": last.get('atr', 0),
                "reasons": reasons
            }
        }
