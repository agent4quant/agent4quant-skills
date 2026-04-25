from __future__ import annotations

import pandas as pd


def add_indicators(frame: pd.DataFrame, indicators: list[str]) -> pd.DataFrame:
    df = frame.copy()
    normalized = [item.strip().lower() for item in indicators if item.strip()]
    returns = df["close"].pct_change()

    for indicator in normalized:
        if indicator.startswith("ma") and indicator[2:].isdigit():
            window = int(indicator[2:])
            df[f"ma_{window}"] = df["close"].rolling(window).mean()
        elif indicator.startswith("ret") and indicator[3:].isdigit():
            periods = int(indicator[3:])
            df[f"ret_{periods}"] = df["close"].pct_change(periods)
        elif indicator.startswith("mom") and indicator[3:].isdigit():
            periods = int(indicator[3:])
            df[f"mom_{periods}"] = df["close"] / df["close"].shift(periods) - 1
        elif indicator.startswith("volatility") and indicator[10:].isdigit():
            window = int(indicator[10:])
            df[f"volatility_{window}"] = returns.rolling(window).std()
        elif indicator.startswith("zscore") and indicator[6:].isdigit():
            window = int(indicator[6:])
            mean = df["close"].rolling(window).mean()
            std = df["close"].rolling(window).std().replace(0, pd.NA)
            df[f"zscore_{window}"] = (df["close"] - mean) / std
        elif indicator.startswith("volma") and indicator[5:].isdigit():
            window = int(indicator[5:])
            df[f"volume_ma_{window}"] = df["volume"].rolling(window).mean()
        elif indicator == "macd":
            ema_fast = df["close"].ewm(span=12, adjust=False).mean()
            ema_slow = df["close"].ewm(span=26, adjust=False).mean()
            df["macd"] = ema_fast - ema_slow
            df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
            df["macd_hist"] = df["macd"] - df["macd_signal"]
        elif indicator == "rsi":
            delta = df["close"].diff()
            up = delta.clip(lower=0).rolling(14).mean()
            down = -delta.clip(upper=0).rolling(14).mean()
            rs = up / down.replace(0, pd.NA)
            df["rsi_14"] = 100 - (100 / (1 + rs))
        elif indicator == "boll":
            middle = df["close"].rolling(20).mean()
            std = df["close"].rolling(20).std()
            df["boll_mid"] = middle
            df["boll_upper"] = middle + 2 * std
            df["boll_lower"] = middle - 2 * std
        elif indicator == "atr":
            prev_close = df["close"].shift(1)
            high_low = df["high"] - df["low"]
            high_close = (df["high"] - prev_close).abs()
            low_close = (df["low"] - prev_close).abs()
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            df["atr_14"] = tr.rolling(14).mean()
        elif indicator == "kdj":
            low_min = df["low"].rolling(9).min()
            high_max = df["high"].rolling(9).max()
            rsv = (df["close"] - low_min) / (high_max - low_min).replace(0, pd.NA) * 100
            df["k"] = rsv.ewm(alpha=1/3, adjust=False).mean()
            df["d"] = df["k"].ewm(alpha=1/3, adjust=False).mean()
            df["j"] = 3 * df["k"] - 2 * df["d"]
        elif indicator == "obv":
            direction = returns.fillna(0.0).apply(lambda value: 1 if value > 0 else (-1 if value < 0 else 0))
            df["obv"] = (direction * df["volume"]).cumsum()

    return df
