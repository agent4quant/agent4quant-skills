from __future__ import annotations

import pandas as pd
import numpy as np


def sma_cross(frame: pd.DataFrame, fast: int = 5, slow: int = 20) -> pd.Series:
    """Simple moving average crossover strategy."""
    fast_ma = frame["close"].rolling(window=fast).mean()
    slow_ma = frame["close"].rolling(window=slow).mean()
    return pd.Series(np.where(fast_ma > slow_ma, 1.0, 0.0), index=frame.index)


def build_positions(frame: pd.DataFrame, strategy: str, params: dict) -> pd.Series:
    """Build position series from strategy name and parameters."""
    if strategy == "sma_cross":
        fast = int(params.get("fast", 5))
        slow = int(params.get("slow", 20))
        if fast >= slow:
            raise ValueError(f"fast ({fast}) must be less than slow ({slow})")
        return sma_cross(frame, fast=fast, slow=slow)
    else:
        raise ValueError(f"Unknown strategy: {strategy}. Supported: sma_cross")
