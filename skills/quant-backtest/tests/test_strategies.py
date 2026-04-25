from __future__ import annotations

import pandas as pd

from agent4quant.backtest.strategies import (
    sma_cross_strategy,
    rsi_strategy,
    momentum_strategy,
)


def test_sma_cross_strategy_generates_signals() -> None:
    """Test SMA cross strategy generates buy/sell signals."""
    data = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=50, freq="D"),
        "close": [10.0 + i * 0.1 for i in range(50)],
        "volume": [1000] * 50,
    })

    signals = sma_cross_strategy(data, fast=5, slow=20)

    assert "signal" in signals.columns
    assert signals["signal"].isin([-1, 0, 1]).all()


def test_sma_cross_strategy_no_signal_at_start() -> None:
    """Test SMA cross strategy has no signal at the start (warming up)."""
    data = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=10, freq="D"),
        "close": [10.0 + i * 0.1 for i in range(10)],
        "volume": [1000] * 10,
    })

    signals = sma_cross_strategy(data, fast=5, slow=20)

    assert signals["signal"].iloc[:4].eq(0).all()


def test_rsi_strategy_generates_signals() -> None:
    """Test RSI strategy generates buy/sell signals."""
    data = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=50, freq="D"),
        "close": [10.0 + i * 0.1 for i in range(50)],
        "volume": [1000] * 50,
    })

    signals = rsi_strategy(data, period=14, oversold=30, overbought=70)

    assert "signal" in signals.columns
    assert signals["signal"].isin([-1, 0, 1]).all()


def test_momentum_strategy_generates_signals() -> None:
    """Test momentum strategy generates buy/sell signals."""
    data = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=50, freq="D"),
        "close": [10.0 + i * 0.1 for i in range(50)],
        "volume": [1000] * 50,
    })

    signals = momentum_strategy(data, period=10)

    assert "signal" in signals.columns
    assert signals["signal"].isin([-1, 0, 1]).all()
