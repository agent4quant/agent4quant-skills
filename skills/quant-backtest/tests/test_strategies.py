from __future__ import annotations

import pandas as pd

from agent4quant.backtest.strategies import (
    sma_cross_strategy,
    rsi_strategy,
    momentum_strategy,
)


def _make_data(n: int = 50, trend: str = "up") -> pd.DataFrame:
    """Create test data with specified trend."""
    if trend == "up":
        close = [10.0 + i * 0.1 for i in range(n)]
    elif trend == "down":
        close = [10.0 - i * 0.1 for i in range(n)]
    else:  # flat
        close = [10.0] * n

    return pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=n, freq="D"),
        "close": close,
        "volume": [1000] * n,
    })


# =============================================================================
# SMA Cross Strategy Tests
# =============================================================================
def test_sma_cross_strategy_has_signal_column() -> None:
    """Test SMA cross strategy returns signal column."""
    data = _make_data(50)
    result = sma_cross_strategy(data, fast=5, slow=20)
    assert "signal" in result.columns


def test_sma_cross_strategy_signal_values() -> None:
    """Test SMA cross strategy signal values are -1, 0, or 1."""
    data = _make_data(50)
    result = sma_cross_strategy(data, fast=5, slow=20)
    assert set(result["signal"].unique()).issubset({-1, 0, 1})


def test_sma_cross_strategy_no_signal_at_start() -> None:
    """Test SMA cross strategy has no signal during warmup period."""
    data = _make_data(30)
    result = sma_cross_strategy(data, fast=5, slow=20)
    assert all(result["signal"].iloc[:5] == 0)


def test_sma_cross_strategy_uptrend_generates_signals() -> None:
    """Test SMA cross strategy generates signals in uptrend."""
    data = _make_data(50, "up")
    result = sma_cross_strategy(data, fast=5, slow=20)
    signals_after_warmup = result["signal"].iloc[20:]
    assert any(signals_after_warmup != 0)


def test_sma_cross_strategy_downtrend_generates_signals() -> None:
    """Test SMA cross strategy generates signals in downtrend."""
    data = _make_data(50, "down")
    result = sma_cross_strategy(data, fast=5, slow=20)
    signals_after_warmup = result["signal"].iloc[20:]
    assert any(signals_after_warmup != 0)


# =============================================================================
# RSI Strategy Tests
# =============================================================================
def test_rsi_strategy_has_signal_column() -> None:
    """Test RSI strategy returns signal column."""
    data = _make_data(50)
    result = rsi_strategy(data, period=14, oversold=30, overbought=70)
    assert "signal" in result.columns


def test_rsi_strategy_signal_values() -> None:
    """Test RSI strategy signal values are -1, 0, or 1."""
    data = _make_data(50)
    result = rsi_strategy(data, period=14, oversold=30, overbought=70)
    assert set(result["signal"].unique()).issubset({-1, 0, 1})


def test_rsi_strategy_custom_thresholds() -> None:
    """Test RSI strategy with custom oversold/overbought thresholds."""
    data = _make_data(50)
    result = rsi_strategy(data, period=14, oversold=20, overbought=80)
    assert "signal" in result.columns


# =============================================================================
# Momentum Strategy Tests
# =============================================================================
def test_momentum_strategy_has_signal_column() -> None:
    """Test momentum strategy returns signal column."""
    data = _make_data(50)
    result = momentum_strategy(data, period=10)
    assert "signal" in result.columns


def test_momentum_strategy_signal_values() -> None:
    """Test momentum strategy signal values are -1, 0, or 1."""
    data = _make_data(50)
    result = momentum_strategy(data, period=10)
    assert set(result["signal"].unique()).issubset({-1, 0, 1})


def test_momentum_strategy_no_signal_at_start() -> None:
    """Test momentum strategy has no signal during warmup period."""
    data = _make_data(50)
    result = momentum_strategy(data, period=10)
    assert all(result["signal"].iloc[:10] == 0)
