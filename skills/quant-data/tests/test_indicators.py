from __future__ import annotations

import pandas as pd

from agent4quant.data.indicators import add_indicators


def test_add_indicators_ret5() -> None:
    """Test return indicator calculation."""
    frame = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=10, freq="D"),
        "open": [10.0] * 10,
        "high": [10.5] * 10,
        "low": [9.5] * 10,
        "close": [10.0, 10.2, 10.4, 10.6, 10.8, 11.0, 10.8, 10.6, 10.4, 10.2],
        "volume": [1000] * 10,
        "symbol": ["TEST.SH"] * 10,
    })

    result = add_indicators(frame, ["ret5"])

    assert "ret_5" in result.columns
    assert pd.isna(result["ret_5"].iloc[0])
    assert pd.notna(result["ret_5"].iloc[-1])


def test_add_indicators_mom10() -> None:
    """Test momentum indicator calculation."""
    frame = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=15, freq="D"),
        "open": [10.0] * 15,
        "high": [10.5] * 15,
        "low": [9.5] * 15,
        "close": [10.0 + i * 0.1 for i in range(15)],
        "volume": [1000] * 15,
        "symbol": ["TEST.SH"] * 15,
    })

    result = add_indicators(frame, ["mom10"])

    assert "mom_10" in result.columns


def test_add_indicators_volatility10() -> None:
    """Test volatility indicator calculation."""
    frame = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=15, freq="D"),
        "open": [10.0] * 15,
        "high": [10.5] * 15,
        "low": [9.5] * 15,
        "close": [10.0 + i * 0.1 for i in range(15)],
        "volume": [1000] * 15,
        "symbol": ["TEST.SH"] * 15,
    })

    result = add_indicators(frame, ["volatility10"])

    assert "volatility_10" in result.columns
    assert all(result["volatility_10"].iloc[10:] >= 0)


def test_add_indicators_obv() -> None:
    """Test On-Balance Volume indicator calculation."""
    frame = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=10, freq="D"),
        "open": [10.0] * 10,
        "high": [10.5] * 10,
        "low": [9.5] * 10,
        "close": [10.0, 10.2, 10.4, 10.2, 10.0, 10.2, 10.4, 10.6, 10.4, 10.2],
        "volume": [1000, 1200, 800, 1500, 1000, 1200, 800, 1500, 1000, 1200],
        "symbol": ["TEST.SH"] * 10,
    })

    result = add_indicators(frame, ["obv"])

    assert "obv" in result.columns
    assert pd.notna(result["obv"].iloc[-1])


def test_add_indicators_multiple() -> None:
    """Test adding multiple indicators at once."""
    frame = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=30, freq="D"),
        "open": [10.0] * 30,
        "high": [10.5] * 30,
        "low": [9.5] * 30,
        "close": [10.0 + i * 0.05 for i in range(30)],
        "volume": [1000 + i * 10 for i in range(30)],
        "symbol": ["TEST.SH"] * 30,
    })

    result = add_indicators(frame, ["ret5", "mom10", "volatility10", "volma5", "obv"])

    assert "ret_5" in result.columns
    assert "mom_10" in result.columns
    assert "volatility_10" in result.columns
    assert "volume_ma_5" in result.columns
    assert "obv" in result.columns
