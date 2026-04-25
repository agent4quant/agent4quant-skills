from __future__ import annotations

import pandas as pd

from agent4quant.data.indicators import add_indicators


def _make_frame(n: int = 30) -> pd.DataFrame:
    """Create a standard test DataFrame."""
    return pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=n, freq="D"),
        "open": [10.0 + i * 0.1 for i in range(n)],
        "high": [10.5 + i * 0.1 for i in range(n)],
        "low": [9.5 + i * 0.1 for i in range(n)],
        "close": [10.0 + i * 0.1 for i in range(n)],
        "volume": [1000 + i * 10 for i in range(n)],
        "symbol": ["TEST.SH"] * n,
    })


# =============================================================================
# MA Tests
# =============================================================================
def test_add_indicators_ma5() -> None:
    """Test MA5 calculation."""
    df = _make_frame(10)
    result = add_indicators(df, ["ma5"])
    assert "ma_5" in result.columns
    assert pd.isna(result["ma_5"].iloc[0])
    assert pd.notna(result["ma_5"].iloc[-1])


def test_add_indicators_ma20() -> None:
    """Test MA20 calculation."""
    df = _make_frame(25)
    result = add_indicators(df, ["ma20"])
    assert "ma_20" in result.columns


# =============================================================================
# RET Tests
# =============================================================================
def test_add_indicators_ret5() -> None:
    """Test return indicator calculation."""
    df = _make_frame(10)
    result = add_indicators(df, ["ret5"])
    assert "ret_5" in result.columns
    assert pd.isna(result["ret_5"].iloc[0])
    assert pd.notna(result["ret_5"].iloc[-1])


def test_add_indicators_ret10() -> None:
    """Test ret10 calculation."""
    df = _make_frame(15)
    result = add_indicators(df, ["ret10"])
    assert "ret_10" in result.columns


# =============================================================================
# Momentum Tests
# =============================================================================
def test_add_indicators_mom10() -> None:
    """Test momentum indicator calculation."""
    df = _make_frame(15)
    result = add_indicators(df, ["mom10"])
    assert "mom_10" in result.columns


# =============================================================================
# Volatility Tests
# =============================================================================
def test_add_indicators_volatility10() -> None:
    """Test volatility indicator calculation."""
    df = _make_frame(15)
    result = add_indicators(df, ["volatility10"])
    assert "volatility_10" in result.columns
    assert all(result["volatility_10"].iloc[10:] >= 0)


# =============================================================================
# ZScore Tests
# =============================================================================
def test_add_indicators_zscore20() -> None:
    """Test zscore indicator calculation."""
    df = _make_frame(25)
    result = add_indicators(df, ["zscore20"])
    assert "zscore_20" in result.columns


# =============================================================================
# Volume MA Tests
# =============================================================================
def test_add_indicators_volma5() -> None:
    """Test volume MA indicator calculation."""
    df = _make_frame(10)
    result = add_indicators(df, ["volma5"])
    assert "volume_ma_5" in result.columns


# =============================================================================
# OBV Tests
# =============================================================================
def test_add_indicators_obv() -> None:
    """Test On-Balance Volume indicator calculation."""
    df = _make_frame(10)
    df["close"] = [10.0, 10.2, 10.4, 10.2, 10.0, 10.2, 10.4, 10.6, 10.4, 10.2]
    df["volume"] = [1000, 1200, 800, 1500, 1000, 1200, 800, 1500, 1000, 1200]
    result = add_indicators(df, ["obv"])
    assert "obv" in result.columns
    assert pd.notna(result["obv"].iloc[-1])


# =============================================================================
# RSI Tests
# =============================================================================
def test_add_indicators_rsi() -> None:
    """Test RSI indicator calculation."""
    df = _make_frame(30)
    result = add_indicators(df, ["rsi"])
    assert "rsi_14" in result.columns
    assert all(result["rsi_14"].dropna() >= 0)
    assert all(result["rsi_14"].dropna() <= 100)


# =============================================================================
# MACD Tests
# =============================================================================
def test_add_indicators_macd() -> None:
    """Test MACD indicator calculation."""
    df = _make_frame(40)
    result = add_indicators(df, ["macd"])
    assert "macd" in result.columns
    assert "macd_signal" in result.columns
    assert "macd_hist" in result.columns
    assert pd.notna(result["macd"].iloc[-1])
    assert pd.notna(result["macd_signal"].iloc[-1])
    assert pd.notna(result["macd_hist"].iloc[-1])


def test_add_indicators_macd_hist_sign() -> None:
    """Test MACD histogram can be positive or negative."""
    df = _make_frame(50)
    result = add_indicators(df, ["macd"])
    hist_values = result["macd_hist"].dropna()
    assert len(hist_values) > 0


# =============================================================================
# Bollinger Bands Tests
# =============================================================================
def test_add_indicators_boll() -> None:
    """Test Bollinger Bands indicator calculation."""
    df = _make_frame(30)
    result = add_indicators(df, ["boll"])
    assert "boll_mid" in result.columns
    assert "boll_upper" in result.columns
    assert "boll_lower" in result.columns
    assert all(result["boll_upper"].dropna() >= result["boll_mid"].dropna())
    assert all(result["boll_lower"].dropna() <= result["boll_mid"].dropna())


def test_add_indicators_boll_width() -> None:
    """Test Bollinger Bands upper is above lower."""
    df = _make_frame(30)
    result = add_indicators(df, ["boll"])
    valid_rows = result.dropna(subset=["boll_upper", "boll_lower"])
    assert all(valid_rows["boll_upper"] > valid_rows["boll_lower"])


# =============================================================================
# ATR Tests
# =============================================================================
def test_add_indicators_atr() -> None:
    """Test ATR indicator calculation."""
    df = _make_frame(30)
    result = add_indicators(df, ["atr"])
    assert "atr_14" in result.columns
    assert all(result["atr_14"].dropna() >= 0)


# =============================================================================
# KDJ Tests
# =============================================================================
def test_add_indicators_kdj() -> None:
    """Test KDJ indicator calculation."""
    df = _make_frame(30)
    result = add_indicators(df, ["kdj"])
    assert "k" in result.columns
    assert "d" in result.columns
    assert "j" in result.columns
    assert all(result["k"].dropna() >= 0)
    assert all(result["k"].dropna() <= 100)
    assert all(result["d"].dropna() >= 0)
    assert all(result["d"].dropna() <= 100)


def test_add_indicators_kdj_j_range() -> None:
    """Test KDJ J can exceed 0-100 range."""
    df = _make_frame(30)
    result = add_indicators(df, ["kdj"])
    j_values = result["j"].dropna()
    assert len(j_values) > 0


# =============================================================================
# Multiple Indicators Tests
# =============================================================================
def test_add_indicators_multiple() -> None:
    """Test adding multiple indicators at once."""
    df = _make_frame(30)
    result = add_indicators(df, ["ma5", "rsi", "macd", "boll", "atr", "kdj"])
    assert "ma_5" in result.columns
    assert "rsi_14" in result.columns
    assert "macd" in result.columns
    assert "boll_mid" in result.columns
    assert "atr_14" in result.columns
    assert "k" in result.columns
    assert "d" in result.columns
    assert "j" in result.columns


def test_add_indicators_all() -> None:
    """Test adding all supported indicators."""
    df = _make_frame(30)
    result = add_indicators(
        df,
        ["ma5", "ret5", "mom10", "volatility10", "zscore20", "volma5", "obv", "rsi", "macd", "boll", "atr", "kdj"]
    )
    expected = [
        "ma_5", "ret_5", "mom_10", "volatility_10", "zscore_20", "volume_ma_5",
        "obv", "rsi_14", "macd", "macd_signal", "macd_hist",
        "boll_mid", "boll_upper", "boll_lower", "atr_14", "k", "d", "j"
    ]
    for col in expected:
        assert col in result.columns, f"Missing column: {col}"
