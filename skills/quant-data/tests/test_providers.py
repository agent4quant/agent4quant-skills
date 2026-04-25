from __future__ import annotations

import sys
import types

import pandas as pd

from agent4quant.data.providers import FetchRequest, DemoProvider, YfinanceProvider


def test_demo_provider_fetch() -> None:
    """Test DemoProvider returns valid data."""
    provider = DemoProvider()
    result = provider.fetch(
        FetchRequest(
            symbol="DEMO.SH",
            start="2025-01-01",
            end="2025-01-10",
            interval="1d",
        )
    )

    assert "date" in result.columns
    assert "open" in result.columns
    assert "high" in result.columns
    assert "low" in result.columns
    assert "close" in result.columns
    assert "volume" in result.columns
    assert "symbol" in result.columns
    assert len(result) > 0


def test_demo_provider_returns_correct_symbol() -> None:
    """Test DemoProvider returns correct symbol."""
    provider = DemoProvider()
    result = provider.fetch(
        FetchRequest(
            symbol="TEST.SZ",
            start="2025-01-01",
            end="2025-01-10",
            interval="1d",
        )
    )

    assert all(result["symbol"] == "TEST.SZ")


def test_demo_provider_respects_date_range() -> None:
    """Test DemoProvider respects start and end dates."""
    provider = DemoProvider()
    result = provider.fetch(
        FetchRequest(
            symbol="DEMO.SH",
            start="2025-01-05",
            end="2025-01-10",
            interval="1d",
        )
    )

    assert len(result) <= 6


def test_yfinance_provider_uses_correct_function(monkeypatch) -> None:
    """Test YfinanceProvider calls correct yfinance function."""
    captured: dict = {}

    def _mock_ticker(symbol: str):
        def _download(start: str, end: str, interval: str):
            captured.update({
                "symbol": symbol,
                "start": start,
                "end": end,
                "interval": interval,
            })
            return pd.DataFrame({
                "Open": [10.0, 10.2],
                "High": [10.5, 10.7],
                "Low": [9.5, 9.7],
                "Close": [10.1, 10.3],
                "Volume": [1000, 1200],
            })
        return _download

    monkeypatch.setattr(sys.modules.get("yfinance", types.SimpleNamespace()), "Ticker", _mock_ticker)

    from agent4quant.data.providers import YfinanceProvider
    provider = YfinanceProvider()
    result = provider.fetch(
        FetchRequest(
            symbol="000001.SZ",
            start="2025-01-02",
            end="2025-01-03",
            interval="1d",
        )
    )

    assert "symbol" in captured
