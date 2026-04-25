from __future__ import annotations

import pandas as pd

from agent4quant.data.providers import FetchRequest, DemoProvider


def test_fetch_request_defaults() -> None:
    """Test FetchRequest default values."""
    req = FetchRequest(
        symbol="TEST.SH",
        start="2025-01-01",
        end="2025-01-10",
        interval="1d",
    )
    assert req.symbol == "TEST.SH"
    assert req.start == "2025-01-01"
    assert req.end == "2025-01-10"
    assert req.interval == "1d"
    assert req.adjust == "none"


def test_demo_provider_returns_dataframe() -> None:
    """Test DemoProvider returns DataFrame."""
    provider = DemoProvider()
    result = provider.fetch(
        FetchRequest(
            symbol="DEMO.SH",
            start="2025-01-01",
            end="2025-01-10",
            interval="1d",
        )
    )
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0


def test_demo_provider_has_required_columns() -> None:
    """Test DemoProvider returns required columns."""
    provider = DemoProvider()
    result = provider.fetch(
        FetchRequest(
            symbol="DEMO.SH",
            start="2025-01-01",
            end="2025-01-10",
            interval="1d",
        )
    )
    required = ["date", "open", "high", "low", "close", "volume", "symbol"]
    for col in required:
        assert col in result.columns


def test_demo_provider_symbol_normalized() -> None:
    """Test DemoProvider normalizes symbol."""
    provider = DemoProvider()
    result = provider.fetch(
        FetchRequest(
            symbol="test.sz",
            start="2025-01-01",
            end="2025-01-10",
            interval="1d",
        )
    )
    assert all(result["symbol"] == "TEST.SZ")


def test_demo_provider_date_range() -> None:
    """Test DemoProvider respects date range."""
    provider = DemoProvider()
    result = provider.fetch(
        FetchRequest(
            symbol="DEMO.SH",
            start="2025-03-01",
            end="2025-03-10",
            interval="1d",
        )
    )
    assert len(result) <= 10


def test_demo_provider_weekly_interval() -> None:
    """Test DemoProvider supports weekly interval."""
    provider = DemoProvider()
    result = provider.fetch(
        FetchRequest(
            symbol="DEMO.SH",
            start="2025-01-01",
            end="2025-03-31",
            interval="1w",
        )
    )
    assert len(result) <= 13  # ~13 weeks in 3 months
