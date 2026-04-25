from __future__ import annotations

import sys
import types

import pandas as pd
import pytest

from agent4quant.data.providers import FetchRequest, YfinanceProvider
from agent4quant.errors import ExternalProviderError


def test_yfinance_provider_uses_expected_download_signature(monkeypatch) -> None:
    captured: dict[str, object] = {}
    cache_locations: list[str] = []

    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool,
        timeout: int,
        multi_level_index: bool,
    ) -> pd.DataFrame:
        captured.update(
            {
                "tickers": tickers,
                "start": start,
                "end": end,
                "interval": interval,
                "auto_adjust": auto_adjust,
                "progress": progress,
                "threads": threads,
                "timeout": timeout,
                "multi_level_index": multi_level_index,
            }
        )
        return pd.DataFrame(
            {
                "Open": [100.0, 101.0],
                "High": [102.0, 103.0],
                "Low": [99.0, 100.0],
                "Close": [101.0, 102.0],
                "Volume": [1000, 1200],
            },
            index=pd.to_datetime(["2025-01-02", "2025-01-03"]),
        )

    fake_module = types.SimpleNamespace(
        download=_download,
        set_tz_cache_location=lambda path: cache_locations.append(path),
    )
    monkeypatch.setitem(sys.modules, "yfinance", fake_module)

    provider = YfinanceProvider()
    result = provider.fetch(
        FetchRequest(
            symbol="AAPL",
            start="2025-01-02",
            end="2025-01-03",
            interval="1d",
            adjust="none",
        )
    )

    assert captured == {
        "tickers": "AAPL",
        "start": "2025-01-02",
        "end": "2025-01-04",
        "interval": "1d",
        "auto_adjust": False,
        "progress": False,
        "threads": False,
        "timeout": 10,
        "multi_level_index": True,
    }
    assert cache_locations
    assert result["symbol"].tolist() == ["AAPL", "AAPL"]
    assert result["close"].tolist() == [101.0, 102.0]


def test_yfinance_provider_rejects_non_none_adjust() -> None:
    provider = YfinanceProvider()

    with pytest.raises(ValueError, match="adjust=none"):
        provider.fetch(
            FetchRequest(
                symbol="AAPL",
                start="2025-01-02",
                end="2025-01-03",
                interval="1d",
                adjust="qfq",
            )
        )


def test_yfinance_provider_maps_hk_numeric_symbol(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool,
        timeout: int,
        multi_level_index: bool,
    ) -> pd.DataFrame:
        captured["tickers"] = tickers
        return pd.DataFrame(
            {
                "Open": [100.0],
                "High": [102.0],
                "Low": [99.0],
                "Close": [101.0],
                "Volume": [1000],
            },
            index=pd.to_datetime(["2025-01-02"]),
        )

    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(download=_download, set_tz_cache_location=lambda _path: None),
    )

    provider = YfinanceProvider()
    result = provider.fetch(
        FetchRequest(
            symbol="700",
            start="2025-01-02",
            end="2025-01-03",
            interval="1d",
            market="hk",
        )
    )

    assert captured["tickers"] == "00700.HK"
    assert result["symbol"].tolist() == ["700"]


def test_yfinance_provider_maps_sh_symbol_to_ss(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool,
        timeout: int,
        multi_level_index: bool,
    ) -> pd.DataFrame:
        captured["tickers"] = tickers
        return pd.DataFrame(
            {
                "Open": [10.0],
                "High": [10.2],
                "Low": [9.8],
                "Close": [10.1],
                "Volume": [1000],
            },
            index=pd.to_datetime(["2025-01-02"]),
        )

    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(download=_download, set_tz_cache_location=lambda _path: None),
    )

    provider = YfinanceProvider()
    provider.fetch(
        FetchRequest(
            symbol="600000.SH",
            start="2025-01-02",
            end="2025-01-03",
            interval="1d",
            market="cn",
        )
    )

    assert captured["tickers"] == "600000.SS"


def test_yfinance_provider_maps_us_class_share_symbol(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool,
        timeout: int,
        multi_level_index: bool,
    ) -> pd.DataFrame:
        captured["tickers"] = tickers
        return pd.DataFrame(
            {
                "Open": [400.0],
                "High": [405.0],
                "Low": [398.0],
                "Close": [403.0],
                "Volume": [800],
            },
            index=pd.to_datetime(["2025-01-02"]),
        )

    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(download=_download, set_tz_cache_location=lambda _path: None),
    )

    provider = YfinanceProvider()
    provider.fetch(
        FetchRequest(
            symbol="BRK.B",
            start="2025-01-02",
            end="2025-01-03",
            interval="1d",
            market="us",
        )
    )

    assert captured["tickers"] == "BRK-B"


def test_yfinance_provider_strips_us_suffix(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool,
        timeout: int,
        multi_level_index: bool,
    ) -> pd.DataFrame:
        captured["tickers"] = tickers
        return pd.DataFrame(
            {
                "Open": [100.0],
                "High": [101.0],
                "Low": [99.0],
                "Close": [100.5],
                "Volume": [1000],
            },
            index=pd.to_datetime(["2025-01-02"]),
        )

    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(download=_download, set_tz_cache_location=lambda _path: None),
    )

    provider = YfinanceProvider()
    provider.fetch(
        FetchRequest(
            symbol="AAPL.US",
            start="2025-01-02",
            end="2025-01-03",
            interval="1d",
            market="us",
        )
    )

    assert captured["tickers"] == "AAPL"


def test_yfinance_provider_flattens_multi_index_columns(monkeypatch) -> None:
    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool,
        timeout: int,
        multi_level_index: bool,
    ) -> pd.DataFrame:
        columns = pd.MultiIndex.from_tuples(
            [
                ("Open", "AAPL"),
                ("High", "AAPL"),
                ("Low", "AAPL"),
                ("Close", "AAPL"),
                ("Volume", "AAPL"),
            ]
        )
        return pd.DataFrame(
            [[100.0, 102.0, 99.0, 101.0, 1000]],
            index=pd.to_datetime(["2025-01-02"]),
            columns=columns,
        )

    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(download=_download, set_tz_cache_location=lambda _path: None),
    )

    provider = YfinanceProvider()
    result = provider.fetch(
        FetchRequest(
            symbol="AAPL",
            start="2025-01-02",
            end="2025-01-03",
            interval="1d",
        )
    )

    assert result["close"].tolist() == [101.0]
    assert result["volume"].tolist() == [1000]


def test_yfinance_provider_raises_upstream_error_from_shared_errors(monkeypatch) -> None:
    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool,
        timeout: int,
        multi_level_index: bool,
    ) -> pd.DataFrame:
        return pd.DataFrame()

    fake_module = types.SimpleNamespace(
        download=_download,
        set_tz_cache_location=lambda _path: None,
        shared=types.SimpleNamespace(_ERRORS={"AAPL": "YFRateLimitError('Too Many Requests')"}),
    )
    monkeypatch.setitem(sys.modules, "yfinance", fake_module)

    provider = YfinanceProvider()
    with pytest.raises(ExternalProviderError, match="rate limited") as exc_info:
        provider.fetch(
            FetchRequest(
                symbol="AAPL",
                start="2025-01-02",
                end="2025-01-03",
                interval="1d",
            )
        )

    assert exc_info.value.category == "rate_limit"
    assert exc_info.value.retryable is True


def test_yfinance_provider_classifies_not_found_error(monkeypatch) -> None:
    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool,
        timeout: int,
        multi_level_index: bool,
    ) -> pd.DataFrame:
        return pd.DataFrame()

    fake_module = types.SimpleNamespace(
        download=_download,
        set_tz_cache_location=lambda _path: None,
        shared=types.SimpleNamespace(_ERRORS={"AAPL": "possibly delisted; no price data found"}),
    )
    monkeypatch.setitem(sys.modules, "yfinance", fake_module)

    provider = YfinanceProvider()
    with pytest.raises(ExternalProviderError, match="symbol not found") as exc_info:
        provider.fetch(
            FetchRequest(
                symbol="AAPL",
                start="2025-01-02",
                end="2025-01-03",
                interval="1d",
            )
        )

    assert exc_info.value.category == "not_found"
    assert exc_info.value.retryable is False


def test_yfinance_provider_classifies_timeout_error(monkeypatch) -> None:
    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool,
        timeout: int,
        multi_level_index: bool,
    ) -> pd.DataFrame:
        raise RuntimeError("curl: (28) Operation timed out after 10001 milliseconds")

    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(download=_download, set_tz_cache_location=lambda _path: None),
    )

    provider = YfinanceProvider()
    with pytest.raises(ExternalProviderError, match="timeout") as exc_info:
        provider.fetch(
            FetchRequest(
                symbol="AAPL",
                start="2025-01-02",
                end="2025-01-03",
                interval="1d",
            )
        )

    assert exc_info.value.category == "timeout"


def test_yfinance_provider_falls_back_to_akshare_for_hk(monkeypatch) -> None:
    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool,
        timeout: int,
        multi_level_index: bool,
    ) -> pd.DataFrame:
        return pd.DataFrame()

    fake_yf = types.SimpleNamespace(
        download=_download,
        set_tz_cache_location=lambda _path: None,
        shared=types.SimpleNamespace(_ERRORS={"00700.HK": "possibly delisted; no price data found"}),
    )
    fake_ak = types.SimpleNamespace(
        stock_hk_daily=lambda symbol, adjust="": pd.DataFrame(
            {
                "date": ["2025-01-02", "2025-01-03"],
                "open": [100.0, 101.0],
                "high": [102.0, 103.0],
                "low": [99.0, 100.0],
                "close": [101.0, 102.0],
                "volume": [1000.0, 1200.0],
                "amount": [0.0, 0.0],
            }
        )
    )
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    provider = YfinanceProvider()
    result = provider.fetch(
        FetchRequest(
            symbol="700",
            start="2025-01-02",
            end="2025-01-03",
            interval="1d",
            market="hk",
        )
    )

    assert result["symbol"].tolist() == ["700", "700"]
    assert result.attrs["source_provider"] == "akshare.stock_hk_daily"
    assert result.attrs["provider_route"] == "yfinance.hk_fallback"


def test_yfinance_provider_hk_fallback_error_mentions_fallback(monkeypatch) -> None:
    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool,
        timeout: int,
        multi_level_index: bool,
    ) -> pd.DataFrame:
        return pd.DataFrame()

    fake_yf = types.SimpleNamespace(
        download=_download,
        set_tz_cache_location=lambda _path: None,
        shared=types.SimpleNamespace(_ERRORS={"00700.HK": "possibly delisted; no price data found"}),
    )
    fake_ak = types.SimpleNamespace(stock_hk_daily=lambda symbol, adjust="": pd.DataFrame())
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    provider = YfinanceProvider()
    with pytest.raises(ExternalProviderError, match="HK fallback via akshare.stock_hk_daily") as exc_info:
        provider.fetch(
            FetchRequest(
                symbol="700",
                start="2025-01-02",
                end="2025-01-03",
                interval="1d",
                market="hk",
            )
        )

    assert exc_info.value.category == "not_found"
