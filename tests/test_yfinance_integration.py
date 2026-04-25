from __future__ import annotations

import sys
import types

import pandas as pd

from agent4quant.alpha.engine import analyze_alpha
from agent4quant.backtest.engine import run_backtest
from agent4quant.data.service import fetch_dataset
from agent4quant.risk.engine import analyze_risk


def _fake_yfinance_module():
    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool = False,
        timeout: int = 10,
        multi_level_index: bool = True,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "Open": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0, 111.0],
                "High": [101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0, 111.0, 112.0],
                "Low": [99.0, 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0],
                "Close": [100.5, 101.5, 102.5, 103.5, 104.5, 105.5, 106.5, 107.5, 108.5, 109.5, 110.5, 111.5],
                "Volume": [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000, 2100],
            },
            index=pd.date_range("2025-01-01", periods=12, freq="D"),
        )

    return types.SimpleNamespace(download=_download)


def test_run_backtest_with_yfinance_provider(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "yfinance", _fake_yfinance_module())

    result = run_backtest(
        provider="yfinance",
        symbol="AAPL",
        benchmark_symbol="MSFT",
        start="2025-01-01",
        end="2025-01-12",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 3, "slow": 5},
    )

    assert result["strategy"] == "sma_cross"
    assert result["symbol"] == "AAPL"
    assert "metrics" in result


def test_alpha_and_risk_with_yfinance_provider(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "yfinance", _fake_yfinance_module())

    alpha_result = analyze_alpha(
        provider="yfinance",
        symbol="AAPL",
        start="2025-01-01",
        end="2025-01-12",
        interval="1d",
        factors=["ma_5"],
        indicators=["ma5"],
        horizon=1,
        ic_window=3,
        quantiles=3,
    )
    risk_result = analyze_risk(
        provider="yfinance",
        symbol="AAPL",
        start="2025-01-01",
        end="2025-01-12",
        interval="1d",
        mode="market",
        rolling_window=3,
    )

    assert alpha_result["metadata"]["provider"] == "yfinance"
    assert alpha_result["best_factor"]
    assert risk_result["metadata"]["provider"] == "yfinance"
    assert len(risk_result["rolling_var"]) > 0


def test_fetch_dataset_exposes_hk_fallback_metadata(monkeypatch) -> None:
    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool = False,
        timeout: int = 10,
        multi_level_index: bool = True,
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

    frame, metadata = fetch_dataset(
        provider="yfinance",
        symbol="700",
        start="2025-01-02",
        end="2025-01-03",
        interval="1d",
        indicators=["ma5"],
        market="hk",
    )

    assert len(frame) == 2
    assert metadata["provider"] == "yfinance"
    assert metadata["source_provider"] == "akshare.stock_hk_daily"
    assert metadata["provider_route"] == "yfinance.hk_fallback"
