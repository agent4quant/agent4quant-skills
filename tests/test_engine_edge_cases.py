from __future__ import annotations

import math
from pathlib import Path

import pandas as pd
import pytest

from agent4quant.backtest.engine import run_backtest
from agent4quant.data.adjustments import apply_price_adjustment
from agent4quant.data.service import fetch_dataset
from agent4quant.risk.engine import analyze_risk


def _market_frame(symbol: str, closes: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=len(closes), freq="D"),
            "open": closes,
            "high": [value + 0.5 for value in closes],
            "low": [max(value - 0.5, 0.01) for value in closes],
            "close": closes,
            "volume": [1000] * len(closes),
            "symbol": [symbol] * len(closes),
        }
    )


def test_run_backtest_rejects_empty_dataset(monkeypatch) -> None:
    monkeypatch.setattr("agent4quant.backtest.engine.fetch_dataset", lambda **_kwargs: (pd.DataFrame(), {}))

    with pytest.raises(ValueError, match="Backtest dataset is empty"):
        run_backtest(
            provider="demo",
            symbol="DEMO.SH",
            start="2025-01-01",
            end="2025-01-02",
            interval="1d",
            strategy="sma_cross",
            strategy_params={"fast": 3, "slow": 8},
        )


def test_run_backtest_rejects_empty_benchmark_dataset(monkeypatch) -> None:
    calls = {"count": 0}

    def _fake_fetch(**kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            return _market_frame(kwargs["symbol"], [10.0, 11.0, 12.0, 13.0]), {}
        return pd.DataFrame(), {}

    monkeypatch.setattr("agent4quant.backtest.engine.fetch_dataset", _fake_fetch)

    with pytest.raises(ValueError, match="Benchmark dataset is empty"):
        run_backtest(
            provider="demo",
            symbol="DEMO.SH",
            benchmark_symbol="BMK.SH",
            start="2025-01-01",
            end="2025-01-04",
            interval="1d",
            strategy="sma_cross",
            strategy_params={"fast": 2, "slow": 3},
        )


def test_run_backtest_handles_no_trade_scenario(monkeypatch) -> None:
    monkeypatch.setattr(
        "agent4quant.backtest.engine.fetch_dataset",
        lambda **kwargs: (_market_frame(kwargs["symbol"], [10.0] * 10), {}),
    )

    result = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        benchmark_symbol="BMK.SH",
        start="2025-01-01",
        end="2025-01-10",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 3, "slow": 5},
    )

    assert result["metrics"]["trade_count"] == 0
    assert result["metrics"]["win_rate"] == 0.0


def test_risk_handles_extreme_volatility(monkeypatch) -> None:
    monkeypatch.setattr(
        "agent4quant.risk.engine.fetch_dataset",
        lambda **kwargs: (_market_frame(kwargs["symbol"], [10.0, 20.0, 5.0, 30.0, 3.0, 40.0]), {}),
    )

    result = analyze_risk(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-01-06",
        interval="1d",
        mode="market",
    )

    assert result["metrics"]["volatility"] > 0
    assert math.isfinite(result["metrics"]["annualized_volatility"])
    assert result["metrics"]["worst_return"] < 0
    assert result["metrics"]["best_return"] > 0


def test_apply_price_adjustment_ignores_invalid_adj_factor_rows() -> None:
    frame = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=3, freq="D"),
            "open": [10.0, 11.0, 12.0],
            "high": [10.2, 11.2, 12.2],
            "low": [9.8, 10.8, 11.8],
            "close": [10.0, 11.0, 12.0],
            "volume": [100, 120, 140],
            "adj_factor": [1.0, -1.0, 1.5],
        }
    )

    adjusted = apply_price_adjustment(frame, "qfq")

    assert adjusted["close"].iloc[1] == pytest.approx(11.0)
    assert adjusted["volume"].iloc[1] == pytest.approx(120.0)
    assert adjusted["close"].iloc[0] == pytest.approx(10.0 / 1.5)


def test_local_provider_missing_market_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        fetch_dataset(
            provider="local",
            symbol="000001.SZ",
            start="2025-01-01",
            end="2025-01-02",
            interval="1d",
            indicators=[],
            data_root=str(tmp_path),
            market="cn",
        )
