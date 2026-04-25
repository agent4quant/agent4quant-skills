from __future__ import annotations

import json
from datetime import date
from pathlib import Path
import math

import pandas as pd

from agent4quant.alpha.engine import analyze_alpha
from agent4quant.backtest.engine import compare_backtests
from agent4quant.backtest.engine import run_backtest, write_backtest_result
from agent4quant.data.service import build_data_manifest
from agent4quant.data.service import batch_fetch_datasets
from agent4quant.data.sync import upsert_5m_to_duckdb
from agent4quant.report.generator import generate_report
from agent4quant.risk.engine import analyze_risk


def _seed_symbol(db_path: Path, symbol: str, close_values: list[float]) -> None:
    timestamps = pd.to_datetime(
        [
            "2026-03-24 09:35:00",
            "2026-03-24 09:40:00",
            "2026-03-24 09:45:00",
            "2026-03-24 09:50:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "trade_date": [pd.Timestamp(date(2026, 3, 24))] * len(close_values),
            "symbol": [symbol] * len(close_values),
            "ts": timestamps[: len(close_values)],
            "open": close_values,
            "high": [value + 0.1 for value in close_values],
            "low": [value - 0.1 for value in close_values],
            "close": close_values,
            "volume": [1000, 1100, 1200, 1300][: len(close_values)],
            "amount": [value * 1000 for value in close_values],
            "amplitude": [0.5] * len(close_values),
            "turnover": [0.01] * len(close_values),
            "pct_chg": [0.1] * len(close_values),
            "chg": [0.01] * len(close_values),
            "source": ["test.e2e"] * len(close_values),
            "ingested_at": [pd.Timestamp.utcnow()] * len(close_values),
        }
    )
    upsert_5m_to_duckdb(frame, str(db_path))


def _intraday_timestamps(trade_date: str) -> pd.DatetimeIndex:
    morning = pd.date_range(f"{trade_date} 09:35:00", f"{trade_date} 11:30:00", freq="5min")
    afternoon = pd.date_range(f"{trade_date} 13:05:00", f"{trade_date} 15:00:00", freq="5min")
    return morning.append(afternoon)


def _seed_large_symbol(db_path: Path, symbol: str, base_price: float, trade_dates: list[str]) -> None:
    for offset, trade_day in enumerate(trade_dates):
        timestamps = _intraday_timestamps(trade_day)
        closes = [
            round(base_price + offset * 0.8 + index * 0.02 + math.sin(index / 3) * 0.25, 4)
            for index in range(len(timestamps))
        ]
        frame = pd.DataFrame(
            {
                "trade_date": [pd.Timestamp(trade_day)] * len(timestamps),
                "symbol": [symbol] * len(timestamps),
                "ts": timestamps,
                "open": closes,
                "high": [value + 0.08 for value in closes],
                "low": [value - 0.08 for value in closes],
                "close": closes,
                "volume": [1000 + (index % 12) * 50 for index in range(len(timestamps))],
                "amount": [value * 1000 for value in closes],
                "amplitude": [0.5] * len(timestamps),
                "turnover": [0.01] * len(timestamps),
                "pct_chg": [0.1] * len(timestamps),
                "chg": [0.01] * len(timestamps),
                "source": ["test.large"] * len(timestamps),
                "ingested_at": [pd.Timestamp.utcnow()] * len(timestamps),
            }
        )
        upsert_5m_to_duckdb(frame, str(db_path))


def test_duckdb_multi_symbol_end_to_end_pipeline(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    output_dir = tmp_path / "batch"
    result_json = tmp_path / "backtest.json"
    report_html = tmp_path / "report.html"

    _seed_symbol(db_path, "000001.SZ", [10.0, 10.2, 10.4, 10.6])
    _seed_symbol(db_path, "600000.SH", [20.0, 20.1, 20.2, 20.3])

    batch = batch_fetch_datasets(
        provider="duckdb",
        symbols=["000001.SZ", "600000.SH"],
        start="2026-03-24 09:35:00",
        end="2026-03-24 09:50:00",
        interval="5m",
        indicators=["ma2"],
        output_dir=str(output_dir),
        output_format="json",
        db_path=str(db_path),
    )

    backtest = run_backtest(
        provider="duckdb",
        symbol="000001.SZ",
        benchmark_symbol="600000.SH",
        start="2026-03-24 09:35:00",
        end="2026-03-24 09:50:00",
        interval="5m",
        strategy="sma_cross",
        strategy_params={"fast": 1, "slow": 2},
        db_path=str(db_path),
    )
    write_backtest_result(backtest, str(result_json))
    generate_report(str(result_json), str(report_html), "DuckDB E2E", output_format="html", watermark="E2E")

    assert batch["symbols"] == 2
    assert json.loads((output_dir / "000001.SZ.json").read_text(encoding="utf-8"))["metadata"]["provider"] == "duckdb"
    assert backtest["benchmark"]["symbol"] == "600000.SH"
    assert report_html.exists()
    assert "DuckDB E2E" in report_html.read_text(encoding="utf-8")


def test_duckdb_large_scale_regression_pipeline(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    output_dir = tmp_path / "batch-large"
    trade_dates = ["2026-03-23", "2026-03-24", "2026-03-25"]
    universe = {
        "000001.SZ": 10.0,
        "600000.SH": 12.0,
        "300750.SZ": 18.0,
        "600519.SH": 22.0,
    }

    for symbol, price in universe.items():
        _seed_large_symbol(db_path, symbol, price, trade_dates)

    batch = batch_fetch_datasets(
        provider="duckdb",
        symbols=list(universe),
        start="2026-03-23 09:35:00",
        end="2026-03-25 15:00:00",
        interval="5m",
        indicators=["ma5", "rsi"],
        output_dir=str(output_dir),
        output_format="json",
        db_path=str(db_path),
    )
    compare = compare_backtests(
        provider="duckdb",
        symbol="000001.SZ",
        benchmark_symbol="600000.SH",
        start="2026-03-23 09:35:00",
        end="2026-03-25 15:00:00",
        interval="5m",
        strategies=["sma_cross", "macd", "boll_breakout"],
        strategy_params_map={"sma_cross": {"fast": 3, "slow": 8}},
        metric="sharpe",
        db_path=str(db_path),
    )
    risk = analyze_risk(
        provider="duckdb",
        symbol="000001.SZ",
        start="2026-03-23 09:35:00",
        end="2026-03-25 15:00:00",
        interval="5m",
        mode="market",
        db_path=str(db_path),
    )
    alpha = analyze_alpha(
        provider="duckdb",
        symbol="000001.SZ",
        start="2026-03-23 09:35:00",
        end="2026-03-25 15:00:00",
        interval="5m",
        factors=["ma_5", "rsi_14"],
        indicators=["ma5", "rsi"],
        horizon=3,
        ic_window=10,
        db_path=str(db_path),
    )
    manifest = build_data_manifest(provider="duckdb", interval="5m", db_path=str(db_path))

    assert batch["symbols"] == 4
    assert manifest["symbols"] == 4
    assert manifest["total_rows"] == 4 * 3 * 48
    assert compare["returned_experiments"] >= 2
    assert risk["metrics"]["observations"] >= 100
    assert alpha["best_factor"] in {"ma_5", "rsi_14"}
