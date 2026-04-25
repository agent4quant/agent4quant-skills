from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from agent4quant.backtest.engine import run_backtest
from agent4quant.data.service import available_symbols, build_data_manifest, fetch_dataset
from agent4quant.data.sync import upsert_5m_to_duckdb


def _seed_duckdb(db_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "trade_date": [pd.Timestamp(date(2026, 3, 24))] * 4,
            "symbol": ["000001.SZ"] * 4,
            "ts": pd.to_datetime(
                [
                    "2026-03-24 09:35:00",
                    "2026-03-24 09:40:00",
                    "2026-03-24 09:45:00",
                    "2026-03-24 09:50:00",
                ]
            ),
            "open": [10.0, 10.1, 10.2, 10.3],
            "high": [10.1, 10.2, 10.3, 10.4],
            "low": [9.9, 10.0, 10.1, 10.2],
            "close": [10.05, 10.15, 10.25, 10.35],
            "volume": [1000, 1200, 1100, 1300],
            "amount": [10050.0, 12180.0, 11275.0, 13455.0],
            "amplitude": [0.5, 0.6, 0.4, 0.3],
            "turnover": [0.01, 0.02, 0.015, 0.018],
            "pct_chg": [0.1, 0.2, 0.1, 0.1],
            "chg": [0.01, 0.02, 0.01, 0.01],
            "source": ["test"] * 4,
            "ingested_at": [pd.Timestamp.utcnow()] * 4,
        }
    )
    upsert_5m_to_duckdb(frame, str(db_path))


def test_duckdb_provider_lists_symbols_and_fetches(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _seed_duckdb(db_path)

    symbols = available_symbols(provider="duckdb", interval="5m", db_path=str(db_path))
    frame, metadata = fetch_dataset(
        provider="duckdb",
        symbol="000001.SZ",
        start="2026-03-24 09:35:00",
        end="2026-03-24 09:50:00",
        interval="5m",
        indicators=["ma2"],
        db_path=str(db_path),
    )

    assert symbols == ["000001.SZ"]
    assert len(frame) == 4
    assert "ma_2" in frame.columns
    assert metadata["provider"] == "duckdb"


def test_duckdb_provider_supports_backtest(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _seed_duckdb(db_path)

    result = run_backtest(
        provider="duckdb",
        symbol="000001.SZ",
        start="2026-03-24 09:35:00",
        end="2026-03-24 09:50:00",
        interval="5m",
        strategy="sma_cross",
        strategy_params={"fast": 1, "slow": 2},
        db_path=str(db_path),
    )

    assert result["symbol"] == "000001.SZ"
    assert "metrics" in result
    assert result["period"]["interval"] == "5m"


def test_duckdb_manifest_exposed_via_service(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _seed_duckdb(db_path)

    manifest = build_data_manifest(provider="duckdb", interval="5m", db_path=str(db_path))

    assert manifest["provider"] == "duckdb"
    assert manifest["symbols"] == 1
    assert manifest["items"][0]["rows"] == 4
