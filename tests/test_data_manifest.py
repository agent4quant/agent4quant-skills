from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from agent4quant.data.manifest import build_duckdb_manifest, build_local_manifest, build_local_metadata
from agent4quant.data.sync import upsert_5m_to_duckdb


def test_build_local_manifest(tmp_path: Path) -> None:
    market_root = tmp_path / "market"
    (market_root / "cn" / "5m").mkdir(parents=True)
    pd.DataFrame(
        {
            "date": ["2025-01-01 09:35:00"],
            "open": [10],
            "high": [11],
            "low": [9.5],
            "close": [10.5],
            "volume": [100],
        }
    ).to_csv(market_root / "cn" / "5m" / "000001.SZ.csv", index=False)

    manifest = build_local_manifest(market_root, "5m", market="cn")
    metadata = build_local_metadata(market_root)

    assert manifest["provider"] == "local"
    assert manifest["symbols"] == 1
    assert manifest["files"][0]["symbol"] == "000001.SZ"
    assert manifest["files"][0]["market"] == "cn"
    assert manifest["files"][0]["rows"] == 1
    assert metadata["markets"]["cn"]["5m"]["symbols"] == 1


def test_build_duckdb_manifest(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    frame = pd.DataFrame(
        {
            "trade_date": [pd.Timestamp(date(2026, 3, 24))] * 2,
            "symbol": ["000001.SZ"] * 2,
            "ts": pd.to_datetime(["2026-03-24 09:35:00", "2026-03-24 09:40:00"]),
            "open": [10.0, 10.1],
            "high": [10.2, 10.3],
            "low": [9.9, 10.0],
            "close": [10.1, 10.2],
            "volume": [1000, 1200],
            "amount": [10000.0, 12000.0],
            "amplitude": [0.5, 0.6],
            "turnover": [0.01, 0.02],
            "pct_chg": [0.1, 0.2],
            "chg": [0.01, 0.02],
            "source": ["test", "test"],
            "ingested_at": [pd.Timestamp.utcnow(), pd.Timestamp.utcnow()],
        }
    )
    upsert_5m_to_duckdb(frame, str(db_path))

    manifest = build_duckdb_manifest(str(db_path), "5m")

    assert manifest["provider"] == "duckdb"
    assert manifest["symbols"] == 1
    assert manifest["total_rows"] == 2
    assert manifest["items"][0]["symbol"] == "000001.SZ"
