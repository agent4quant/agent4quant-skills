from __future__ import annotations

from pathlib import Path

import pytest
import pandas as pd

from agent4quant.data.catalog import list_symbols, resolve_symbol_file
from agent4quant.data.providers import _read_market_file
from agent4quant.data.service import (
    available_symbols,
    batch_fetch_datasets,
    build_local_data_index,
    fetch_dataset,
    write_local_directory_metadata,
    validate_dataset,
)


def _write_sample(root: Path, symbol: str, interval: str) -> None:
    target = root / interval
    target.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=5, freq="D"),
            "open": [1, 2, 3, 4, 5],
            "high": [2, 3, 4, 5, 6],
            "low": [0.5, 1.5, 2.5, 3.5, 4.5],
            "close": [1.5, 2.5, 3.5, 4.5, 5.5],
            "volume": [10, 11, 12, 13, 14],
        }
    )
    frame.to_csv(target / f"{symbol}.csv", index=False)


def _write_adjusted_sample(root: Path, market: str, symbol: str, interval: str) -> None:
    target = root / market / interval
    target.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=3, freq="D"),
            "open": [10.0, 11.0, 12.0],
            "high": [10.2, 11.2, 12.2],
            "low": [9.8, 10.8, 11.8],
            "close": [10.0, 11.0, 12.0],
            "volume": [100, 120, 140],
            "adj_factor": [1.0, 1.2, 1.5],
        }
    )
    frame.to_csv(target / f"{symbol}.csv", index=False)


def test_catalog_discovers_symbols(tmp_path: Path) -> None:
    _write_sample(tmp_path, "000001.SZ", "5m")
    _write_sample(tmp_path, "600000.SH", "5m")

    assert resolve_symbol_file(tmp_path, "000001.SZ", "5m").name == "000001.SZ.csv"
    assert list_symbols(tmp_path, "5m") == ["000001.SZ", "600000.SH"]


def test_local_provider_fetches_windowed_dataset(tmp_path: Path) -> None:
    _write_sample(tmp_path, "000001.SZ", "5m")

    frame, metadata = fetch_dataset(
        provider="local",
        symbol="000001.SZ",
        start="2025-01-02",
        end="2025-01-04",
        interval="5m",
        indicators=["ma2"],
        data_root=str(tmp_path),
    )

    assert len(frame) == 3
    assert metadata["skill"] == "quant-data"
    assert "ma_2" in frame.columns
    assert available_symbols(provider="local", interval="5m", data_root=str(tmp_path)) == ["000001.SZ"]


def test_local_provider_validates_directory(tmp_path: Path) -> None:
    _write_sample(tmp_path, "000001.SZ", "5m")
    result = validate_dataset(provider="local", interval="5m", data_root=str(tmp_path))

    assert result["symbols"] == 1
    assert result["valid_symbols"] == 1
    assert result["invalid_symbols"] == 0
    assert result["results"][0]["symbol"] == "000001.SZ"


def test_batch_fetch_writes_multiple_outputs(tmp_path: Path) -> None:
    _write_sample(tmp_path, "000001.SZ", "5m")
    _write_sample(tmp_path, "600000.SH", "5m")
    output_dir = tmp_path / "exports"

    result = batch_fetch_datasets(
        provider="local",
        symbols=["000001.SZ", "600000.SH"],
        start="2025-01-01",
        end="2025-01-05",
        interval="5m",
        indicators=["ma2"],
        output_dir=str(output_dir),
        output_format="csv",
        data_root=str(tmp_path),
    )

    assert result["symbols"] == 2
    assert (output_dir / "000001.SZ.csv").exists()
    assert (output_dir / "600000.SH.csv").exists()


def test_catalog_prefers_parquet_path_when_both_exist(tmp_path: Path) -> None:
    _write_sample(tmp_path, "000001.SZ", "1d")
    parquet_path = tmp_path / "1d" / "000001.SZ.parquet"
    parquet_path.write_text("placeholder", encoding="utf-8")

    resolved = resolve_symbol_file(tmp_path, "000001.SZ", "1d", use_index=False)

    assert resolved.suffix == ".parquet"


def test_read_market_file_falls_back_to_csv_when_parquet_unavailable(monkeypatch, tmp_path: Path) -> None:
    _write_sample(tmp_path, "000001.SZ", "1d")
    parquet_path = tmp_path / "1d" / "000001.SZ.parquet"
    parquet_path.write_text("placeholder", encoding="utf-8")

    def _boom(_: Path):
        raise ImportError("pyarrow missing")

    monkeypatch.setattr(pd, "read_parquet", _boom)
    frame = _read_market_file(parquet_path, "000001.SZ", "2025-01-01", "2025-01-05")

    assert len(frame) == 5
    assert frame["symbol"].iloc[0] == "000001.SZ"


def test_build_local_index_and_resolve_with_index(tmp_path: Path) -> None:
    _write_sample(tmp_path, "000001.SZ", "5m")
    result = build_local_data_index(data_root=str(tmp_path), interval="5m")

    resolved = resolve_symbol_file(tmp_path, "000001.SZ", "5m")

    assert result["symbols"] == 1
    assert resolved.name == "000001.SZ.csv"
    assert (tmp_path / ".a4q-data-index.json").exists()


def test_local_provider_supports_adjustment_and_market_layers(tmp_path: Path) -> None:
    _write_adjusted_sample(tmp_path, "cn", "000001.SZ", "1d")

    qfq_frame, qfq_meta = fetch_dataset(
        provider="local",
        symbol="000001.SZ",
        start="2025-01-01",
        end="2025-01-03",
        interval="1d",
        indicators=[],
        data_root=str(tmp_path),
        adjust="qfq",
        market="cn",
    )
    hfq_frame, _ = fetch_dataset(
        provider="local",
        symbol="000001.SZ",
        start="2025-01-01",
        end="2025-01-03",
        interval="1d",
        indicators=[],
        data_root=str(tmp_path),
        adjust="hfq",
        market="cn",
    )

    assert qfq_meta["adjust"] == "qfq"
    assert qfq_meta["market"] == "cn"
    assert qfq_frame["close"].iloc[-1] == pytest.approx(12.0)
    assert qfq_frame["close"].iloc[0] == pytest.approx(10.0 / 1.5)
    assert hfq_frame["close"].iloc[0] == pytest.approx(10.0)
    assert hfq_frame["close"].iloc[-1] == pytest.approx(18.0)
    assert available_symbols(provider="local", interval="1d", data_root=str(tmp_path), market="cn") == ["000001.SZ"]
    assert list_symbols(tmp_path, "1d", market="cn") == ["000001.SZ"]
    assert resolve_symbol_file(tmp_path, "000001.SZ", "1d", market="cn").name == "000001.SZ.csv"


def test_write_local_directory_metadata_and_market_index(tmp_path: Path) -> None:
    _write_sample(tmp_path / "cn", "000001.SZ", "5m")
    _write_sample(tmp_path / "us", "AAPL", "1d")

    index_result = build_local_data_index(data_root=str(tmp_path), market="cn")
    metadata_result = write_local_directory_metadata(data_root=str(tmp_path))

    assert index_result["market"] == "cn"
    assert index_result["symbols"] == 1
    assert metadata_result["entries"] == 2
    assert metadata_result["markets"] == ["cn", "us"]
    assert (tmp_path / ".a4q-dir-metadata.json").exists()
