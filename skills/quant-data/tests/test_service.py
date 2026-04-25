from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from agent4quant.data.service import (
    write_dataset,
    validate_dataset,
    repair_dataset,
)
from agent4quant.data.validation import validate_market_file, repair_frame


# =============================================================================
# write_dataset Tests
# =============================================================================
def test_write_dataset_json(tmp_path: Path) -> None:
    """Test writing dataset as JSON."""
    df = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=5, freq="D"),
        "open": [10.0] * 5,
        "high": [10.5] * 5,
        "low": [9.5] * 5,
        "close": [10.1, 10.2, 10.3, 10.4, 10.5],
        "volume": [1000] * 5,
        "symbol": ["TEST.SH"] * 5,
    })
    metadata = {"provider": "demo", "interval": "1d"}

    output_path = tmp_path / "output.json"
    write_dataset(df, metadata, str(output_path), "json")

    assert output_path.exists()
    with open(output_path) as f:
        package = json.load(f)
    assert "metadata" in package
    assert "rows" in package
    assert len(package["rows"]) == 5


def test_write_dataset_csv(tmp_path: Path) -> None:
    """Test writing dataset as CSV."""
    df = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=5, freq="D"),
        "open": [10.0] * 5,
        "high": [10.5] * 5,
        "low": [9.5] * 5,
        "close": [10.1, 10.2, 10.3, 10.4, 10.5],
        "volume": [1000] * 5,
        "symbol": ["TEST.SH"] * 5,
    })
    metadata = {"provider": "demo", "interval": "1d"}

    output_path = tmp_path / "output.csv"
    write_dataset(df, metadata, str(output_path), "csv")

    assert output_path.exists()
    meta_path = tmp_path / "output.meta.json"
    assert meta_path.exists()
    with open(meta_path) as f:
        saved_meta = json.load(f)
    assert saved_meta["provider"] == "demo"


def test_write_dataset_unsupported_format(tmp_path: Path) -> None:
    """Test unsupported output format raises error."""
    df = pd.DataFrame({"close": [10.0]})
    metadata = {}
    output_path = tmp_path / "output.xlsx"
    with pytest.raises(ValueError, match="Unsupported output format"):
        write_dataset(df, metadata, str(output_path), "xlsx")


# =============================================================================
# validate_dataset Tests
# =============================================================================
def test_validate_dataset_csv(tmp_path: Path) -> None:
    """Test CSV validation."""
    csv_path = tmp_path / "test.csv"
    df = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=10, freq="D"),
        "open": [10.0] * 10,
        "high": [10.5] * 10,
        "low": [9.5] * 10,
        "close": [10.1] * 10,
        "volume": [1000] * 10,
        "symbol": ["TEST.SH"] * 10,
    })
    df.to_csv(csv_path, index=False)

    result = validate_dataset(
        provider="csv",
        interval="1d",
        input_path=str(csv_path),
        symbol="TEST.SH",
    )

    assert "status" in result
    assert "rows" in result
    assert result["rows"] == 10


# =============================================================================
# repair_frame Tests
# =============================================================================
def test_repair_frame_removes_duplicates(tmp_path: Path) -> None:
    """Test repair removes duplicate dates."""
    df = pd.DataFrame({
        "date": ["2025-01-01", "2025-01-01", "2025-01-02", "2025-01-03"],
        "open": [10.0, 10.0, 10.1, 10.2],
        "high": [10.5, 10.5, 10.6, 10.7],
        "low": [9.5, 9.5, 9.6, 9.7],
        "close": [10.1, 10.1, 10.2, 10.3],
        "volume": [1000, 1000, 1100, 1200],
        "symbol": ["TEST.SH"] * 4,
    })
    df["date"] = pd.to_datetime(df["date"])

    repaired, summary = repair_frame(df, "TEST.SH", "1d", duplicate_policy="last")
    assert len(repaired) == 3
    assert summary["duplicates_removed"] == 1


def test_repair_frame_drops_nulls(tmp_path: Path) -> None:
    """Test repair drops rows with null values."""
    df = pd.DataFrame({
        "date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04"],
        "open": [10.0, None, 10.2, 10.3],
        "high": [10.5, 10.6, 10.7, 10.8],
        "low": [9.5, 9.6, None, 9.8],
        "close": [10.1, 10.2, 10.3, 10.4],
        "volume": [1000, 1100, 1200, 1300],
        "symbol": ["TEST.SH"] * 4,
    })
    df["date"] = pd.to_datetime(df["date"])

    repaired, summary = repair_frame(df, "TEST.SH", "1d", null_policy="drop")
    assert len(repaired) == 2
    assert summary["null_rows_dropped"] == 2


def test_repair_frame_drops_invalid_prices(tmp_path: Path) -> None:
    """Test repair drops rows with invalid prices."""
    df = pd.DataFrame({
        "date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04"],
        "open": [10.0, 10.1, 10.2, 10.3],
        "high": [10.5, 10.6, 10.7, 10.8],
        "low": [9.5, 10.0, 9.7, 9.8],  # low > close on 2025-01-02
        "close": [10.1, 10.0, 10.3, 10.4],
        "volume": [1000, 1100, 1200, 1300],
        "symbol": ["TEST.SH"] * 4,
    })
    df["date"] = pd.to_datetime(df["date"])

    repaired, summary = repair_frame(df, "TEST.SH", "1d", invalid_price_policy="drop")
    assert len(repaired) == 3
    assert summary["invalid_rows_dropped"] == 1


def test_repair_frame_summary_fields(tmp_path: Path) -> None:
    """Test repair returns all summary fields."""
    df = pd.DataFrame({
        "date": ["2025-01-01", "2025-01-02", "2025-01-03"],
        "open": [10.0, 10.1, 10.2],
        "high": [10.5, 10.6, 10.7],
        "low": [9.5, 9.6, 9.7],
        "close": [10.1, 10.2, 10.3],
        "volume": [1000, 1100, 1200],
        "symbol": ["TEST.SH"] * 3,
    })
    df["date"] = pd.to_datetime(df["date"])

    _, summary = repair_frame(df, "TEST.SH", "1d")
    assert "source" in summary
    assert "output" in summary
    assert "rows_before" in summary
    assert "rows_after" in summary
    assert "duplicates_removed" in summary
    assert "null_rows_dropped" in summary
    assert "invalid_rows_dropped" in summary


# =============================================================================
# repair_dataset Tests
# =============================================================================
def test_repair_dataset_csv_output_csv(tmp_path: Path) -> None:
    """Test CSV repair outputs CSV file."""
    csv_path = tmp_path / "input.csv"
    df = pd.DataFrame({
        "date": ["2025-01-01", "2025-01-02", "2025-01-03"],
        "open": [10.0, 10.1, 10.2],
        "high": [10.5, 10.6, 10.7],
        "low": [9.5, 9.6, 9.7],
        "close": [10.1, 10.2, 10.3],
        "volume": [1000, 1100, 1200],
        "symbol": ["TEST.SH"] * 3,
    })
    df.to_csv(csv_path, index=False)

    output_path = tmp_path / "repaired.csv"
    result = repair_dataset(
        provider="csv",
        interval="1d",
        input_path=str(csv_path),
        output_path=str(output_path),
    )

    assert Path(result["output"]).exists()
    assert result["rows_before"] == 3
    assert result["rows_after"] == 3


def test_repair_dataset_duplicate_policy_first(tmp_path: Path) -> None:
    """Test repair with duplicate_policy='first'."""
    csv_path = tmp_path / "input.csv"
    df = pd.DataFrame({
        "date": ["2025-01-01", "2025-01-01", "2025-01-02"],
        "open": [10.0, 10.0, 10.1],
        "high": [10.5, 10.5, 10.6],
        "low": [9.5, 9.5, 9.6],
        "close": [10.1, 10.1, 10.2],
        "volume": [1000, 1000, 1100],
        "symbol": ["TEST.SH"] * 3,
    })
    df.to_csv(csv_path, index=False)

    output_path = tmp_path / "repaired.csv"
    result = repair_dataset(
        provider="csv",
        interval="1d",
        input_path=str(csv_path),
        output_path=str(output_path),
        duplicate_policy="first",
    )

    assert result["duplicates_removed"] == 1
    repaired = pd.read_csv(output_path)
    assert len(repaired) == 2
