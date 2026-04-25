from __future__ import annotations

import pandas as pd

from agent4quant.data.validation import repair_frame, validate_frame


def test_validate_frame_detects_unsorted_duplicates_and_bad_prices() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2025-01-02", "2025-01-01", "2025-01-01"],
            "open": [10, 9, -1],
            "high": [11, 8, 0],
            "low": [9, 10, 0.5],
            "close": [10.5, 9.5, 0.8],
            "volume": [100, 200, 0],
        }
    )

    result = validate_frame(frame, "BAD")

    assert result["valid"] is False
    joined = " ".join(result["issues"])
    assert "not sorted" in joined
    assert "Duplicate timestamps" in joined
    assert "Non-positive values detected in column: open" in joined
    assert "Inconsistent high values detected" in joined
    assert result["warnings"]
    assert result["repair_plan"]


def test_repair_frame_sorts_deduplicates_and_normalizes_symbol() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2025-01-02 09:35:00", "2025-01-01 09:35:00", "2025-01-01 09:35:00", "2025-01-02 09:40:00"],
            "open": [10, 9, 9.2, -1],
            "high": [11, 10, 10.2, 0],
            "low": [9, 8.8, 9.0, 0],
            "close": [10.5, 9.5, 9.7, 0.5],
            "volume": [100, 200, 250, 50],
        }
    )

    repaired, summary = repair_frame(frame, "sz000001", "5m")

    assert summary["symbol"] == "000001.SZ"
    assert summary["input_rows"] == 4
    assert summary["output_rows"] == 2
    assert repaired["date"].is_monotonic_increasing
    assert repaired["symbol"].tolist() == ["000001.SZ", "000001.SZ"]
    assert repaired["date"].duplicated().sum() == 0
