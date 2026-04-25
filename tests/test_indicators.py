from __future__ import annotations

import pandas as pd

from agent4quant.data.indicators import add_indicators


def test_add_indicators_supports_extended_factor_columns() -> None:
    frame = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=30, freq="D"),
            "open": range(1, 31),
            "high": range(2, 32),
            "low": range(1, 31),
            "close": range(1, 31),
            "volume": [100 + idx * 10 for idx in range(30)],
            "symbol": ["DEMO.SH"] * 30,
        }
    )

    result = add_indicators(frame, ["ret5", "mom10", "volatility10", "zscore20", "volma5", "obv"])

    assert "ret_5" in result.columns
    assert "mom_10" in result.columns
    assert "volatility_10" in result.columns
    assert "zscore_20" in result.columns
    assert "volume_ma_5" in result.columns
    assert "obv" in result.columns
    assert pd.notna(result["obv"].iloc[-1])
