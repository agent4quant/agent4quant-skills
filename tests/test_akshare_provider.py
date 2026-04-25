from __future__ import annotations

import sys
import types

import pandas as pd

from agent4quant.data.providers import FetchRequest, AkshareProvider


def test_akshare_provider_uses_documented_stock_zh_a_hist_signature(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def _stock_zh_a_hist(symbol: str, period: str, start_date: str, end_date: str, adjust: str):
        captured.update(
            {
                "symbol": symbol,
                "period": period,
                "start_date": start_date,
                "end_date": end_date,
                "adjust": adjust,
            }
        )
        return pd.DataFrame(
            {
                "日期": ["2025-01-02", "2025-01-03"],
                "开盘": [10.0, 10.2],
                "最高": [10.3, 10.4],
                "最低": [9.9, 10.1],
                "收盘": [10.1, 10.3],
                "成交量": [1000, 1200],
            }
        )

    fake_module = types.SimpleNamespace(stock_zh_a_hist=_stock_zh_a_hist)
    monkeypatch.setitem(sys.modules, "akshare", fake_module)

    provider = AkshareProvider()
    result = provider.fetch(
        FetchRequest(
            symbol="000001.SZ",
            start="2025-01-02",
            end="2025-01-03",
            interval="1d",
            adjust="hfq",
        )
    )

    assert captured == {
        "symbol": "000001",
        "period": "daily",
        "start_date": "20250102",
        "end_date": "20250103",
        "adjust": "hfq",
    }
    assert result["symbol"].tolist() == ["000001.SZ", "000001.SZ"]


def test_akshare_provider_maps_none_adjust_to_empty_string(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def _stock_zh_a_hist(symbol: str, period: str, start_date: str, end_date: str, adjust: str):
        captured["adjust"] = adjust
        return pd.DataFrame(
            {
                "日期": ["2025-01-02"],
                "开盘": [10.0],
                "最高": [10.3],
                "最低": [9.9],
                "收盘": [10.1],
                "成交量": [1000],
            }
        )

    fake_module = types.SimpleNamespace(stock_zh_a_hist=_stock_zh_a_hist)
    monkeypatch.setitem(sys.modules, "akshare", fake_module)

    provider = AkshareProvider()
    provider.fetch(
        FetchRequest(
            symbol="600000.SH",
            start="2025-01-02",
            end="2025-01-03",
            interval="1d",
            adjust="none",
        )
    )

    assert captured["adjust"] == ""
