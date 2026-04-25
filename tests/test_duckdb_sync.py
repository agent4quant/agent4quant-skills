from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
import types

import pandas as pd

from agent4quant.data.sync import (
    ensure_duckdb_schema,
    fetch_5m,
    fetch_akshare_5m,
    fetch_eastmoney_5m,
    import_5m_directory_to_duckdb,
    import_5m_file_to_duckdb,
    normalize_imported_5m_frame,
    sync_5m_batch_to_duckdb,
    sync_5m_to_duckdb,
    upsert_5m_to_duckdb,
)


def test_upsert_5m_to_duckdb_replaces_same_symbol_same_day(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    ensure_duckdb_schema(str(db_path))

    frame = pd.DataFrame(
        {
            "trade_date": [pd.Timestamp(date(2026, 3, 24)), pd.Timestamp(date(2026, 3, 24))],
            "symbol": ["000001.SZ", "000001.SZ"],
            "ts": [pd.Timestamp("2026-03-24 09:35:00"), pd.Timestamp("2026-03-24 09:40:00")],
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
    first = upsert_5m_to_duckdb(frame, str(db_path))
    second = upsert_5m_to_duckdb(frame.iloc[:1].copy(), str(db_path))

    assert first.rows == 2
    assert second.rows == 1
    assert second.symbol == "000001.SZ"
    assert second.source == "test"
    assert db_path.exists()


def test_normalize_symbol_for_a_share_suffix() -> None:
    from agent4quant.data.sync import normalize_symbol

    code, canonical = normalize_symbol("000001.SZ")
    assert code == "000001"
    assert canonical == "000001.SZ"


def test_normalize_symbol_without_suffix() -> None:
    from agent4quant.data.sync import normalize_symbol

    code, canonical = normalize_symbol("sz000001")
    assert code == "000001"
    assert canonical == "000001.SZ"


def test_sync_5m_batch_to_duckdb_aggregates_results(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"

    def fake_fetch(symbol: str, trade_date: date):
        return pd.DataFrame(
            {
                "trade_date": [pd.Timestamp(trade_date)],
                "symbol": [symbol],
                "ts": [pd.Timestamp(f"{trade_date.isoformat()} 09:35:00")],
                "open": [10.0],
                "high": [10.1],
                "low": [9.9],
                "close": [10.0],
                "volume": [1000],
                "amount": [10000.0],
                "amplitude": [0.5],
                "turnover": [0.01],
                "pct_chg": [0.1],
                "chg": [0.01],
                "source": ["test.batch"],
                "ingested_at": [pd.Timestamp.utcnow()],
            }
        )

    monkeypatch.setattr("agent4quant.data.sync.fetch_5m", lambda symbol, trade_date, provider="auto": fake_fetch(symbol, trade_date))
    result = sync_5m_batch_to_duckdb(
        symbols=["000001.SZ", "600000.SH"],
        trade_date=date(2026, 3, 24),
        db_path=str(db_path),
    )

    assert len(result.results) == 2
    assert result.results[0].rows == 1
    assert result.results[1].symbol == "600000.SH"
    assert result.results[0].source == "test.batch"


def test_fetch_akshare_5m_uses_documented_minute_signature(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def _stock_zh_a_hist_min_em(symbol: str, start_date: str, end_date: str, period: str, adjust: str):
        captured.update(
            {
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "period": period,
                "adjust": adjust,
            }
        )
        return pd.DataFrame(
            {
                "时间": ["2026-03-24 09:35:00", "2026-03-24 09:40:00"],
                "开盘": [10.0, 10.1],
                "收盘": [10.1, 10.2],
                "最高": [10.2, 10.3],
                "最低": [9.9, 10.0],
                "成交量": [1000, 1200],
                "成交额": [10000.0, 12200.0],
                "振幅": [0.5, 0.6],
                "涨跌幅": [0.1, 0.2],
                "涨跌额": [0.01, 0.02],
                "换手率": [0.01, 0.02],
            }
        )

    fake_module = types.SimpleNamespace(stock_zh_a_hist_min_em=_stock_zh_a_hist_min_em)
    monkeypatch.setitem(sys.modules, "akshare", fake_module)

    frame = fetch_akshare_5m("000001.SZ", date(2026, 3, 24))

    assert captured == {
        "symbol": "000001",
        "start_date": "2026-03-24 09:30:00",
        "end_date": "2026-03-24 15:00:00",
        "period": "5",
        "adjust": "",
    }
    assert frame["symbol"].tolist() == ["000001.SZ", "000001.SZ"]
    assert frame["source"].iloc[0] == "akshare.stock_zh_a_hist_min_em.5m"


def test_fetch_5m_prefers_akshare_in_auto_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        "agent4quant.data.sync.fetch_akshare_5m",
        lambda symbol, trade_date: pd.DataFrame(
            {
                "trade_date": [pd.Timestamp(trade_date)],
                "symbol": [symbol],
                "ts": [pd.Timestamp(f"{trade_date.isoformat()} 09:35:00")],
                "open": [10.0],
                "high": [10.1],
                "low": [9.9],
                "close": [10.0],
                "volume": [1000],
                "amount": [10000.0],
                "amplitude": [0.5],
                "turnover": [0.01],
                "pct_chg": [0.1],
                "chg": [0.01],
                "source": ["akshare.stock_zh_a_hist_min_em.5m"],
                "ingested_at": [pd.Timestamp.utcnow()],
            }
        ),
    )
    monkeypatch.setattr("agent4quant.data.sync.fetch_eastmoney_5m", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not fallback")))

    frame = fetch_5m("000001.SZ", date(2026, 3, 24), provider="auto")
    assert frame["source"].iloc[0] == "akshare.stock_zh_a_hist_min_em.5m"


def test_fetch_5m_falls_back_to_eastmoney_when_akshare_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        "agent4quant.data.sync.fetch_akshare_5m",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("akshare down")),
    )
    monkeypatch.setattr(
        "agent4quant.data.sync.fetch_eastmoney_5m",
        lambda symbol, trade_date: pd.DataFrame(
            {
                "trade_date": [pd.Timestamp(trade_date)],
                "symbol": [symbol],
                "ts": [pd.Timestamp(f"{trade_date.isoformat()} 09:35:00")],
                "open": [10.0],
                "high": [10.1],
                "low": [9.9],
                "close": [10.0],
                "volume": [1000],
                "amount": [10000.0],
                "amplitude": [0.5],
                "turnover": [0.01],
                "pct_chg": [0.1],
                "chg": [0.01],
                "source": ["eastmoney.kline.5m"],
                "ingested_at": [pd.Timestamp.utcnow()],
            }
        ),
    )

    frame = fetch_5m("000001.SZ", date(2026, 3, 24), provider="auto")
    assert frame["source"].iloc[0] == "eastmoney.kline.5m"


def test_sync_5m_to_duckdb_respects_provider(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    monkeypatch.setattr(
        "agent4quant.data.sync.fetch_5m",
        lambda symbol, trade_date, provider="auto": pd.DataFrame(
            {
                "trade_date": [pd.Timestamp(trade_date)],
                "symbol": [symbol],
                "ts": [pd.Timestamp(f"{trade_date.isoformat()} 09:35:00")],
                "open": [10.0],
                "high": [10.1],
                "low": [9.9],
                "close": [10.0],
                "volume": [1000],
                "amount": [10000.0],
                "amplitude": [0.5],
                "turnover": [0.01],
                "pct_chg": [0.1],
                "chg": [0.01],
                "source": [f"{provider}.5m"],
                "ingested_at": [pd.Timestamp.utcnow()],
            }
        ),
    )

    result = sync_5m_to_duckdb(
        symbol="000001.SZ",
        trade_date=date(2026, 3, 24),
        db_path=str(db_path),
        provider="eastmoney",
    )

    assert result.source == "eastmoney.5m"


def test_fetch_eastmoney_5m_reports_empty_payload_as_eastmoney_error(monkeypatch) -> None:
    monkeypatch.setattr("agent4quant.data.sync._eastmoney_get", lambda *_args, **_kwargs: {"data": {"klines": []}})

    try:
        fetch_eastmoney_5m("000001.SZ", date(2026, 3, 24), retries=1, retry_delay=0)
    except ValueError as exc:
        assert "Eastmoney returned empty 5m data" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected ValueError for empty Eastmoney payload")


def test_fetch_eastmoney_5m_reports_network_failure_as_eastmoney_error(monkeypatch) -> None:
    def _raise(*_args, **_kwargs):
        raise ConnectionError("boom")

    monkeypatch.setattr("agent4quant.data.sync._eastmoney_get", _raise)

    try:
        fetch_eastmoney_5m("000001.SZ", date(2026, 3, 24), retries=1, retry_delay=0)
    except RuntimeError as exc:
        assert "Eastmoney 5m request failed" in str(exc)
        assert isinstance(exc.__cause__, ConnectionError)
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected RuntimeError for Eastmoney network failure")


def test_normalize_imported_5m_frame_supports_chinese_columns() -> None:
    frame = pd.DataFrame(
        {
            "时间": ["2026-03-24 09:35:00", "2026-03-24 09:40:00"],
            "开盘": [10.0, 10.1],
            "最高": [10.2, 10.3],
            "最低": [9.9, 10.0],
            "收盘": [10.1, 10.2],
            "成交量": [1000, 1200],
            "成交额": [10000.0, 12200.0],
        }
    )

    normalized = normalize_imported_5m_frame(
        frame,
        source="test.import",
        symbol="000001.SZ",
        trade_date=date(2026, 3, 24),
    )

    assert list(normalized.columns) == [
        "trade_date",
        "symbol",
        "ts",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "amplitude",
        "turnover",
        "pct_chg",
        "chg",
        "source",
        "ingested_at",
    ]
    assert normalized["symbol"].iloc[0] == "000001.SZ"
    assert len(normalized) == 2


def test_import_5m_file_to_duckdb_from_csv(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    input_path = tmp_path / "000001.SZ_2026-03-24.csv"
    pd.DataFrame(
        {
            "datetime": ["2026-03-24 09:35:00", "2026-03-24 09:40:00"],
            "open": [10.0, 10.1],
            "high": [10.2, 10.3],
            "low": [9.9, 10.0],
            "close": [10.1, 10.2],
            "volume": [1000, 1200],
        }
    ).to_csv(input_path, index=False)

    result = import_5m_file_to_duckdb(input_path=str(input_path), db_path=str(db_path))

    assert result.symbol == "000001.SZ"
    assert result.rows == 2


def test_import_5m_directory_to_duckdb_from_multiple_files(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    input_dir = tmp_path / "offline"
    input_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "datetime": ["2026-03-24 09:35:00"],
            "open": [10.0],
            "high": [10.2],
            "low": [9.9],
            "close": [10.1],
            "volume": [1000],
        }
    ).to_csv(input_dir / "000001.SZ_2026-03-24.csv", index=False)
    pd.DataFrame(
        {
            "datetime": ["2026-03-24 09:35:00"],
            "open": [20.0],
            "high": [20.2],
            "low": [19.9],
            "close": [20.1],
            "volume": [1500],
            "symbol": ["600000.SH"],
        }
    ).to_csv(input_dir / "batch2.csv", index=False)

    result = import_5m_directory_to_duckdb(input_dir=str(input_dir), db_path=str(db_path), trade_date=date(2026, 3, 24))

    assert len(result.results) == 2
    assert {item.symbol for item in result.results} == {"000001.SZ", "600000.SH"}
