from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import json
import re
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from agent4quant.data.symbols import normalize_symbol as _normalize_symbol, split_symbol


def previous_calendar_day() -> date:
    return date.today() - timedelta(days=1)


def normalize_symbol(symbol: str) -> tuple[str, str]:
    code, canonical = split_symbol(symbol)
    return code, canonical if "." in canonical else _normalize_symbol(symbol)


def _session_window(trade_date: date) -> tuple[str, str]:
    trade_day = trade_date.isoformat()
    return f"{trade_day} 09:30:00", f"{trade_day} 15:00:00"


def _secid(code: str, canonical_symbol: str) -> str:
    if canonical_symbol.endswith(".SH"):
        return f"1.{code}"
    if canonical_symbol.endswith(".SZ") or canonical_symbol.endswith(".BJ"):
        return f"0.{code}"
    if code.startswith(("600", "601", "603", "605", "688", "689")):
        return f"1.{code}"
    return f"0.{code}"


def _eastmoney_get(url: str, params: dict[str, str]) -> dict:
    full_url = f"{url}?{urlencode(params)}"
    request = Request(
        full_url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Referer": "https://quote.eastmoney.com/",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "close",
        },
    )
    with urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


def _normalize_online_5m_frame(
    frame: pd.DataFrame,
    *,
    symbol: str,
    trade_date: date,
    source: str,
) -> pd.DataFrame:
    return normalize_imported_5m_frame(
        frame,
        source=source,
        symbol=symbol,
        trade_date=trade_date,
    )


def fetch_akshare_5m(symbol: str, trade_date: date) -> pd.DataFrame:
    try:
        import akshare as ak
    except ImportError as exc:
        raise RuntimeError("AkShare is not installed in the current environment.") from exc

    code, canonical_symbol = split_symbol(symbol)
    start_date, end_date = _session_window(trade_date)
    try:
        frame = ak.stock_zh_a_hist_min_em(
            symbol=code,
            start_date=start_date,
            end_date=end_date,
            period="5",
            adjust="",
        )
    except Exception as exc:  # pragma: no cover - upstream network / provider flakiness
        raise RuntimeError(f"AkShare 5m request failed for {canonical_symbol} on {trade_date.isoformat()}") from exc

    if frame.empty:
        raise ValueError(f"AkShare returned empty 5m data for {canonical_symbol} on {trade_date.isoformat()}")

    normalized = _normalize_online_5m_frame(
        frame,
        symbol=canonical_symbol,
        trade_date=trade_date,
        source="akshare.stock_zh_a_hist_min_em.5m",
    )
    if normalized.empty:
        raise ValueError(f"AkShare returned empty 5m data for {canonical_symbol} on {trade_date.isoformat()}")
    return normalized


def fetch_eastmoney_5m(symbol: str, trade_date: date, retries: int = 3, retry_delay: float = 1.5) -> pd.DataFrame:
    code, canonical_symbol = split_symbol(symbol)
    start_date, end_date = _session_window(trade_date)
    last_error: Exception | None = None
    frame = pd.DataFrame()
    for attempt in range(1, retries + 1):
        try:
            payload = _eastmoney_get(
                "https://push2his.eastmoney.com/api/qt/stock/kline/get",
                {
                    "fields1": "f1,f2,f3,f4,f5,f6",
                    "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                    "ut": "7eea3edcaed734bea9cbfc24409ed989",
                    "klt": "5",
                    "fqt": "0",
                    "secid": _secid(code, canonical_symbol),
                    "beg": "0",
                    "end": "20500000",
                    "_": "1630930917857",
                },
            )
            klines = payload.get("data", {}).get("klines", [])
            if not klines:
                frame = pd.DataFrame()
            else:
                frame = pd.DataFrame([item.split(",") for item in klines])
                frame.columns = [
                    "ts",
                    "open",
                    "close",
                    "high",
                    "low",
                    "volume",
                    "amount",
                    "amplitude",
                    "pct_chg",
                    "chg",
                    "turnover",
                ]
                frame["ts"] = pd.to_datetime(frame["ts"])
                frame = frame[(frame["ts"] >= pd.Timestamp(start_date)) & (frame["ts"] <= pd.Timestamp(end_date))]
            break
        except Exception as exc:  # pragma: no cover - network flakiness
            last_error = exc
            if attempt == retries:
                raise RuntimeError(
                    f"Eastmoney 5m request failed for {canonical_symbol} on {trade_date.isoformat()} after {retries} attempts"
                ) from exc
            time.sleep(retry_delay)

    if frame.empty:
        raise ValueError(f"Eastmoney returned empty 5m data for {canonical_symbol} on {trade_date.isoformat()}")

    return _normalize_online_5m_frame(
        frame,
        symbol=canonical_symbol,
        trade_date=trade_date,
        source="eastmoney.kline.5m",
    )


def fetch_5m(symbol: str, trade_date: date, provider: str = "auto") -> pd.DataFrame:
    if provider == "akshare":
        return fetch_akshare_5m(symbol, trade_date)
    if provider == "eastmoney":
        return fetch_eastmoney_5m(symbol, trade_date)
    if provider != "auto":
        raise ValueError(f"Unsupported 5m provider: {provider}")

    akshare_error: Exception | None = None
    try:
        return fetch_akshare_5m(symbol, trade_date)
    except Exception as exc:
        akshare_error = exc

    try:
        return fetch_eastmoney_5m(symbol, trade_date)
    except Exception as exc:
        raise RuntimeError(
            f"All 5m providers failed for {split_symbol(symbol)[1]} on {trade_date.isoformat()}: "
            f"akshare={akshare_error!r}; eastmoney={exc!r}"
        ) from exc


@dataclass(slots=True)
class SyncResult:
    symbol: str
    trade_date: str
    rows: int
    db_path: str
    source: str


@dataclass(slots=True)
class BatchSyncResult:
    trade_date: str
    db_path: str
    results: list[SyncResult]


def _read_input_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported 5m input file type: {path}")


def _rename_import_columns(frame: pd.DataFrame) -> pd.DataFrame:
    aliases = {
        "date": "ts",
        "datetime": "ts",
        "time": "ts",
        "日期": "ts",
        "时间": "ts",
        "日期时间": "ts",
        "timestamp": "ts",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
        "成交额": "amount",
        "振幅": "amplitude",
        "换手率": "turnover",
        "涨跌幅": "pct_chg",
        "涨跌额": "chg",
        "代码": "symbol",
        "证券代码": "symbol",
    }
    rename_map = {column: aliases[column] for column in frame.columns if column in aliases}
    return frame.rename(columns=rename_map)


def _infer_symbol_from_path(path: Path) -> str | None:
    stem = path.stem
    match = re.match(r"^([A-Za-z0-9]+(?:\.[A-Za-z]{2})?)(?:[_-]\d{4}[-_]\d{2}[-_]\d{2})?$", stem)
    if not match:
        candidate = split_symbol(stem)[1]
        return candidate if "." in candidate else None
    candidate = split_symbol(match.group(1))[1]
    return candidate if "." in candidate else None


def normalize_imported_5m_frame(
    frame: pd.DataFrame,
    *,
    source: str,
    symbol: str | None = None,
    trade_date: date | None = None,
) -> pd.DataFrame:
    renamed = _rename_import_columns(frame.copy())
    required = {"ts", "open", "high", "low", "close", "volume"}
    missing = required - set(renamed.columns)
    if missing:
        raise ValueError(f"Imported 5m data missing required columns: {sorted(missing)}")

    renamed["ts"] = pd.to_datetime(renamed["ts"])
    for column in ["open", "high", "low", "close", "volume", "amount", "amplitude", "turnover", "pct_chg", "chg"]:
        if column in renamed.columns:
            renamed[column] = pd.to_numeric(renamed[column], errors="coerce")

    resolved_symbol = symbol or renamed.get("symbol")
    if isinstance(resolved_symbol, pd.Series):
        non_null = resolved_symbol.dropna().astype(str)
        if non_null.empty:
            raise ValueError("Imported 5m data requires --symbol or a symbol column.")
        resolved_symbol = non_null.iloc[0]
    if not resolved_symbol:
        raise ValueError("Imported 5m data requires --symbol or a symbol column.")
    canonical_symbol = split_symbol(str(resolved_symbol))[1]

    if trade_date is not None:
        resolved_trade_date = pd.Timestamp(trade_date).date()
    elif "trade_date" in renamed.columns:
        values = pd.to_datetime(renamed["trade_date"]).dt.date.dropna().unique().tolist()
        if len(values) != 1:
            raise ValueError("Imported 5m data must map to a single trade date.")
        resolved_trade_date = values[0]
    else:
        values = renamed["ts"].dt.date.dropna().unique().tolist()
        if len(values) != 1:
            raise ValueError("Imported 5m data must map to a single trade date or use --trade-date.")
        resolved_trade_date = values[0]

    normalized = renamed[["ts", "open", "high", "low", "close", "volume"]].copy()
    for column in ["amount", "amplitude", "turnover", "pct_chg", "chg"]:
        normalized[column] = renamed[column] if column in renamed.columns else 0.0
    normalized["trade_date"] = pd.Timestamp(resolved_trade_date)
    normalized["symbol"] = canonical_symbol
    normalized["source"] = source
    normalized["ingested_at"] = pd.Timestamp.utcnow()
    normalized = normalized[
        [
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
    ]
    return normalized.sort_values("ts").drop_duplicates(subset=["ts"], keep="last").reset_index(drop=True)


def import_5m_file_to_duckdb(
    *,
    input_path: str,
    db_path: str,
    symbol: str | None = None,
    trade_date: date | None = None,
    source: str = "file.import.5m",
) -> SyncResult:
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"5m input file not found: {path}")
    inferred_symbol = symbol or _infer_symbol_from_path(path)
    frame = normalize_imported_5m_frame(
        _read_input_frame(path),
        source=source,
        symbol=inferred_symbol,
        trade_date=trade_date,
    )
    return upsert_5m_to_duckdb(frame, db_path)


def import_5m_directory_to_duckdb(
    *,
    input_dir: str,
    db_path: str,
    trade_date: date | None = None,
    source: str = "dir.import.5m",
) -> BatchSyncResult:
    root = Path(input_dir)
    if not root.exists():
        raise FileNotFoundError(f"5m input directory not found: {root}")
    files = sorted(path for path in root.rglob("*") if path.is_file() and path.suffix.lower() in {".csv", ".parquet"})
    if not files:
        raise ValueError(f"No 5m csv/parquet files found under {root}")
    results = [
        import_5m_file_to_duckdb(
            input_path=str(path),
            db_path=db_path,
            symbol=_infer_symbol_from_path(path),
            trade_date=trade_date,
            source=source,
        )
        for path in files
    ]
    resolved_trade_date = trade_date.isoformat() if trade_date else results[0].trade_date
    return BatchSyncResult(
        trade_date=resolved_trade_date,
        db_path=str(Path(db_path)),
        results=results,
    )


def _connect_duckdb(db_path: str):
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("DuckDB is not installed. Run: pip install -e '.[duckdb]'") from exc

    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def ensure_duckdb_schema(db_path: str) -> None:
    conn = _connect_duckdb(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_data_5m (
                trade_date DATE,
                symbol TEXT,
                ts TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                amount DOUBLE,
                amplitude DOUBLE,
                turnover DOUBLE,
                pct_chg DOUBLE,
                chg DOUBLE,
                source TEXT,
                ingested_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_data_5m_symbol_ts
            ON market_data_5m(symbol, ts)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_data_5m_sync_log (
                symbol TEXT,
                trade_date DATE,
                rows INTEGER,
                status TEXT,
                source TEXT,
                synced_at TIMESTAMP
            )
            """
        )
    finally:
        conn.close()


def upsert_5m_to_duckdb(frame: pd.DataFrame, db_path: str) -> SyncResult:
    ensure_duckdb_schema(db_path)
    conn = _connect_duckdb(db_path)
    try:
        symbol = str(frame["symbol"].iloc[0])
        trade_date = pd.Timestamp(frame["trade_date"].iloc[0]).date()
        conn.register("incoming_frame", frame)
        conn.execute(
            """
            DELETE FROM market_data_5m
            WHERE symbol = ? AND trade_date = ?
            """,
            [symbol, trade_date],
        )
        conn.execute("INSERT INTO market_data_5m SELECT * FROM incoming_frame")
        conn.execute(
            """
            INSERT INTO market_data_5m_sync_log(symbol, trade_date, rows, status, source, synced_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [symbol, trade_date, len(frame), "success", str(frame["source"].iloc[0])],
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM market_data_5m WHERE symbol = ? AND trade_date = ?",
            [symbol, trade_date],
        ).fetchone()[0]
    finally:
        conn.close()

    return SyncResult(
        symbol=symbol,
        trade_date=trade_date.isoformat(),
        rows=int(count),
        db_path=str(Path(db_path)),
        source=str(frame["source"].iloc[0]),
    )


def sync_5m_to_duckdb(*, symbol: str, trade_date: date, db_path: str, provider: str = "auto") -> SyncResult:
    frame = fetch_5m(symbol, trade_date, provider=provider)
    return upsert_5m_to_duckdb(frame, db_path)


def sync_5m_batch_to_duckdb(*, symbols: list[str], trade_date: date, db_path: str, provider: str = "auto") -> BatchSyncResult:
    results = []
    for symbol in symbols:
        results.append(sync_5m_to_duckdb(symbol=symbol, trade_date=trade_date, db_path=db_path, provider=provider))
    return BatchSyncResult(
        trade_date=trade_date.isoformat(),
        db_path=str(Path(db_path)),
        results=results,
    )
