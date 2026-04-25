from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from agent4quant.data.catalog import _metadata_path, list_symbols, resolve_symbol_file, scan_symbol_files


def _read_market_dates(path: Path) -> tuple[int, str | None, str | None]:
    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path, usecols=["date"])
    elif path.suffix.lower() == ".parquet":
        try:
            frame = pd.read_parquet(path, columns=["date"])
        except Exception:
            fallback = path.with_suffix(".csv")
            if not fallback.exists():
                raise
            frame = pd.read_csv(fallback, usecols=["date"])
    else:
        raise ValueError(f"Unsupported file suffix: {path.suffix}")

    if frame.empty:
        return 0, None, None
    values = pd.to_datetime(frame["date"])
    return (
        int(len(frame)),
        values.min().strftime("%Y-%m-%d %H:%M:%S"),
        values.max().strftime("%Y-%m-%d %H:%M:%S"),
    )


def build_local_manifest(root: Path, interval: str, market: str | None = None) -> dict:
    files = []
    for symbol in list_symbols(root, interval, market=market):
        path = resolve_symbol_file(root, symbol, interval, market=market)
        stat = path.stat()
        rows, start_ts, end_ts = _read_market_dates(path)
        resolved_market = None
        for entry in scan_symbol_files(root, interval=interval, market=market):
            if entry["symbol"] == symbol and entry["path"] == str(path):
                resolved_market = entry.get("market")
                break
        files.append(
            {
                "symbol": symbol,
                "market": resolved_market,
                "interval": interval,
                "path": str(path),
                "suffix": path.suffix.lower(),
                "rows": rows,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "size_bytes": stat.st_size,
                "updated_at": pd.Timestamp(stat.st_mtime, unit="s").strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    updated_values = [item["updated_at"] for item in files]
    return {
        "provider": "local",
        "market": market,
        "interval": interval,
        "symbols": len(files),
        "last_updated_at": max(updated_values) if updated_values else None,
        "files": files,
    }


def build_local_metadata(root: Path, interval: str | None = None, market: str | None = None) -> dict:
    entries = scan_symbol_files(root, interval=interval, market=market)
    grouped: dict[str, dict[str, dict[str, object]]] = {}
    for entry in entries:
        market_key = entry.get("market") or "default"
        interval_key = entry["interval"]
        stat = Path(entry["path"]).stat()
        rows, start_ts, end_ts = _read_market_dates(Path(entry["path"]))
        summary = grouped.setdefault(market_key, {}).setdefault(
            interval_key,
            {
                "symbols": 0,
                "rows": 0,
                "start_ts": None,
                "end_ts": None,
                "last_updated_at": None,
            },
        )
        summary["symbols"] = int(summary["symbols"]) + 1
        summary["rows"] = int(summary["rows"]) + rows
        summary["start_ts"] = min(filter(None, [summary["start_ts"], start_ts]), default=start_ts)
        summary["end_ts"] = max(filter(None, [summary["end_ts"], end_ts]), default=end_ts)
        updated_at = pd.Timestamp(stat.st_mtime, unit="s").strftime("%Y-%m-%d %H:%M:%S")
        summary["last_updated_at"] = max(filter(None, [summary["last_updated_at"], updated_at]), default=updated_at)

    return {
        "provider": "local",
        "root": str(root),
        "market": market,
        "interval": interval,
        "markets": grouped,
        "generated_at": pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "entries": len(entries),
    }


def write_local_metadata(root: Path, interval: str | None = None, market: str | None = None, output_path: str | None = None) -> dict:
    payload = build_local_metadata(root, interval=interval, market=market)
    target = Path(output_path) if output_path else _metadata_path(root)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "path": str(target),
        "entries": payload["entries"],
        "markets": sorted(payload["markets"]),
        "interval": interval,
        "market": market,
    }


def build_duckdb_manifest(db_path: str, interval: str) -> dict:
    if interval != "5m":
        raise ValueError("DuckDB manifest currently supports interval=5m only.")

    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("DuckDB is not available in the current environment.") from exc

    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {path}")

    conn = duckdb.connect(str(path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT
                symbol,
                COUNT(*) AS rows,
                MIN(ts) AS start_ts,
                MAX(ts) AS end_ts,
                MAX(ingested_at) AS last_ingested_at,
                COUNT(DISTINCT trade_date) AS trade_days
            FROM market_data_5m
            GROUP BY symbol
            ORDER BY symbol
            """
        ).df()
        total_rows = conn.execute("SELECT COUNT(*) FROM market_data_5m").fetchone()[0]
    finally:
        conn.close()

    symbols = [
        {
            "symbol": row.symbol,
            "rows": int(row.rows),
            "trade_days": int(row.trade_days),
            "start_ts": pd.Timestamp(row.start_ts).strftime("%Y-%m-%d %H:%M:%S"),
            "end_ts": pd.Timestamp(row.end_ts).strftime("%Y-%m-%d %H:%M:%S"),
            "last_ingested_at": pd.Timestamp(row.last_ingested_at).strftime("%Y-%m-%d %H:%M:%S"),
        }
        for row in rows.itertuples()
    ]
    last_updated = max((item["last_ingested_at"] for item in symbols), default=None)
    return {
        "provider": "duckdb",
        "interval": interval,
        "db_path": str(path),
        "symbols": len(symbols),
        "total_rows": int(total_rows),
        "last_updated_at": last_updated,
        "items": symbols,
    }
