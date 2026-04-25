from __future__ import annotations

from pathlib import Path

import pandas as pd

from agent4quant.data.catalog import list_symbols, resolve_symbol_file
from agent4quant.data.providers import _read_market_file
from agent4quant.data.symbols import is_canonical_symbol, normalize_symbol

REQUIRED_COLUMNS = {"date", "open", "high", "low", "close", "volume"}


def _check_symbol(symbol: str) -> list[str]:
    if not symbol:
        return ["Missing symbol."]
    canonical = normalize_symbol(symbol)
    if is_canonical_symbol(canonical):
        if canonical == symbol:
            return []
        return [f"Symbol normalized to canonical format: {symbol} -> {canonical}"]
    if is_canonical_symbol(symbol):
        return []
    return [f"Symbol does not match preferred format 000001.SZ/600000.SH/430001.BJ: {symbol}"]


def _interval_alias(interval: str | None) -> str | None:
    if not interval:
        return None
    normalized = interval.lower()
    if normalized in {"5m", "5min", "5minute"}:
        return "5m"
    if normalized in {"1d", "1day", "daily"}:
        return "1d"
    return None


def _detect_time_gaps(data: pd.DataFrame, interval: str | None) -> list[str]:
    normalized = _interval_alias(interval)
    if normalized is None or len(data) < 2:
        return []

    delta = pd.Timedelta(minutes=5) if normalized == "5m" else pd.Timedelta(days=1)
    gaps = data["date"].diff().dropna()
    bad = gaps[gaps > delta]
    if bad.empty:
        return []
    preview = bad.iloc[0]
    return [
        f"Detected {len(bad)} time gaps larger than expected {normalized} interval. "
        f"First gap={preview}."
    ]


def validate_frame(frame: pd.DataFrame, symbol: str, interval: str | None = None) -> dict:
    issues: list[str] = []
    warnings: list[str] = []

    missing = REQUIRED_COLUMNS - set(frame.columns)
    if missing:
        issues.append(f"Missing required columns: {sorted(missing)}")
        return {
            "symbol": symbol,
            "rows": len(frame),
            "valid": False,
            "issues": issues,
            "warnings": warnings,
        }

    warnings.extend(_check_symbol(symbol))

    data = frame.copy()
    try:
        data["date"] = pd.to_datetime(data["date"])
    except Exception as exc:  # pragma: no cover - pandas error text is enough
        issues.append(f"Failed to parse date column: {exc}")
        return {
            "symbol": symbol,
            "rows": len(frame),
            "valid": False,
            "issues": issues,
            "warnings": warnings,
        }

    if not data["date"].is_monotonic_increasing:
        issues.append("Date column is not sorted ascending.")

    duplicate_count = int(data["date"].duplicated().sum())
    if duplicate_count:
        issues.append(f"Duplicate timestamps detected: {duplicate_count}")

    null_counts = data[list(REQUIRED_COLUMNS)].isna().sum()
    null_issues = {key: int(value) for key, value in null_counts.items() if int(value) > 0}
    if null_issues:
        issues.append(f"Null values detected: {null_issues}")

    for column in ["open", "high", "low", "close", "volume"]:
        if column in data.columns and (data[column] <= 0).any():
            issues.append(f"Non-positive values detected in column: {column}")

    if {"open", "high", "low", "close"}.issubset(data.columns):
        bad_high = data["high"] < data[["open", "close", "low"]].max(axis=1)
        bad_low = data["low"] > data[["open", "close", "high"]].min(axis=1)
        if bool(bad_high.any()):
            issues.append(f"Inconsistent high values detected: {int(bad_high.sum())}")
        if bool(bad_low.any()):
            issues.append(f"Inconsistent low values detected: {int(bad_low.sum())}")

    warnings.extend(_detect_time_gaps(data.sort_values("date"), interval))

    return {
        "symbol": normalize_symbol(symbol),
        "rows": len(data),
        "valid": len(issues) == 0,
        "issues": issues,
        "warnings": warnings,
        "repair_plan": build_repair_plan(issues),
    }


def build_repair_plan(issues: list[str]) -> list[str]:
    plan: list[str] = []
    issue_blob = " ".join(issues)
    if "Date column is not sorted ascending." in issue_blob:
        plan.append("Sort rows by `date` ascending before ingestion.")
    if "Duplicate timestamps detected" in issue_blob:
        plan.append("Drop duplicate timestamps and keep the latest bar for each `date`.")
    if "Null values detected" in issue_blob:
        plan.append("Drop rows with missing required fields, or repair upstream source before import.")
    if "Non-positive values detected" in issue_blob or "Inconsistent high values detected" in issue_blob:
        plan.append("Remove invalid OHLCV rows with non-positive prices/volume or inconsistent high/low ranges.")
    if not plan:
        plan.append("No repair needed.")
    return plan


def repair_frame(
    frame: pd.DataFrame,
    symbol: str,
    interval: str | None = None,
    *,
    duplicate_policy: str = "last",
    null_policy: str = "drop",
    invalid_price_policy: str = "drop",
) -> tuple[pd.DataFrame, dict]:
    data = frame.copy()
    missing = REQUIRED_COLUMNS - set(data.columns)
    if missing:
        raise ValueError(f"Input market file missing required columns: {sorted(missing)}")

    data["date"] = pd.to_datetime(data["date"])
    numeric_columns = ["open", "high", "low", "close", "volume"]
    for column in numeric_columns:
        data[column] = pd.to_numeric(data[column], errors="coerce")

    input_rows = len(data)
    data = data.sort_values("date").reset_index(drop=True)

    duplicate_count = int(data["date"].duplicated().sum())
    if duplicate_count:
        if duplicate_policy == "error":
            raise ValueError(f"Duplicate timestamps detected: {duplicate_count}")
        data = data.drop_duplicates(subset=["date"], keep=duplicate_policy).reset_index(drop=True)

    null_mask = data[list(REQUIRED_COLUMNS)].isna().any(axis=1)
    null_rows = int(null_mask.sum())
    if null_rows:
        if null_policy == "error":
            raise ValueError(f"Null values detected: {null_rows}")
        data = data.loc[~null_mask].reset_index(drop=True)

    invalid_price_mask = (
        (data["open"] <= 0)
        | (data["high"] <= 0)
        | (data["low"] <= 0)
        | (data["close"] <= 0)
        | (data["volume"] < 0)
        | (data["high"] < data[["open", "close", "low"]].max(axis=1))
        | (data["low"] > data[["open", "close", "high"]].min(axis=1))
    )
    invalid_rows = int(invalid_price_mask.sum())
    if invalid_rows:
        if invalid_price_policy == "error":
            raise ValueError(f"Invalid OHLCV rows detected: {invalid_rows}")
        data = data.loc[~invalid_price_mask].reset_index(drop=True)

    canonical_symbol = normalize_symbol(symbol)
    data["symbol"] = canonical_symbol
    validation = validate_frame(data, canonical_symbol, interval)
    summary = {
        "symbol": canonical_symbol,
        "interval": interval,
        "input_rows": input_rows,
        "output_rows": len(data),
        "dropped_rows": input_rows - len(data),
        "duplicate_policy": duplicate_policy,
        "null_policy": null_policy,
        "invalid_price_policy": invalid_price_policy,
        "warnings": validation["warnings"],
        "repair_plan": validation["repair_plan"],
    }
    return data, summary


def validate_market_file(path: Path, symbol: str, interval: str | None = None) -> dict:
    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path)
    elif path.suffix.lower() == ".parquet":
        frame = pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported file type: {path}")

    result = validate_frame(frame, symbol, interval)
    result["path"] = str(path)
    return result


def validate_data_root(root: Path, interval: str, market: str | None = None) -> dict:
    results = []
    for symbol in list_symbols(root, interval, market=market):
        path = resolve_symbol_file(root, symbol, interval, market=market)
        results.append(validate_market_file(path, symbol, interval))

    return {
        "interval": interval,
        "market": market,
        "symbols": len(results),
        "valid_symbols": sum(1 for item in results if item["valid"]),
        "invalid_symbols": sum(1 for item in results if not item["valid"]),
        "results": results,
    }


def load_and_validate_symbol(
    root: Path,
    symbol: str,
    interval: str,
    start: str,
    end: str,
    market: str | None = None,
) -> tuple[pd.DataFrame, dict]:
    path = resolve_symbol_file(root, symbol, interval, market=market)
    validation = validate_market_file(path, symbol, interval)
    frame = _read_market_file(path, symbol, start, end)
    return frame, validation
