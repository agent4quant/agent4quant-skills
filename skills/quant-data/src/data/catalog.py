from __future__ import annotations

import json
from pathlib import Path

from agent4quant.data.symbols import normalize_symbol


SUPPORTED_SUFFIXES = (".csv", ".parquet")
INDEX_FILENAME = ".a4q-data-index.json"
METADATA_FILENAME = ".a4q-dir-metadata.json"
KNOWN_INTERVALS = {"1d", "5m"}


def _candidates(root: Path, symbol: str, interval: str, market: str | None = None) -> list[Path]:
    normalized_interval = interval.lower()
    canonical = normalize_symbol(symbol)
    variants = [symbol]
    if canonical and canonical not in variants:
        variants.append(canonical)

    names: list[Path] = []
    for item in variants:
        if market:
            names.extend(
                [
                    root / market / normalized_interval / f"{item}.parquet",
                    root / market / normalized_interval / f"{item}.csv",
                    root / market / item / f"{normalized_interval}.parquet",
                    root / market / item / f"{normalized_interval}.csv",
                    root / market / f"{item}_{normalized_interval}.parquet",
                    root / market / f"{item}_{normalized_interval}.csv",
                ]
            )
        names.extend(
            [
                root / f"{item}.parquet",
                root / f"{item}.csv",
                root / normalized_interval / f"{item}.parquet",
                root / normalized_interval / f"{item}.csv",
                root / item / f"{normalized_interval}.parquet",
                root / item / f"{normalized_interval}.csv",
                root / f"{item}_{normalized_interval}.parquet",
                root / f"{item}_{normalized_interval}.csv",
            ]
        )
    return names


def _index_path(root: Path) -> Path:
    return root / INDEX_FILENAME


def _metadata_path(root: Path) -> Path:
    return root / METADATA_FILENAME


def load_data_index(root: Path) -> dict | None:
    path = _index_path(root)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _infer_entry(root: Path, path: Path) -> dict[str, str] | None:
    relative = path.relative_to(root)
    parts = relative.parts
    if not parts:
        return None

    stem = path.stem
    interval: str | None = None
    symbol: str | None = None
    market: str | None = None

    if len(parts) >= 2 and parts[-2].lower() in KNOWN_INTERVALS:
        interval = parts[-2].lower()
        symbol = stem
        market = parts[-3] if len(parts) >= 3 else None
    elif stem.lower() in KNOWN_INTERVALS and len(parts) >= 2:
        interval = stem.lower()
        symbol = parts[-2]
        market = parts[-3] if len(parts) >= 3 else None
    else:
        for candidate in KNOWN_INTERVALS:
            suffix = f"_{candidate}"
            if stem.lower().endswith(suffix):
                interval = candidate
                symbol = stem[: -len(suffix)]
                market = parts[-2] if len(parts) >= 2 and parts[-2].lower() not in KNOWN_INTERVALS else None
                break

    if not interval or not symbol:
        return None

    return {
        "symbol": normalize_symbol(symbol),
        "interval": interval,
        "market": market,
        "path": str(path),
        "suffix": path.suffix.lower(),
    }


def scan_symbol_files(root: Path, interval: str | None = None, market: str | None = None) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in SUPPORTED_SUFFIXES:
            continue
        entry = _infer_entry(root, path)
        if entry is None:
            continue
        if interval and entry["interval"] != interval.lower():
            continue
        if market and entry.get("market") != market:
            continue
        entries.append(entry)
    return entries


def write_data_index(
    root: Path,
    interval: str | None = None,
    output_path: str | None = None,
    market: str | None = None,
) -> dict:
    intervals = [interval] if interval else sorted(KNOWN_INTERVALS)
    entries = scan_symbol_files(root, interval=interval, market=market)
    by_interval: dict[str, dict[str, str]] = {item: {} for item in intervals}
    by_market: dict[str, dict[str, dict[str, str]]] = {}
    for entry in entries:
        entry_interval = entry["interval"]
        if entry_interval not in by_interval:
            by_interval[entry_interval] = {}
        by_interval[entry_interval][entry["symbol"]] = entry["path"]
        if entry.get("market"):
            by_market.setdefault(entry["market"], {}).setdefault(entry_interval, {})[entry["symbol"]] = entry["path"]

    payload: dict[str, object] = {
        "intervals": intervals,
        "market": market,
        "symbols": by_interval,
        "markets": by_market,
        "entries": entries,
    }
    target = Path(output_path) if output_path else _index_path(root)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "path": str(target),
        "intervals": intervals,
        "market": market,
        "symbols": sum(len(mapping) for mapping in by_interval.values()),
        "entries": len(entries),
    }


def resolve_symbol_file(
    root: Path,
    symbol: str,
    interval: str,
    market: str | None = None,
    *,
    use_index: bool = True,
) -> Path:
    canonical = normalize_symbol(symbol)
    if use_index:
        index = load_data_index(root)
        indexed = None
        if index:
            if market:
                indexed = index.get("markets", {}).get(market, {}).get(interval, {}).get(canonical)
            if indexed is None:
                indexed = index.get("symbols", {}).get(interval, {}).get(canonical)
        if indexed:
            indexed_path = Path(indexed)
            if indexed_path.exists():
                return indexed_path

    for candidate in _candidates(root, symbol, interval, market=market):
        if candidate.exists():
            return candidate

    for entry in scan_symbol_files(root, interval=interval, market=market):
        if entry["symbol"] == canonical:
            return Path(entry["path"])

    scope = f"market={market}, " if market else ""
    raise FileNotFoundError(
        f"No local market data file found for {scope}symbol={canonical}, interval={interval} under {root}"
    )


def list_symbols(root: Path, interval: str | None = None, market: str | None = None) -> list[str]:
    entries = scan_symbol_files(root, interval=interval, market=market)
    return sorted({entry["symbol"] for entry in entries if entry["symbol"]})
