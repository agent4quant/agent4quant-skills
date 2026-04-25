from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from agent4quant.config import resolve_data_root, resolve_duckdb_path, resolve_provider_market
from agent4quant.compliance import build_metadata
from agent4quant.data.catalog import resolve_symbol_file, write_data_index
from agent4quant.data.capabilities import build_provider_capabilities
from agent4quant.data.adjustments import apply_price_adjustment, validate_adjust_mode
from agent4quant.data.indicators import add_indicators
from agent4quant.data.manifest import build_duckdb_manifest, build_local_manifest, write_local_metadata
from agent4quant.data.providers import FetchRequest, get_provider
from agent4quant.data.validation import repair_frame, validate_data_root, validate_market_file


def fetch_dataset(
    *,
    provider: str,
    symbol: str,
    start: str,
    end: str,
    interval: str,
    indicators: list[str],
    input_path: str | None = None,
    data_root: str | None = None,
    db_path: str | None = None,
    adjust: str = "none",
    market: str | None = None,
    provider_profile: str | None = None,
) -> tuple[pd.DataFrame, dict[str, str]]:
    resolved_market = resolve_provider_market(market, provider, provider_profile)
    adjust_mode = validate_adjust_mode(adjust)
    request = FetchRequest(
        symbol=symbol,
        start=start,
        end=end,
        interval=interval,
        input_path=input_path,
        data_root=data_root,
        db_path=db_path,
        adjust=adjust_mode,
        market=resolved_market,
        provider_profile=provider_profile,
    )
    frame = get_provider(provider).fetch(request)
    frame = apply_price_adjustment(frame, adjust_mode)
    dataset = add_indicators(frame, indicators)
    metadata = build_metadata("quant-data", provider, interval)
    source_provider = frame.attrs.get("source_provider")
    provider_route = frame.attrs.get("provider_route")
    if source_provider:
        metadata["source_provider"] = str(source_provider)
    if provider_route:
        metadata["provider_route"] = str(provider_route)
    metadata["adjust"] = adjust_mode
    if resolved_market:
        metadata["market"] = resolved_market
    if provider_profile:
        metadata["provider_profile"] = provider_profile
    return dataset, metadata


def available_symbols(
    *,
    provider: str,
    interval: str,
    data_root: str | None = None,
    db_path: str | None = None,
    market: str | None = None,
    provider_profile: str | None = None,
) -> list[str]:
    request = FetchRequest(
        symbol="",
        start="1970-01-01",
        end="2100-01-01",
        interval=interval,
        data_root=data_root,
        db_path=db_path,
        market=resolve_provider_market(market, provider, provider_profile),
        provider_profile=provider_profile,
    )
    return get_provider(provider).available_symbols(request)


def list_provider_capabilities() -> dict:
    return build_provider_capabilities()


def build_data_manifest(
    *,
    provider: str,
    interval: str,
    data_root: str | None = None,
    db_path: str | None = None,
    market: str | None = None,
    provider_profile: str | None = None,
) -> dict:
    if provider == "local":
        root = resolve_data_root(data_root, provider_profile)
        if root is None:
            raise ValueError("Local manifest requires --data-root, provider profile or A4Q_MARKET_DATA_ROOT.")
        return build_local_manifest(root, interval, market=resolve_provider_market(market, provider, provider_profile))
    if provider == "duckdb":
        path = resolve_duckdb_path(db_path, provider_profile)
        if path is None:
            raise ValueError("DuckDB manifest requires --db-path or provider profile.")
        return build_duckdb_manifest(str(path), interval)
    raise ValueError("Manifest currently supports provider=local or provider=duckdb.")


def build_local_data_index(
    *,
    data_root: str | None = None,
    interval: str | None = None,
    output_path: str | None = None,
    market: str | None = None,
    provider_profile: str | None = None,
) -> dict:
    root = resolve_data_root(data_root, provider_profile)
    if root is None:
        raise ValueError("Local index requires --data-root, provider profile or A4Q_MARKET_DATA_ROOT.")
    return write_data_index(root, interval, output_path, market=resolve_provider_market(market, "local", provider_profile))


def write_local_directory_metadata(
    *,
    data_root: str | None = None,
    interval: str | None = None,
    market: str | None = None,
    output_path: str | None = None,
    provider_profile: str | None = None,
) -> dict:
    root = resolve_data_root(data_root, provider_profile)
    if root is None:
        raise ValueError("Local metadata requires --data-root, provider profile or A4Q_MARKET_DATA_ROOT.")
    return write_local_metadata(
        root,
        interval=interval,
        market=resolve_provider_market(market, "local", provider_profile),
        output_path=output_path,
    )


def write_dataset(
    frame: pd.DataFrame,
    metadata: dict[str, str],
    output_path: str,
    output_format: str,
) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = frame.copy()
    payload["date"] = payload["date"].dt.strftime("%Y-%m-%d %H:%M:%S")

    if output_format == "csv":
        payload.to_csv(path, index=False)
        meta_path = path.with_suffix(".meta.json")
        meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        return

    if output_format == "json":
        package = {
            "metadata": metadata,
            "rows": payload.to_dict(orient="records"),
        }
        path.write_text(json.dumps(package, ensure_ascii=False, indent=2), encoding="utf-8")
        return

    raise ValueError(f"Unsupported output format: {output_format}")


def validate_dataset(
    *,
    provider: str,
    interval: str,
    input_path: str | None = None,
    data_root: str | None = None,
    symbol: str | None = None,
    market: str | None = None,
    provider_profile: str | None = None,
) -> dict:
    if provider == "csv":
        if not input_path:
            raise ValueError("CSV validation requires --input.")
        path = Path(input_path)
        return validate_market_file(path, symbol or path.stem, interval)

    if provider == "local":
        root = resolve_data_root(data_root, provider_profile)
        if root is None:
            raise ValueError("Local validation requires --data-root, provider profile or A4Q_MARKET_DATA_ROOT.")
        resolved_market = resolve_provider_market(market, provider, provider_profile)
        if symbol:
            path = resolve_symbol_file(root, symbol, interval, market=resolved_market)
            return validate_market_file(path, symbol, interval)
        return validate_data_root(root, interval, market=resolved_market)

    raise ValueError("Validation currently supports provider=csv or provider=local.")


def repair_dataset(
    *,
    provider: str,
    interval: str,
    output_path: str,
    input_path: str | None = None,
    data_root: str | None = None,
    symbol: str | None = None,
    market: str | None = None,
    provider_profile: str | None = None,
    duplicate_policy: str = "last",
    null_policy: str = "drop",
    invalid_price_policy: str = "drop",
) -> dict:
    if provider == "csv":
        if not input_path:
            raise ValueError("CSV repair requires --input.")
        source_path = Path(input_path)
        if source_path.suffix.lower() == ".csv":
            frame = pd.read_csv(source_path)
        elif source_path.suffix.lower() == ".parquet":
            frame = pd.read_parquet(source_path)
        else:
            raise ValueError(f"Unsupported file type: {source_path}")
        resolved_symbol = symbol or source_path.stem
    elif provider == "local":
        root = resolve_data_root(data_root, provider_profile)
        if root is None:
            raise ValueError("Local repair requires --data-root, provider profile or A4Q_MARKET_DATA_ROOT.")
        if not symbol:
            raise ValueError("Local repair requires --symbol.")
        source_path = resolve_symbol_file(root, symbol, interval, market=resolve_provider_market(market, provider, provider_profile))
        if source_path.suffix.lower() == ".csv":
            frame = pd.read_csv(source_path)
        elif source_path.suffix.lower() == ".parquet":
            frame = pd.read_parquet(source_path)
        else:
            raise ValueError(f"Unsupported file type: {source_path}")
        resolved_symbol = symbol
    else:
        raise ValueError("Repair currently supports provider=csv or provider=local.")

    repaired, summary = repair_frame(
        frame,
        resolved_symbol,
        interval,
        duplicate_policy=duplicate_policy,
        null_policy=null_policy,
        invalid_price_policy=invalid_price_policy,
    )

    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.suffix.lower() == ".csv":
        repaired.to_csv(target, index=False)
    elif target.suffix.lower() == ".parquet":
        repaired.to_parquet(target, index=False)
    else:
        raise ValueError("Repair output currently supports .csv or .parquet.")

    return {
        "source": str(source_path),
        "output": str(target),
        **summary,
    }


def batch_fetch_datasets(
    *,
    provider: str,
    symbols: list[str],
    start: str,
    end: str,
    interval: str,
    indicators: list[str],
    output_dir: str,
    output_format: str,
    input_path: str | None = None,
    data_root: str | None = None,
    db_path: str | None = None,
    adjust: str = "none",
    market: str | None = None,
    provider_profile: str | None = None,
) -> dict:
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    results = []
    for symbol in symbols:
        frame, metadata = fetch_dataset(
            provider=provider,
            symbol=symbol,
            start=start,
            end=end,
            interval=interval,
            indicators=indicators,
            input_path=input_path,
            data_root=data_root,
            db_path=db_path,
            adjust=adjust,
            market=market,
            provider_profile=provider_profile,
        )
        suffix = "csv" if output_format == "csv" else "json"
        output_path = output_root / f"{symbol}.{suffix}"
        write_dataset(frame, metadata, str(output_path), output_format)
        results.append(
            {
                "symbol": symbol,
                "rows": len(frame),
                "output": str(output_path),
            }
        )

    return {
        "provider": provider,
        "interval": interval,
        "adjust": validate_adjust_mode(adjust),
        "market": resolve_provider_market(market, provider, provider_profile),
        "provider_profile": provider_profile,
        "symbols": len(results),
        "results": results,
    }
