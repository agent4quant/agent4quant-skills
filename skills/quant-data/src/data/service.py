from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

from .providers import FetchRequest, get_provider
from .indicators import add_indicators
from ..config import resolve_data_root, resolve_provider_market
from ..compliance import build_metadata


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
    adjust: str = "none",
    market: str | None = None,
) -> tuple[pd.DataFrame, dict]:
    resolved_market = resolve_provider_market(market)
    request = FetchRequest(
        symbol=symbol,
        start=start,
        end=end,
        interval=interval,
        input_path=input_path,
        data_root=data_root,
        adjust=adjust,
        market=resolved_market,
    )
    frame = get_provider(provider).fetch(request)
    dataset = add_indicators(frame, indicators)
    metadata = build_metadata("quant-data", provider, interval)
    metadata["adjust"] = adjust
    if resolved_market:
        metadata["market"] = resolved_market
    return dataset, metadata


def list_provider_capabilities() -> dict:
    from .providers import list_provider_capabilities as _list
    return _list()


def write_dataset(frame: pd.DataFrame, metadata: dict, output_path: str, output_format: str) -> None:
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