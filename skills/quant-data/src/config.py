from __future__ import annotations

import os
from pathlib import Path


def resolve_data_root(data_root: str | None = None) -> Path | None:
    if data_root:
        path = Path(data_root).expanduser().resolve()
        return path if path.exists() else None
    env_root = os.getenv("A4Q_MARKET_DATA_ROOT")
    if env_root:
        path = Path(env_root).expanduser().resolve()
        return path if path.exists() else None
    return None


def resolve_provider_market(market: str | None = None) -> str | None:
    return market
