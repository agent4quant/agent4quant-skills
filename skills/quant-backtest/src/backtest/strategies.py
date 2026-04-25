from __future__ import annotations

import pandas as pd

from agent4quant.data.indicators import add_indicators


def _with_defaults(raw: dict[str, float], defaults: dict[str, float]) -> dict[str, float]:
    result = defaults.copy()
    result.update(raw)
    return result


def build_positions(frame: pd.DataFrame, strategy: str, params: dict[str, float]) -> tuple[pd.Series, dict[str, float]]:
    if strategy == "sma_cross":
        config = _with_defaults(params, {"fast": 5, "slow": 20})
        enriched = add_indicators(frame, [f"ma{int(config['fast'])}", f"ma{int(config['slow'])}"])
        signal = (enriched[f"ma_{int(config['fast'])}"] > enriched[f"ma_{int(config['slow'])}"]).astype(int)
        return signal.fillna(0), config

    if strategy == "macd":
        config = _with_defaults(params, {})
        enriched = add_indicators(frame, ["macd"])
        signal = (enriched["macd"] > enriched["macd_signal"]).astype(int)
        return signal.fillna(0), config

    if strategy == "boll_breakout":
        config = _with_defaults(params, {})
        enriched = add_indicators(frame, ["boll"])
        signal = (enriched["close"] > enriched["boll_upper"]).astype(int)
        signal = signal.where(enriched["close"] >= enriched["boll_mid"], 0)
        return signal.fillna(0), config

    raise ValueError("Unsupported strategy. Use: sma_cross, macd, boll_breakout")

