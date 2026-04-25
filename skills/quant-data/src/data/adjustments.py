from __future__ import annotations

import pandas as pd


SUPPORTED_ADJUSTMENTS = {"none", "qfq", "hfq"}


def validate_adjust_mode(adjust: str) -> str:
    mode = (adjust or "none").lower()
    if mode not in SUPPORTED_ADJUSTMENTS:
        raise ValueError(f"Unsupported adjust mode: {adjust}. Supported: {sorted(SUPPORTED_ADJUSTMENTS)}")
    return mode


def apply_price_adjustment(frame: pd.DataFrame, adjust: str = "none") -> pd.DataFrame:
    mode = validate_adjust_mode(adjust)
    if mode == "none" or "adj_factor" not in frame.columns or frame.empty:
        return frame

    adjusted = frame.copy()
    adjusted["adj_factor"] = pd.to_numeric(adjusted["adj_factor"], errors="coerce")
    valid = adjusted.loc[adjusted["adj_factor"] > 0, "adj_factor"].dropna()
    if valid.empty:
        return frame

    base = valid.iloc[-1] if mode == "qfq" else valid.iloc[0]
    if base == 0:
        return frame

    ratio = adjusted["adj_factor"].where(adjusted["adj_factor"] > 0) / float(base)
    ratio = ratio.fillna(1.0)
    for column in ("open", "high", "low", "close"):
        if column in adjusted.columns:
            adjusted[column] = pd.to_numeric(adjusted[column], errors="coerce") * ratio
    if "volume" in adjusted.columns:
        adjusted["volume"] = pd.to_numeric(adjusted["volume"], errors="coerce") / ratio.replace(0, pd.NA)
    return adjusted
