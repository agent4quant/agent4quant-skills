from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from jinja2 import Environment, FileSystemLoader

from agent4quant.compliance import DISCLAIMER, build_metadata
from agent4quant.data.service import fetch_dataset


def _template_env() -> Environment:
    template_dir = Path(__file__).resolve().parents[1] / "templates"
    return Environment(loader=FileSystemLoader(template_dir), autoescape=False)


def _rolling_ic(series_a: pd.Series, series_b: pd.Series, window: int) -> pd.Series:
    values = []
    index = []
    for end in range(window, len(series_a) + 1):
        window_a = series_a.iloc[end - window : end]
        window_b = series_b.iloc[end - window : end]
        if window_a.isna().any() or window_b.isna().any():
            continue
        values.append(float(window_a.corr(window_b)))
        index.append(series_a.index[end - 1])
    return pd.Series(values, index=index, dtype="float64")


def _factor_metrics(frame: pd.DataFrame, factor: str, forward_returns: pd.Series, ic_window: int) -> dict:
    if factor not in frame.columns:
        raise ValueError(f"Factor column not found: {factor}")

    data = pd.DataFrame({"factor": frame[factor], "forward_return": forward_returns}).dropna()
    if len(data) < max(3, ic_window):
        raise ValueError(f"Factor {factor} does not have enough observations for IC window={ic_window}.")

    ic = float(data["factor"].corr(data["forward_return"], method="pearson"))
    rank_ic = float(data["factor"].corr(data["forward_return"], method="spearman"))
    rolling_ic = _rolling_ic(data["factor"], data["forward_return"], ic_window)
    rolling_rank_ic = _rolling_ic(data["factor"].rank(), data["forward_return"].rank(), ic_window)

    ir = float(rolling_ic.mean() / rolling_ic.std(ddof=0)) if len(rolling_ic) > 1 and rolling_ic.std(ddof=0) else 0.0
    rank_ir = (
        float(rolling_rank_ic.mean() / rolling_rank_ic.std(ddof=0))
        if len(rolling_rank_ic) > 1 and rolling_rank_ic.std(ddof=0)
        else 0.0
    )

    return {
        "factor": factor,
        "observations": int(len(data)),
        "ic": round(ic, 6),
        "rank_ic": round(rank_ic, 6),
        "ir": round(ir, 6),
        "rank_ir": round(rank_ir, 6),
        "positive_ic_rate": round(float((rolling_ic > 0).mean()) if len(rolling_ic) else 0.0, 6),
        "forward_return_mean": round(float(data["forward_return"].mean()), 6),
    }


def _factor_quantile_returns(frame: pd.DataFrame, factor: str, forward_returns: pd.Series, quantiles: int) -> dict:
    if quantiles < 2:
        return {
            "factor": factor,
            "quantiles": quantiles,
            "buckets": [],
            "top_bottom_spread": 0.0,
        }

    data = pd.DataFrame({"factor": frame[factor], "forward_return": forward_returns}).dropna()
    if len(data) < quantiles:
        return {
            "factor": factor,
            "quantiles": quantiles,
            "buckets": [],
            "top_bottom_spread": 0.0,
        }

    ranked = data["factor"].rank(method="first")
    labels = [f"Q{idx}" for idx in range(1, quantiles + 1)]
    bucketed = pd.qcut(ranked, q=quantiles, labels=labels)
    grouped = data.assign(bucket=bucketed).groupby("bucket", observed=False)["forward_return"].mean()
    buckets = [{"bucket": str(bucket), "mean_forward_return": round(float(value), 6)} for bucket, value in grouped.items()]
    spread = float(grouped.iloc[-1] - grouped.iloc[0]) if len(grouped) >= 2 else 0.0
    return {
        "factor": factor,
        "quantiles": quantiles,
        "buckets": buckets,
        "top_bottom_spread": round(spread, 6),
    }


def _add_composite_factor(frame: pd.DataFrame, factors: list[str]) -> tuple[pd.DataFrame, str | None]:
    if len(factors) < 2:
        return frame, None

    composite_columns = []
    for factor in factors:
        if factor not in frame.columns:
            continue
        series = pd.to_numeric(frame[factor], errors="coerce")
        std = float(series.std(ddof=0))
        if std == 0 or pd.isna(std):
            normalized = series * 0.0
        else:
            normalized = (series - float(series.mean())) / std
        composite_columns.append(normalized)

    if len(composite_columns) < 2:
        return frame, None

    enriched = frame.copy()
    enriched["composite_factor"] = pd.concat(composite_columns, axis=1).mean(axis=1)
    return enriched, "composite_factor"


def analyze_alpha(
    *,
    provider: str,
    symbol: str,
    start: str,
    end: str,
    interval: str,
    factors: list[str],
    indicators: list[str],
    horizon: int = 1,
    ic_window: int = 20,
    input_path: str | None = None,
    data_root: str | None = None,
    db_path: str | None = None,
    adjust: str = "none",
    market: str | None = None,
    provider_profile: str | None = None,
    quantiles: int = 5,
    include_composite: bool = True,
) -> dict:
    if horizon <= 0:
        raise ValueError("horizon must be positive.")
    if ic_window <= 1:
        raise ValueError("ic_window must be greater than 1.")
    if not factors:
        raise ValueError("At least one factor is required.")
    if quantiles < 2:
        raise ValueError("quantiles must be at least 2.")

    frame, _ = fetch_dataset(
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
    frame = frame.sort_values("date").reset_index(drop=True)
    frame["forward_return"] = frame["close"].shift(-horizon) / frame["close"] - 1
    forward_returns = frame["forward_return"]

    working_frame = frame
    effective_factors = list(factors)
    composite_factor = None
    if include_composite:
        working_frame, composite_factor = _add_composite_factor(frame, factors)
        if composite_factor:
            effective_factors.append(composite_factor)

    results = [_factor_metrics(working_frame, factor, forward_returns, ic_window) for factor in effective_factors]
    results.sort(key=lambda item: abs(item["ic"]), reverse=True)
    best_factor = results[0]["factor"]
    quantile_returns = [
        _factor_quantile_returns(working_frame, factor, forward_returns, quantiles)
        for factor in effective_factors
    ]

    return {
        "metadata": build_metadata("quant-alpha", provider, interval),
        "symbol": symbol,
        "period": {"start": start, "end": end, "interval": interval},
        "config": {
            "factors": factors,
            "indicators": indicators,
            "horizon": horizon,
            "ic_window": ic_window,
            "quantiles": quantiles,
            "include_composite": include_composite,
            "adjust": adjust,
            "market": market,
            "provider_profile": provider_profile,
        },
        "results": results,
        "quantile_returns": quantile_returns,
        "composite_factor": composite_factor,
        "best_factor": best_factor,
        "trading_notes": DISCLAIMER,
    }


def write_alpha_result(result: dict, output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


def write_alpha_html(result: dict, output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    template = _template_env().get_template("alpha_report.j2")
    html = template.render(result=result)
    path.write_text(html, encoding="utf-8")
