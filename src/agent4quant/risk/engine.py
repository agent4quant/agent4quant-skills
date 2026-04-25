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


def _periods_per_year(interval: str) -> int:
    return 252 if interval == "1d" else 252 * 48


def _load_backtest_returns(input_path: str, source: str) -> tuple[pd.Series, dict]:
    payload = json.loads(Path(input_path).read_text(encoding="utf-8"))
    curve = pd.DataFrame(payload["equity_curve"])
    curve["date"] = pd.to_datetime(curve["date"])
    if source == "strategy":
        returns = curve["equity"].pct_change().fillna(0.0)
    elif source == "gross_strategy":
        returns = curve["gross_equity"].pct_change().fillna(0.0)
    else:
        raise ValueError("Backtest risk source must be `strategy` or `gross_strategy`.")
    returns.index = curve["date"]

    context = {
        "provider": payload["metadata"]["provider"],
        "interval": payload["period"]["interval"],
        "symbol": payload["symbol"],
        "benchmark_symbol": payload.get("benchmark", {}).get("symbol"),
        "mode": "backtest",
        "strategy": payload.get("strategy"),
        "source": source,
    }
    return returns, context


def _load_market_returns(
    *,
    provider: str,
    symbol: str,
    start: str,
    end: str,
    interval: str,
    input_path: str | None = None,
    data_root: str | None = None,
    db_path: str | None = None,
    adjust: str = "none",
    market: str | None = None,
    provider_profile: str | None = None,
) -> tuple[pd.Series, dict]:
    frame, _ = fetch_dataset(
        provider=provider,
        symbol=symbol,
        start=start,
        end=end,
        interval=interval,
        indicators=[],
        input_path=input_path,
        data_root=data_root,
        db_path=db_path,
        adjust=adjust,
        market=market,
        provider_profile=provider_profile,
    )
    frame = frame.sort_values("date").reset_index(drop=True)
    returns = frame["close"].pct_change().fillna(0.0)
    returns.index = pd.to_datetime(frame["date"])
    context = {
        "provider": provider,
        "interval": interval,
        "symbol": symbol,
        "mode": "market",
        "source": "asset",
        "adjust": adjust,
        "market": market,
        "provider_profile": provider_profile,
    }
    return returns, context


def _serialize_index_label(value) -> str:
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _series_points(series: pd.Series, value_key: str) -> list[dict]:
    clean = pd.Series(series, dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    points = []
    for idx, value in enumerate(clean.items()):
        label, numeric_value = value
        points.append(
            {
                "index": idx,
                "label": _serialize_index_label(label),
                value_key: round(float(numeric_value), 6),
            }
        )
    return points


def _calculate_risk_metrics(returns: pd.Series, interval: str, confidence_level: float) -> dict:
    clean = pd.Series(returns, dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        raise ValueError("Risk analysis requires at least one valid return observation.")

    percentile = 1 - confidence_level
    value_at_risk = float(clean.quantile(percentile))
    tail = clean[clean <= value_at_risk]
    conditional_var = float(tail.mean()) if not tail.empty else value_at_risk

    equity = (1 + clean).cumprod()
    drawdown = equity / equity.cummax() - 1
    periods = _periods_per_year(interval)
    volatility = float(clean.std(ddof=0))
    annualized_volatility = float(volatility * np.sqrt(periods))
    mean_return = float(clean.mean())
    annualized_return = float(equity.iloc[-1] ** (periods / len(clean)) - 1) if len(clean) else 0.0
    downside = clean.clip(upper=0.0)
    downside_volatility = float(np.sqrt(np.mean(np.square(downside)))) if len(downside) else 0.0
    annualized_downside_volatility = float(downside_volatility * np.sqrt(periods))
    max_drawdown = float(drawdown.min())
    sortino_ratio = float((mean_return * periods) / annualized_downside_volatility) if annualized_downside_volatility else 0.0
    calmar_ratio = float(annualized_return / abs(max_drawdown)) if max_drawdown < 0 else 0.0
    skewness = float(clean.skew()) if len(clean) > 2 else 0.0
    kurtosis = float(clean.kurt()) if len(clean) > 3 else 0.0

    return {
        "confidence_level": round(confidence_level, 4),
        "observations": int(len(clean)),
        "mean_return": round(mean_return, 6),
        "annualized_return": round(annualized_return, 6),
        "volatility": round(volatility, 6),
        "annualized_volatility": round(annualized_volatility, 6),
        "downside_volatility": round(downside_volatility, 6),
        "annualized_downside_volatility": round(annualized_downside_volatility, 6),
        "sortino_ratio": round(sortino_ratio, 6),
        "skewness": round(skewness, 6),
        "kurtosis": round(kurtosis, 6),
        "value_at_risk": round(value_at_risk, 6),
        "conditional_var": round(float(conditional_var), 6),
        "max_drawdown": round(max_drawdown, 6),
        "calmar_ratio": round(calmar_ratio, 6),
        "worst_return": round(float(clean.min()), 6),
        "best_return": round(float(clean.max()), 6),
    }


def _calculate_rolling_var(returns: pd.Series, confidence_level: float, window: int) -> list[dict]:
    clean = pd.Series(returns, dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    if len(clean) < window or window <= 1:
        return []

    quantile = 1 - confidence_level
    rolling = clean.rolling(window=window).quantile(quantile).dropna()
    return _series_points(rolling, "rolling_var")


def _calculate_rolling_volatility(returns: pd.Series, interval: str, window: int) -> list[dict]:
    clean = pd.Series(returns, dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    if len(clean) < window or window <= 1:
        return []

    rolling = clean.rolling(window=window).std(ddof=0).dropna() * np.sqrt(_periods_per_year(interval))
    return _series_points(rolling, "rolling_annualized_volatility")


def _calculate_drawdown_series(returns: pd.Series) -> list[dict]:
    clean = pd.Series(returns, dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return []

    equity = (1 + clean).cumprod()
    drawdown = equity / equity.cummax() - 1
    return _series_points(drawdown, "drawdown")


def _calculate_stress_results(returns: pd.Series, shocks: list[float]) -> list[dict]:
    clean = pd.Series(returns, dtype="float64").replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return []

    base_mean = float(clean.mean())
    base_worst = float(clean.min())
    return [
        {
            "shock": round(float(shock), 6),
            "stressed_mean_return": round(base_mean + float(shock), 6),
            "stressed_worst_return": round(base_worst + float(shock), 6),
        }
        for shock in shocks
    ]


def analyze_risk(
    *,
    provider: str | None = None,
    symbol: str | None = None,
    start: str | None = None,
    end: str | None = None,
    interval: str = "1d",
    confidence_level: float = 0.95,
    input_path: str | None = None,
    data_root: str | None = None,
    db_path: str | None = None,
    mode: str = "market",
    source: str = "asset",
    adjust: str = "none",
    market: str | None = None,
    provider_profile: str | None = None,
    rolling_window: int = 20,
    stress_shocks: list[float] | None = None,
) -> dict:
    if not 0 < confidence_level < 1:
        raise ValueError("confidence_level must be between 0 and 1.")
    if rolling_window <= 1:
        raise ValueError("rolling_window must be greater than 1.")

    if mode == "backtest":
        if not input_path:
            raise ValueError("Backtest risk analysis requires --input.")
        returns, context = _load_backtest_returns(input_path, source)
        provider_name = context["provider"]
    else:
        if not all([provider, symbol, start, end]):
            raise ValueError("Market risk analysis requires provider, symbol, start and end.")
        returns, context = _load_market_returns(
            provider=provider,
            symbol=symbol,
            start=start,
            end=end,
            interval=interval,
            input_path=input_path,
            data_root=data_root,
            db_path=db_path,
            adjust=adjust,
            market=market,
            provider_profile=provider_profile,
        )
        provider_name = str(provider)

    metrics = _calculate_risk_metrics(returns, context["interval"], confidence_level)
    shocks = stress_shocks or [-0.02, -0.05, -0.1]
    rolling_var = _calculate_rolling_var(returns, confidence_level, rolling_window)
    rolling_volatility = _calculate_rolling_volatility(returns, context["interval"], rolling_window)
    drawdown_series = _calculate_drawdown_series(returns)
    stress_results = _calculate_stress_results(returns, shocks)
    result = {
        "metadata": build_metadata("quant-risk", provider_name, context["interval"]),
        "mode": context["mode"],
        "source": context["source"],
        "symbol": context["symbol"],
        "benchmark_symbol": context.get("benchmark_symbol"),
        "strategy": context.get("strategy"),
        "period": {
            "start": start,
            "end": end,
            "interval": context["interval"],
        },
        "data_config": {
            "adjust": context.get("adjust", adjust),
            "market": context.get("market", market),
            "provider_profile": context.get("provider_profile", provider_profile),
        },
        "analysis_config": {
            "rolling_window": rolling_window,
            "stress_shocks": shocks,
        },
        "metrics": metrics,
        "rolling_var": rolling_var,
        "rolling_volatility": rolling_volatility,
        "drawdown_series": drawdown_series,
        "stress_results": stress_results,
        "trading_notes": DISCLAIMER,
    }
    return result


def write_risk_result(result: dict, output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


def write_risk_html(result: dict, output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    template = _template_env().get_template("risk_report.j2")
    html = template.render(result=result)
    path.write_text(html, encoding="utf-8")
