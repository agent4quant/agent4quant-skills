from __future__ import annotations

import json
import operator
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from jinja2 import Environment, FileSystemLoader

from agent4quant.compliance import DISCLAIMER, build_metadata
from agent4quant.data.service import fetch_dataset
from agent4quant.backtest.strategies import build_positions


def _template_env() -> Environment:
    template_dir = Path(__file__).resolve().parents[1] / "templates"
    return Environment(loader=FileSystemLoader(template_dir), autoescape=False)


@dataclass(slots=True)
class CostConfig:
    commission_bps: float = 0.0
    slippage_bps: float = 0.0
    stamp_duty_bps: float = 0.0

    @property
    def buy_rate(self) -> float:
        return (self.commission_bps + self.slippage_bps) / 10_000

    @property
    def sell_rate(self) -> float:
        return (self.commission_bps + self.slippage_bps + self.stamp_duty_bps) / 10_000


def _resolve_costs(costs: dict[str, float] | None) -> CostConfig:
    payload = costs or {}
    return CostConfig(
        commission_bps=float(payload.get("commission_bps", 0.0)),
        slippage_bps=float(payload.get("slippage_bps", 0.0)),
        stamp_duty_bps=float(payload.get("stamp_duty_bps", 0.0)),
    )


def _apply_experiment_filters(
    experiments: list[dict],
    filters: list[dict[str, float | str]] | None,
) -> tuple[list[dict], int]:
    if not filters:
        return experiments, 0

    operators = {
        ">": operator.gt,
        ">=": operator.ge,
        "<": operator.lt,
        "<=": operator.le,
        "==": operator.eq,
        "!=": operator.ne,
    }

    filtered = experiments
    for item in filters:
        metric = str(item["metric"])
        symbol = str(item["operator"])
        value = float(item["value"])
        comparator = operators.get(symbol)
        if comparator is None:
            raise ValueError(f"Unsupported filter operator: {symbol}")
        if not filtered:
            break
        if metric not in filtered[0]:
            raise ValueError(f"Unsupported filter metric: {metric}")
        filtered = [experiment for experiment in filtered if comparator(float(experiment[metric]), value)]

    return filtered, len(experiments) - len(filtered)


def _build_trade_log(frame: pd.DataFrame, costs: CostConfig) -> tuple[list[dict], int, float]:
    trades: list[dict] = []
    open_trade: dict | None = None

    for idx in range(1, len(frame)):
        previous = frame.iloc[idx - 1]["position"]
        current = frame.iloc[idx]["position"]
        if previous == 0 and current == 1:
            open_trade = {
                "entry_date": frame.iloc[idx]["date"].strftime("%Y-%m-%d %H:%M:%S"),
                "entry_price": round(float(frame.iloc[idx]["close"]), 6),
                "entry_index": idx,
            }
        elif previous == 1 and current == 0:
            if open_trade is None:
                continue
            entry_price = float(open_trade["entry_price"])
            exit_price = float(frame.iloc[idx]["close"])
            gross_return = exit_price / entry_price - 1
            net_return = (exit_price * (1 - costs.sell_rate)) / (entry_price * (1 + costs.buy_rate)) - 1
            holding_bars = idx - int(open_trade["entry_index"])
            trades.append(
                {
                    "entry_date": open_trade["entry_date"],
                    "entry_price": round(entry_price, 6),
                    "exit_date": frame.iloc[idx]["date"].strftime("%Y-%m-%d %H:%M:%S"),
                    "exit_price": round(exit_price, 6),
                    "gross_return": round(float(gross_return), 6),
                    "net_return": round(float(net_return), 6),
                    "holding_bars": holding_bars,
                    "cost_rate": round(float(costs.buy_rate + costs.sell_rate), 6),
                    "status": "closed",
                }
            )
            open_trade = None

    if open_trade is not None and len(frame) > 0:
        last_price = float(frame.iloc[-1]["close"])
        entry_price = float(open_trade["entry_price"])
        gross_return = last_price / entry_price - 1
        net_return = (last_price * (1 - costs.sell_rate)) / (entry_price * (1 + costs.buy_rate)) - 1
        trades.append(
            {
                "entry_date": open_trade["entry_date"],
                "entry_price": round(entry_price, 6),
                "exit_date": frame.iloc[-1]["date"].strftime("%Y-%m-%d %H:%M:%S"),
                "exit_price": round(last_price, 6),
                "gross_return": round(float(gross_return), 6),
                "net_return": round(float(net_return), 6),
                "holding_bars": len(frame) - 1 - int(open_trade["entry_index"]),
                "cost_rate": round(float(costs.buy_rate + costs.sell_rate), 6),
                "status": "open",
            }
        )

    closed = [item for item in trades if item["status"] == "closed"]
    pair_count = len(closed)
    if pair_count == 0:
        return trades, 0, 0.0

    wins = sum(item["net_return"] > 0 for item in closed)
    return trades, pair_count, wins / pair_count


def run_backtest(
    *,
    provider: str,
    symbol: str,
    start: str,
    end: str,
    interval: str,
    strategy: str,
    strategy_params: dict[str, float],
    input_path: str | None = None,
    data_root: str | None = None,
    db_path: str | None = None,
    adjust: str = "none",
    market: str | None = None,
    provider_profile: str | None = None,
    costs: dict[str, float] | None = None,
    benchmark_symbol: str | None = None,
) -> dict:
    if strategy == "sma_cross":
        fast = float(strategy_params.get("fast", 5))
        slow = float(strategy_params.get("slow", 20))
        if fast <= 0 or slow <= 0:
            raise ValueError("sma_cross parameters must be positive.")
        if fast >= slow:
            raise ValueError("sma_cross requires fast < slow.")

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
    if frame.empty:
        raise ValueError(f"Backtest dataset is empty for symbol={symbol}, start={start}, end={end}, interval={interval}.")

    data = frame[["date", "open", "high", "low", "close", "volume", "symbol"]].copy()
    signal, resolved_params = build_positions(data, strategy, strategy_params)
    cost_config = _resolve_costs(costs)
    data["signal"] = signal
    data["position"] = data["signal"].shift(1).fillna(0)
    data["asset_return"] = data["close"].pct_change().fillna(0)
    data["strategy_return_gross"] = data["position"] * data["asset_return"]
    data["position_change"] = data["position"].diff().fillna(data["position"]).abs()
    data["sell_change"] = (-data["position"].diff().fillna(data["position"])).clip(lower=0)
    data["cost_return"] = (
        data["position_change"] * ((cost_config.commission_bps + cost_config.slippage_bps) / 10_000)
        + data["sell_change"] * (cost_config.stamp_duty_bps / 10_000)
    )
    data["strategy_return"] = data["strategy_return_gross"] - data["cost_return"]
    data["equity_curve_gross"] = (1 + data["strategy_return_gross"]).cumprod()
    data["equity_curve"] = (1 + data["strategy_return"]).cumprod()
    data["rolling_peak"] = data["equity_curve"].cummax()
    data["drawdown"] = data["equity_curve"] / data["rolling_peak"] - 1

    periods_per_year = 252 if interval == "1d" else 252 * 48
    total_return = float(data["equity_curve"].iloc[-1] - 1)
    gross_total_return = float(data["equity_curve_gross"].iloc[-1] - 1)
    annualized_return = float((1 + total_return) ** (periods_per_year / max(len(data), 1)) - 1)
    volatility = float(data["strategy_return"].std(ddof=0) * np.sqrt(periods_per_year))
    sharpe = float(annualized_return / volatility) if volatility else 0.0
    max_drawdown = float(data["drawdown"].min())
    total_cost = float(data["cost_return"].sum())
    trades, trade_count, win_rate = _build_trade_log(data, cost_config)

    benchmark_return = 0.0
    benchmark_annualized_return = 0.0
    excess_total_return = 0.0
    benchmark_curve: list[dict] = []
    benchmark_used = benchmark_symbol or symbol

    benchmark_frame, _ = fetch_dataset(
        provider=provider,
        symbol=benchmark_used,
        start=start,
        end=end,
        interval=interval,
        indicators=[],
        input_path=input_path if provider == "csv" and benchmark_used == symbol else None,
        data_root=data_root,
        db_path=db_path,
        adjust=adjust,
        market=market,
        provider_profile=provider_profile,
    )
    if benchmark_frame.empty:
        raise ValueError(
            f"Benchmark dataset is empty for symbol={benchmark_used}, start={start}, end={end}, interval={interval}."
        )
    benchmark_data = benchmark_frame[["date", "close"]].copy()
    benchmark_data["benchmark_return"] = benchmark_data["close"].pct_change().fillna(0)
    benchmark_data["benchmark_equity"] = (1 + benchmark_data["benchmark_return"]).cumprod()
    benchmark_return = float(benchmark_data["benchmark_equity"].iloc[-1] - 1)
    benchmark_annualized_return = float(
        (1 + benchmark_return) ** (periods_per_year / max(len(benchmark_data), 1)) - 1
    )
    excess_total_return = total_return - benchmark_return
    benchmark_curve = [
        {
            "date": row.date.strftime("%Y-%m-%d %H:%M:%S"),
            "benchmark_equity": round(float(row.benchmark_equity), 6),
        }
        for row in benchmark_data.itertuples()
    ]

    metadata = build_metadata("quant-backtest", provider, interval)
    result = {
        "metadata": metadata,
        "strategy": strategy,
        "strategy_params": resolved_params,
        "cost_model": {
            "commission_bps": cost_config.commission_bps,
            "slippage_bps": cost_config.slippage_bps,
            "stamp_duty_bps": cost_config.stamp_duty_bps,
        },
        "benchmark": {"symbol": benchmark_used},
        "data_config": {"adjust": adjust, "market": market, "provider_profile": provider_profile},
        "symbol": symbol,
        "period": {"start": start, "end": end, "interval": interval},
        "metrics": {
            "total_return": round(total_return, 6),
            "gross_total_return": round(gross_total_return, 6),
            "benchmark_total_return": round(benchmark_return, 6),
            "excess_total_return": round(excess_total_return, 6),
            "annualized_return": round(annualized_return, 6),
            "benchmark_annualized_return": round(benchmark_annualized_return, 6),
            "volatility": round(volatility, 6),
            "sharpe": round(sharpe, 6),
            "max_drawdown": round(max_drawdown, 6),
            "total_cost": round(total_cost, 6),
            "trade_count": trade_count,
            "win_rate": round(win_rate, 6),
        },
        "equity_curve": [
            {
                "date": row.date.strftime("%Y-%m-%d %H:%M:%S"),
                "equity": round(float(row.equity_curve), 6),
                "gross_equity": round(float(row.equity_curve_gross), 6),
                "drawdown": round(float(row.drawdown), 6),
                "cost_return": round(float(row.cost_return), 6),
            }
            for row in data.itertuples()
        ],
        "benchmark_curve": benchmark_curve,
        "trades": trades,
        "trading_notes": DISCLAIMER,
    }
    return result


def sweep_backtest(
    *,
    provider: str,
    symbol: str,
    start: str,
    end: str,
    interval: str,
    strategy: str,
    parameter_grid: dict[str, list[float]],
    metric: str,
    input_path: str | None = None,
    data_root: str | None = None,
    db_path: str | None = None,
    adjust: str = "none",
    market: str | None = None,
    provider_profile: str | None = None,
    costs: dict[str, float] | None = None,
    benchmark_symbol: str | None = None,
    top_n: int | None = None,
    filters: list[dict[str, float | str]] | None = None,
) -> dict:
    if not parameter_grid:
        raise ValueError("Parameter grid cannot be empty.")

    unsupported = set()
    for values in parameter_grid.values():
        if not values:
            unsupported.add("empty")
    if unsupported:
        raise ValueError("Each parameter in the sweep must provide at least one value.")

    from itertools import product

    keys = list(parameter_grid)
    experiments = []
    filtered_out = 0
    for combo in product(*(parameter_grid[key] for key in keys)):
        params = {key: value for key, value in zip(keys, combo, strict=True)}
        try:
            result = run_backtest(
                provider=provider,
                symbol=symbol,
                start=start,
                end=end,
                interval=interval,
                strategy=strategy,
                strategy_params=params,
                input_path=input_path,
                data_root=data_root,
                db_path=db_path,
                adjust=adjust,
                market=market,
                provider_profile=provider_profile,
                costs=costs,
                benchmark_symbol=benchmark_symbol,
            )
        except ValueError:
            filtered_out += 1
            continue
        experiments.append(
            {
                "strategy_params": result["strategy_params"],
                **result["metrics"],
            }
        )

    if not experiments:
        raise ValueError("No valid sweep experiments remain after applying parameter constraints.")

    if metric not in experiments[0]:
        raise ValueError(f"Unsupported metric for ranking: {metric}")

    experiments.sort(key=lambda item: item[metric], reverse=True)
    filtered_experiments, result_filtered_out = _apply_experiment_filters(experiments, filters)
    if not filtered_experiments:
        raise ValueError("No sweep experiments remain after applying result filters.")
    ranked = filtered_experiments[:top_n] if top_n and top_n > 0 else filtered_experiments
    best = filtered_experiments[0]
    return {
        "metadata": build_metadata("quant-backtest-sweep", provider, interval),
        "symbol": symbol,
        "strategy": strategy,
        "metric": metric,
        "ranking_metric": metric,
        "applied_filters": [str(item["expression"]) for item in (filters or [])],
        "benchmark": {"symbol": benchmark_symbol or symbol},
        "data_config": {"adjust": adjust, "market": market, "provider_profile": provider_profile},
        "cost_model": {
            "commission_bps": _resolve_costs(costs).commission_bps,
            "slippage_bps": _resolve_costs(costs).slippage_bps,
            "stamp_duty_bps": _resolve_costs(costs).stamp_duty_bps,
        },
        "period": {"start": start, "end": end, "interval": interval},
        "total_experiments": len(experiments),
        "constraint_filtered_out_experiments": filtered_out,
        "result_filtered_out_experiments": result_filtered_out,
        "filtered_out_experiments": filtered_out + result_filtered_out,
        "returned_experiments": len(ranked),
        "experiments": ranked,
        "best_params": best["strategy_params"],
        "best_metric_value": best[metric],
    }


def compare_backtests(
    *,
    provider: str,
    symbol: str,
    start: str,
    end: str,
    interval: str,
    strategies: list[str],
    strategy_params_map: dict[str, dict[str, float]],
    metric: str,
    input_path: str | None = None,
    data_root: str | None = None,
    db_path: str | None = None,
    adjust: str = "none",
    market: str | None = None,
    provider_profile: str | None = None,
    costs: dict[str, float] | None = None,
    benchmark_symbol: str | None = None,
    top_n: int | None = None,
    filters: list[dict[str, float | str]] | None = None,
) -> dict:
    if not strategies:
        raise ValueError("At least one strategy is required for comparison.")

    experiments = []
    for strategy in strategies:
        result = run_backtest(
            provider=provider,
            symbol=symbol,
            start=start,
            end=end,
            interval=interval,
            strategy=strategy,
            strategy_params=strategy_params_map.get(strategy, {}),
            input_path=input_path,
            data_root=data_root,
                db_path=db_path,
                adjust=adjust,
                market=market,
                provider_profile=provider_profile,
                costs=costs,
                benchmark_symbol=benchmark_symbol,
            )
        experiments.append(
            {
                "strategy": strategy,
                "strategy_params": result["strategy_params"],
                **result["metrics"],
            }
        )

    if metric not in experiments[0]:
        raise ValueError(f"Unsupported metric for ranking: {metric}")

    experiments.sort(key=lambda item: item[metric], reverse=True)
    filtered_experiments, result_filtered_out = _apply_experiment_filters(experiments, filters)
    if not filtered_experiments:
        raise ValueError("No compare experiments remain after applying result filters.")
    ranked = filtered_experiments[:top_n] if top_n and top_n > 0 else filtered_experiments
    best = filtered_experiments[0]
    return {
        "metadata": build_metadata("quant-backtest-compare", provider, interval),
        "symbol": symbol,
        "metric": metric,
        "ranking_metric": metric,
        "applied_filters": [str(item["expression"]) for item in (filters or [])],
        "benchmark": {"symbol": benchmark_symbol or symbol},
        "data_config": {"adjust": adjust, "market": market, "provider_profile": provider_profile},
        "cost_model": {
            "commission_bps": _resolve_costs(costs).commission_bps,
            "slippage_bps": _resolve_costs(costs).slippage_bps,
            "stamp_duty_bps": _resolve_costs(costs).stamp_duty_bps,
        },
        "period": {"start": start, "end": end, "interval": interval},
        "total_experiments": len(experiments),
        "result_filtered_out_experiments": result_filtered_out,
        "filtered_out_experiments": result_filtered_out,
        "returned_experiments": len(ranked),
        "experiments": ranked,
        "best_strategy": best["strategy"],
        "best_metric_value": best[metric],
    }


def write_backtest_result(result: dict, output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


def write_backtest_html(result: dict, output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    template = _template_env().get_template("backtest_report.j2")
    html = template.render(result=result)
    path.write_text(html, encoding="utf-8")


def write_sweep_html(result: dict, output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    template = _template_env().get_template("sweep_report.j2")
    html = template.render(result=result)
    path.write_text(html, encoding="utf-8")


def write_compare_html(result: dict, output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    template = _template_env().get_template("compare_report.j2")
    html = template.render(result=result)
    path.write_text(html, encoding="utf-8")


def write_sweep_result(result: dict, output_json: str, output_csv: str | None = None) -> None:
    path = Path(output_json)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    if output_csv:
        csv_path = Path(output_csv)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        rows = []
        for item in result["experiments"]:
            row = item["strategy_params"].copy()
            for key, value in item.items():
                if key != "strategy_params":
                    row[key] = value
            rows.append(row)
        pd.DataFrame(rows).to_csv(csv_path, index=False)


def write_compare_result(result: dict, output_json: str, output_csv: str | None = None) -> None:
    path = Path(output_json)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    if output_csv:
        csv_path = Path(output_csv)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        rows = []
        for item in result["experiments"]:
            row = {"strategy": item["strategy"], **item["strategy_params"]}
            for key, value in item.items():
                if key not in {"strategy", "strategy_params"}:
                    row[key] = value
            rows.append(row)
        pd.DataFrame(rows).to_csv(csv_path, index=False)
