from __future__ import annotations

import json
import operator
from dataclasses import dataclass
from pathlib import Path
from textwrap import wrap

import numpy as np
import pandas as pd
from jinja2 import Environment, FileSystemLoader

from ..compliance import DISCLAIMER
from .strategies import build_positions


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


def run_backtest(
    *,
    data_path: str,
    strategy: str,
    strategy_params: dict,
    benchmark_symbol: str | None = None,
    commission_bps: float = 0.0,
    slippage_bps: float = 0.0,
    stamp_duty_bps: float = 0.0,
    output_json: str | None = None,
    output_html: str | None = None,
) -> dict:
    """Run backtest on data file."""
    path = Path(data_path)
    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path, parse_dates=["date"])
    elif path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = payload.get("rows", payload)
        frame = pd.DataFrame(rows)
        if "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"])
    else:
        raise ValueError(f"Unsupported file format: {path.suffix}")

    frame = frame.sort_values("date").reset_index(drop=True)

    positions = build_positions(frame, strategy, strategy_params)
    frame["position"] = positions

    costs = CostConfig(
        commission_bps=commission_bps,
        slippage_bps=slippage_bps,
        stamp_duty_bps=stamp_duty_bps,
    )

    trades, total_return, win_rate = _build_trade_log(frame, costs)

    equity = [1.0]
    for i in range(1, len(frame)):
        ret = frame.iloc[i]["close"] / frame.iloc[i - 1]["close"] - 1
        position = frame.iloc[i]["position"]
        equity.append(equity[-1] * (1 + ret * position))

    equity = np.array(equity)
    metrics = {
        "total_return": round(float(equity[-1] / equity[0] - 1), 6),
        "annual_return": round(float((equity[-1] / equity[0]) ** (252 / len(equity)) - 1), 6) if len(equity) > 1 else 0.0,
        "max_drawdown": round(float(float(np.min(np.minimum.accumulate(equity) / equity) - 1)), 6),
        "sharpe_ratio": round(float(np.mean(np.diff(equity) / equity[:-1]) / np.std(np.diff(equity) / equity[:-1]) * np.sqrt(252)), 4) if len(equity) > 2 else 0.0,
        "total_trades": len(trades),
        "win_rate": round(float(win_rate), 4) if trades else 0.0,
    }

    result = {
        "symbol": frame["symbol"].iloc[0] if "symbol" in frame.columns else "UNKNOWN",
        "strategy": strategy,
        "strategy_params": strategy_params,
        "period": {
            "start": str(frame["date"].iloc[0].date()) if hasattr(frame["date"].iloc[0], "date") else str(frame["date"].iloc[0])[:10],
            "end": str(frame["date"].iloc[-1].date()) if hasattr(frame["date"].iloc[-1], "date") else str(frame["date"].iloc[-1])[:10],
        },
        "cost_model": {
            "commission_bps": commission_bps,
            "slippage_bps": slippage_bps,
            "stamp_duty_bps": stamp_duty_bps,
        },
        "metrics": metrics,
        "equity_curve": [round(float(x), 6) for x in equity.tolist()],
        "trades": trades,
        "disclaimer": DISCLAIMER,
    }

    if output_json:
        Path(output_json).parent.mkdir(parents=True, exist_ok=True)
        Path(output_json).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    if output_html:
        Path(output_html).parent.mkdir(parents=True, exist_ok=True)
        _write_html_report(result, output_html)

    return result


def _build_trade_log(frame: pd.DataFrame, costs: CostConfig) -> tuple[list[dict], float, float]:
    trades = []
    open_trade = None
    wins = 0

    for idx in range(1, len(frame)):
        previous = frame.iloc[idx - 1]["position"]
        current = frame.iloc[idx]["position"]
        if previous == 0 and current == 1:
            open_trade = {
                "entry_date": str(frame.iloc[idx]["date"])[:10],
                "entry_price": round(float(frame.iloc[idx]["close"]), 6),
            }
        elif previous == 1 and current == 0:
            if open_trade is None:
                continue
            entry_price = float(open_trade["entry_price"])
            exit_price = float(frame.iloc[idx]["close"])
            net_return = (exit_price * (1 - costs.sell_rate)) / (entry_price * (1 + costs.buy_rate)) - 1
            holding_bars = idx - 0
            trades.append({
                **open_trade,
                "exit_date": str(frame.iloc[idx]["date"])[:10],
                "exit_price": round(exit_price, 6),
                "net_return": round(net_return, 6),
                "holding_bars": holding_bars,
            })
            if net_return > 0:
                wins += 1
            open_trade = None

    total_return = sum(t["net_return"] for t in trades) if trades else 0.0
    win_rate = wins / len(trades) if trades else 0.0
    return trades, total_return, win_rate


def _write_html_report(result: dict, output_path: str) -> None:
    template = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Backtest Report</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 900px; margin: 0 auto; padding: 20px; }
        h1, h2 { color: #333; }
        table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background: #f5f5f5; }
        .metric { font-weight: bold; color: #2196F3; }
        .disclaimer { margin-top: 40px; padding: 15px; background: #fff3cd; border-radius: 5px; font-size: 12px; }
    </style>
</head>
<body>
    <h1>Backtest Report</h1>
    <p><strong>Symbol:</strong> {{ result.symbol }}</p>
    <p><strong>Strategy:</strong> {{ result.strategy }} ({{ result.strategy_params | tojson }})</p>
    <p><strong>Period:</strong> {{ result.period.start }} to {{ result.period.end }}</p>

    <h2>Metrics</h2>
    <table>
        <tr><th>Metric</th><th>Value</th></tr>
        {% for key, value in result.metrics.items() %}
        <tr><td>{{ key }}</td><td class="metric">{{ value }}</td></tr>
        {% endfor %}
    </table>

    <h2>Cost Model</h2>
    <table>
        <tr><td>Commission (bps)</td><td>{{ result.cost_model.commission_bps }}</td></tr>
        <tr><td>Slippage (bps)</td><td>{{ result.cost_model.slippage_bps }}</td></tr>
        <tr><td>Stamp Duty (bps)</td><td>{{ result.cost_model.stamp_duty_bps }}</td></tr>
    </table>

    <h2>Trades ({{ result.trades|length }})</h2>
    {% if result.trades %}
    <table>
        <tr><th>Entry</th><th>Exit</th><th>Entry Price</th><th>Exit Price</th><th>Net Return</th></tr>
        {% for trade in result.trades[:20] %}
        <tr>
            <td>{{ trade.entry_date }}</td>
            <td>{{ trade.exit_date }}</td>
            <td>{{ trade.entry_price }}</td>
            <td>{{ trade.exit_price }}</td>
            <td>{{ (trade.net_return * 100)|round(2) }}%</td>
        </tr>
        {% endfor %}
    </table>
    {% else %}
    <p>No trades executed.</p>
    {% endif %}

    <div class="disclaimer">{{ result.disclaimer }}</div>
</body>
</html>
"""
    from jinja2 import Template
    t = Template(template)
    html = t.render(result=result)
    Path(output_path).write_text(html, encoding="utf-8")
