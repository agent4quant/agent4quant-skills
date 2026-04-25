from __future__ import annotations

import json
from pathlib import Path

from agent4quant.backtest.engine import (
    run_backtest,
    compare_backtests,
    sweep_backtest,
    write_backtest_html,
    write_compare_html,
    write_compare_result,
    write_sweep_html,
    write_sweep_result,
)


def test_write_sweep_html_report(tmp_path: Path) -> None:
    result = sweep_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        parameter_grid={"fast": [5, 10], "slow": [20, 30]},
        metric="sharpe",
        benchmark_symbol="BMK.SH",
        top_n=2,
        filters=[{"metric": "sharpe", "operator": ">", "value": -10.0, "expression": "sharpe>-10"}],
    )

    html_path = tmp_path / "sweep.html"
    json_path = tmp_path / "sweep.json"
    write_sweep_result(result, str(json_path))
    write_sweep_html(result, str(html_path))

    assert json.loads(json_path.read_text(encoding="utf-8"))["returned_experiments"] == 2
    content = html_path.read_text(encoding="utf-8")
    assert "Sweep Report" in content
    assert "BMK.SH" in content
    assert "sharpe&gt;-10" in content or "sharpe>-10" in content


def test_write_compare_html_report(tmp_path: Path) -> None:
    result = compare_backtests(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategies=["sma_cross", "macd", "boll_breakout"],
        strategy_params_map={"sma_cross": {"fast": 5, "slow": 20}},
        metric="sharpe",
        benchmark_symbol="BMK.SH",
        top_n=2,
        filters=[{"metric": "sharpe", "operator": ">", "value": -10.0, "expression": "sharpe>-10"}],
    )

    html_path = tmp_path / "compare.html"
    json_path = tmp_path / "compare.json"
    write_compare_result(result, str(json_path))
    write_compare_html(result, str(html_path))

    assert json.loads(json_path.read_text(encoding="utf-8"))["returned_experiments"] == 2
    content = html_path.read_text(encoding="utf-8")
    assert "多策略对比" in content
    assert "BMK.SH" in content
    assert "sharpe&gt;-10" in content or "sharpe>-10" in content
    assert "plotly" in content.lower()


def test_write_backtest_html_report_with_plotly(tmp_path: Path) -> None:
    result = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        benchmark_symbol="BMK.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
        adjust="qfq",
        market="cn",
    )
    html_path = tmp_path / "backtest.html"
    write_backtest_html(result, str(html_path))

    content = html_path.read_text(encoding="utf-8")
    assert "equity-chart" in content
    assert "cost-chart" in content
    assert "plotly" in content.lower()
    assert "Adjust: qfq / Market: cn" in content
