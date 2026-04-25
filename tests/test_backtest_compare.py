from __future__ import annotations

from agent4quant.backtest.engine import compare_backtests


def test_backtest_compare_sorts_by_metric_descending() -> None:
    result = compare_backtests(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategies=["sma_cross", "macd", "boll_breakout"],
        strategy_params_map={"sma_cross": {"fast": 5, "slow": 20}},
        metric="sharpe",
    )

    assert result["best_strategy"]
    assert len(result["experiments"]) == 3
    sharpe_values = [item["sharpe"] for item in result["experiments"]]
    assert sharpe_values == sorted(sharpe_values, reverse=True)


def test_backtest_compare_supports_top_n() -> None:
    result = compare_backtests(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategies=["sma_cross", "macd", "boll_breakout"],
        strategy_params_map={"sma_cross": {"fast": 5, "slow": 20}},
        metric="sharpe",
        top_n=2,
    )

    assert result["total_experiments"] == 3
    assert result["returned_experiments"] == 2
    assert len(result["experiments"]) == 2


def test_backtest_compare_supports_result_filters() -> None:
    baseline = compare_backtests(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategies=["sma_cross", "macd", "boll_breakout"],
        strategy_params_map={"sma_cross": {"fast": 5, "slow": 20}},
        metric="sharpe",
    )
    threshold = (baseline["experiments"][0]["sharpe"] + baseline["experiments"][1]["sharpe"]) / 2

    result = compare_backtests(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategies=["sma_cross", "macd", "boll_breakout"],
        strategy_params_map={"sma_cross": {"fast": 5, "slow": 20}},
        metric="sharpe",
        filters=[{"metric": "sharpe", "operator": ">=", "value": threshold, "expression": f"sharpe>={threshold}"}],
    )

    assert result["filtered_out_experiments"] >= 1
    assert result["returned_experiments"] == 1
    assert result["applied_filters"]
