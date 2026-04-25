from __future__ import annotations

import pytest

from agent4quant.backtest.engine import sweep_backtest


def test_backtest_sweep_sorts_by_metric_descending() -> None:
    result = sweep_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        parameter_grid={"fast": [5, 10], "slow": [20, 30]},
        metric="sharpe",
    )

    assert result["best_params"]
    assert len(result["experiments"]) == 4
    sharpe_values = [item["sharpe"] for item in result["experiments"]]
    assert sharpe_values == sorted(sharpe_values, reverse=True)


def test_backtest_sweep_filters_invalid_combos_and_supports_top_n() -> None:
    result = sweep_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        parameter_grid={"fast": [5, 20], "slow": [10, 20]},
        metric="sharpe",
        top_n=2,
    )

    assert result["total_experiments"] == 2
    assert result["filtered_out_experiments"] == 2
    assert result["returned_experiments"] == 2
    assert len(result["experiments"]) == 2
    assert all(item["strategy_params"]["fast"] < item["strategy_params"]["slow"] for item in result["experiments"])


def test_backtest_run_rejects_invalid_sma_window_order() -> None:
    from agent4quant.backtest.engine import run_backtest

    with pytest.raises(ValueError, match="fast < slow"):
        run_backtest(
            provider="demo",
            symbol="DEMO.SH",
            start="2025-01-01",
            end="2025-03-31",
            interval="1d",
            strategy="sma_cross",
            strategy_params={"fast": 20, "slow": 10},
        )


def test_backtest_sweep_supports_result_filters() -> None:
    baseline = sweep_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        parameter_grid={"fast": [5, 10], "slow": [20, 30]},
        metric="sharpe",
    )
    threshold = (baseline["experiments"][0]["sharpe"] + baseline["experiments"][1]["sharpe"]) / 2

    result = sweep_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        parameter_grid={"fast": [5, 10], "slow": [20, 30]},
        metric="sharpe",
        filters=[{"metric": "sharpe", "operator": ">=", "value": threshold, "expression": f"sharpe>={threshold}"}],
    )

    assert result["result_filtered_out_experiments"] >= 1
    assert result["returned_experiments"] == 1
    assert result["applied_filters"]
