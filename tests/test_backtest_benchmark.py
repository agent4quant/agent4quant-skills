from __future__ import annotations

from agent4quant.backtest.engine import run_backtest


def test_backtest_emits_benchmark_metrics() -> None:
    result = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        benchmark_symbol="BMK.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
    )

    assert result["benchmark"]["symbol"] == "BMK.SH"
    assert "benchmark_total_return" in result["metrics"]
    assert "benchmark_annualized_return" in result["metrics"]
    assert "excess_total_return" in result["metrics"]
    assert "benchmark_curve" in result

