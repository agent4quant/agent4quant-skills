from __future__ import annotations

from agent4quant.backtest.engine import run_backtest


def test_backtest_costs_reduce_return_and_emit_trades() -> None:
    baseline = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
    )
    with_costs = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
        costs={"commission_bps": 2, "slippage_bps": 3, "stamp_duty_bps": 10},
    )

    assert with_costs["metrics"]["total_cost"] > 0
    assert with_costs["metrics"]["gross_total_return"] >= with_costs["metrics"]["total_return"]
    assert with_costs["metrics"]["total_return"] <= baseline["metrics"]["total_return"]
    assert with_costs["cost_model"]["stamp_duty_bps"] == 10
    assert "trades" in with_costs
    assert isinstance(with_costs["trades"], list)
    if with_costs["trades"]:
        first = with_costs["trades"][0]
        assert "entry_date" in first
        assert "net_return" in first

