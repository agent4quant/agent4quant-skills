from __future__ import annotations

from agent4quant.backtest.engine import run_backtest


def test_run_backtest_returns_result() -> None:
    """Test run_backtest returns a valid result dict."""
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

    assert isinstance(result, dict)
    assert "symbol" in result
    assert "benchmark_symbol" in result
    assert "start" in result
    assert "end" in result
    assert "interval" in result


def test_run_backtest_includes_performance_metrics() -> None:
    """Test run_backtest includes performance metrics."""
    result = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
    )

    assert "total_return" in result
    assert "annualized_return" in result
    assert "max_drawdown" in result
    assert "sharpe_ratio" in result
    assert "win_rate" in result


def test_run_backtest_includes_strategy_info() -> None:
    """Test run_backtest includes strategy information."""
    result = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
    )

    assert "strategy" in result
    assert result["strategy"] == "sma_cross"
    assert "strategy_params" in result
    assert result["strategy_params"]["fast"] == 5
    assert result["strategy_params"]["slow"] == 20


def test_run_backtest_includes_trades() -> None:
    """Test run_backtest includes trades list."""
    result = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
    )

    assert "trades" in result
    assert isinstance(result["trades"], list)


def test_run_backtest_includes_equity_curve() -> None:
    """Test run_backtest includes equity curve."""
    result = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
    )

    assert "equity_curve" in result
    assert isinstance(result["equity_curve"], list)


def test_run_backtest_includes_cost_model() -> None:
    """Test run_backtest includes cost model."""
    result = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
    )

    assert "cost_model" in result
    assert "commission" in result["cost_model"]
    assert "slippage" in result["cost_model"]
