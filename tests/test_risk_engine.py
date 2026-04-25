from __future__ import annotations

import json
from pathlib import Path

from agent4quant.backtest.engine import run_backtest, write_backtest_result
from agent4quant.risk.engine import analyze_risk, write_risk_html


def test_risk_analyze_market_mode() -> None:
    result = analyze_risk(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        confidence_level=0.95,
        mode="market",
        adjust="hfq",
        market="cn",
        rolling_window=10,
        stress_shocks=[-0.03, -0.07],
    )

    assert result["mode"] == "market"
    assert result["source"] == "asset"
    assert "value_at_risk" in result["metrics"]
    assert "conditional_var" in result["metrics"]
    assert "max_drawdown" in result["metrics"]
    assert "annualized_volatility" in result["metrics"]
    assert "sortino_ratio" in result["metrics"]
    assert "calmar_ratio" in result["metrics"]
    assert "skewness" in result["metrics"]
    assert "kurtosis" in result["metrics"]
    assert len(result["rolling_var"]) > 0
    assert len(result["rolling_volatility"]) > 0
    assert len(result["drawdown_series"]) > 0
    assert result["stress_results"][0]["shock"] == -0.03
    assert result["data_config"]["adjust"] == "hfq"
    assert result["data_config"]["market"] == "cn"


def test_risk_analyze_backtest_mode_and_html(tmp_path: Path) -> None:
    backtest = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        benchmark_symbol="BMK.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
    )
    backtest_json = tmp_path / "backtest.json"
    write_backtest_result(backtest, str(backtest_json))

    result = analyze_risk(
        input_path=str(backtest_json),
        mode="backtest",
        source="strategy",
        confidence_level=0.95,
    )
    html_path = tmp_path / "risk.html"
    write_risk_html(result, str(html_path))

    assert result["mode"] == "backtest"
    assert result["strategy"] == "sma_cross"
    assert result["benchmark_symbol"] == "BMK.SH"
    assert json.loads((tmp_path / "backtest.json").read_text(encoding="utf-8"))["symbol"] == "DEMO.SH"
    assert "风险分析" in html_path.read_text(encoding="utf-8")
    assert "Rolling VaR" in html_path.read_text(encoding="utf-8")
    assert "Rolling Volatility" in html_path.read_text(encoding="utf-8")
    assert "Drawdown Series" in html_path.read_text(encoding="utf-8")
