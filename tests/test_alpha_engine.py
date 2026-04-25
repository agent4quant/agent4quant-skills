from __future__ import annotations

from pathlib import Path

from agent4quant.alpha.engine import analyze_alpha, write_alpha_html


def test_alpha_analyze_outputs_factor_table() -> None:
    result = analyze_alpha(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        factors=["ma_5", "rsi_14"],
        indicators=["ma5", "rsi"],
        horizon=1,
        ic_window=10,
        quantiles=4,
        adjust="qfq",
        market="cn",
    )

    assert result["best_factor"]
    assert len(result["results"]) == 3
    assert "ic" in result["results"][0]
    assert "rank_ic" in result["results"][0]
    assert "ir" in result["results"][0]
    assert result["composite_factor"] == "composite_factor"
    assert len(result["quantile_returns"]) == 3
    assert result["config"]["adjust"] == "qfq"
    assert result["config"]["market"] == "cn"


def test_alpha_write_html_report(tmp_path: Path) -> None:
    result = analyze_alpha(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        factors=["ma_5"],
        indicators=["ma5"],
        horizon=1,
        ic_window=10,
    )
    output = tmp_path / "alpha.html"
    write_alpha_html(result, str(output))

    content = output.read_text(encoding="utf-8")
    assert "因子分析" in content
    assert "ma_5" in content
    assert "分层收益" in content


def test_alpha_analyze_supports_extended_indicator_library() -> None:
    result = analyze_alpha(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        factors=["ret_5", "volatility_10", "obv"],
        indicators=["ret5", "volatility10", "obv"],
        horizon=1,
        ic_window=10,
        quantiles=4,
    )

    factors = {item["factor"] for item in result["results"]}
    assert "ret_5" in factors
    assert "volatility_10" in factors
    assert "obv" in factors
