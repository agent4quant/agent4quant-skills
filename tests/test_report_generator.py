from __future__ import annotations

import json
from pathlib import Path

from agent4quant.backtest.engine import run_backtest
from agent4quant.report.generator import generate_report


def test_generate_html_report_with_watermark(tmp_path: Path) -> None:
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
    source = tmp_path / "backtest.json"
    source.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    output = tmp_path / "report.html"
    generate_report(str(source), str(output), "Demo Report", output_format="html", watermark="Internal Only")

    content = output.read_text(encoding="utf-8")
    assert "Internal Only" in content
    assert "Benchmark BMK.SH" in content
    assert "Demo Report" in content


def test_generate_pdf_report(tmp_path: Path) -> None:
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
    source = tmp_path / "backtest.json"
    source.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    output = tmp_path / "report.pdf"
    generate_report(str(source), str(output), "Demo PDF Report", output_format="pdf", watermark="Internal PDF")

    assert output.exists()
    assert output.read_bytes().startswith(b"%PDF")


def test_generate_html_report_without_optional_backtest_sections(tmp_path: Path) -> None:
    payload = json.loads(Path("output/demo-backtest.json").read_text(encoding="utf-8"))
    payload.pop("benchmark", None)
    payload.pop("trades", None)
    payload.pop("cost_model", None)

    source = tmp_path / "backtest.json"
    source.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    output = tmp_path / "report.html"
    generate_report(str(source), str(output), "Demo Report Without Benchmark", output_format="html", watermark="Internal Only")

    content = output.read_text(encoding="utf-8")
    assert "Benchmark -" in content or "Benchmark -" in content.replace(" / ", " ")
    assert "无完整交易记录" in content
    assert "当前输入未提供成本模型或策略参数" not in content
    assert "fast" in content
    assert "slow" in content
