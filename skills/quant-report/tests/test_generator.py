from __future__ import annotations

import json
from pathlib import Path

from agent4quant.backtest.engine import run_backtest
from agent4quant.report.generator import generate_report


def test_generate_html_report(tmp_path: Path) -> None:
    """Test HTML report generation."""
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
    generate_report(str(source), str(output), "Test Report", output_format="html")

    assert output.exists()
    content = output.read_text(encoding="utf-8")
    assert "Test Report" in content
    assert "DEMO.SH" in content


def test_generate_html_report_with_watermark(tmp_path: Path) -> None:
    """Test HTML report with watermark."""
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
    generate_report(str(source), str(output), "Watermark Report", output_format="html", watermark="Internal Use")

    assert output.exists()
    content = output.read_text(encoding="utf-8")
    assert "Internal Use" in content


def test_generate_markdown_report(tmp_path: Path) -> None:
    """Test Markdown report generation."""
    result = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
    )

    source = tmp_path / "backtest.json"
    source.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    output = tmp_path / "report.md"
    generate_report(str(source), str(output), "Markdown Report", output_format="markdown")

    assert output.exists()
    content = output.read_text(encoding="utf-8")
    assert "# Markdown Report" in content
    assert "DEMO.SH" in content


def test_generate_pdf_report(tmp_path: Path) -> None:
    """Test PDF report generation."""
    result = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
    )

    source = tmp_path / "backtest.json"
    source.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    output = tmp_path / "report.pdf"
    generate_report(str(source), str(output), "PDF Report", output_format="pdf")

    assert output.exists()
    assert output.read_bytes().startswith(b"%PDF")


def test_generate_report_without_benchmark(tmp_path: Path) -> None:
    """Test report generation without benchmark data."""
    result = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
    )
    result.pop("benchmark", None)
    result.pop("benchmark_symbol", None)

    source = tmp_path / "backtest.json"
    source.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    output = tmp_path / "report.html"
    generate_report(str(source), str(output), "No Benchmark Report", output_format="html")

    assert output.exists()
    content = output.read_text(encoding="utf-8")
    assert "No Benchmark Report" in content
