from __future__ import annotations

import json
from pathlib import Path
from textwrap import wrap

from jinja2 import Environment, FileSystemLoader
import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


def _template_env() -> Environment:
    template_dir = Path(__file__).parent / "templates"
    return Environment(loader=FileSystemLoader(template_dir), autoescape=False)


def generate_report(
    input_path: str,
    output_path: str,
    title: str,
    *,
    output_format: str = "markdown",
    watermark: str | None = "Agent4Quant Research Draft",
) -> None:
    """Generate report from backtest result JSON."""
    source = Path(input_path)
    payload = json.loads(source.read_text(encoding="utf-8"))

    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    if output_format == "pdf":
        _generate_pdf_report(payload, target, title, watermark)
        return

    template_name = "research_report.j2" if output_format == "markdown" else "research_report_html.j2"
    template = _template_env().get_template(template_name)
    rendered = template.render(title=title, result=payload, watermark=watermark)
    target.write_text(rendered, encoding="utf-8")


def _text_block(ax, title: str, lines: list[str]) -> None:
    ax.axis("off")
    ax.set_title(title, loc="left", fontsize=16, fontweight="bold")
    y = 0.95
    for line in lines:
        wrapped = wrap(line, width=84) or [""]
        for item in wrapped:
            ax.text(0.02, y, item, fontsize=10, va="top", ha="left", transform=ax.transAxes)
            y -= 0.045
            if y < 0.06:
                return


def _generate_pdf_report(payload: dict, target: Path, title: str, watermark: str | None) -> None:
    with PdfPages(target) as pdf:
        figure = plt.figure(figsize=(11.69, 8.27))
        axis = figure.add_subplot(111)
        lines = [
            f"Title: {title}",
            f"Symbol: {payload.get('symbol')}",
            f"Strategy: {payload.get('strategy')}",
            f"Period: {payload.get('period', {}).get('start')} to {payload.get('period', {}).get('end')}",
            f"Watermark: {watermark or 'disabled'}",
            "Disclaimer: research and educational use only.",
        ]
        _text_block(axis, "Agent4Quant PDF Report", lines)
        pdf.savefig(figure, bbox_inches="tight")
        plt.close(figure)

        metrics_figure = plt.figure(figsize=(11.69, 8.27))
        metrics_axis = metrics_figure.add_subplot(111)
        metric_lines = [f"{key}: {value}" for key, value in payload.get("metrics", {}).items()]
        _text_block(metrics_axis, "Key Metrics", metric_lines)
        pdf.savefig(metrics_figure, bbox_inches="tight")
        plt.close(metrics_figure)
