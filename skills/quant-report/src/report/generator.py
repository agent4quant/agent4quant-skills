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
    template_dir = Path(__file__).resolve().parents[1] / "templates"
    return Environment(loader=FileSystemLoader(template_dir), autoescape=False)


def generate_report(
    input_path: str,
    output_path: str,
    title: str,
    *,
    output_format: str = "markdown",
    watermark: str | None = "Agent4Quant Research Draft",
) -> None:
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


def generate_markdown_report(input_path: str, output_path: str, title: str) -> None:
    generate_report(input_path, output_path, title, output_format="markdown")


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


def _is_backtest_payload(payload: dict) -> bool:
    required = {"symbol", "period", "metrics", "equity_curve"}
    return required.issubset(payload)


def _generate_pdf_report(payload: dict, target: Path, title: str, watermark: str | None) -> None:
    with PdfPages(target) as pdf:
        if _is_backtest_payload(payload):
            _write_backtest_pdf(pdf, payload, title, watermark)
        else:
            _write_generic_pdf(pdf, payload, title, watermark)


def _write_backtest_pdf(pdf: PdfPages, payload: dict, title: str, watermark: str | None) -> None:
    figure = plt.figure(figsize=(11.69, 8.27))
    axis = figure.add_subplot(111)
    lines = [
        f"Title: {title}",
        f"Symbol: {payload.get('symbol')}",
        f"Strategy: {payload.get('strategy')}",
        f"Benchmark: {payload.get('benchmark', {}).get('symbol')}",
        f"Period: {payload.get('period', {}).get('start')} to {payload.get('period', {}).get('end')} / {payload.get('period', {}).get('interval')}",
        f"Adjust: {payload.get('data_config', {}).get('adjust', 'none')}",
        f"Market: {payload.get('data_config', {}).get('market') or 'default'}",
        f"Watermark: {watermark or 'disabled'}",
        "Disclaimer: research and educational use only.",
    ]
    _text_block(axis, "Agent4Quant PDF Report", lines)
    pdf.savefig(figure, bbox_inches="tight")
    plt.close(figure)

    metrics_figure = plt.figure(figsize=(11.69, 8.27))
    metrics_axis = metrics_figure.add_subplot(111)
    metric_lines = [f"{key}: {value}" for key, value in payload.get("metrics", {}).items()]
    metric_lines.extend([f"param {key}: {value}" for key, value in payload.get("strategy_params", {}).items()])
    metric_lines.extend([f"cost {key}: {value}" for key, value in payload.get("cost_model", {}).items()])
    _text_block(metrics_axis, "Key Metrics", metric_lines)
    pdf.savefig(metrics_figure, bbox_inches="tight")
    plt.close(metrics_figure)

    equity = payload.get("equity_curve", [])
    if equity:
        curve = plt.figure(figsize=(11.69, 8.27))
        axis = curve.add_subplot(211)
        dates = [item["date"] for item in equity]
        axis.plot(dates, [item["equity"] for item in equity], label="strategy", color="#0f766e")
        gross = [item.get("gross_equity") for item in equity]
        if any(item is not None for item in gross):
            axis.plot(dates, gross, label="gross", color="#1d4ed8", alpha=0.65)
        benchmark = payload.get("benchmark_curve", [])
        if benchmark:
            axis.plot(
                [item["date"] for item in benchmark],
                [item["benchmark_equity"] for item in benchmark],
                label="benchmark",
                color="#f59e0b",
            )
        axis.set_title("Equity Curve")
        axis.grid(alpha=0.2)
        axis.legend()
        axis.tick_params(axis="x", labelrotation=20)

        axis2 = curve.add_subplot(212)
        axis2.plot(dates, [item["drawdown"] for item in equity], label="drawdown", color="#b91c1c")
        axis2.bar(dates, [item.get("cost_return", 0.0) for item in equity], label="cost", color="#7c3aed", alpha=0.35)
        axis2.set_title("Drawdown And Cost")
        axis2.grid(alpha=0.2)
        axis2.legend()
        axis2.tick_params(axis="x", labelrotation=20)
        curve.tight_layout()
        pdf.savefig(curve, bbox_inches="tight")
        plt.close(curve)

    trades = payload.get("trades", [])[:12]
    trade_figure = plt.figure(figsize=(11.69, 8.27))
    trade_axis = trade_figure.add_subplot(111)
    trade_lines = [
        f"{item['entry_date']} -> {item['exit_date']} | gross {item['gross_return']} | net {item['net_return']} | {item['status']}"
        for item in trades
    ] or ["No completed trades."]
    _text_block(trade_axis, "Recent Trades", trade_lines)
    pdf.savefig(trade_figure, bbox_inches="tight")
    plt.close(trade_figure)


def _write_generic_pdf(pdf: PdfPages, payload: dict, title: str, watermark: str | None) -> None:
    figure = plt.figure(figsize=(11.69, 8.27))
    axis = figure.add_subplot(111)
    lines = [f"标题: {title}", f"水印: {watermark or 'disabled'}"]
    lines.extend(json.dumps(payload, ensure_ascii=False, indent=2).splitlines())
    _text_block(axis, "Agent4Quant PDF Report", lines)
    pdf.savefig(figure, bbox_inches="tight")
    plt.close(figure)
