"""quant-report CLI entry point."""

from __future__ import annotations

import argparse

from report.generator import generate_report


def _main():
    parser = argparse.ArgumentParser(prog="quant-report", description="quant-report: generate research reports")
    parser.add_argument("--input", required=True, help="Input JSON file (backtest result)")
    parser.add_argument("--output", required=True, help="Output file path")
    parser.add_argument("--title", required=True, help="Report title")
    parser.add_argument("--format", default="html", choices=["markdown", "html", "pdf"])
    parser.add_argument("--watermark", help="Watermark text")

    args = parser.parse_args()

    generate_report(
        input_path=args.input,
        output_path=args.output,
        title=args.title,
        output_format=args.format,
        watermark=args.watermark,
    )
    print(f"Report generated: {args.output}")


if __name__ == "__main__":
    _main()
