"""quant-backtest CLI entry point."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from backtest.engine import run_backtest


def _main():
    parser = argparse.ArgumentParser(prog="quant-backtest", description="quant-backtest: strategy backtesting")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("run", help="Run backtest")
    p.add_argument("--input", required=True, help="Input data file (CSV or JSON)")
    p.add_argument("--strategy", required=True, help="Strategy name, e.g., sma_cross")
    p.add_argument("--strategy-params", default="", help="Comma-separated key=value pairs, e.g., fast=5,slow=20")
    p.add_argument("--commission-bps", type=float, default=0.0)
    p.add_argument("--slippage-bps", type=float, default=0.0)
    p.add_argument("--stamp-duty-bps", type=float, default=0.0)
    p.add_argument("--result-json", help="Output JSON path")
    p.add_argument("--report-html", help="Output HTML report path")

    args = parser.parse_args()

    if args.command == "run":
        params = {}
        if args.strategy_params:
            for item in args.strategy_params.split(","):
                key, _, value = item.partition("=")
                if key and value:
                    params[key.strip()] = float(value.strip())

        result = run_backtest(
            data_path=args.input,
            strategy=args.strategy,
            strategy_params=params,
            commission_bps=args.commission_bps,
            slippage_bps=args.slippage_bps,
            stamp_duty_bps=args.stamp_duty_bps,
            output_json=args.result_json,
            output_html=args.report_html,
        )

        print(f"Backtest completed. Total return: {result['metrics']['total_return']:.2%}")
        if args.result_json:
            print(f"Result saved to {args.result_json}")
        if args.report_html:
            print(f"HTML report saved to {args.report_html}")


if __name__ == "__main__":
    _main()
