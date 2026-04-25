"""quant-data CLI entry point."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from data.service import fetch_dataset, list_provider_capabilities, write_dataset


def _main():
    parser = argparse.ArgumentParser(prog="quant-data", description="quant-data: online research data fetch")
    sub = parser.add_subparsers(dest="command", required=True)

    # providers
    p = sub.add_parser("providers", help="List available data providers and capabilities")

    # fetch
    f = sub.add_parser("fetch", help="Fetch dataset from provider")
    f.add_argument("--provider", required=True, choices=["demo", "csv", "akshare", "yfinance"])
    f.add_argument("--symbol", required=True)
    f.add_argument("--start", required=True)
    f.add_argument("--end", required=True)
    f.add_argument("--interval", default="1d")
    f.add_argument("--adjust", default="none")
    f.add_argument("--indicators", default="", help="Comma-separated list, e.g., ma5,rsi,macd")
    f.add_argument("--input", help="Input file for csv provider")
    f.add_argument("--output", required=True)
    f.add_argument("--format", default="json", choices=["csv", "json"])

    args = parser.parse_args()

    if args.command == "providers":
        caps = list_provider_capabilities()
        print(json.dumps(caps, indent=2, ensure_ascii=False))
        return

    if args.command == "fetch":
        indicators = [x.strip() for x in args.indicators.split(",") if x.strip()] if args.indicators else []
        frame, metadata = fetch_dataset(
            provider=args.provider,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            interval=args.interval,
            indicators=indicators,
            input_path=args.input,
            adjust=args.adjust,
        )
        write_dataset(frame, metadata, args.output, args.format)
        print(f"Data saved to {args.output}")
        return


if __name__ == "__main__":
    _main()