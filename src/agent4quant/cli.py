from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path

from agent4quant.alpha.engine import analyze_alpha, write_alpha_html, write_alpha_result
from agent4quant.backtest.engine import (
    compare_backtests,
    run_backtest,
    sweep_backtest,
    write_backtest_html,
    write_compare_html,
    write_compare_result,
    write_backtest_result,
    write_sweep_html,
    write_sweep_result,
)
from agent4quant.compliance import DISCLAIMER
from agent4quant.commercial import (
    create_account,
    create_lead,
    create_subscription,
    list_accounts,
    list_plans,
    list_subscriptions,
)
from agent4quant.data.service import (
    available_symbols,
    batch_fetch_datasets,
    build_data_manifest,
    build_local_data_index,
    fetch_dataset,
    list_provider_capabilities,
    repair_dataset,
    validate_dataset,
    write_local_directory_metadata,
    write_dataset,
)
from agent4quant.data.sync import sync_5m_batch_to_duckdb, sync_5m_to_duckdb
from agent4quant.data.sync import import_5m_directory_to_duckdb, import_5m_file_to_duckdb
from agent4quant.errors import DependencyUnavailableError, ExternalProviderError
from agent4quant.report.generator import generate_report
from agent4quant.risk.engine import analyze_risk, write_risk_html, write_risk_result


def _parse_key_values(raw: str) -> dict[str, float]:
    if not raw:
        return {}

    result: dict[str, float] = {}
    for item in raw.split(","):
        key, _, value = item.partition("=")
        if not key or not value:
            raise ValueError(f"Invalid strategy parameter: {item}")
        result[key.strip()] = float(value.strip())
    return result


def _parse_grid(items: list[str]) -> dict[str, list[float]]:
    grid: dict[str, list[float]] = {}
    for item in items:
        key, _, raw_values = item.partition("=")
        if not key or not raw_values:
            raise ValueError(f"Invalid sweep parameter: {item}")
        values = [float(value.strip()) for value in raw_values.split("|") if value.strip()]
        if not values:
            raise ValueError(f"Sweep parameter must provide values: {item}")
        grid[key.strip()] = values
    return grid


def _parse_strategy_param_overrides(items: list[str]) -> dict[str, dict[str, float]]:
    overrides: dict[str, dict[str, float]] = {}
    for item in items:
        strategy, sep, raw_params = item.partition(":")
        if not strategy or not sep or not raw_params:
            raise ValueError(f"Invalid strategy override: {item}")
        overrides[strategy.strip()] = _parse_key_values(raw_params)
    return overrides


def _parse_result_filters(items: list[str]) -> list[dict[str, float | str]]:
    parsed = []
    pattern = re.compile(r"^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(>=|<=|==|!=|>|<)\s*(-?\d+(?:\.\d+)?)\s*$")
    for item in items:
        match = pattern.match(item)
        if not match:
            raise ValueError(f"Invalid filter expression: {item}")
        metric, operator_symbol, value = match.groups()
        parsed.append(
            {
                "metric": metric,
                "operator": operator_symbol,
                "value": float(value),
                "expression": item,
            }
        )
    return parsed


def _cost_args(args: argparse.Namespace) -> dict[str, float]:
    return {
        "commission_bps": float(getattr(args, "commission_bps", 0.0)),
        "slippage_bps": float(getattr(args, "slippage_bps", 0.0)),
        "stamp_duty_bps": float(getattr(args, "stamp_duty_bps", 0.0)),
    }


def _parse_trade_date(value: str | None) -> date:
    if value:
        return date.fromisoformat(value)
    return date.today() - timedelta(days=1)


def _parse_symbols(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _print_disclaimer() -> None:
    print(DISCLAIMER, file=sys.stderr)


def _format_cli_error(exc: Exception) -> str:
    if isinstance(exc, ExternalProviderError):
        return f"error[{exc.provider}/{exc.category}]: {exc}"
    if isinstance(exc, DependencyUnavailableError):
        return f"error[{exc.component}/dependency]: {exc}"
    return f"error: {exc}"


def _data_options(parser: argparse.ArgumentParser, *, include_adjust: bool = False) -> None:
    parser.add_argument("--market", help="Optional market segment for local layered directories, e.g. cn/us/hk")
    parser.add_argument("--provider-profile", help="Optional external provider profile name from TOML config")
    if include_adjust:
        parser.add_argument("--adjust", choices=["none", "qfq", "hfq"], default="none")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="a4q", description="Agent4Quant CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    data_parser = subparsers.add_parser("data", help="quant-data services")
    data_subparsers = data_parser.add_subparsers(dest="data_command", required=True)
    fetch_parser = data_subparsers.add_parser("fetch", help="Fetch dataset")
    fetch_parser.add_argument("--provider", choices=["demo", "csv", "local", "duckdb", "akshare", "yfinance"], default="demo")
    fetch_parser.add_argument("--symbol", required=True)
    fetch_parser.add_argument("--start", required=True)
    fetch_parser.add_argument("--end", required=True)
    fetch_parser.add_argument("--interval", choices=["1d", "5m"], default="1d")
    fetch_parser.add_argument("--input", help="Input CSV path when provider=csv")
    fetch_parser.add_argument("--data-root", help="Local market data root when provider=local")
    fetch_parser.add_argument("--db-path", help="DuckDB file path when provider=duckdb")
    _data_options(fetch_parser, include_adjust=True)
    fetch_parser.add_argument("--indicators", default="ma5,ma20,macd")
    fetch_parser.add_argument("--format", choices=["csv", "json"], default="csv")
    fetch_parser.add_argument("--output", required=True)
    batch_fetch_parser = data_subparsers.add_parser("batch-fetch", help="Fetch datasets for multiple symbols")
    batch_fetch_parser.add_argument("--provider", choices=["local", "duckdb", "demo", "akshare", "yfinance"], default="local")
    batch_fetch_parser.add_argument("--symbols", help="Comma-separated symbols. If omitted with local provider, all symbols are used.")
    batch_fetch_parser.add_argument("--start", required=True)
    batch_fetch_parser.add_argument("--end", required=True)
    batch_fetch_parser.add_argument("--interval", choices=["1d", "5m"], default="1d")
    batch_fetch_parser.add_argument("--data-root", help="Local market data root when provider=local")
    batch_fetch_parser.add_argument("--db-path", help="DuckDB file path when provider=duckdb")
    _data_options(batch_fetch_parser, include_adjust=True)
    batch_fetch_parser.add_argument("--indicators", default="ma5,ma20,macd")
    batch_fetch_parser.add_argument("--format", choices=["csv", "json"], default="csv")
    batch_fetch_parser.add_argument("--output-dir", required=True)
    providers_parser = data_subparsers.add_parser("providers", help="Show provider capability matrix")
    providers_parser.add_argument("--format", choices=["text", "json"], default="text")
    symbols_parser = data_subparsers.add_parser("symbols", help="List available symbols")
    symbols_parser.add_argument("--provider", choices=["local", "duckdb"], default="local")
    symbols_parser.add_argument("--interval", choices=["1d", "5m"], default="5m")
    symbols_parser.add_argument("--data-root", help="Local market data root")
    symbols_parser.add_argument("--db-path", help="DuckDB file path when provider=duckdb")
    _data_options(symbols_parser)
    symbols_parser.add_argument("--format", choices=["text", "json"], default="text")
    manifest_parser = data_subparsers.add_parser("manifest", help="Build manifest for local directory or DuckDB store")
    manifest_parser.add_argument("--provider", choices=["local", "duckdb"], required=True)
    manifest_parser.add_argument("--interval", choices=["1d", "5m"], default="5m")
    manifest_parser.add_argument("--data-root", help="Local market data root when provider=local")
    manifest_parser.add_argument("--db-path", help="DuckDB file path when provider=duckdb")
    _data_options(manifest_parser)
    manifest_parser.add_argument("--format", choices=["text", "json"], default="text")
    index_parser = data_subparsers.add_parser("index", help="Build local market data cache index")
    index_parser.add_argument("--data-root", help="Local market data root")
    index_parser.add_argument("--interval", choices=["1d", "5m"])
    index_parser.add_argument("--output", help="Optional index output path")
    _data_options(index_parser)
    index_parser.add_argument("--format", choices=["text", "json"], default="text")
    metadata_parser = data_subparsers.add_parser("metadata", help="Write directory-level metadata for local market data")
    metadata_parser.add_argument("--data-root", help="Local market data root")
    metadata_parser.add_argument("--interval", choices=["1d", "5m"])
    metadata_parser.add_argument("--output", help="Optional metadata output path")
    _data_options(metadata_parser)
    metadata_parser.add_argument("--format", choices=["text", "json"], default="text")
    validate_parser = data_subparsers.add_parser("validate", help="Validate market data")
    validate_parser.add_argument("--provider", choices=["csv", "local"], required=True)
    validate_parser.add_argument("--input", help="Input CSV/Parquet path when provider=csv")
    validate_parser.add_argument("--data-root", help="Local market data root when provider=local")
    validate_parser.add_argument("--interval", choices=["1d", "5m"], default="5m")
    validate_parser.add_argument("--symbol", help="Validate a single symbol when provider=local")
    _data_options(validate_parser)
    validate_parser.add_argument("--format", choices=["text", "json"], default="text")
    repair_parser = data_subparsers.add_parser("repair", help="Repair market data file with deterministic cleanup rules")
    repair_parser.add_argument("--provider", choices=["csv", "local"], required=True)
    repair_parser.add_argument("--input", help="Input CSV/Parquet path when provider=csv")
    repair_parser.add_argument("--data-root", help="Local market data root when provider=local")
    repair_parser.add_argument("--interval", choices=["1d", "5m"], default="5m")
    repair_parser.add_argument("--symbol", help="Required when provider=local; optional override when provider=csv")
    _data_options(repair_parser)
    repair_parser.add_argument("--duplicate-policy", choices=["first", "last", "error"], default="last")
    repair_parser.add_argument("--null-policy", choices=["drop", "error"], default="drop")
    repair_parser.add_argument("--invalid-price-policy", choices=["drop", "error"], default="drop")
    repair_parser.add_argument("--output", required=True)
    repair_parser.add_argument("--format", choices=["text", "json"], default="text")
    sync_parser = data_subparsers.add_parser("sync-5m", help="Redownload 5m data for a trade date into DuckDB")
    sync_parser.add_argument("--provider", choices=["auto", "akshare", "eastmoney"], default="auto")
    sync_parser.add_argument("--symbol", help="Single A-share symbol, e.g. 000001.SZ")
    sync_parser.add_argument("--symbols", help="Comma-separated A-share symbols for batch sync.")
    sync_parser.add_argument("--trade-date", help="Trade date in YYYY-MM-DD format. Defaults to yesterday.")
    sync_parser.add_argument("--db-path", required=True, help="DuckDB file path")
    sync_parser.add_argument("--format", choices=["text", "json"], default="text")
    import_parser = data_subparsers.add_parser("import-5m", help="Import offline 5m csv/parquet data into DuckDB")
    import_parser.add_argument("--input", help="Single 5m csv/parquet file")
    import_parser.add_argument("--input-dir", help="Directory containing 5m csv/parquet files")
    import_parser.add_argument("--symbol", help="Required when single file has no symbol column or canonical file name")
    import_parser.add_argument("--trade-date", help="Optional trade date override in YYYY-MM-DD")
    import_parser.add_argument("--source", default="file.import.5m", help="Source tag stored into DuckDB sync log")
    import_parser.add_argument("--db-path", required=True, help="DuckDB file path")
    import_parser.add_argument("--format", choices=["text", "json"], default="text")

    backtest_parser = subparsers.add_parser("backtest", help="quant-backtest services")
    backtest_subparsers = backtest_parser.add_subparsers(dest="backtest_command", required=True)
    run_parser = backtest_subparsers.add_parser("run", help="Run a strategy backtest")
    run_parser.add_argument("--provider", choices=["demo", "csv", "local", "duckdb", "akshare", "yfinance"], default="demo")
    run_parser.add_argument("--input", help="Input CSV path when provider=csv")
    run_parser.add_argument("--data-root", help="Local market data root when provider=local")
    run_parser.add_argument("--db-path", help="DuckDB file path when provider=duckdb")
    _data_options(run_parser, include_adjust=True)
    run_parser.add_argument("--symbol", default="DEMO.SH")
    run_parser.add_argument("--benchmark-symbol", help="Optional benchmark symbol. Defaults to the same symbol.")
    run_parser.add_argument("--start", default="2025-01-01")
    run_parser.add_argument("--end", default="2025-03-31")
    run_parser.add_argument("--interval", choices=["1d", "5m"], default="1d")
    run_parser.add_argument("--strategy", choices=["sma_cross", "macd", "boll_breakout"], default="sma_cross")
    run_parser.add_argument("--strategy-params", default="")
    run_parser.add_argument("--commission-bps", type=float, default=0.0)
    run_parser.add_argument("--slippage-bps", type=float, default=0.0)
    run_parser.add_argument("--stamp-duty-bps", type=float, default=0.0)
    run_parser.add_argument("--result-json", required=True)
    run_parser.add_argument("--report-html")
    sweep_parser = backtest_subparsers.add_parser("sweep", help="Run parameter sweep")
    sweep_parser.add_argument("--provider", choices=["demo", "csv", "local", "duckdb", "akshare", "yfinance"], default="demo")
    sweep_parser.add_argument("--input", help="Input CSV path when provider=csv")
    sweep_parser.add_argument("--data-root", help="Local market data root when provider=local")
    sweep_parser.add_argument("--db-path", help="DuckDB file path when provider=duckdb")
    _data_options(sweep_parser, include_adjust=True)
    sweep_parser.add_argument("--symbol", default="DEMO.SH")
    sweep_parser.add_argument("--benchmark-symbol", help="Optional benchmark symbol. Defaults to the same symbol.")
    sweep_parser.add_argument("--start", default="2025-01-01")
    sweep_parser.add_argument("--end", default="2025-03-31")
    sweep_parser.add_argument("--interval", choices=["1d", "5m"], default="1d")
    sweep_parser.add_argument("--strategy", choices=["sma_cross", "macd", "boll_breakout"], default="sma_cross")
    sweep_parser.add_argument("--param", action="append", default=[], help="Parameter grid, e.g. fast=5|10|20")
    sweep_parser.add_argument("--metric", default="sharpe")
    sweep_parser.add_argument("--filter", action="append", default=[], help="Result filter, e.g. sharpe>=1.0")
    sweep_parser.add_argument("--commission-bps", type=float, default=0.0)
    sweep_parser.add_argument("--slippage-bps", type=float, default=0.0)
    sweep_parser.add_argument("--stamp-duty-bps", type=float, default=0.0)
    sweep_parser.add_argument("--top-n", type=int, help="Only keep the top N ranked experiments in output.")
    sweep_parser.add_argument("--result-json", required=True)
    sweep_parser.add_argument("--result-csv")
    sweep_parser.add_argument("--report-html")
    compare_parser = backtest_subparsers.add_parser("compare", help="Compare multiple strategies")
    compare_parser.add_argument("--provider", choices=["demo", "csv", "local", "duckdb", "akshare", "yfinance"], default="demo")
    compare_parser.add_argument("--input", help="Input CSV path when provider=csv")
    compare_parser.add_argument("--data-root", help="Local market data root when provider=local")
    compare_parser.add_argument("--db-path", help="DuckDB file path when provider=duckdb")
    _data_options(compare_parser, include_adjust=True)
    compare_parser.add_argument("--symbol", default="DEMO.SH")
    compare_parser.add_argument("--benchmark-symbol", help="Optional benchmark symbol. Defaults to the same symbol.")
    compare_parser.add_argument("--start", default="2025-01-01")
    compare_parser.add_argument("--end", default="2025-03-31")
    compare_parser.add_argument("--interval", choices=["1d", "5m"], default="1d")
    compare_parser.add_argument(
        "--strategy",
        action="append",
        default=[],
        choices=["sma_cross", "macd", "boll_breakout"],
        help="Repeat this flag to compare multiple strategies.",
    )
    compare_parser.add_argument(
        "--strategy-param",
        action="append",
        default=[],
        help="Per-strategy params, e.g. sma_cross:fast=5,slow=20",
    )
    compare_parser.add_argument("--metric", default="sharpe")
    compare_parser.add_argument("--filter", action="append", default=[], help="Result filter, e.g. sharpe>=1.0")
    compare_parser.add_argument("--commission-bps", type=float, default=0.0)
    compare_parser.add_argument("--slippage-bps", type=float, default=0.0)
    compare_parser.add_argument("--stamp-duty-bps", type=float, default=0.0)
    compare_parser.add_argument("--top-n", type=int, help="Only keep the top N ranked strategies in output.")
    compare_parser.add_argument("--result-json", required=True)
    compare_parser.add_argument("--result-csv")
    compare_parser.add_argument("--report-html")

    report_parser = subparsers.add_parser("report", help="quant-report services")
    report_subparsers = report_parser.add_subparsers(dest="report_command", required=True)
    generate_parser = report_subparsers.add_parser("generate", help="Generate markdown report")
    generate_parser.add_argument("--input", required=True)
    generate_parser.add_argument("--output", required=True)
    generate_parser.add_argument("--title", default="Quant Research Report")
    generate_parser.add_argument("--format", choices=["markdown", "html", "pdf"], default="markdown")
    generate_parser.add_argument("--watermark", default="Agent4Quant Research Draft")
    generate_parser.add_argument("--no-watermark", action="store_true")

    alpha_parser = subparsers.add_parser("alpha", help="quant-alpha services")
    alpha_subparsers = alpha_parser.add_subparsers(dest="alpha_command", required=True)
    alpha_analyze_parser = alpha_subparsers.add_parser("analyze", help="Analyze factor IC/IR")
    alpha_analyze_parser.add_argument("--provider", choices=["demo", "csv", "local", "duckdb", "akshare", "yfinance"], default="demo")
    alpha_analyze_parser.add_argument("--symbol", required=True)
    alpha_analyze_parser.add_argument("--start", required=True)
    alpha_analyze_parser.add_argument("--end", required=True)
    alpha_analyze_parser.add_argument("--interval", choices=["1d", "5m"], default="1d")
    alpha_analyze_parser.add_argument("--input", help="Input CSV path when provider=csv")
    alpha_analyze_parser.add_argument("--data-root", help="Local market data root when provider=local")
    alpha_analyze_parser.add_argument("--db-path", help="DuckDB file path when provider=duckdb")
    _data_options(alpha_analyze_parser, include_adjust=True)
    alpha_analyze_parser.add_argument("--factor", action="append", default=[], help="Factor column name, repeatable")
    alpha_analyze_parser.add_argument("--indicators", default="", help="Indicators to precompute, comma-separated")
    alpha_analyze_parser.add_argument("--horizon", type=int, default=1)
    alpha_analyze_parser.add_argument("--ic-window", type=int, default=20)
    alpha_analyze_parser.add_argument("--quantiles", type=int, default=5)
    alpha_analyze_parser.add_argument("--no-composite", action="store_true")
    alpha_analyze_parser.add_argument("--result-json", required=True)
    alpha_analyze_parser.add_argument("--report-html")

    risk_parser = subparsers.add_parser("risk", help="quant-risk services")
    risk_subparsers = risk_parser.add_subparsers(dest="risk_command", required=True)
    analyze_parser = risk_subparsers.add_parser("analyze", help="Analyze asset or strategy risk")
    analyze_parser.add_argument("--mode", choices=["market", "backtest"], default="market")
    analyze_parser.add_argument("--provider", choices=["demo", "csv", "local", "duckdb", "akshare", "yfinance"])
    analyze_parser.add_argument("--symbol")
    analyze_parser.add_argument("--start")
    analyze_parser.add_argument("--end")
    analyze_parser.add_argument("--interval", choices=["1d", "5m"], default="1d")
    analyze_parser.add_argument("--input", help="CSV path for provider=csv or backtest JSON when mode=backtest")
    analyze_parser.add_argument("--data-root", help="Local market data root when provider=local")
    analyze_parser.add_argument("--db-path", help="DuckDB file path when provider=duckdb")
    _data_options(analyze_parser, include_adjust=True)
    analyze_parser.add_argument("--source", choices=["asset", "strategy", "gross_strategy"], default="asset")
    analyze_parser.add_argument("--confidence-level", type=float, default=0.95)
    analyze_parser.add_argument("--rolling-window", type=int, default=20)
    analyze_parser.add_argument("--stress-shock", action="append", default=[], help="Repeatable shock values, e.g. -0.02")
    analyze_parser.add_argument("--result-json", required=True)
    analyze_parser.add_argument("--report-html")

    commercial_parser = subparsers.add_parser("commercial", help="commercialization and conversion services")
    commercial_subparsers = commercial_parser.add_subparsers(dest="commercial_command", required=True)
    commercial_plans_parser = commercial_subparsers.add_parser("plans", help="List plan definitions")
    commercial_plans_parser.add_argument("--format", choices=["text", "json"], default="text")

    commercial_lead_parser = commercial_subparsers.add_parser("lead", help="Create a lead record")
    commercial_lead_parser.add_argument("--name", required=True)
    commercial_lead_parser.add_argument("--email", required=True)
    commercial_lead_parser.add_argument("--use-case", required=True)
    commercial_lead_parser.add_argument("--plan", choices=["community", "professional", "research_suite"], required=True)
    commercial_lead_parser.add_argument("--company")
    commercial_lead_parser.add_argument("--notes")
    commercial_lead_parser.add_argument("--source", default="cli")
    commercial_lead_parser.add_argument("--format", choices=["text", "json"], default="text")

    commercial_account_parser = commercial_subparsers.add_parser("account-create", help="Create an account record")
    commercial_account_parser.add_argument("--name", required=True)
    commercial_account_parser.add_argument("--email", required=True)
    commercial_account_parser.add_argument("--plan", choices=["community", "professional", "research_suite"], required=True)
    commercial_account_parser.add_argument("--company")
    commercial_account_parser.add_argument("--format", choices=["text", "json"], default="text")

    commercial_accounts_parser = commercial_subparsers.add_parser("accounts", help="List account records")
    commercial_accounts_parser.add_argument("--limit", type=int, default=20)
    commercial_accounts_parser.add_argument("--format", choices=["text", "json"], default="text")

    commercial_subscription_parser = commercial_subparsers.add_parser("subscription-create", help="Create a subscription record")
    commercial_subscription_parser.add_argument("--account-id", required=True)
    commercial_subscription_parser.add_argument("--plan", choices=["community", "professional", "research_suite"], required=True)
    commercial_subscription_parser.add_argument("--billing-cycle", default="monthly")
    commercial_subscription_parser.add_argument("--provider", default="manual")
    commercial_subscription_parser.add_argument("--format", choices=["text", "json"], default="text")

    commercial_subscriptions_parser = commercial_subparsers.add_parser("subscriptions", help="List subscription records")
    commercial_subscriptions_parser.add_argument("--limit", type=int, default=20)
    commercial_subscriptions_parser.add_argument("--format", choices=["text", "json"], default="text")

    return parser


def _dispatch(args: argparse.Namespace) -> None:
    if args.command == "data" and args.data_command == "fetch":
        indicators = [item.strip() for item in args.indicators.split(",") if item.strip()]
        frame, metadata = fetch_dataset(
            provider=args.provider,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            interval=args.interval,
            indicators=indicators,
            input_path=args.input,
            data_root=args.data_root,
            db_path=args.db_path,
            adjust=args.adjust,
            market=args.market,
            provider_profile=args.provider_profile,
        )
        write_dataset(frame, metadata, args.output, args.format)
        print(f"Dataset written to {Path(args.output)}")
        _print_disclaimer()
        return

    if args.command == "data" and args.data_command == "symbols":
        symbols = available_symbols(
            provider=args.provider,
            interval=args.interval,
            data_root=args.data_root,
            db_path=args.db_path,
            market=args.market,
            provider_profile=args.provider_profile,
        )
        if args.format == "json":
            print(json.dumps({"symbols": symbols}, ensure_ascii=False, indent=2))
        else:
            for symbol in symbols:
                print(symbol)
        return

    if args.command == "data" and args.data_command == "providers":
        payload = list_provider_capabilities()
        if args.format == "json":
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print("Primary online providers:")
            for provider in payload["providers"]:
                if provider["role"] != "primary_online":
                    continue
                print(
                    f"- {provider['provider']}: markets={','.join(provider['markets'])} "
                    f"intervals={','.join(provider['intervals'])} "
                    f"adjust={','.join(provider['adjust_modes'])}"
                )
            print("Compatibility providers:")
            for provider in payload["providers"]:
                if provider["role"] == "primary_online":
                    continue
                print(
                    f"- {provider['provider']}: role={provider['role']} "
                    f"intervals={','.join(provider['intervals'])}"
                )
            if payload["configured_external_profiles"]:
                print("Configured external profiles:")
                for profile in payload["configured_external_profiles"]:
                    location = profile.get("data_root") or profile.get("db_path") or "-"
                    default_suffix = " [default]" if profile.get("default") else ""
                    print(
                        f"- {profile['provider']}/{profile['profile']}{default_suffix}: "
                        f"market={profile.get('market') or '-'} path={location}"
                    )
        return

    if args.command == "data" and args.data_command == "manifest":
        manifest = build_data_manifest(
            provider=args.provider,
            interval=args.interval,
            data_root=args.data_root,
            db_path=args.db_path,
            market=args.market,
            provider_profile=args.provider_profile,
        )
        if args.format == "json":
            print(json.dumps(manifest, ensure_ascii=False, indent=2))
        else:
            print(
                f"{manifest['provider']} manifest interval={manifest['interval']} "
                f"symbols={manifest['symbols']} last_updated_at={manifest.get('last_updated_at')}"
            )
        _print_disclaimer()
        return

    if args.command == "data" and args.data_command == "index":
        result = build_local_data_index(
            data_root=args.data_root,
            interval=args.interval,
            output_path=args.output,
            market=args.market,
            provider_profile=args.provider_profile,
        )
        if args.format == "json":
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"Index written to {result['path']} (symbols={result['symbols']}, intervals={','.join(result['intervals'])})")
        _print_disclaimer()
        return

    if args.command == "data" and args.data_command == "metadata":
        result = write_local_directory_metadata(
            data_root=args.data_root,
            interval=args.interval,
            market=args.market,
            output_path=args.output,
            provider_profile=args.provider_profile,
        )
        if args.format == "json":
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"Metadata written to {result['path']} (entries={result['entries']}, markets={','.join(result['markets'])})")
        _print_disclaimer()
        return

    if args.command == "data" and args.data_command == "validate":
        result = validate_dataset(
            provider=args.provider,
            interval=args.interval,
            input_path=args.input,
            data_root=args.data_root,
            symbol=args.symbol,
            market=args.market,
            provider_profile=args.provider_profile,
        )
        if args.format == "json":
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            if "results" in result:
                print(
                    f"Validated {result['symbols']} symbols: "
                    f"{result['valid_symbols']} valid, {result['invalid_symbols']} invalid"
                )
                for item in result["results"]:
                    status = "OK" if item["valid"] else "FAIL"
                    print(f"[{status}] {item['symbol']} -> {item['path']}")
                    for issue in item["issues"]:
                        print(f"  issue: {issue}")
                    for warning in item["warnings"]:
                        print(f"  warning: {warning}")
            else:
                status = "OK" if result["valid"] else "FAIL"
                print(f"[{status}] {result['symbol']} -> {result['path']}")
                for issue in result["issues"]:
                    print(f"issue: {issue}")
                for warning in result["warnings"]:
                    print(f"warning: {warning}")
        _print_disclaimer()
        return

    if args.command == "data" and args.data_command == "repair":
        result = repair_dataset(
            provider=args.provider,
            interval=args.interval,
            input_path=args.input,
            data_root=args.data_root,
            symbol=args.symbol,
            market=args.market,
            provider_profile=args.provider_profile,
            duplicate_policy=args.duplicate_policy,
            null_policy=args.null_policy,
            invalid_price_policy=args.invalid_price_policy,
            output_path=args.output,
        )
        if args.format == "json":
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(
                f"Repaired {result['symbol']} -> {result['output']} "
                f"(input_rows={result['input_rows']}, output_rows={result['output_rows']}, dropped_rows={result['dropped_rows']})"
            )
            for warning in result["warnings"]:
                print(f"warning: {warning}")
        _print_disclaimer()
        return

    if args.command == "data" and args.data_command == "batch-fetch":
        indicators = [item.strip() for item in args.indicators.split(",") if item.strip()]
        symbols = (
            [item.strip() for item in args.symbols.split(",") if item.strip()]
            if args.symbols
            else available_symbols(
                provider=args.provider,
                interval=args.interval,
                data_root=args.data_root,
                db_path=args.db_path,
                market=args.market,
                provider_profile=args.provider_profile,
            )
        )
        result = batch_fetch_datasets(
            provider=args.provider,
            symbols=symbols,
            start=args.start,
            end=args.end,
            interval=args.interval,
            indicators=indicators,
            output_dir=args.output_dir,
            output_format=args.format,
            data_root=args.data_root,
            db_path=args.db_path,
            adjust=args.adjust,
            market=args.market,
            provider_profile=args.provider_profile,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        _print_disclaimer()
        return

    if args.command == "data" and args.data_command == "sync-5m":
        symbols = _parse_symbols(args.symbols)
        if args.symbol:
            symbols = [args.symbol, *symbols]
        if not symbols:
            raise ValueError("sync-5m requires --symbol or --symbols.")
        trade_date = _parse_trade_date(args.trade_date)
        if len(symbols) == 1:
            result = sync_5m_to_duckdb(
                symbol=symbols[0],
                trade_date=trade_date,
                db_path=args.db_path,
                provider=args.provider,
            )
            payload = {
                "symbol": result.symbol,
                "trade_date": result.trade_date,
                "rows": result.rows,
                "db_path": result.db_path,
                "source": result.source,
            }
        else:
            batch = sync_5m_batch_to_duckdb(
                symbols=symbols,
                trade_date=trade_date,
                db_path=args.db_path,
                provider=args.provider,
            )
            payload = {
                "trade_date": batch.trade_date,
                "db_path": batch.db_path,
                "symbols": len(batch.results),
                "results": [
                    {
                        "symbol": item.symbol,
                        "trade_date": item.trade_date,
                        "rows": item.rows,
                        "db_path": item.db_path,
                        "source": item.source,
                    }
                    for item in batch.results
                ],
            }
        if args.format == "json":
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            if "symbol" in payload:
                print(
                    f"Synced {payload['symbol']} 5m data for {payload['trade_date']} "
                    f"into {payload['db_path']} ({payload['rows']} rows, source={payload['source']})"
                )
            else:
                print(
                    f"Synced {payload['symbols']} symbols for {payload['trade_date']} into {payload['db_path']}"
                )
                for item in payload["results"]:
                    print(f"{item['symbol']}: {item['rows']} rows ({item['source']})")
        _print_disclaimer()
        return

    if args.command == "data" and args.data_command == "import-5m":
        trade_date = date.fromisoformat(args.trade_date) if args.trade_date else None
        if bool(args.input) == bool(args.input_dir):
            raise ValueError("import-5m requires exactly one of --input or --input-dir.")
        if args.input:
            result = import_5m_file_to_duckdb(
                input_path=args.input,
                db_path=args.db_path,
                symbol=args.symbol,
                trade_date=trade_date,
                source=args.source,
            )
            payload = {
                "symbol": result.symbol,
                "trade_date": result.trade_date,
                "rows": result.rows,
                "db_path": result.db_path,
                "source": args.source,
            }
        else:
            batch = import_5m_directory_to_duckdb(
                input_dir=args.input_dir,
                db_path=args.db_path,
                trade_date=trade_date,
                source=args.source,
            )
            payload = {
                "trade_date": batch.trade_date,
                "db_path": batch.db_path,
                "source": args.source,
                "symbols": len(batch.results),
                "results": [
                    {
                        "symbol": item.symbol,
                        "trade_date": item.trade_date,
                        "rows": item.rows,
                        "db_path": item.db_path,
                    }
                    for item in batch.results
                ],
            }
        if args.format == "json":
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            if "symbol" in payload:
                print(
                    f"Imported {payload['symbol']} 5m data for {payload['trade_date']} "
                    f"into {payload['db_path']} ({payload['rows']} rows)"
                )
            else:
                print(
                    f"Imported {payload['symbols']} symbols from offline 5m files "
                    f"into {payload['db_path']} ({payload['trade_date']})"
                )
                for item in payload["results"]:
                    print(f"{item['symbol']}: {item['rows']} rows")
        _print_disclaimer()
        return

    if args.command == "backtest" and args.backtest_command == "run":
        provider = "csv" if args.input else args.provider
        result = run_backtest(
            provider=provider,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            interval=args.interval,
            strategy=args.strategy,
            strategy_params=_parse_key_values(args.strategy_params),
            input_path=args.input,
            data_root=args.data_root,
            db_path=args.db_path,
            adjust=args.adjust,
            market=args.market,
            provider_profile=args.provider_profile,
            costs=_cost_args(args),
            benchmark_symbol=args.benchmark_symbol,
        )
        write_backtest_result(result, args.result_json)
        print(f"Backtest JSON written to {Path(args.result_json)}")
        if args.report_html:
            write_backtest_html(result, args.report_html)
            print(f"Backtest HTML report written to {Path(args.report_html)}")
        _print_disclaimer()
        return

    if args.command == "backtest" and args.backtest_command == "sweep":
        provider = "csv" if args.input else args.provider
        result = sweep_backtest(
            provider=provider,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            interval=args.interval,
            strategy=args.strategy,
            parameter_grid=_parse_grid(args.param),
            metric=args.metric,
            input_path=args.input,
            data_root=args.data_root,
            db_path=args.db_path,
            adjust=args.adjust,
            market=args.market,
            provider_profile=args.provider_profile,
            costs=_cost_args(args),
            benchmark_symbol=args.benchmark_symbol,
            top_n=args.top_n,
            filters=_parse_result_filters(args.filter),
        )
        write_sweep_result(result, args.result_json, args.result_csv)
        print(f"Sweep JSON written to {Path(args.result_json)}")
        if args.result_csv:
            print(f"Sweep CSV written to {Path(args.result_csv)}")
        if args.report_html:
            write_sweep_html(result, args.report_html)
            print(f"Sweep HTML report written to {Path(args.report_html)}")
        _print_disclaimer()
        return

    if args.command == "backtest" and args.backtest_command == "compare":
        provider = "csv" if args.input else args.provider
        strategies = args.strategy or ["sma_cross", "macd", "boll_breakout"]
        result = compare_backtests(
            provider=provider,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            interval=args.interval,
            strategies=strategies,
            strategy_params_map=_parse_strategy_param_overrides(args.strategy_param),
            metric=args.metric,
            input_path=args.input,
            data_root=args.data_root,
            db_path=args.db_path,
            adjust=args.adjust,
            market=args.market,
            provider_profile=args.provider_profile,
            costs=_cost_args(args),
            benchmark_symbol=args.benchmark_symbol,
            top_n=args.top_n,
            filters=_parse_result_filters(args.filter),
        )
        write_compare_result(result, args.result_json, args.result_csv)
        print(f"Compare JSON written to {Path(args.result_json)}")
        if args.result_csv:
            print(f"Compare CSV written to {Path(args.result_csv)}")
        if args.report_html:
            write_compare_html(result, args.report_html)
            print(f"Compare HTML report written to {Path(args.report_html)}")
        _print_disclaimer()
        return

    if args.command == "report" and args.report_command == "generate":
        generate_report(
            args.input,
            args.output,
            args.title,
            output_format=args.format,
            watermark=None if args.no_watermark else args.watermark,
        )
        print(f"{args.format.title()} report written to {Path(args.output)}")
        _print_disclaimer()
        return

    if args.command == "alpha" and args.alpha_command == "analyze":
        provider = "csv" if args.input else args.provider
        indicators = [item.strip() for item in args.indicators.split(",") if item.strip()]
        result = analyze_alpha(
            provider=provider,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            interval=args.interval,
            factors=args.factor,
            indicators=indicators,
            horizon=args.horizon,
            ic_window=args.ic_window,
            input_path=args.input,
            data_root=args.data_root,
            db_path=args.db_path,
            adjust=args.adjust,
            market=args.market,
            provider_profile=args.provider_profile,
            quantiles=args.quantiles,
            include_composite=not args.no_composite,
        )
        write_alpha_result(result, args.result_json)
        print(f"Alpha JSON written to {Path(args.result_json)}")
        if args.report_html:
            write_alpha_html(result, args.report_html)
            print(f"Alpha HTML report written to {Path(args.report_html)}")
        _print_disclaimer()
        return

    if args.command == "risk" and args.risk_command == "analyze":
        provider = None
        if args.mode == "market":
            provider = "csv" if args.input and args.provider is None else args.provider
        result = analyze_risk(
            provider=provider,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            interval=args.interval,
            confidence_level=args.confidence_level,
            input_path=args.input,
            data_root=args.data_root,
            db_path=args.db_path,
            adjust=args.adjust,
            market=args.market,
            provider_profile=args.provider_profile,
            mode=args.mode,
            source=args.source,
            rolling_window=args.rolling_window,
            stress_shocks=[float(item) for item in args.stress_shock] if args.stress_shock else None,
        )
        write_risk_result(result, args.result_json)
        print(f"Risk JSON written to {Path(args.result_json)}")
        if args.report_html:
            write_risk_html(result, args.report_html)
            print(f"Risk HTML report written to {Path(args.report_html)}")
        _print_disclaimer()
        return

    if args.command == "commercial" and args.commercial_command == "plans":
        payload = {"plans": list_plans()}
        if args.format == "json":
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            for item in payload["plans"]:
                print(f"{item['plan']}: {item['name']} ¥{item['price_label']}/{item['billing_cycle']}")
                print(f"  features={','.join(item['features'])}")
        return

    if args.command == "commercial" and args.commercial_command == "lead":
        result = create_lead(
            name=args.name,
            email=args.email,
            use_case=args.use_case,
            plan_interest=args.plan,
            company=args.company,
            notes=args.notes,
            source=args.source,
        )
        if args.format == "json":
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"Lead created: {result['lead_id']} ({result['plan_interest']})")
        return

    if args.command == "commercial" and args.commercial_command == "account-create":
        result = create_account(
            name=args.name,
            email=args.email,
            plan=args.plan,
            company=args.company,
        )
        if args.format == "json":
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"Account created: {result['account_id']} ({result['plan']})")
        return

    if args.command == "commercial" and args.commercial_command == "accounts":
        payload = {"items": list_accounts(limit=args.limit)}
        if args.format == "json":
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            for item in payload["items"]:
                print(f"{item['account_id']} {item['email']} plan={item['plan']} status={item['status']}")
        return

    if args.command == "commercial" and args.commercial_command == "subscription-create":
        result = create_subscription(
            account_id=args.account_id,
            plan=args.plan,
            billing_cycle=args.billing_cycle,
            provider=args.provider,
        )
        if args.format == "json":
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"Subscription created: {result['subscription_id']} ({result['plan']}/{result['status']})")
        return

    if args.command == "commercial" and args.commercial_command == "subscriptions":
        payload = {"items": list_subscriptions(limit=args.limit)}
        if args.format == "json":
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            for item in payload["items"]:
                print(
                    f"{item['subscription_id']} account={item['account_id']} "
                    f"plan={item['plan']} status={item['status']}"
                )
        return


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        _dispatch(args)
    except (DependencyUnavailableError, ExternalProviderError, FileNotFoundError, ValueError, RuntimeError) as exc:
        print(_format_cli_error(exc), file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
