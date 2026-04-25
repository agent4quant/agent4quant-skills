#!/usr/bin/env bash

set -euo pipefail

# Online-first examples for the local-first toolkit path, focused on quant-data / quant-backtest / quant-report.

a4q data providers --format json

a4q data fetch \
  --provider demo \
  --symbol DEMO.SH \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --indicators ma5,ma20,macd,rsi,boll,atr,kdj \
  --format csv \
  --output output/demo-data.csv

a4q data fetch \
  --provider akshare \
  --symbol 000001.SZ \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --adjust qfq \
  --indicators ma5,rsi \
  --format json \
  --output output/000001-akshare.json

a4q data fetch \
  --provider yfinance \
  --symbol AAPL \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --indicators ma5,rsi \
  --format json \
  --output output/aapl-yfinance.json

a4q data validate \
  --provider csv \
  --input output/demo-data.csv \
  --interval 1d

a4q data repair \
  --provider csv \
  --input output/demo-data.csv \
  --interval 1d \
  --symbol DEMO.SH \
  --output output/demo-data.cleaned.csv

a4q backtest run \
  --provider demo \
  --symbol DEMO.SH \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --strategy sma_cross \
  --strategy-params fast=5,slow=20 \
  --benchmark-symbol BMK.SH \
  --commission-bps 2 \
  --slippage-bps 3 \
  --stamp-duty-bps 10 \
  --result-json output/demo-backtest.json \
  --report-html output/demo-backtest.html

a4q backtest sweep \
  --provider demo \
  --symbol DEMO.SH \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --strategy sma_cross \
  --param 'fast=5|10|20' \
  --param 'slow=20|30|60' \
  --metric sharpe \
  --benchmark-symbol BMK.SH \
  --top-n 3 \
  --commission-bps 2 \
  --slippage-bps 3 \
  --stamp-duty-bps 10 \
  --result-json output/demo-sweep.json \
  --result-csv output/demo-sweep.csv \
  --report-html output/demo-sweep.html

a4q backtest compare \
  --provider demo \
  --symbol DEMO.SH \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --strategy sma_cross \
  --strategy macd \
  --strategy boll_breakout \
  --strategy-param sma_cross:fast=5,slow=20 \
  --metric sharpe \
  --benchmark-symbol BMK.SH \
  --top-n 2 \
  --commission-bps 2 \
  --slippage-bps 3 \
  --stamp-duty-bps 10 \
  --result-json output/demo-compare.json \
  --result-csv output/demo-compare.csv \
  --report-html output/demo-compare.html

a4q risk analyze \
  --provider demo \
  --symbol DEMO.SH \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --confidence-level 0.95 \
  --rolling-window 10 \
  --stress-shock -0.03 \
  --stress-shock -0.07 \
  --result-json output/demo-risk.json \
  --report-html output/demo-risk.html

a4q alpha analyze \
  --provider demo \
  --symbol DEMO.SH \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --indicators ma5,rsi \
  --factor ma_5 \
  --factor rsi_14 \
  --horizon 1 \
  --ic-window 10 \
  --quantiles 4 \
  --result-json output/demo-alpha.json \
  --report-html output/demo-alpha.html

a4q report generate \
  --input output/demo-backtest.json \
  --output output/demo-report.md \
  --title "Agent4Quant Demo Research"

a4q report generate \
  --input output/demo-backtest.json \
  --output output/demo-report.html \
  --title "Agent4Quant Demo Research" \
  --format html \
  --watermark "Internal Draft"

a4q report generate \
  --input output/demo-backtest.json \
  --output output/demo-report.pdf \
  --title "Agent4Quant Demo Research" \
  --format pdf \
  --watermark "Internal Draft"

# Optional compatibility examples for user-managed external data.

a4q data fetch \
  --provider csv \
  --input /absolute/path/to/dataset.csv \
  --symbol 000001.SZ \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --format json \
  --output output/from-csv.json

a4q data fetch \
  --provider local \
  --data-root /absolute/path/to/market-data \
  --market cn \
  --symbol 000001.SZ \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --adjust qfq \
  --indicators ma5,rsi \
  --format json \
  --output output/local-qfq.json

export A4Q_EXTERNAL_PROVIDER_CONFIG=/absolute/path/to/external-providers.toml
a4q data fetch \
  --provider local \
  --provider-profile cn_daily \
  --symbol 000001.SZ \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --format json \
  --output output/local-profile.json

a4q commercial plans --format json

a4q commercial lead \
  --name "Alice" \
  --email "alice@example.com" \
  --use-case "Need PDF reports and async tasks" \
  --plan professional \
  --company "A4Q Labs" \
  --format json
