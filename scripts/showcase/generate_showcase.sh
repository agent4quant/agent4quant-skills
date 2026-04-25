#!/usr/bin/env bash

set -euo pipefail

REGRESSION_ROOT="${1:-output/regression-yfinance-smoke/latest}"
OUTPUT_DIR="${2:-output/showcase/latest}"

run_demo_commands() {
  PYTHONPATH=src python -m agent4quant.cli data providers --format json
  PYTHONPATH=src python -m agent4quant.cli data fetch \
    --provider demo \
    --symbol DEMO.SH \
    --start 2025-01-01 \
    --end 2025-03-31 \
    --interval 1d \
    --indicators ma5,ma20,macd,rsi,boll,atr,kdj \
    --format csv \
    --output output/demo-data.csv
  PYTHONPATH=src python -m agent4quant.cli data fetch \
    --provider akshare \
    --symbol 000001.SZ \
    --start 2025-01-01 \
    --end 2025-03-31 \
    --interval 1d \
    --adjust qfq \
    --indicators ma5,rsi \
    --format json \
    --output output/000001-akshare.json
  PYTHONPATH=src python -m agent4quant.cli backtest run \
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
  PYTHONPATH=src python -m agent4quant.cli alpha analyze \
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
  PYTHONPATH=src python -m agent4quant.cli risk analyze \
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
  PYTHONPATH=src python -m agent4quant.cli report generate \
    --input output/demo-backtest.json \
    --output output/demo-report.pdf \
    --title "Agent4Quant Demo Research" \
    --format pdf \
    --watermark "Internal Draft"
  PYTHONPATH=src python -m agent4quant.cli report generate \
    --input output/demo-backtest.json \
    --output output/demo-report.html \
    --title "Agent4Quant Demo Research" \
    --format html \
    --watermark "Internal Draft"
}

if [[ "${RUN_DEMO_COMMANDS:-1}" == "1" ]]; then
  run_demo_commands
fi

bash scripts/showcase/prepare_showcase.sh "${REGRESSION_ROOT}" "${OUTPUT_DIR}"
