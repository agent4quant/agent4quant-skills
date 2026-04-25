#!/usr/bin/env bash

set -euo pipefail

DB_PATH="${1:-output/market/market.duckdb}"
SYMBOL="${2:-000001.SZ}"
BENCHMARK_SYMBOL="${3:-600000.SH}"
START="${4:-2026-03-24 09:35:00}"
END="${5:-2026-03-24 15:00:00}"
OUTPUT_DIR="${6:-output/regression-real-5m}"

mkdir -p "${OUTPUT_DIR}"

PYTHONPATH=src python -m agent4quant.cli data manifest \
  --provider duckdb \
  --interval 5m \
  --db-path "${DB_PATH}" \
  --format json > "${OUTPUT_DIR}/manifest.json"

PYTHONPATH=src python -m agent4quant.cli data fetch \
  --provider duckdb \
  --symbol "${SYMBOL}" \
  --start "${START}" \
  --end "${END}" \
  --interval 5m \
  --db-path "${DB_PATH}" \
  --indicators ma5,rsi \
  --format json \
  --output "${OUTPUT_DIR}/dataset.json"

PYTHONPATH=src python -m agent4quant.cli backtest run \
  --provider duckdb \
  --symbol "${SYMBOL}" \
  --benchmark-symbol "${BENCHMARK_SYMBOL}" \
  --start "${START}" \
  --end "${END}" \
  --interval 5m \
  --strategy sma_cross \
  --strategy-params fast=3,slow=8 \
  --db-path "${DB_PATH}" \
  --result-json "${OUTPUT_DIR}/backtest.json" \
  --report-html "${OUTPUT_DIR}/backtest.html"

PYTHONPATH=src python -m agent4quant.cli report generate \
  --input "${OUTPUT_DIR}/backtest.json" \
  --output "${OUTPUT_DIR}/backtest.pdf" \
  --title "Real 5m Regression" \
  --format pdf \
  --watermark "A4Q Internal"

PYTHONPATH=src python -m agent4quant.cli backtest compare \
  --provider duckdb \
  --symbol "${SYMBOL}" \
  --benchmark-symbol "${BENCHMARK_SYMBOL}" \
  --start "${START}" \
  --end "${END}" \
  --interval 5m \
  --strategy sma_cross \
  --strategy macd \
  --strategy boll_breakout \
  --strategy-param sma_cross:fast=3,slow=8 \
  --metric sharpe \
  --db-path "${DB_PATH}" \
  --result-json "${OUTPUT_DIR}/compare.json" \
  --report-html "${OUTPUT_DIR}/compare.html"

PYTHONPATH=src python -m agent4quant.cli risk analyze \
  --mode market \
  --provider duckdb \
  --symbol "${SYMBOL}" \
  --start "${START}" \
  --end "${END}" \
  --interval 5m \
  --db-path "${DB_PATH}" \
  --result-json "${OUTPUT_DIR}/risk.json" \
  --report-html "${OUTPUT_DIR}/risk.html"

PYTHONPATH=src python -m agent4quant.cli alpha analyze \
  --provider duckdb \
  --symbol "${SYMBOL}" \
  --start "${START}" \
  --end "${END}" \
  --interval 5m \
  --db-path "${DB_PATH}" \
  --indicators ma5,rsi \
  --factor ma_5 \
  --factor rsi_14 \
  --horizon 2 \
  --ic-window 10 \
  --result-json "${OUTPUT_DIR}/alpha.json" \
  --report-html "${OUTPUT_DIR}/alpha.html"

python - <<'PY' "${OUTPUT_DIR}"
from __future__ import annotations

import json
import sys
from pathlib import Path

output_dir = Path(sys.argv[1])
manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
backtest = json.loads((output_dir / "backtest.json").read_text(encoding="utf-8"))
compare = json.loads((output_dir / "compare.json").read_text(encoding="utf-8"))
risk = json.loads((output_dir / "risk.json").read_text(encoding="utf-8"))
alpha = json.loads((output_dir / "alpha.json").read_text(encoding="utf-8"))

summary = {
    "manifest": {
        "symbols": manifest["symbols"],
        "total_rows": manifest["total_rows"],
    },
    "backtest": backtest["metrics"],
    "compare": {
        "best_strategy": compare["best_strategy"],
        "best_metric_value": compare["best_metric_value"],
        "returned_experiments": compare["returned_experiments"],
    },
    "risk": risk["metrics"],
    "alpha": {
        "best_factor": alpha["best_factor"],
        "results": alpha["results"],
    },
}
(output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(summary, ensure_ascii=False, indent=2))
PY
