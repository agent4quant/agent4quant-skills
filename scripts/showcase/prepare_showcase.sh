#!/usr/bin/env bash

set -euo pipefail

REGRESSION_ROOT="${1:-output/regression-yfinance-smoke/latest}"
OUTPUT_DIR="${2:-output/showcase/latest}"

mkdir -p "${OUTPUT_DIR}"
rm -f "${OUTPUT_DIR}"/*

copy_if_exists() {
  local src="$1"
  local dst="$2"
  if [[ -f "${src}" ]]; then
    cp "${src}" "${dst}"
  fi
}

copy_if_exists "output/demo-backtest.html" "${OUTPUT_DIR}/demo-backtest.html"
copy_if_exists "output/demo-report.pdf" "${OUTPUT_DIR}/demo-report.pdf"
copy_if_exists "output/demo-report.html" "${OUTPUT_DIR}/demo-report.html"
copy_if_exists "output/demo-data.csv" "${OUTPUT_DIR}/demo-data.csv"
copy_if_exists "output/000001-akshare.json" "${OUTPUT_DIR}/000001-akshare.json"
copy_if_exists "${REGRESSION_ROOT}/summary.json" "${OUTPUT_DIR}/real-5m-summary.json"
copy_if_exists "${REGRESSION_ROOT}/duckdb-manifest.json" "${OUTPUT_DIR}/real-5m-manifest.json"

if [[ -d "${REGRESSION_ROOT}" ]]; then
  first_date="$(find "${REGRESSION_ROOT}" -maxdepth 1 -mindepth 1 -type d | sort | head -n 1)"
  if [[ -n "${first_date}" ]]; then
    copy_if_exists "${first_date}/regression/backtest.html" "${OUTPUT_DIR}/real-5m-backtest.html"
    copy_if_exists "${first_date}/regression/backtest.pdf" "${OUTPUT_DIR}/real-5m-backtest.pdf"
  fi
fi

python - <<'PY' "${OUTPUT_DIR}"
from __future__ import annotations

import json
import sys
from pathlib import Path

output_dir = Path(sys.argv[1])
items = []
for path in sorted(output_dir.iterdir()):
    if not path.is_file():
        continue
    items.append(
        {
            "name": path.name,
            "size_bytes": path.stat().st_size,
            "type": path.suffix.lower().lstrip("."),
            "path": str(path),
        }
    )

manifest = {
    "output_dir": str(output_dir),
    "items": items,
}
(output_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(manifest, ensure_ascii=False, indent=2))
PY
