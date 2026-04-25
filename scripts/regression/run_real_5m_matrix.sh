#!/usr/bin/env bash

set -euo pipefail

DB_PATH="${1:-output/market/market.duckdb}"
SYMBOLS="${2:-000001.SZ,600000.SH,300750.SZ,601318.SH}"
BENCHMARK_SYMBOL="${3:-600000.SH}"
TRADE_DATES="${4:-}"
OUTPUT_ROOT="${5:-output/regression-real-5m-matrix}"
PRIMARY_SYMBOL="${6:-}"
PROVIDER="${7:-auto}"

if [[ -z "${PRIMARY_SYMBOL}" ]]; then
  PRIMARY_SYMBOL="${SYMBOLS%%,*}"
fi

if [[ -z "${TRADE_DATES}" ]]; then
  TRADE_DATES="$(
    python - <<'PY'
from __future__ import annotations

from datetime import date, timedelta

today = date.today()
cursor = today - timedelta(days=1)
dates: list[str] = []
while len(dates) < 5:
    if cursor.weekday() < 5:
        dates.append(cursor.isoformat())
    cursor -= timedelta(days=1)
print(",".join(sorted(dates)))
PY
  )"
fi

RUN_ID="$(date '+%Y%m%d-%H%M%S')"
ARCHIVE_DIR="${OUTPUT_ROOT}/archive/${RUN_ID}"
LATEST_DIR="${OUTPUT_ROOT}/latest"

mkdir -p "${ARCHIVE_DIR}"
printf '%s\n' "${SYMBOLS}" | tr ',' '\n' > "${ARCHIVE_DIR}/symbols.txt"
printf '%s\n' "${TRADE_DATES}" | tr ',' '\n' > "${ARCHIVE_DIR}/trade_dates.txt"

IFS=',' read -r -a DATE_ITEMS <<< "${TRADE_DATES}"
for trade_date in "${DATE_ITEMS[@]}"; do
  DATE_DIR="${ARCHIVE_DIR}/${trade_date}"
  mkdir -p "${DATE_DIR}"

  PYTHONPATH=src python -m agent4quant.cli data sync-5m \
    --provider "${PROVIDER}" \
    --symbols "${SYMBOLS}" \
    --trade-date "${trade_date}" \
    --db-path "${DB_PATH}" \
    --format json > "${DATE_DIR}/sync.json"

  bash scripts/regression/run_real_5m_regression.sh \
    "${DB_PATH}" \
    "${PRIMARY_SYMBOL}" \
    "${BENCHMARK_SYMBOL}" \
    "${trade_date} 09:35:00" \
    "${trade_date} 15:00:00" \
    "${DATE_DIR}/regression"
done

PYTHONPATH=src python -m agent4quant.cli data manifest \
  --provider duckdb \
  --interval 5m \
  --db-path "${DB_PATH}" \
  --format json > "${ARCHIVE_DIR}/duckdb-manifest.json"

python - <<'PY' "${ARCHIVE_DIR}" "${RUN_ID}" "${DB_PATH}" "${PRIMARY_SYMBOL}" "${BENCHMARK_SYMBOL}" "${SYMBOLS}" "${TRADE_DATES}" "${PROVIDER}"
from __future__ import annotations

import json
import sys
from pathlib import Path

archive_dir = Path(sys.argv[1])
run_id = sys.argv[2]
db_path = sys.argv[3]
primary_symbol = sys.argv[4]
benchmark_symbol = sys.argv[5]
symbols = [item for item in sys.argv[6].split(",") if item]
trade_dates = [item for item in sys.argv[7].split(",") if item]
provider = sys.argv[8]

duckdb_manifest = json.loads((archive_dir / "duckdb-manifest.json").read_text(encoding="utf-8"))
days: list[dict] = []
total_synced_rows = 0
source_breakdown: dict[str, int] = {}

for trade_date in trade_dates:
    date_dir = archive_dir / trade_date
    sync = json.loads((date_dir / "sync.json").read_text(encoding="utf-8"))
    regression_summary = json.loads((date_dir / "regression" / "summary.json").read_text(encoding="utf-8"))
    synced_rows = sum(item["rows"] for item in sync["results"])
    total_synced_rows += synced_rows
    for item in sync["results"]:
        source_breakdown[item["source"]] = source_breakdown.get(item["source"], 0) + 1
    days.append(
        {
            "trade_date": trade_date,
            "synced_symbols": sync["symbols"],
            "synced_rows": synced_rows,
            "sources": sorted({item["source"] for item in sync["results"]}),
            "regression": regression_summary,
        }
    )

summary = {
    "run_id": run_id,
    "db_path": db_path,
    "provider": provider,
    "primary_symbol": primary_symbol,
    "benchmark_symbol": benchmark_symbol,
    "symbols": symbols,
    "trade_dates": trade_dates,
    "days": days,
    "totals": {
        "trade_days": len(trade_dates),
        "symbols": len(symbols),
        "sync_tasks": len(trade_dates) * len(symbols),
        "synced_rows": total_synced_rows,
        "source_breakdown": source_breakdown,
    },
    "duckdb_manifest": duckdb_manifest,
}

(archive_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(summary, ensure_ascii=False, indent=2))
PY

rm -rf "${LATEST_DIR}"
cp -R "${ARCHIVE_DIR}" "${LATEST_DIR}"

echo "Archived run: ${ARCHIVE_DIR}"
echo "Latest snapshot: ${LATEST_DIR}"
