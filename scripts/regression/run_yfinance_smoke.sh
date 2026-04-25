#!/usr/bin/env bash

set -euo pipefail

START="${1:-}"
END="${2:-}"
OUTPUT_ROOT="${3:-output/regression-yfinance-smoke}"

if [[ -z "${START}" || -z "${END}" ]]; then
  read -r START END < <(
    python - <<'PY'
from __future__ import annotations

from datetime import date, timedelta

end = date.today()
start = end - timedelta(days=10)
print(start.isoformat(), end.isoformat())
PY
  )
fi

RUN_ID="$(date '+%Y%m%d-%H%M%S')"
ARCHIVE_DIR="${OUTPUT_ROOT}/archive/${RUN_ID}"
LATEST_DIR="${OUTPUT_ROOT}/latest"

mkdir -p "${ARCHIVE_DIR}"

PYTHONPATH=src python - <<'PY' "${START}" "${END}" "${ARCHIVE_DIR}"
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

from agent4quant.data.service import fetch_dataset

start = sys.argv[1]
end = sys.argv[2]
archive_dir = Path(sys.argv[3])

cases = [
    {"label": "US-AAPL", "symbol": "AAPL", "market": "us"},
    {"label": "HK-700", "symbol": "700", "market": "hk"},
    {"label": "CN-600000.SH", "symbol": "600000.SH", "market": "cn"},
]

results: list[dict] = []
for case in cases:
    item = {
        "label": case["label"],
        "symbol": case["symbol"],
        "market": case["market"],
        "start": start,
        "end": end,
    }
    try:
        frame, metadata = fetch_dataset(
            provider="yfinance",
            symbol=case["symbol"],
            start=start,
            end=end,
            interval="1d",
            indicators=["ma5"],
            adjust="none",
            market=case["market"],
        )
        item.update(
            {
                "status": "ok",
                "rows": len(frame),
                "first_date": None if frame.empty else str(frame["date"].min().date()),
                "last_date": None if frame.empty else str(frame["date"].max().date()),
                "last_close": None if frame.empty else float(frame["close"].iloc[-1]),
                "provider": metadata["provider"],
                "source_provider": metadata.get("source_provider"),
                "provider_route": metadata.get("provider_route"),
            }
        )
    except Exception as exc:  # noqa: BLE001
        item.update(
            {
                "status": "error",
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        )
    results.append(item)

summary = {
    "generated_at": datetime.now().isoformat(timespec="seconds"),
    "provider": "yfinance",
    "interval": "1d",
    "start": start,
    "end": end,
    "cases": results,
}

(archive_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(summary, ensure_ascii=False, indent=2))
PY

rm -rf "${LATEST_DIR}"
cp -R "${ARCHIVE_DIR}" "${LATEST_DIR}"

echo "Archived run: ${ARCHIVE_DIR}"
echo "Latest snapshot: ${LATEST_DIR}"
