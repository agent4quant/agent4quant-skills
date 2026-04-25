from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import zlib

import numpy as np
import pandas as pd


@dataclass(slots=True)
class FetchRequest:
    symbol: str
    start: str
    end: str
    interval: str
    input_path: str | None = None
    data_root: str | None = None
    adjust: str = "none"
    market: str | None = None


def _normalize_symbol(symbol: str) -> str:
    return "".join(ch for ch in symbol if ch.isdigit()) or symbol


def split_symbol(symbol: str) -> tuple[str, str]:
    parts = symbol.rsplit(".", 1)
    if len(parts) == 2:
        return parts[0], symbol
    return symbol, symbol


class BaseProvider:
    name = "base"

    def fetch(self, request: FetchRequest) -> pd.DataFrame:
        raise NotImplementedError


class DemoProvider(BaseProvider):
    name = "demo"

    def fetch(self, request: FetchRequest) -> pd.DataFrame:
        start = pd.Timestamp(request.start)
        end = pd.Timestamp(request.end)
        dates = pd.bdate_range(start, end)

        seed = zlib.crc32(f"{request.symbol}|{request.interval}".encode("utf-8"))
        rng = np.random.default_rng(seed)
        base = 100 + np.cumsum(rng.normal(0, 1.2, len(dates)))
        close = np.maximum(base, 1)
        open_ = close + rng.normal(0, 0.8, len(dates))
        high = np.maximum(open_, close) + np.abs(rng.normal(0, 0.6, len(dates)))
        low = np.minimum(open_, close) - np.abs(rng.normal(0, 0.6, len(dates)))
        volume = rng.integers(20_000, 200_000, len(dates))

        return pd.DataFrame({
            "date": dates,
            "open": open_.round(2),
            "high": high.round(2),
            "low": low.round(2),
            "close": close.round(2),
            "volume": volume,
            "symbol": request.symbol,
        })


class CsvProvider(BaseProvider):
    name = "csv"

    def fetch(self, request: FetchRequest) -> pd.DataFrame:
        if not request.input_path:
            raise ValueError("CSV provider requires --input.")

        path = Path(request.input_path)
        if not path.exists():
            raise FileNotFoundError(f"Input CSV not found: {path}")

        df = pd.read_csv(path)
        required = {"date", "open", "high", "low", "close", "volume"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Input CSV missing required columns: {sorted(missing)}")

        df["date"] = pd.to_datetime(df["date"])
        start = pd.Timestamp(request.start)
        end = pd.Timestamp(request.end)
        mask = (df["date"] >= start) & (df["date"] <= end)
        result = df.loc[mask].copy()
        result["symbol"] = result.get("symbol", request.symbol)
        return result.sort_values("date").reset_index(drop=True)


class AkshareProvider(BaseProvider):
    name = "akshare"

    def fetch(self, request: FetchRequest) -> pd.DataFrame:
        if request.interval != "1d":
            raise ValueError("AkShare provider currently supports interval=1d only.")

        try:
            import akshare as ak
        except ImportError as exc:
            raise ImportError("AkShare is not installed. Run: pip install akshare>=1.17") from exc

        code, canonical_symbol = split_symbol(request.symbol)
        adjust = "" if request.adjust == "none" else request.adjust
        df = ak.stock_zh_a_hist(
            symbol=_normalize_symbol(code),
            period="daily",
            start_date=pd.Timestamp(request.start).strftime("%Y%m%d"),
            end_date=pd.Timestamp(request.end).strftime("%Y%m%d"),
            adjust=adjust,
        )
        if df.empty:
            raise ValueError("AkShare returned empty data.")

        renamed = df.rename(columns={
            "日期": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
        })
        result = renamed[["date", "open", "high", "low", "close", "volume"]].copy()
        result["date"] = pd.to_datetime(result["date"])
        result["symbol"] = canonical_symbol
        return result.sort_values("date").reset_index(drop=True)


def _to_yfinance_symbol(symbol: str) -> str:
    clean = symbol.strip().upper()

    if clean.endswith(".US"):
        clean = clean[:-3]

    clean = clean.replace("/", "-")
    if "." in clean and not clean.startswith("^"):
        left, right = clean.rsplit(".", 1)
        if len(right) == 1 and left:
            return f"{left}-{right}"
    return clean


def _normalize_yfinance_frame(frame: pd.DataFrame) -> pd.DataFrame:
    payload = frame.copy()
    if isinstance(payload.columns, pd.MultiIndex):
        level_0 = payload.columns.get_level_values(0)
        level_1 = payload.columns.get_level_values(1)
        if len(set(level_1)) == 1:
            payload.columns = level_0
        elif len(set(level_0)) == 1:
            payload.columns = level_1
    return payload


class YfinanceProvider(BaseProvider):
    name = "yfinance"

    def fetch(self, request: FetchRequest) -> pd.DataFrame:
        if request.interval != "1d":
            raise ValueError("yfinance provider currently supports interval=1d only.")
        if request.adjust != "none":
            raise ValueError("yfinance provider currently supports adjust=none only.")

        try:
            import yfinance as yf
        except ImportError as exc:
            raise ImportError("yfinance is not installed. Run: pip install yfinance>=0.2") from exc

        yf_symbol = _to_yfinance_symbol(request.symbol)
        end = (pd.Timestamp(request.end) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        frame = yf.download(
            tickers=yf_symbol,
            start=pd.Timestamp(request.start).strftime("%Y-%m-%d"),
            end=end,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
            timeout=10,
            multi_level_index=True,
        )

        if frame.empty:
            raise ValueError(f"yfinance returned empty data for symbol={request.symbol}.")

        payload = _normalize_yfinance_frame(frame).reset_index()
        date_column = "Date" if "Date" in payload.columns else payload.columns[0]
        renamed = payload.rename(columns={
            date_column: "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        })
        required = ["date", "open", "high", "low", "close", "volume"]
        missing = [column for column in required if column not in renamed.columns]
        if missing:
            raise ValueError(f"yfinance result missing required columns: {missing}")

        result = renamed[required].copy()
        result["date"] = pd.to_datetime(result["date"])
        result["symbol"] = request.symbol
        return result.sort_values("date").reset_index(drop=True)


PROVIDERS = {
    "demo": DemoProvider,
    "csv": CsvProvider,
    "akshare": AkshareProvider,
    "yfinance": YfinanceProvider,
}


def get_provider(name: str) -> BaseProvider:
    provider_class = PROVIDERS.get(name)
    if provider_class is None:
        raise ValueError(f"Unsupported provider: {name}. Supported: {', '.join(PROVIDERS)}")
    return provider_class()


def list_provider_capabilities() -> dict:
    return {
        "providers": {
            "demo": {"intervals": ["1d"], "adjusts": ["none"], "markets": []},
            "csv": {"intervals": ["1d", "5m"], "adjusts": ["none"], "markets": []},
            "akshare": {"intervals": ["1d"], "adjusts": ["qfq", "hfq", "none"], "markets": ["A"]},
            "yfinance": {"intervals": ["1d"], "adjusts": ["none"], "markets": ["HK", "US"]},
        },
        "indicators": ["ma", "ret", "mom", "volatility", "zscore", "volma", "macd", "rsi", "boll", "atr", "kdj", "obv"],
        "formats": ["csv", "json"],
    }
