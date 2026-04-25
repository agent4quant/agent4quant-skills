from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import zlib

import numpy as np
import pandas as pd

from agent4quant.config import resolve_data_root, resolve_duckdb_path, resolve_provider_market
from agent4quant.data.catalog import list_symbols, resolve_symbol_file
from agent4quant.data.symbols import normalize_symbol, split_symbol
from agent4quant.errors import DependencyUnavailableError, classify_external_provider_error


def _normalize_symbol(symbol: str) -> str:
    return "".join(ch for ch in symbol if ch.isdigit()) or symbol


@dataclass(slots=True)
class FetchRequest:
    symbol: str
    start: str
    end: str
    interval: str
    input_path: str | None = None
    data_root: str | None = None
    db_path: str | None = None
    adjust: str = "none"
    market: str | None = None
    provider_profile: str | None = None


class BaseProvider:
    name = "base"

    def fetch(self, request: FetchRequest) -> pd.DataFrame:
        raise NotImplementedError

    def available_symbols(self, request: FetchRequest) -> list[str]:
        return []


class DemoProvider(BaseProvider):
    name = "demo"

    def fetch(self, request: FetchRequest) -> pd.DataFrame:
        start = pd.Timestamp(request.start)
        end = pd.Timestamp(request.end)
        if request.interval == "5m":
            dates = pd.date_range(start, end + pd.Timedelta(days=1), freq="5min")
        else:
            dates = pd.bdate_range(start, end)

        if dates.empty:
            raise ValueError("No dates generated for the selected range.")

        seed = zlib.crc32(f"{request.symbol}|{request.interval}".encode("utf-8"))
        rng = np.random.default_rng(seed)
        base = 100 + np.cumsum(rng.normal(0, 1.2, len(dates)))
        close = np.maximum(base, 1)
        open_ = close + rng.normal(0, 0.8, len(dates))
        high = np.maximum(open_, close) + np.abs(rng.normal(0, 0.6, len(dates)))
        low = np.minimum(open_, close) - np.abs(rng.normal(0, 0.6, len(dates)))
        volume = rng.integers(20_000, 200_000, len(dates))

        return pd.DataFrame(
            {
                "date": dates,
                "open": open_.round(2),
                "high": high.round(2),
                "low": low.round(2),
                "close": close.round(2),
                "volume": volume,
                "symbol": request.symbol,
            }
        )


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


class LocalProvider(BaseProvider):
    name = "local"

    def fetch(self, request: FetchRequest) -> pd.DataFrame:
        root = resolve_data_root(request.data_root, request.provider_profile)
        if root is None:
            raise ValueError("Local provider requires --data-root, provider profile or A4Q_MARKET_DATA_ROOT.")

        canonical_symbol = normalize_symbol(request.symbol)
        market = resolve_provider_market(request.market, self.name, request.provider_profile)
        path = resolve_symbol_file(root, canonical_symbol, request.interval, market=market)
        return _read_market_file(path, canonical_symbol, request.start, request.end)

    def available_symbols(self, request: FetchRequest) -> list[str]:
        root = resolve_data_root(request.data_root, request.provider_profile)
        if root is None:
            raise ValueError("Local provider requires --data-root, provider profile or A4Q_MARKET_DATA_ROOT.")
        market = resolve_provider_market(request.market, self.name, request.provider_profile)
        return list_symbols(root, request.interval, market=market)


class DuckdbProvider(BaseProvider):
    name = "duckdb"

    def _connect(self, request: FetchRequest):
        path = resolve_duckdb_path(request.db_path, request.provider_profile)
        if path is None:
            raise ValueError("DuckDB provider requires --db-path or provider profile.")

        try:
            import duckdb
        except ImportError as exc:
            raise DependencyUnavailableError("duckdb", "DuckDB is not available in the current environment.") from exc

        return duckdb.connect(str(path), read_only=True)

    def fetch(self, request: FetchRequest) -> pd.DataFrame:
        if request.interval != "5m":
            raise ValueError("DuckDB provider currently supports interval=5m only.")

        conn = self._connect(request)
        canonical_symbol = normalize_symbol(request.symbol)
        try:
            frame = conn.execute(
                """
                SELECT
                    ts AS date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    symbol
                FROM market_data_5m
                WHERE symbol = ?
                  AND ts >= ?
                  AND ts <= ?
                ORDER BY ts
                """,
                [canonical_symbol, pd.Timestamp(request.start), pd.Timestamp(request.end)],
            ).df()
        finally:
            conn.close()

        if frame.empty:
            raise ValueError(
                f"DuckDB returned empty data for symbol={canonical_symbol}, start={request.start}, end={request.end}"
            )
        frame["date"] = pd.to_datetime(frame["date"])
        return frame.reset_index(drop=True)

    def available_symbols(self, request: FetchRequest) -> list[str]:
        conn = self._connect(request)
        try:
            rows = conn.execute("SELECT DISTINCT symbol FROM market_data_5m ORDER BY symbol").fetchall()
        finally:
            conn.close()
        return [row[0] for row in rows]


class AkshareProvider(BaseProvider):
    name = "akshare"

    def fetch(self, request: FetchRequest) -> pd.DataFrame:
        if request.interval != "1d":
            raise ValueError("AkShare provider currently supports interval=1d only.")

        try:
            import akshare as ak
        except ImportError as exc:
            raise DependencyUnavailableError("akshare", "AkShare is not installed in the current environment.") from exc

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

        renamed = df.rename(
            columns={
                "日期": "date",
                "开盘": "open",
                "最高": "high",
                "最低": "low",
                "收盘": "close",
                "成交量": "volume",
            }
        )
        result = renamed[["date", "open", "high", "low", "close", "volume"]].copy()
        result["date"] = pd.to_datetime(result["date"])
        result["symbol"] = canonical_symbol
        return result.sort_values("date").reset_index(drop=True)


def _to_yfinance_symbol(symbol: str, market: str | None = None) -> str:
    clean = symbol.strip().upper()
    market_hint = (market or "").strip().lower()
    canonical = normalize_symbol(symbol)

    if canonical.endswith(".BJ"):
        raise ValueError("yfinance provider does not support BJ symbols.")

    if market_hint == "hk":
        if clean.endswith(".HK"):
            return clean
        digits = "".join(ch for ch in clean if ch.isdigit())
        if not digits:
            return clean
        return f"{int(digits):05d}.HK"

    if clean.endswith(".US"):
        clean = clean[:-3]

    if market_hint == "us":
        clean = clean.replace("/", "-")
        if "." in clean and not clean.startswith("^"):
            left, right = clean.rsplit(".", 1)
            if len(right) == 1 and left:
                return f"{left}-{right}"
        return clean

    if market_hint == "cn" or canonical.endswith(".SH") or canonical.endswith(".SZ"):
        if canonical.endswith(".SH"):
            return canonical.replace(".SH", ".SS")
        if canonical.endswith(".SZ"):
            return canonical

    if clean.endswith((".HK", ".SH", ".SZ", ".SS", ".BJ")):
        return clean
    clean = clean.replace("/", "-")
    if "." in clean and not clean.startswith("^"):
        left, right = clean.rsplit(".", 1)
        if len(right) == 1 and left:
            return f"{left}-{right}"
    return clean


def _normalize_yfinance_frame(frame: pd.DataFrame, yf_symbol: str) -> pd.DataFrame:
    payload = frame.copy()
    if isinstance(payload.columns, pd.MultiIndex):
        level_0 = payload.columns.get_level_values(0)
        level_1 = payload.columns.get_level_values(1)
        if len(set(level_1)) == 1:
            payload.columns = level_0
        elif len(set(level_0)) == 1:
            payload.columns = level_1
        elif yf_symbol in set(level_1):
            payload = payload.xs(yf_symbol, axis=1, level=1, drop_level=True)
        else:
            raise ValueError("yfinance returned unsupported multi-symbol column layout.")
    return payload


def _is_hk_request(request: FetchRequest, yf_symbol: str) -> bool:
    market_hint = (request.market or "").strip().lower()
    return market_hint == "hk" or yf_symbol.endswith(".HK")


def _to_akshare_hk_symbol(symbol: str) -> str:
    digits = "".join(ch for ch in symbol if ch.isdigit())
    if not digits:
        raise ValueError(f"Unable to map HK symbol for akshare fallback: {symbol}")
    return f"{int(digits):05d}"


def _fetch_hk_via_akshare(request: FetchRequest) -> pd.DataFrame:
    try:
        import akshare as ak
    except ImportError as exc:
        raise DependencyUnavailableError("akshare", "AkShare is not installed in the current environment.") from exc

    frame = ak.stock_hk_daily(symbol=_to_akshare_hk_symbol(request.symbol), adjust="")
    if frame.empty:
        raise ValueError(f"AkShare HK fallback returned empty data for symbol={request.symbol}")

    payload = frame.copy()
    if "date" not in payload.columns:
        raise ValueError(f"AkShare HK fallback returned unsupported columns for symbol={request.symbol}")
    payload["date"] = pd.to_datetime(payload["date"])
    mask = (payload["date"] >= pd.Timestamp(request.start)) & (payload["date"] <= pd.Timestamp(request.end))
    result = payload.loc[mask, ["date", "open", "high", "low", "close", "volume"]].copy()
    if result.empty:
        raise ValueError(
            f"AkShare HK fallback returned empty data for symbol={request.symbol}. Check trading dates or upstream coverage."
        )
    result["symbol"] = request.symbol
    result.attrs["source_provider"] = "akshare.stock_hk_daily"
    result.attrs["provider_route"] = "yfinance.hk_fallback"
    return result.sort_values("date").reset_index(drop=True)


class YfinanceProvider(BaseProvider):
    name = "yfinance"

    def _raise_or_fallback_hk(
        self,
        *,
        request: FetchRequest,
        yf_symbol: str,
        detail: str,
    ) -> pd.DataFrame:
        if not _is_hk_request(request, yf_symbol):
            raise classify_external_provider_error(
                provider="yfinance",
                detail=detail,
                symbol=request.symbol,
            )
        try:
            return _fetch_hk_via_akshare(request)
        except Exception:
            hint = (
                f"{detail}; HK fallback via akshare.stock_hk_daily also failed or returned no data"
            )
            raise classify_external_provider_error(
                provider="yfinance",
                detail=hint,
                symbol=request.symbol,
            )

    def fetch(self, request: FetchRequest) -> pd.DataFrame:
        if request.interval != "1d":
            raise ValueError("yfinance provider currently supports interval=1d only.")
        if request.adjust != "none":
            raise ValueError("yfinance provider currently supports adjust=none only.")

        try:
            import yfinance as yf
        except ImportError as exc:
            raise DependencyUnavailableError("yfinance", "yfinance is not installed in the current environment.") from exc

        cache_root = Path("output") / "yfinance-cache"
        cache_root.mkdir(parents=True, exist_ok=True)
        if hasattr(yf, "set_tz_cache_location"):
            yf.set_tz_cache_location(str(cache_root.resolve()))

        yf_symbol = _to_yfinance_symbol(request.symbol, request.market)
        end = (pd.Timestamp(request.end) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        try:
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
        except Exception as exc:
            return self._raise_or_fallback_hk(
                request=request,
                yf_symbol=yf_symbol,
                detail=str(exc),
            )
        if frame.empty:
            errors = getattr(getattr(yf, "shared", None), "_ERRORS", {}) or {}
            upstream_error = errors.get(yf_symbol) or errors.get(request.symbol)
            if upstream_error:
                return self._raise_or_fallback_hk(
                    request=request,
                    yf_symbol=yf_symbol,
                    detail=str(upstream_error),
                )
            raise ValueError(
                f"yfinance returned empty data for symbol={request.symbol}. Check market, symbol or trading dates."
            )

        payload = _normalize_yfinance_frame(frame, yf_symbol).reset_index()
        date_column = "Date" if "Date" in payload.columns else payload.columns[0]
        renamed = payload.rename(
            columns={
                date_column: "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            }
        )
        required = ["date", "open", "high", "low", "close", "volume"]
        missing = [column for column in required if column not in renamed.columns]
        if missing:
            raise ValueError(f"yfinance result missing required columns for symbol={request.symbol}: {missing}")

        result = renamed[required].copy()
        result["date"] = pd.to_datetime(result["date"])
        result["symbol"] = request.symbol
        result.attrs["source_provider"] = "yfinance"
        result.attrs["provider_route"] = "yfinance"
        return result.sort_values("date").reset_index(drop=True)


def _read_market_file(path: Path, symbol: str, start: str, end: str) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    elif path.suffix.lower() == ".parquet":
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            fallback = path.with_suffix(".csv")
            if fallback.exists():
                df = pd.read_csv(fallback)
            else:
                if isinstance(exc, ImportError):
                    raise DependencyUnavailableError(
                        "parquet",
                        "Parquet support requires optional pyarrow dependency.",
                    ) from exc
                raise
    else:
        raise ValueError(f"Unsupported file suffix: {path.suffix}")

    required = {"date", "open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input market file missing required columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"])
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    mask = (df["date"] >= start_ts) & (df["date"] <= end_ts)
    result = df.loc[mask].copy()
    result["symbol"] = result.get("symbol", symbol)
    if "adj_factor" in result.columns:
        result["adj_factor"] = pd.to_numeric(result["adj_factor"], errors="coerce")
    return result.sort_values("date").reset_index(drop=True)


PROVIDERS: dict[str, type[BaseProvider]] = {
    "demo": DemoProvider,
    "csv": CsvProvider,
    "local": LocalProvider,
    "duckdb": DuckdbProvider,
    "akshare": AkshareProvider,
    "yfinance": YfinanceProvider,
}


def get_provider(name: str) -> BaseProvider:
    provider_class = PROVIDERS.get(name)
    if provider_class is None:
        raise ValueError(f"Unsupported provider: {name}. Supported: {', '.join(PROVIDERS)}")
    return provider_class()
