from __future__ import annotations

from pathlib import Path
import sys
import time
import types

from fastapi.testclient import TestClient
import pandas as pd

from agent4quant.api import app
from agent4quant.backtest.engine import run_backtest, write_backtest_result


client = TestClient(app)


def test_health_endpoint() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["service"] == "agent4quant-api"
    assert "task_store" in payload


def test_data_fetch_endpoint_demo() -> None:
    response = client.post(
        "/data/fetch",
        json={
            "provider": "demo",
            "symbol": "DEMO.SH",
            "start": "2025-01-01",
            "end": "2025-01-05",
            "interval": "1d",
            "indicators": ["ma5"],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] > 0
    assert payload["metadata"]["skill"] == "quant-data"
    assert "ma_5" in payload["rows"][0]


def test_backtest_endpoint_demo() -> None:
    response = client.post(
        "/backtest/run",
        json={
            "provider": "demo",
            "symbol": "DEMO.SH",
            "benchmark_symbol": "BMK.SH",
            "start": "2025-01-01",
            "end": "2025-03-31",
            "interval": "1d",
            "strategy": "sma_cross",
            "strategy_params": {"fast": 5, "slow": 20},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["strategy"] == "sma_cross"
    assert "metrics" in payload


def test_symbols_endpoint_local(tmp_path: Path) -> None:
    root = tmp_path / "cn" / "1d"
    root.mkdir(parents=True, exist_ok=True)
    (root / "000001.SZ.csv").write_text(
        "date,open,high,low,close,volume\n2025-01-01,1,2,0.5,1.5,10\n",
        encoding="utf-8",
    )

    response = client.post(
        "/data/symbols",
        json={
            "provider": "local",
            "interval": "1d",
            "data_root": str(tmp_path),
            "market": "cn",
        },
    )

    assert response.status_code == 200
    assert response.json()["symbols"] == ["000001.SZ"]


def test_data_providers_endpoint_exposes_capability_matrix() -> None:
    response = client.get("/data/providers")

    assert response.status_code == 200
    payload = response.json()
    assert payload["online_first"] is True
    assert payload["primary_online_providers"] == ["akshare", "yfinance"]
    providers = {item["provider"]: item for item in payload["providers"]}
    assert providers["yfinance"]["intervals"] == ["1d"]
    assert providers["duckdb"]["role"] == "external_readonly"
    assert "configured_external_profiles" in payload


def test_data_fetch_endpoint_returns_404_for_missing_file(tmp_path: Path) -> None:
    response = client.post(
        "/data/fetch",
        json={
            "provider": "local",
            "symbol": "000001.SZ",
            "start": "2025-01-01",
            "end": "2025-01-02",
            "interval": "1d",
            "indicators": [],
            "data_root": str(tmp_path),
            "market": "cn",
        },
    )

    assert response.status_code == 404


def test_async_backtest_task_lifecycle() -> None:
    submit = client.post(
        "/tasks/backtest",
        json={
            "provider": "demo",
            "symbol": "DEMO.SH",
            "benchmark_symbol": "BMK.SH",
            "start": "2025-01-01",
            "end": "2025-03-31",
            "interval": "1d",
            "strategy": "sma_cross",
            "strategy_params": {"fast": 5, "slow": 20},
        },
    )

    assert submit.status_code == 200
    task_id = submit.json()["task_id"]

    for _ in range(50):
        detail = client.get(f"/tasks/{task_id}")
        assert detail.status_code == 200
        payload = detail.json()
        if payload["status"] == "completed":
            assert payload["result"]["strategy"] == "sma_cross"
            break
        if payload["status"] == "failed":
            raise AssertionError(payload["error"])
        time.sleep(0.05)
    else:  # pragma: no cover - defensive
        raise AssertionError("Task did not complete in time")


def test_list_tasks_returns_submitted_items() -> None:
    response = client.get("/tasks")

    assert response.status_code == 200
    assert "items" in response.json()


def test_task_summary_and_filters() -> None:
    submit = client.post(
        "/tasks/data-fetch",
        json={
            "provider": "demo",
            "symbol": "DEMO.SH",
            "start": "2025-01-01",
            "end": "2025-01-05",
            "interval": "1d",
            "indicators": ["ma5"],
        },
    )

    assert submit.status_code == 200

    summary = client.get("/tasks/summary")
    assert summary.status_code == 200
    summary_payload = summary.json()
    assert summary_payload["total"] >= 1
    assert "data.fetch" in summary_payload["by_task_type"]

    listing = client.get("/tasks", params={"task_type": "data.fetch"})
    assert listing.status_code == 200
    payload = listing.json()
    assert payload["filters"]["task_type"] == "data.fetch"
    assert all(item["task_type"] == "data.fetch" for item in payload["items"])


def test_report_generate_endpoint(tmp_path: Path) -> None:
    backtest = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        benchmark_symbol="BMK.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
    )
    source = tmp_path / "demo-backtest.json"
    target = tmp_path / "demo-report.html"
    write_backtest_result(backtest, str(source))

    response = client.post(
        "/report/generate",
        json={
            "input_path": str(source),
            "output_path": str(target),
            "title": "Demo API Report",
            "output_format": "html",
            "watermark": "Internal Draft",
        },
    )

    assert response.status_code == 200
    assert response.json()["output_path"] == str(target)
    assert target.exists()
    assert "Demo API Report" in target.read_text(encoding="utf-8")


def test_commercial_endpoints_round_trip() -> None:
    plans = client.get("/commercial/plans")
    assert plans.status_code == 200
    assert len(plans.json()["plans"]) >= 3

    lead = client.post(
        "/commercial/leads",
        json={
            "name": "Alice",
            "email": "alice@example.com",
            "use_case": "Need API and PDF reporting",
            "plan_interest": "professional",
            "company": "A4Q Labs",
        },
    )
    assert lead.status_code == 200
    lead_payload = lead.json()
    assert lead_payload["plan_interest"] == "professional"

    account = client.post(
        "/commercial/accounts",
        json={
            "name": "Alice",
            "email": "alice@example.com",
            "plan": "professional",
            "company": "A4Q Labs",
        },
    )
    assert account.status_code == 200
    account_payload = account.json()
    assert "pdf_report" in account_payload["enabled_features"]

    subscription = client.post(
        "/commercial/subscriptions",
        json={
            "account_id": account_payload["account_id"],
            "plan": "professional",
            "billing_cycle": "monthly",
            "provider": "manual",
        },
    )
    assert subscription.status_code == 200
    subscription_payload = subscription.json()
    assert subscription_payload["status"] == "pending"

    detail = client.get(f"/commercial/accounts/{account_payload['account_id']}")
    assert detail.status_code == 200
    assert detail.json()["email"] == "alice@example.com"


def test_async_report_task_lifecycle(tmp_path: Path) -> None:
    backtest = run_backtest(
        provider="demo",
        symbol="DEMO.SH",
        benchmark_symbol="BMK.SH",
        start="2025-01-01",
        end="2025-03-31",
        interval="1d",
        strategy="sma_cross",
        strategy_params={"fast": 5, "slow": 20},
    )
    source = tmp_path / "demo-backtest.json"
    target = tmp_path / "demo-report.pdf"
    write_backtest_result(backtest, str(source))

    submit = client.post(
        "/tasks/report",
        json={
            "input_path": str(source),
            "output_path": str(target),
            "title": "Demo Task Report",
            "output_format": "pdf",
            "watermark": "Internal Draft",
        },
    )

    assert submit.status_code == 200
    task_id = submit.json()["task_id"]

    for _ in range(50):
        detail = client.get(f"/tasks/{task_id}")
        assert detail.status_code == 200
        payload = detail.json()
        if payload["status"] == "completed":
            assert payload["result"]["output_format"] == "pdf"
            break
        if payload["status"] == "failed":
            raise AssertionError(payload["error"])
        time.sleep(0.05)
    else:  # pragma: no cover - defensive
        raise AssertionError("Task did not complete in time")

    assert target.exists()
    assert target.read_bytes().startswith(b"%PDF")


def test_risk_endpoint_supports_v2_parameters() -> None:
    response = client.post(
        "/risk/analyze",
        json={
            "provider": "demo",
            "symbol": "DEMO.SH",
            "start": "2025-01-01",
            "end": "2025-03-31",
            "interval": "1d",
            "rolling_window": 10,
            "stress_shocks": [-0.03, -0.07],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["analysis_config"]["rolling_window"] == 10
    assert payload["analysis_config"]["stress_shocks"] == [-0.03, -0.07]
    assert len(payload["rolling_var"]) > 0
    assert payload["stress_results"][0]["shock"] == -0.03


def test_alpha_endpoint_supports_v2_parameters() -> None:
    response = client.post(
        "/alpha/analyze",
        json={
            "provider": "demo",
            "symbol": "DEMO.SH",
            "start": "2025-01-01",
            "end": "2025-03-31",
            "interval": "1d",
            "factors": ["ma_5", "rsi_14"],
            "indicators": ["ma5", "rsi"],
            "ic_window": 10,
            "quantiles": 4,
            "include_composite": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["config"]["quantiles"] == 4
    assert payload["config"]["include_composite"] is True
    assert payload["composite_factor"] == "composite_factor"
    assert len(payload["quantile_returns"]) == 3


def test_data_fetch_endpoint_yfinance(monkeypatch) -> None:
    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool = False,
        timeout: int = 10,
        multi_level_index: bool = True,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "Open": [100.0, 101.0],
                "High": [102.0, 103.0],
                "Low": [99.0, 100.0],
                "Close": [101.0, 102.0],
                "Volume": [1000, 1200],
            },
            index=pd.to_datetime(["2025-01-02", "2025-01-03"]),
        )

    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(download=_download, set_tz_cache_location=lambda _path: None),
    )

    response = client.post(
        "/data/fetch",
        json={
            "provider": "yfinance",
            "symbol": "AAPL",
            "start": "2025-01-02",
            "end": "2025-01-03",
            "interval": "1d",
            "indicators": ["ma5"],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 2
    assert payload["metadata"]["provider"] == "yfinance"
    assert payload["rows"][0]["symbol"] == "AAPL"


def test_data_fetch_endpoint_yfinance_rate_limit_returns_429(monkeypatch) -> None:
    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool = False,
        timeout: int = 10,
        multi_level_index: bool = True,
    ) -> pd.DataFrame:
        return pd.DataFrame()

    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(
            download=_download,
            set_tz_cache_location=lambda _path: None,
            shared=types.SimpleNamespace(_ERRORS={"AAPL": "YFRateLimitError('Too Many Requests')"}),
        ),
    )

    response = client.post(
        "/data/fetch",
        json={
            "provider": "yfinance",
            "symbol": "AAPL",
            "start": "2025-01-02",
            "end": "2025-01-03",
            "interval": "1d",
            "indicators": [],
        },
    )

    assert response.status_code == 429
    payload = response.json()
    assert payload["provider"] == "yfinance"
    assert payload["category"] == "rate_limit"
    assert payload["retryable"] is True


def test_data_fetch_endpoint_yfinance_not_found_returns_404(monkeypatch) -> None:
    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool = False,
        timeout: int = 10,
        multi_level_index: bool = True,
    ) -> pd.DataFrame:
        return pd.DataFrame()

    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(
            download=_download,
            set_tz_cache_location=lambda _path: None,
            shared=types.SimpleNamespace(_ERRORS={"AAPL": "possibly delisted; no price data found"}),
        ),
    )

    response = client.post(
        "/data/fetch",
        json={
            "provider": "yfinance",
            "symbol": "AAPL",
            "start": "2025-01-02",
            "end": "2025-01-03",
            "interval": "1d",
            "indicators": [],
        },
    )

    assert response.status_code == 404
    payload = response.json()
    assert payload["provider"] == "yfinance"
    assert payload["category"] == "not_found"
    assert payload["retryable"] is False


def test_data_fetch_endpoint_yfinance_timeout_returns_504(monkeypatch) -> None:
    def _download(
        tickers: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
        progress: bool,
        threads: bool = False,
        timeout: int = 10,
        multi_level_index: bool = True,
    ) -> pd.DataFrame:
        raise RuntimeError("curl: (28) Operation timed out after 10001 milliseconds")

    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        types.SimpleNamespace(download=_download, set_tz_cache_location=lambda _path: None),
    )

    response = client.post(
        "/data/fetch",
        json={
            "provider": "yfinance",
            "symbol": "AAPL",
            "start": "2025-01-02",
            "end": "2025-01-03",
            "interval": "1d",
            "indicators": [],
        },
    )

    assert response.status_code == 504
    payload = response.json()
    assert payload["provider"] == "yfinance"
    assert payload["category"] == "timeout"
