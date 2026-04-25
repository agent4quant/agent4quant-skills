from __future__ import annotations

import json

import pandas as pd
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from agent4quant import __version__
from agent4quant.alpha.engine import analyze_alpha
from agent4quant.backtest.engine import run_backtest
from agent4quant.commercial import (
    create_account,
    create_lead,
    create_subscription,
    get_account,
    get_subscription,
    list_accounts,
    list_leads,
    list_plans,
    list_subscriptions,
)
from agent4quant.compliance import DISCLAIMER
from agent4quant.data.service import available_symbols, build_data_manifest, fetch_dataset, list_provider_capabilities
from agent4quant.errors import DependencyUnavailableError, ExternalProviderError
from agent4quant.report.generator import generate_report
from agent4quant.risk.engine import analyze_risk
from agent4quant.tasks import LocalTaskQueue


def _frame_rows(frame: pd.DataFrame) -> list[dict]:
    payload = frame.copy()
    if "date" in payload.columns:
        payload["date"] = pd.to_datetime(payload["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return json.loads(payload.to_json(orient="records"))


class DataFetchRequest(BaseModel):
    provider: str
    symbol: str
    start: str
    end: str
    interval: str
    indicators: list[str] = Field(default_factory=list)
    input_path: str | None = None
    data_root: str | None = None
    db_path: str | None = None
    adjust: str = "none"
    market: str | None = None
    provider_profile: str | None = None


class DataManifestRequest(BaseModel):
    provider: str
    interval: str
    data_root: str | None = None
    db_path: str | None = None
    market: str | None = None
    provider_profile: str | None = None


class SymbolsRequest(BaseModel):
    provider: str
    interval: str
    data_root: str | None = None
    db_path: str | None = None
    market: str | None = None
    provider_profile: str | None = None


class BacktestRequest(BaseModel):
    provider: str
    symbol: str
    start: str
    end: str
    interval: str
    strategy: str
    strategy_params: dict[str, float] = Field(default_factory=dict)
    input_path: str | None = None
    data_root: str | None = None
    db_path: str | None = None
    adjust: str = "none"
    market: str | None = None
    provider_profile: str | None = None
    costs: dict[str, float] | None = None
    benchmark_symbol: str | None = None


class RiskRequest(BaseModel):
    provider: str | None = None
    symbol: str | None = None
    start: str | None = None
    end: str | None = None
    interval: str = "1d"
    confidence_level: float = 0.95
    input_path: str | None = None
    data_root: str | None = None
    db_path: str | None = None
    mode: str = "market"
    source: str = "asset"
    adjust: str = "none"
    market: str | None = None
    provider_profile: str | None = None
    rolling_window: int = 20
    stress_shocks: list[float] | None = None


class AlphaRequest(BaseModel):
    provider: str
    symbol: str
    start: str
    end: str
    interval: str
    factors: list[str]
    indicators: list[str] = Field(default_factory=list)
    horizon: int = 1
    ic_window: int = 20
    input_path: str | None = None
    data_root: str | None = None
    db_path: str | None = None
    adjust: str = "none"
    market: str | None = None
    provider_profile: str | None = None
    quantiles: int = 5
    include_composite: bool = True


class ReportGenerateRequest(BaseModel):
    input_path: str
    output_path: str
    title: str
    output_format: str = "markdown"
    watermark: str | None = "Agent4Quant Research Draft"


class CommercialLeadRequest(BaseModel):
    name: str
    email: str
    use_case: str
    plan_interest: str
    company: str | None = None
    notes: str | None = None
    source: str = "site"


class CommercialAccountRequest(BaseModel):
    name: str
    email: str
    plan: str
    company: str | None = None


class CommercialSubscriptionRequest(BaseModel):
    account_id: str
    plan: str
    billing_cycle: str = "monthly"
    provider: str = "manual"


class TaskSubmitResponse(BaseModel):
    task_id: str
    task_type: str
    status: str


def _run_data_fetch(payload: DataFetchRequest) -> dict:
    frame, metadata = fetch_dataset(**payload.model_dump())
    return {
        "metadata": metadata,
        "rows": _frame_rows(frame),
        "count": len(frame),
    }


def _run_report_generate(payload: ReportGenerateRequest) -> dict:
    generate_report(
        payload.input_path,
        payload.output_path,
        payload.title,
        output_format=payload.output_format,
        watermark=payload.watermark,
    )
    return {
        "input_path": payload.input_path,
        "output_path": payload.output_path,
        "title": payload.title,
        "output_format": payload.output_format,
        "watermark": payload.watermark,
    }


def create_app() -> FastAPI:
    app = FastAPI(
        title="Agent4Quant API",
        version=__version__,
        description="Compliance-first API wrapper for agent4quant research workflows.",
    )
    app.state.task_queue = LocalTaskQueue()

    @app.exception_handler(FileNotFoundError)
    async def _handle_not_found(_request: Request, exc: FileNotFoundError):
        return JSONResponse(status_code=404, content={"error": str(exc)})

    @app.exception_handler(ValueError)
    async def _handle_value_error(_request: Request, exc: ValueError):
        return JSONResponse(status_code=400, content={"error": str(exc)})

    @app.exception_handler(DependencyUnavailableError)
    async def _handle_dependency_error(_request: Request, exc: DependencyUnavailableError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": str(exc), "code": exc.error_code, "component": exc.component},
        )

    @app.exception_handler(ExternalProviderError)
    async def _handle_external_provider_error(_request: Request, exc: ExternalProviderError):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": str(exc),
                "code": exc.error_code,
                "provider": exc.provider,
                "category": exc.category,
                "retryable": exc.retryable,
            },
        )

    @app.exception_handler(RuntimeError)
    async def _handle_runtime_error(_request: Request, exc: RuntimeError):
        return JSONResponse(status_code=502, content={"error": str(exc)})

    @app.get("/health")
    async def health() -> dict:
        queued_tasks = len(app.state.task_queue.list(limit=1000))
        return {
            "status": "ok",
            "version": __version__,
            "service": "agent4quant-api",
            "disclaimer": DISCLAIMER,
            "task_store": str(app.state.task_queue.root),
            "known_tasks": queued_tasks,
        }

    @app.post("/data/fetch")
    async def data_fetch(payload: DataFetchRequest) -> dict:
        return _run_data_fetch(payload)

    @app.post("/data/manifest")
    async def data_manifest(payload: DataManifestRequest) -> dict:
        return build_data_manifest(**payload.model_dump())

    @app.post("/data/symbols")
    async def data_symbols(payload: SymbolsRequest) -> dict:
        symbols = available_symbols(**payload.model_dump())
        return {
            "provider": payload.provider,
            "interval": payload.interval,
            "market": payload.market,
            "symbols": symbols,
            "count": len(symbols),
        }

    @app.get("/data/providers")
    async def data_providers() -> dict:
        return list_provider_capabilities()

    @app.get("/commercial/plans")
    async def commercial_plans() -> dict:
        return {"plans": list_plans()}

    @app.post("/commercial/leads")
    async def commercial_lead(payload: CommercialLeadRequest) -> dict:
        return create_lead(**payload.model_dump())

    @app.get("/commercial/leads")
    async def commercial_leads(limit: int = 20) -> dict:
        items = list_leads(limit=limit)
        return {"count": len(items), "items": items}

    @app.post("/commercial/accounts")
    async def commercial_account(payload: CommercialAccountRequest) -> dict:
        return create_account(**payload.model_dump())

    @app.get("/commercial/accounts")
    async def commercial_accounts(limit: int = 20) -> dict:
        items = list_accounts(limit=limit)
        return {"count": len(items), "items": items}

    @app.get("/commercial/accounts/{account_id}")
    async def commercial_account_detail(account_id: str) -> dict:
        return get_account(account_id)

    @app.post("/commercial/subscriptions")
    async def commercial_subscription(payload: CommercialSubscriptionRequest) -> dict:
        return create_subscription(**payload.model_dump())

    @app.get("/commercial/subscriptions")
    async def commercial_subscriptions(limit: int = 20) -> dict:
        items = list_subscriptions(limit=limit)
        return {"count": len(items), "items": items}

    @app.get("/commercial/subscriptions/{subscription_id}")
    async def commercial_subscription_detail(subscription_id: str) -> dict:
        return get_subscription(subscription_id)

    @app.post("/backtest/run")
    async def backtest_run(payload: BacktestRequest) -> dict:
        return run_backtest(**payload.model_dump())

    @app.post("/risk/analyze")
    async def risk_analyze(payload: RiskRequest) -> dict:
        return analyze_risk(**payload.model_dump())

    @app.post("/alpha/analyze")
    async def alpha_analyze(payload: AlphaRequest) -> dict:
        return analyze_alpha(**payload.model_dump())

    @app.post("/report/generate")
    async def report_generate(payload: ReportGenerateRequest) -> dict:
        return _run_report_generate(payload)

    @app.post("/tasks/backtest", response_model=TaskSubmitResponse)
    async def task_backtest(payload: BacktestRequest) -> dict:
        task = app.state.task_queue.submit(
            task_type="backtest.run",
            payload=payload.model_dump(),
            runner=lambda: run_backtest(**payload.model_dump()),
        )
        return {
            "task_id": task["task_id"],
            "task_type": task["task_type"],
            "status": task["status"],
        }

    @app.post("/tasks/risk", response_model=TaskSubmitResponse)
    async def task_risk(payload: RiskRequest) -> dict:
        task = app.state.task_queue.submit(
            task_type="risk.analyze",
            payload=payload.model_dump(),
            runner=lambda: analyze_risk(**payload.model_dump()),
        )
        return {
            "task_id": task["task_id"],
            "task_type": task["task_type"],
            "status": task["status"],
        }

    @app.post("/tasks/alpha", response_model=TaskSubmitResponse)
    async def task_alpha(payload: AlphaRequest) -> dict:
        task = app.state.task_queue.submit(
            task_type="alpha.analyze",
            payload=payload.model_dump(),
            runner=lambda: analyze_alpha(**payload.model_dump()),
        )
        return {
            "task_id": task["task_id"],
            "task_type": task["task_type"],
            "status": task["status"],
        }

    @app.post("/tasks/data-fetch", response_model=TaskSubmitResponse)
    async def task_data_fetch(payload: DataFetchRequest) -> dict:
        task = app.state.task_queue.submit(
            task_type="data.fetch",
            payload=payload.model_dump(),
            runner=lambda: _run_data_fetch(payload),
        )
        return {
            "task_id": task["task_id"],
            "task_type": task["task_type"],
            "status": task["status"],
        }

    @app.post("/tasks/report", response_model=TaskSubmitResponse)
    async def task_report(payload: ReportGenerateRequest) -> dict:
        task = app.state.task_queue.submit(
            task_type="report.generate",
            payload=payload.model_dump(),
            runner=lambda: _run_report_generate(payload),
        )
        return {
            "task_id": task["task_id"],
            "task_type": task["task_type"],
            "status": task["status"],
        }

    @app.get("/tasks")
    async def list_tasks(limit: int = 20, status: str | None = None, task_type: str | None = None) -> dict:
        items = app.state.task_queue.list(limit=limit, status=status, task_type=task_type)
        return {
            "count": len(items),
            "filters": {
                "status": status,
                "task_type": task_type,
            },
            "items": items,
        }

    @app.get("/tasks/summary")
    async def task_summary() -> dict:
        return app.state.task_queue.summary()

    @app.get("/tasks/{task_id}")
    async def get_task(task_id: str) -> dict:
        return app.state.task_queue.get(task_id)

    return app


app = create_app()
