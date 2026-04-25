from __future__ import annotations


PLAN_DEFINITIONS: list[dict] = [
    {
        "plan": "community",
        "name": "Community",
        "price_label": "0",
        "billing_cycle": "monthly",
        "features": [
            "demo_data",
            "online_daily_fetch",
            "basic_backtest",
            "markdown_report",
            "docs_access",
        ],
        "limits": {
            "task_queue": "basic",
            "report_formats": ["markdown"],
            "support": "community",
        },
    },
    {
        "plan": "professional",
        "name": "Professional",
        "price_label": "99",
        "billing_cycle": "monthly",
        "features": [
            "demo_data",
            "online_daily_fetch",
            "basic_backtest",
            "markdown_report",
            "html_report",
            "pdf_report",
            "async_tasks",
            "showcase_assets",
            "provider_profiles",
            "priority_support",
        ],
        "limits": {
            "task_queue": "extended",
            "report_formats": ["markdown", "html", "pdf"],
            "support": "priority",
        },
    },
    {
        "plan": "research_suite",
        "name": "Research Suite",
        "price_label": "169",
        "billing_cycle": "monthly",
        "features": [
            "demo_data",
            "online_daily_fetch",
            "basic_backtest",
            "markdown_report",
            "html_report",
            "pdf_report",
            "async_tasks",
            "showcase_assets",
            "provider_profiles",
            "priority_support",
            "team_workspace",
            "commercial_api",
            "lead_intake",
        ],
        "limits": {
            "task_queue": "team",
            "report_formats": ["markdown", "html", "pdf"],
            "support": "team",
        },
    },
]


def list_plans() -> list[dict]:
    return PLAN_DEFINITIONS


def get_plan(plan: str) -> dict:
    normalized = plan.strip().lower()
    for item in PLAN_DEFINITIONS:
        if item["plan"] == normalized:
            return item
    raise ValueError(f"Unknown plan: {plan}")


def feature_enabled(plan: str, feature: str) -> bool:
    payload = get_plan(plan)
    return feature in payload["features"]
