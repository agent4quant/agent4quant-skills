from __future__ import annotations

from pathlib import Path

from agent4quant.commercial import (
    create_account,
    create_lead,
    create_subscription,
    feature_enabled,
    get_plan,
    list_accounts,
    list_leads,
    list_plans,
    list_subscriptions,
)


def test_plan_registry_exposes_feature_flags() -> None:
    assert len(list_plans()) >= 3
    assert get_plan("professional")["plan"] == "professional"
    assert feature_enabled("professional", "pdf_report") is True
    assert feature_enabled("community", "pdf_report") is False


def test_commercial_storage_round_trip(tmp_path: Path) -> None:
    account = create_account(
        name="Alice",
        email="alice@example.com",
        plan="professional",
        company="A4Q Labs",
        root=str(tmp_path),
    )
    lead = create_lead(
        name="Alice",
        email="alice@example.com",
        use_case="Need PDF and API",
        plan_interest="professional",
        root=str(tmp_path),
    )
    subscription = create_subscription(
        account_id=account["account_id"],
        plan="professional",
        root=str(tmp_path),
    )

    assert account["status"] == "active"
    assert lead["status"] == "new"
    assert subscription["status"] == "pending"
    assert len(list_accounts(root=str(tmp_path))) == 1
    assert len(list_leads(root=str(tmp_path))) == 1
    assert len(list_subscriptions(root=str(tmp_path))) == 1
