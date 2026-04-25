from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import uuid

from agent4quant.commercial.plans import get_plan


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _root(root: str | None = None) -> Path:
    path = Path(root or "output/commercial")
    path.mkdir(parents=True, exist_ok=True)
    return path


def _entity_dir(entity: str, root: str | None = None) -> Path:
    path = _root(root) / entity
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_record(entity: str, record_id: str, payload: dict, root: str | None = None) -> dict:
    target = _entity_dir(entity, root) / f"{record_id}.json"
    temp = target.with_suffix(".json.tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(target)
    return payload


def _read_record(entity: str, record_id: str, root: str | None = None) -> dict:
    target = _entity_dir(entity, root) / f"{record_id}.json"
    if not target.exists():
        raise FileNotFoundError(f"{entity.rstrip('s').capitalize()} not found: {record_id}")
    return json.loads(target.read_text(encoding="utf-8"))


def _list_records(entity: str, limit: int = 50, root: str | None = None) -> list[dict]:
    items = []
    for path in _entity_dir(entity, root).glob("*.json"):
        items.append(json.loads(path.read_text(encoding="utf-8")))
    items.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    return items[:limit]


def create_account(*, name: str, email: str, plan: str, company: str | None = None, root: str | None = None) -> dict:
    plan_payload = get_plan(plan)
    account_id = uuid.uuid4().hex
    record = {
        "account_id": account_id,
        "name": name.strip(),
        "email": email.strip().lower(),
        "company": company.strip() if company else None,
        "plan": plan_payload["plan"],
        "enabled_features": plan_payload["features"],
        "status": "active",
        "created_at": _utc_now(),
    }
    return _write_record("accounts", account_id, record, root)


def list_accounts(limit: int = 50, root: str | None = None) -> list[dict]:
    return _list_records("accounts", limit=limit, root=root)


def get_account(account_id: str, root: str | None = None) -> dict:
    return _read_record("accounts", account_id, root)


def create_lead(
    *,
    name: str,
    email: str,
    use_case: str,
    plan_interest: str,
    company: str | None = None,
    notes: str | None = None,
    source: str = "site",
    root: str | None = None,
) -> dict:
    lead_id = uuid.uuid4().hex
    record = {
        "lead_id": lead_id,
        "name": name.strip(),
        "email": email.strip().lower(),
        "company": company.strip() if company else None,
        "use_case": use_case.strip(),
        "plan_interest": get_plan(plan_interest)["plan"],
        "notes": notes.strip() if notes else None,
        "source": source,
        "status": "new",
        "created_at": _utc_now(),
    }
    return _write_record("leads", lead_id, record, root)


def list_leads(limit: int = 50, root: str | None = None) -> list[dict]:
    return _list_records("leads", limit=limit, root=root)


def create_subscription(
    *,
    account_id: str,
    plan: str,
    billing_cycle: str = "monthly",
    provider: str = "manual",
    root: str | None = None,
) -> dict:
    account = get_account(account_id, root)
    plan_payload = get_plan(plan)
    subscription_id = uuid.uuid4().hex
    record = {
        "subscription_id": subscription_id,
        "account_id": account["account_id"],
        "plan": plan_payload["plan"],
        "billing_cycle": billing_cycle,
        "provider": provider,
        "status": "pending",
        "enabled_features": plan_payload["features"],
        "created_at": _utc_now(),
    }
    return _write_record("subscriptions", subscription_id, record, root)


def list_subscriptions(limit: int = 50, root: str | None = None) -> list[dict]:
    return _list_records("subscriptions", limit=limit, root=root)


def get_subscription(subscription_id: str, root: str | None = None) -> dict:
    return _read_record("subscriptions", subscription_id, root)
