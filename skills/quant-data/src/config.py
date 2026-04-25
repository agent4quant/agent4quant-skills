from __future__ import annotations

import os
from pathlib import Path
import tomllib


def _config_candidates() -> list[Path]:
    candidates: list[Path] = []
    for env_name in ("A4Q_CONFIG_PATH", "A4Q_EXTERNAL_PROVIDER_CONFIG"):
        raw = os.getenv(env_name)
        if raw:
            candidates.append(Path(raw).expanduser())
    candidates.extend(
        [
            Path("config/external-providers.toml"),
            Path("config/market-data.toml"),
        ]
    )
    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        deduped.append(candidate)
        seen.add(key)
    return deduped


def _load_config() -> dict:
    for candidate in _config_candidates():
        if not candidate.exists():
            continue
        with candidate.open("rb") as handle:
            return tomllib.load(handle)
    return {}


def _coerce_existing_path(raw: str | None, *, missing_message: str) -> Path | None:
    if not raw:
        return None
    path = Path(raw).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"{missing_message}: {path}")
    return path


def _profile_settings(provider: str, provider_profile: str | None = None) -> dict:
    config = _load_config()
    profiles = config.get("external_providers", {}).get(provider, {})
    if not isinstance(profiles, dict):
        return {}

    if provider_profile:
        payload = profiles.get(provider_profile, {})
        return payload if isinstance(payload, dict) else {}

    default_profile = profiles.get("default_profile")
    if isinstance(default_profile, str):
        payload = profiles.get(default_profile, {})
        return payload if isinstance(payload, dict) else {}
    return {}


def resolve_data_root(data_root: str | None = None, provider_profile: str | None = None) -> Path | None:
    candidate = data_root or os.getenv("A4Q_MARKET_DATA_ROOT")
    if candidate:
        return _coerce_existing_path(candidate, missing_message="Configured data root does not exist")

    profile = _profile_settings("local", provider_profile)
    if "data_root" in profile:
        return _coerce_existing_path(str(profile["data_root"]), missing_message="Configured local provider root does not exist")

    market_root = _load_config().get("market_data", {}).get("root")
    if market_root:
        return _coerce_existing_path(str(market_root), missing_message="Configured data root does not exist")
    return None


def resolve_duckdb_path(db_path: str | None = None, provider_profile: str | None = None) -> Path | None:
    if db_path:
        return _coerce_existing_path(db_path, missing_message="Configured DuckDB path does not exist")

    profile = _profile_settings("duckdb", provider_profile)
    if "db_path" in profile:
        return _coerce_existing_path(str(profile["db_path"]), missing_message="Configured DuckDB path does not exist")
    return None


def resolve_provider_market(market: str | None = None, provider: str | None = None, provider_profile: str | None = None) -> str | None:
    if market:
        return market
    if not provider:
        return None

    profile = _profile_settings(provider, provider_profile)
    resolved = profile.get("market")
    return str(resolved) if resolved else None


def list_configured_external_profiles() -> list[dict]:
    config = _load_config()
    providers = config.get("external_providers", {})
    if not isinstance(providers, dict):
        return []

    items: list[dict] = []
    for provider, raw_profiles in providers.items():
        if not isinstance(raw_profiles, dict):
            continue
        default_profile = raw_profiles.get("default_profile")
        for profile_name, payload in raw_profiles.items():
            if profile_name == "default_profile" or not isinstance(payload, dict):
                continue
            items.append(
                {
                    "provider": provider,
                    "profile": profile_name,
                    "default": profile_name == default_profile,
                    "market": payload.get("market"),
                    "data_root": payload.get("data_root"),
                    "db_path": payload.get("db_path"),
                    "description": payload.get("description"),
                }
            )
    items.sort(key=lambda item: (str(item["provider"]), str(item["profile"])))
    return items
