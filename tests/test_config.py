from __future__ import annotations

from pathlib import Path

from agent4quant.config import (
    list_configured_external_profiles,
    resolve_data_root,
    resolve_duckdb_path,
    resolve_provider_market,
)


def test_resolve_external_provider_config_profiles(monkeypatch, tmp_path: Path) -> None:
    market_root = tmp_path / "market-data"
    market_root.mkdir()
    db_path = tmp_path / "research.duckdb"
    db_path.write_text("", encoding="utf-8")
    config_path = tmp_path / "external-providers.toml"
    config_path.write_text(
        f"""
[market_data]
root = "{market_root}"

[external_providers.local]
default_profile = "cn_daily"

[external_providers.local.cn_daily]
data_root = "{market_root}"
market = "cn"
description = "CN local profile"

[external_providers.duckdb.research]
db_path = "{db_path}"
market = "cn"
description = "DuckDB research profile"
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("A4Q_EXTERNAL_PROVIDER_CONFIG", str(config_path))

    assert resolve_data_root(provider_profile="cn_daily") == market_root.resolve()
    assert resolve_data_root() == market_root.resolve()
    assert resolve_duckdb_path(provider_profile="research") == db_path.resolve()
    assert resolve_provider_market(provider="local", provider_profile="cn_daily") == "cn"
    assert resolve_provider_market(provider="duckdb", provider_profile="research") == "cn"

    profiles = list_configured_external_profiles()
    assert len(profiles) == 2
    assert profiles[0]["provider"] == "duckdb"
    assert profiles[1]["default"] is True
