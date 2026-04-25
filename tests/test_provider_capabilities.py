from __future__ import annotations

from agent4quant.data.service import list_provider_capabilities


def test_provider_capabilities_matrix_marks_primary_online_routes() -> None:
    payload = list_provider_capabilities()

    assert payload["online_first"] is True
    assert payload["primary_online_providers"] == ["akshare", "yfinance"]
    assert "configured_external_profiles" in payload

    providers = {item["provider"]: item for item in payload["providers"]}
    assert providers["akshare"]["role"] == "primary_online"
    assert providers["akshare"]["markets"] == ["cn"]
    assert providers["yfinance"]["role"] == "primary_online"
    assert providers["yfinance"]["intervals"] == ["1d"]
    assert "cn" in providers["yfinance"]["markets"]
    assert providers["duckdb"]["role"] == "external_readonly"
    assert providers["duckdb"]["supports_manifest"] is True
