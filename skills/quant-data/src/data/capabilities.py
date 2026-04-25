from __future__ import annotations

from agent4quant.config import list_configured_external_profiles


PROVIDER_CAPABILITIES: list[dict] = [
    {
        "provider": "akshare",
        "role": "primary_online",
        "source_type": "online",
        "markets": ["cn"],
        "intervals": ["1d"],
        "adjust_modes": ["none", "qfq", "hfq"],
        "workflows": ["data.fetch", "data.batch_fetch", "backtest", "alpha", "risk"],
        "supports_symbols_listing": False,
        "supports_manifest": False,
        "notes": [
            "A-share primary online provider",
            "Current implementation supports daily bars only",
        ],
    },
    {
        "provider": "yfinance",
        "role": "primary_online",
        "source_type": "online",
        "markets": ["us", "hk", "cn"],
        "intervals": ["1d"],
        "adjust_modes": ["none"],
        "workflows": ["data.fetch", "data.batch_fetch", "backtest", "alpha", "risk"],
        "supports_symbols_listing": False,
        "supports_manifest": False,
        "notes": [
            "HK numeric symbols are mapped to zero-padded .HK codes",
            "A-share SH symbols are mapped to Yahoo SS suffix",
            "BJ symbols are not supported",
        ],
    },
    {
        "provider": "demo",
        "role": "demo",
        "source_type": "synthetic",
        "markets": ["demo"],
        "intervals": ["1d", "5m"],
        "adjust_modes": ["none", "qfq", "hfq"],
        "workflows": ["data.fetch", "data.batch_fetch", "backtest", "alpha", "risk"],
        "supports_symbols_listing": False,
        "supports_manifest": False,
        "notes": [
            "Synthetic data for examples and tests",
        ],
    },
    {
        "provider": "csv",
        "role": "compatibility_input",
        "source_type": "file",
        "markets": ["user_managed"],
        "intervals": ["1d", "5m"],
        "adjust_modes": ["none", "qfq", "hfq"],
        "workflows": ["data.fetch", "backtest", "alpha", "risk", "data.validate", "data.repair"],
        "supports_symbols_listing": False,
        "supports_manifest": False,
        "notes": [
            "User-managed single-file compatibility input",
        ],
    },
    {
        "provider": "local",
        "role": "compatibility_input",
        "source_type": "directory",
        "markets": ["cn", "hk", "us", "user_managed"],
        "intervals": ["1d", "5m"],
        "adjust_modes": ["none", "qfq", "hfq"],
        "workflows": [
            "data.fetch",
            "data.batch_fetch",
            "data.symbols",
            "data.manifest",
            "data.index",
            "data.metadata",
            "data.validate",
            "data.repair",
            "backtest",
            "alpha",
            "risk",
        ],
        "supports_symbols_listing": True,
        "supports_manifest": True,
        "notes": [
            "Compatibility path for layered local directories",
        ],
    },
    {
        "provider": "duckdb",
        "role": "external_readonly",
        "source_type": "database",
        "markets": ["user_managed"],
        "intervals": ["5m"],
        "adjust_modes": ["none", "qfq", "hfq"],
        "workflows": ["data.fetch", "data.batch_fetch", "data.symbols", "data.manifest", "backtest", "alpha", "risk"],
        "supports_symbols_listing": True,
        "supports_manifest": True,
        "notes": [
            "User-owned DuckDB compatibility input",
            "Read-only in the current project positioning",
        ],
    },
]


def build_provider_capabilities() -> dict:
    return {
        "online_first": True,
        "primary_online_providers": ["akshare", "yfinance"],
        "compatibility_providers": ["csv", "local", "duckdb"],
        "configured_external_profiles": list_configured_external_profiles(),
        "providers": PROVIDER_CAPABILITIES,
    }
