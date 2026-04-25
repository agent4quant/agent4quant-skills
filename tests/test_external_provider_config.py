from __future__ import annotations

from pathlib import Path

from agent4quant.data.service import available_symbols, fetch_dataset


def test_local_provider_profile_can_supply_root_and_market(monkeypatch, tmp_path: Path) -> None:
    market_root = tmp_path / "market-root"
    data_dir = market_root / "cn" / "1d"
    data_dir.mkdir(parents=True)
    (data_dir / "000001.SZ.csv").write_text(
        "date,open,high,low,close,volume\n2025-01-02,10,11,9,10.5,1000\n2025-01-03,10.5,11.2,10.2,11,1200\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "external-providers.toml"
    config_path.write_text(
        f"""
[external_providers.local]
default_profile = "cn_daily"

[external_providers.local.cn_daily]
data_root = "{market_root}"
market = "cn"
description = "Default CN local profile"
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("A4Q_EXTERNAL_PROVIDER_CONFIG", str(config_path))

    frame, metadata = fetch_dataset(
        provider="local",
        symbol="000001.SZ",
        start="2025-01-02",
        end="2025-01-03",
        interval="1d",
        indicators=[],
        provider_profile="cn_daily",
    )

    assert len(frame) == 2
    assert metadata["market"] == "cn"
    assert metadata["provider_profile"] == "cn_daily"

    symbols = available_symbols(provider="local", interval="1d", provider_profile="cn_daily")
    assert symbols == ["000001.SZ"]
