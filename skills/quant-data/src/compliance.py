from __future__ import annotations

SITE_NAME = "Agent4Quant"
SITE_URL = "https://agent4quant.com"
DISCLAIMER = (
    "Disclaimer: This tool is for quantitative research and educational purposes only. "
    "It does not provide investment advice, real trading, or raw data services."
)


def build_metadata(skill: str, provider: str, interval: str) -> dict[str, str]:
    return {
        "site_name": SITE_NAME,
        "site_url": SITE_URL,
        "skill": skill,
        "provider": provider,
        "interval": interval,
        "disclaimer": DISCLAIMER,
    }
