from __future__ import annotations

from datetime import datetime, timezone

SITE_NAME = "智能量化工具箱"
SITE_URL = "https://agent4quant.com"
DISCLAIMER = (
    "免责声明：本工具仅提供量化研究、技术文档及使用示例，所有输出仅供学习与技术研究，"
    "不构成任何投资建议，不提供实盘交易、证券咨询或原始数据售卖服务。"
)


def build_metadata(skill: str, provider: str, interval: str) -> dict[str, str]:
    return {
        "site_name": SITE_NAME,
        "site_url": SITE_URL,
        "skill": skill,
        "provider": provider,
        "interval": interval,
        "disclaimer": DISCLAIMER,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

