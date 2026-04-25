from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (PROJECT_ROOT / path).read_text(encoding="utf-8")


def test_skill_page_uses_online_first_positioning() -> None:
    content = _read("apps/site/skills.html")
    cases = _read("apps/site/cases.html")
    contact = _read("apps/site/contact.html")
    home = _read("apps/site/index.html")
    trust = _read("apps/site/trust.html")
    readme = _read("README.md")
    todo = _read("TODO.md")
    github_launch = _read("docs/release/GITHUB_LAUNCH_CHECKLIST.md")
    github_release = _read("docs/release/GITHUB_FIRST_RELEASE_DRAFT.md")
    github_profile = _read("docs/release/GITHUB_PROFILE_COPY.md")
    github_posts = _read("docs/release/GITHUB_POST_TEMPLATES.md")
    publishing_sequence = _read("docs/release/PUBLISHING_SEQUENCE.md")
    screenshot_checklist = _read("docs/release/SCREENSHOT_CHECKLIST.md")
    publishing_bundle_map = _read("docs/release/PUBLISHING_BUNDLE_MAP.md")
    playbook = _read("docs/showcase/CORE_SKILLS_PLAYBOOK.md")
    quant_data_case = _read("docs/showcase/quant-data-case.md")
    quant_data_compat = _read("docs/showcase/quant-data-compat-case.md")
    quant_data_boundary = _read("docs/showcase/quant-data-boundary.md")
    quant_backtest_case = _read("docs/showcase/quant-backtest-case.md")
    quant_backtest_compare = _read("docs/showcase/quant-backtest-compare-case.md")
    quant_backtest_cost = _read("docs/showcase/quant-backtest-benchmark-cost.md")
    quant_report_case = _read("docs/showcase/quant-report-case.md")
    quant_report_pack = _read("docs/showcase/quant-report-template-pack.md")
    quant_report_faq = _read("docs/showcase/quant-report-faq.md")

    assert "AkShare" in content
    assert "yfinance" in content
    assert "外部兼容输入" in content
    assert "DuckDB 主链路" not in content
    assert "历史仓库治理" not in content
    assert "案例页" in cases
    assert "咨询与预约入口" in contact
    assert "策略不上云" in home
    assert "官方 Skills 开源可审计" in trust
    assert "联网仅用于用户显式选择的数据源调用" in trust
    assert "quant-data" in cases
    assert "quant-backtest" in cases
    assert "quant-report" in cases
    assert "本地优先的量化 Skill 工具站" in readme
    assert "量化 Skill 社区场景" not in readme
    assert "本地优先的量化 Skill 工具站" in todo
    assert "GitHub 启动清单" in github_launch
    assert "首个 GitHub Release" in github_launch
    assert "Local-first Quant Research Toolkit" in github_release
    assert "info@agent4quant.com" in github_release
    assert "Local-first quant research toolkit" in github_profile
    assert "策略不上云" in github_posts
    assert "GitHub 首发" in publishing_sequence
    assert "quant-data" in screenshot_checklist
    assert "demo-report.pdf" in publishing_bundle_map
    assert "quant-data" in playbook
    assert "quant-backtest" in playbook
    assert "quant-report" in playbook
    assert "A 股在线研究准备" in quant_data_case
    assert "DuckDB" in quant_data_compat
    assert "Out of Scope" in quant_data_boundary
    assert "Plotly HTML" in quant_backtest_case
    assert "多策略对比" in quant_backtest_compare
    assert "成本模型" in quant_backtest_cost
    assert "研究交付物生成器" in quant_report_case
    assert "模板包" in quant_report_pack
    assert "FAQ" in quant_report_faq


def test_skill_docs_match_current_feature_state() -> None:
    quant_data = _read("skills/quant-data/SKILL.md")
    quant_backtest = _read("skills/quant-backtest/SKILL.md")
    quant_report = _read("skills/quant-report/SKILL.md")
    quant_alpha = _read("skills/quant-alpha/SKILL.md")
    quant_risk = _read("skills/quant-risk/SKILL.md")

    assert "akshare" in quant_data.lower()
    assert "yfinance" in quant_data.lower()
    assert "项目内保存" in quant_data
    assert "DuckDB 5m 主链路" not in quant_data
    assert "同步到 DuckDB 后再读取" not in quant_data

    assert "PDF" in quant_report
    assert "当前不包含 PDF 导出" not in quant_report
    assert "当前未实现" not in quant_report

    for content in (
        quant_data,
        quant_backtest,
        quant_report,
        quant_alpha,
        quant_risk,
    ):
        assert "https://agent4quant.com" in content
        assert "info@agent4quant.com" in content
        assert "更多高级功能请前往 agent4quant.com" in content
        assert "安全声明" in content
        assert "不托管策略" in content

    assert "压力测试" in quant_risk
    assert "rolling VaR" in quant_risk or "rolling" in quant_risk
    assert "当前不包含压力测试" not in quant_risk
