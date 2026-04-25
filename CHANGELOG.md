# Changelog

## Unreleased

### Added

- GitHub 首发材料：`docs/release/GITHUB_LAUNCH_CHECKLIST.md`、`docs/release/GITHUB_FIRST_RELEASE_DRAFT.md`
- 增长与定位文档：`docs/implementation/16-客户增长与增收路线图.md`、`docs/implementation/17-定位与发布调整.md`

### Changed

- `README.md` 首屏与文档索引补齐 GitHub 首发入口
- `docs/release/CLAWHUB_RELEASE_CHECKLIST.md` 改为 GitHub / ClawHub 双入口发布清单
- `docs/showcase/README.md` 增加 GitHub 首发展示物建议
- `.github/ISSUE_TEMPLATE/feature-request.yml` 收口为本地优先工具站口径

## v0.2.0 - 2026-03-26

### Added

- `quant-data`：`akshare` / `yfinance` 在线数据主入口、provider capability matrix、扩展指标库
- external provider 最小配置层：`config/external-providers.toml`、`--provider-profile`、`configured_external_profiles`
- `quant-risk`：下行波动率、Sortino、Calmar、偏度、峰度、rolling 波动率、drawdown series
- FastAPI：`/report/generate`、`/tasks/data-fetch`、`/tasks/report`、`/tasks/summary`
- commercial：`/commercial/plans`、`/commercial/leads`、`/commercial/accounts`、`/commercial/subscriptions`
- showcase：`scripts/showcase/generate_showcase.sh`
- 官网新增：`cases.html`、`contact.html`
- 反馈闭环：`docs/implementation/14-Feedback-Loop.md` 与 GitHub issue templates
- 商业化最小闭环文档：`docs/implementation/15-Commercial-Flow.md`
- 文档一致性自动检查：`tests/test_docs_consistency.py`

### Changed

- 官网 `skills.html`、`pricing.html` 与各 `SKILL.md` 已统一到在线数据优先定位
- 定价页、案例页、咨询页已对齐计划矩阵和转化入口
- PDF 生成改为非 GUI backend，支持在任务线程中稳定导出
- `yfinance` 补了常见美股类股代码兼容，如 `BRK.B -> BRK-B`、`AAPL.US -> AAPL`
- `yfinance` 错误语义补了 `not_found` / `access_denied`
- handoff、API、README、TODO、showcase 文档已同步当前能力状态

### Fixed

- 修复后台线程生成 PDF 时的 Matplotlib macOS backend 错误
- 修复任务层缺少数据抓取、报告生成、过滤和汇总的问题
- 修复商业化入口只停留在文档、未落地到代码的问题

### Verification

- `PYTHONPATH=src python -m pytest tests/test_api_service.py tests/test_report_generator.py tests/test_risk_engine.py tests/test_alpha_engine.py tests/test_indicators.py tests/test_docs_consistency.py -q`

## v0.1.0 - 2026-03-26

### Added

- `quant-data`：本地目录分层、metadata、manifest、DuckDB 5m 主链路、离线 5m 导入、复权读取
- `quant-backtest`：成本模型、trade log、benchmark、sweep / compare、Plotly HTML 报告
- `quant-report`：Markdown / HTML / PDF 导出与水印
- `quant-alpha`：因子 IC / Rank IC / IR / Rank IR 分析
- `quant-risk`：VaR / CVaR / 波动率 / 最大回撤分析
- 真实 5m 回归脚本、展示样品整理脚本、发布与备案范围文档

### Changed

- `data sync-5m` 改为 `AkShare` 优先、`Eastmoney` 回退
- CLI 与文档统一到 `openclaw` 环境和 `PYTHONPATH=src` 运行方式
- 错误文案区分 `AkShare` 与 `Eastmoney` 来源，便于排障

### Fixed

- `akshare` 日线 provider 不再写死 `qfq`，会正确透传 `none/qfq/hfq`
- demo provider 改为稳定随机种子，避免不同进程产生漂移

### Verification

- `PYTHONPATH=src python -m pytest -q`
- 真实 5m 单日固定回归
- 真实 5m 多日多标的归档回归
