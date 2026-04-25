# Agent4Quant TODO

本文档只保留“后续计划”。当前项目已明确转向“本地优先的量化 Skill 工具站”，不再把项目内数据存储、数据仓治理和落库能力作为主路线。

优先级说明：

- `P0`：当前必须推进，直接影响产品方向与核心交付
- `P1`：高优先级，影响首个稳定可发布版本
- `P2`：中优先级，偏工程化和平台化
- `P3`：后续扩展
- `P4`：外部依赖或商业化后置事项

## 当前状态摘要

- [x] 项目定位已调整为本地优先的量化 Skill 工具站与官网，不再以项目内市场数据仓库为核心。
- [x] 当前研究链路已覆盖 `quant-data`、`quant-backtest`、`quant-report`、`quant-alpha`、`quant-risk`、API、任务层。
- [x] A 股当前主数据入口明确为 `AkShare`。
- [x] 港美股主数据入口 `yfinance` 最小版已接入代码，当前支持 `1d` 取数。
- [x] `demo` / `csv` / `local` / `duckdb` 仍保留，作为兼容输入、测试和外部数据适配路径，不再作为产品主路线。
- [x] 自动化测试当前为 `104 passed`。

## P0 立即执行

- [x] 统一“去社区化、工具站化、核心三件套优先”的产品口径。
  目标：
  1. 首页、README、handoff、路线图统一使用“本地优先的 Skill 工具站”表达
  2. 对外主叙事聚焦 `quant-data` / `quant-backtest` / `quant-report`
  3. `quant-alpha` / `quant-risk` 作为增强模块而不是首页第一入口
- [x] 落地客户增长与增收路线图。
  目标：
  1. 明确未来 90 天的内容、案例、咨询、模板包与服务包节奏
  2. 明确每周产出、每月增收动作和线索转化目标
- [x] 启用 GitHub 作为第一批对外发布阵地。
  目标：
  1. 完成仓库对外主页、项目说明和信任体系说明
  2. 准备首批 Release、Issue 模板和 Showcase 入口
  3. 先于官网正式上线启动对外内容输出
- [x] 聚焦核心三件套的对外案例与成交路径。
  范围：
  1. `quant-data`：在线研究数据准备案例、兼容输入案例
  2. `quant-backtest`：策略验证案例、多策略对比案例
  3. `quant-report`：PDF / HTML 报告交付案例与模板包样品
- [x] 接入 `yfinance` provider，作为港美股主数据入口。
  已落地：
  1. 支持 `1d`
  2. 已接 `data fetch`
  3. 已可打通 `backtest`、`alpha`、`risk`
- [x] 已把在线数据源能力矩阵写清楚，并同步到 README、API、Skill 发布文案。
  已落地：
  1. A 股：`akshare`
  2. 港美股：`yfinance`
  3. 外部数据：用户自有文件 / 自有数据库，按兼容输入处理
  4. 已补 `13-Provider-Capability-Matrix.md`
- [x] 清理“项目内存储主链路”遗留文档口径。
  范围：
  1. Skill 文案
  2. 需求文档
  3. handoff
  4. 示例命令

## P1 高优先级

- [x] 为 `yfinance` 补 CLI、API、测试与示例命令。
  已完成最小验收：
  1. `data fetch`
  2. `backtest run`
  3. `alpha analyze`
  4. `risk analyze`
- [x] 为在线取数链路补更清晰的错误语义。
  已完成：
  1. `yfinance` 上游限流语义
  2. `yfinance` timeout / network 语义
  3. API `429` / `504` / `503` 状态映射
  4. CLI 失败时无 traceback 的简洁输出
- [x] 重写官网与 Skill 发布页的“数据能力描述”。
  原则：
  1. 不承诺项目内置历史库
  2. 不承诺项目内保存数据
  3. 强调工具能力、流程提效、研究分析
- [x] 已梳理“外部自有数据库只读适配”的接口边界。
  已落地：
  1. 不在当前项目内保存数据
  2. 不做数据库托管
  3. 已补 `12-External-Provider-Design.md`

## P2 中优先级

- [x] FastAPI 服务层已落地。
- [x] 本地异步任务层已落地。
- [x] alpha / risk 第二版能力已落地。
- [x] 继续完善 API / Task 编排能力，适配 Skill 场景。
- [x] 继续沉淀 showcase 样品、案例页素材与发布包装。
- [x] 补 provider 文档一致性自动检查。
  当前已完成：
  1. provider capability source
  2. CLI / API 输出入口
  3. 基础测试覆盖
  4. `data-fetch` / `report` 异步任务
  5. `/tasks` 过滤与 `/tasks/summary`
  6. `showcase/generate_showcase.sh`
  7. 文档一致性自动检查
- [x] 继续扩展 `yfinance` 的市场覆盖与错误语义。
  当前已完成：
  1. `1d` 取数
  2. CLI / API / 基础研究链路
  3. HK 数字代码映射
  4. A 股 `SH -> SS` 映射
  5. MultiIndex 列兼容
  6. 上游限流 / 连接错误语义收敛
  7. HK 路线失败时自动 fallback 到 `akshare.stock_hk_daily`
  8. 美股类股 symbol 兼容，如 `BRK.B -> BRK-B`
  9. `not_found` / `access_denied` 错误语义

## P3 后续扩展

- [x] 增加更完整的因子库和组合研究工作流。
- [x] 增加更完整的风险暴露与研究展示。
- [x] 增加 ClawHub 发布打包流程。
- [x] 建立用户反馈闭环：邮箱、表单、FAQ、使用日志。

## P4 外部依赖 / 商业化后置

- [ ] 备案通过后上线 `skills.html`、`pricing.html` 与案例页。
  阻断原因：
  1. 需等待备案通过后再正式对外发布
- [x] 已补最小商业化记录层，支持计划矩阵、线索、账号与手动开通记录。
  当前说明：
  1. 在线支付与收款暂不继续开发
  2. 当前以 `info@agent4quant.com` 和人工开通流程承接
  3. 现有记录层仅保留为咨询与运营支撑
- [x] 增加账户体系和免费版 / 专业版功能开关。
  已落地：
  1. 账号记录模型
  2. 计划矩阵
  3. feature flags
- [x] 增加企业版预约入口和 CRM 流程。
  已落地：
  1. `contact.html`
  2. `commercial/leads`
  3. CLI 留资入口
- [x] 完善官网正式转化流程与咨询入口。
  已落地：
  1. `cases.html`
  2. `contact.html`
  3. pricing CTA

## 当前推荐执行顺序

1. 先完成工具站定位收口、增长路线图与 GitHub 对外包装。
2. 然后补核心三件套 showcase、案例与模板包样品。
3. 再按真实需求继续扩 external provider 的只读 adapter。
4. 最后等待备案通过后正式上线页面。

## 当前聚焦的 3 个 Skills

### `quant-data`

- [x] 补首个 GitHub / 官网案例：A 股在线研究数据准备
- [x] 补自有 CSV / DuckDB 兼容输入案例
- [x] 补“AkShare / yfinance / 外部输入”三层边界图

### `quant-backtest`

- [x] 补首个 GitHub / 官网案例：从取数到回测到 Plotly HTML
- [x] 补多策略对比截图和输出样品
- [x] 补成本模型与 benchmark 的展示页摘要

### `quant-report`

- [x] 补 PDF / HTML 报告样品说明页
- [x] 补报告模板包样品与升级说明
- [x] 补“研究交付物”视角的 FAQ 和对外截图

## 维护规则

- 完成的任务直接勾选，不删除。
- 只记录未完成或仍需持续推进的事项。
- 如果任务被外部依赖阻断，要在条目后补阻断原因。
- 面向发布的任务，完成时必须同步补文档、样品或测试。
