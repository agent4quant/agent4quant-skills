# Skill 发布页内容骨架

## 目标

为官网、ClawHub 或其他发布渠道提供统一的 Skill 落地页内容结构，突出“研究能力”和“在线工具”，避免把项目写成数据仓产品。

## 推荐页面结构

1. Hero
   - Skill 名称
   - 一句话价值主张
   - 适用用户
   - 合规声明
   - 安全声明
2. 解决什么问题
   - 典型研究场景
   - 现有手工流程痛点
   - Skill 输出结果
3. 输入 / 输出
   - 输入参数样例
   - 输出文件类型
   - 结果示意
4. 能力边界
   - 做什么
   - 不做什么
   - 联网边界
5. 示例产物
   - HTML 报告
   - PDF 报告
   - JSON 摘要
6. FAQ
   - 支持哪些市场
   - 依赖哪些在线数据源
   - 是否保存市场数据
   - 和云端量化平台有什么区别
   - 是否提供投资建议
7. 版本与更新
   - 当前版本
   - 最近更新点
   - 已知限制

## quant-data 发布页骨架

### 标题

`quant-data | 在线研究数据获取与整理工具`

### 一句话介绍

面向量化研究用户的数据准备工具，当前以 A 股 `AkShare` 在线取数为主，并已补港美股 `yfinance` 的最小版统一接入口。

### 安全声明

1. 官方 Skill 开源可审计
2. 联网仅用于用户显式选择的数据源调用
3. 不托管策略、研究结果与本地数据库

### 适合人群

1. 需要快速拿到研究数据的个人开发者
2. 不想自建数据服务、只想直接进入研究流程的用户
3. 已有自有文件或数据库、需要兼容输入的量化研究者

### 输入示例

```bash
# 安装
pip install -e skills/quant-data

# 数据获取
a4q data fetch \
  --provider akshare \
  --symbol 000001.SZ \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --adjust qfq \
  --indicators ma5,rsi,macd,boll,atr,kdj \
  --format json \
  --output output/000001-akshare.json

# 数据校验
a4q data validate \
  --provider csv \
  --input output/000001-akshare.json \
  --interval 1d

# 数据修复
a4q data repair \
  --provider csv \
  --input output/000001-akshare.json \
  --output output/repaired.csv \
  --duplicate-policy last \
  --null-policy drop
```

### 输出示例

1. 数据集 JSON / CSV
2. 技术指标结果
3. 免责声明与元信息

### 能力矩阵说明

1. A 股主在线入口：`AkShare`
2. 港美股主在线入口：`yfinance`
3. `demo` / `csv` / `local` / `duckdb` 仅作为兼容输入或外部只读接入
4. 项目不保存市场数据，不售卖原始数据
5. 支持 12 种技术指标：MA、RSI、MACD、Bollinger Bands、ATR、KDJ、Ret、Momentum、Volatility、ZScore、Volume MA、OBV
6. 支持数据校验（validate）和修复（repair）命令

## quant-backtest 发布页骨架

### 标题

`quant-backtest | 研究型策略回测与参数扫描工具`

### 一句话介绍

支持基础策略模板、benchmark、交易成本、参数扫描和多策略对比，适合快速验证研究假设。

### 输入示例

```bash
# 安装
pip install -e skills/quant-backtest

# 回测运行
a4q backtest run \
  --provider akshare \
  --symbol 000001.SZ \
  --benchmark-symbol 000300.SH \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --adjust qfq \
  --strategy sma_cross \
  --strategy-params fast=5,slow=20 \
  --commission-bps 2 \
  --slippage-bps 3 \
  --stamp-duty-bps 10 \
  --result-json output/000001-backtest.json \
  --report-html output/000001-backtest.html

# 参数扫描
a4q backtest sweep \
  --provider demo \
  --symbol DEMO.SH \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --strategy sma_cross \
  --fast-range 5,10,15 \
  --slow-range 20,30,40 \
  --output-json output/sweep.json

# 策略对比
a4q backtest compare \
  --provider demo \
  --symbol DEMO.SH \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --strategies sma_cross,rsi,momentum \
  --output-json output/compare.json
```

### 输出示例

1. `backtest.json`
2. `backtest.html`
3. `compare.json`
4. `compare.html`

## quant-report 发布页骨架

### 标题

`quant-report | 自动生成研究报告`

### 一句话介绍

把回测结果、风险指标和摘要自动整理为 Markdown、HTML、PDF 报告，适合展示、留档和对外说明。

### 输入示例

```bash
# 安装
pip install -e skills/quant-report

# 生成报告
a4q report generate \
  --input output/backtest.json \
  --output output/report.pdf \
  --title "Demo Research Report" \
  --format pdf \
  --watermark "A4Q Internal"

# 生成 HTML 报告
a4q report generate \
  --input output/backtest.json \
  --output output/report.html \
  --title "Research Report" \
  --format html

# 生成 Markdown 报告
a4q report generate \
  --input output/backtest.json \
  --output output/report.md \
  --title "Research Report" \
  --format markdown
```

### 输出示例

1. Markdown 报告
2. HTML 报告
3. PDF 报告

## 通用 FAQ

1. 是否内置或售卖市场数据？
   - 不内置、不售卖，项目核心是工具能力与研究工作流。
2. 当前支持哪些在线数据源？
   - A 股当前主入口是 `AkShare`；港美股 `yfinance` 已落地最小版 `1d`。
   - 如需完整矩阵，可引用 `a4q data providers --format json` 或 `GET /data/providers`
3. 是否支持用户自有文件或数据库？
   - 支持兼容输入，但这不是当前主产品路线。
4. 是否提供实盘、荐股或投资建议？
   - 不提供。
5. 当前推荐运行环境是什么？
   - `conda activate openclaw`，并使用 `PYTHONPATH=src` 运行。
6. 和云端量化平台有什么区别？
   - 当前项目定位是本地优先的工具链，不要求把策略上传到平台侧执行。
