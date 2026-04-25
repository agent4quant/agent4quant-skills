# agent4quant-skills

量化研究 Skills 工具集，服务本地优先的量化研究工作流。

> Local-first quant research toolkit. Strategy stays local.

## 项目背景

**为什么是这个项目？**

Agent4Quant 不追求"大而全量化平台"，而是把最常见、最重复、最值得标准化的三步做扎实：

```
data → backtest → report
```

真正价值在于：

1. 少重复拼装数据、回测、报告链路
2. 少维护临时脚本和不稳定输出
3. 直接获得可复用的样品、模板和交付物
4. 在本地完成研究，不把策略和结果交给平台托管

## 三大核心 Skill

### 1. [quant-data](skills/quant-data/) - 数据获取

量化研究数据获取与整理工具。

**功能：**
- 在线数据源：A 股 AkShare、港美股 yfinance
- 本地兼容：demo、csv、local、duckdb
- 技术指标：MA、RSI、MACD、Bollinger Bands、ATR、KDJ、Ret、Momentum、Volatility、ZScore、Volume MA、OBV
- 数据校验与修复

**快速开始：**
```bash
pip install -e skills/quant-data

a4q data fetch \
  --provider akshare \
  --symbol 000001.SZ \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --indicators ma5,rsi,macd \
  --format json \
  --output data.json
```

### 2. [quant-backtest](skills/quant-backtest/) - 策略回测

研究型策略回测与参数扫描工具。

**功能：**
- 单策略回测：SMA Cross、RSI、Momentum
- 参数扫描与策略对比
- Benchmark 对照与交易成本模型
- Plotly HTML 可视化报告

**快速开始：**
```bash
pip install -e skills/quant-backtest

a4q backtest run \
  --provider akshare \
  --symbol 000001.SZ \
  --benchmark-symbol 000300.SH \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --strategy sma_cross \
  --strategy-params fast=5,slow=20 \
  --result-json result.json \
  --report-html result.html
```

### 3. [quant-report](skills/quant-report/) - 报告生成

自动生成研究报告工具。

**功能：**
- 多格式输出：Markdown、HTML、PDF
- 水印控制
- 完整摘要：成本模型、benchmark、交易摘要
- 自动保留研究用途免责声明

**快速开始：**
```bash
pip install -e skills/quant-report

a4q report generate \
  --input result.json \
  --output report.pdf \
  --title "Research Report" \
  --format pdf \
  --watermark "Internal Use"
```

## 信任与边界

1. 官方 Skills 开源可审计
2. 计算默认在用户本地完成
3. 不托管策略、回测结果与研究数据
4. 联网仅用于用户显式选择的数据源调用
5. 不提供实盘、荐股或投资建议

## 本地执行架构

```
[User Strategy / Research Files]
          |
          v
[OpenClaw + Agent4Quant Official Skills] --> [Local Outputs: JSON / HTML / PDF]
          |
          +-- optional online fetch --> [AkShare / yfinance]

- no strategy hosting
- no research data custody
- no hidden telemetry
```

## 当前最适合谁

1. 需要本地研究闭环的个人量化开发者
2. 需要统一展示物和模板的小型研究团队
3. 需要案例、样品和可审计工具链的量化内容创作者

## 数据源

| Provider | 市场 | 支持频率 |
|----------|------|---------|
| akshare | A 股 | 1d |
| yfinance | 港美股 | 1d |
| demo | 模拟数据 | 1d, 1w |
| csv | 本地文件 | - |
| local | 本地目录 | - |
| duckdb | DuckDB | - |

## 完整工作流

```bash
# 1. 获取数据
a4q data fetch --provider akshare --symbol 000001.SZ --indicators ma5,rsi --output data.json

# 2. 运行回测
a4q backtest run --input data.json --strategy sma_cross --strategy-params fast=5,slow=20 --result-json result.json

# 3. 生成报告
a4q report generate --input result.json --output report.pdf --format pdf
```

## 联系我们

- 官网：https://agent4quant.com
- 邮箱：info@agent4quant.com
- GitHub Issues：https://github.com/agent4quant/agent4quant-skills/issues

## 许可证

MIT License
