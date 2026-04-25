# quant-backtest

研究型策略回测与参数扫描工具。

## 功能

- **单策略回测**：SMA Cross、RSI、Momentum
- **参数扫描**：快速验证不同参数组合
- **多策略对比**：compare 命令对比多个策略
- **Benchmark**：支持对照指数对比
- **交易成本**：手续费、滑点、印花税
- **可视化报告**：Plotly HTML 报告

## 安装

```bash
pip install -e .
```

## 快速开始

```bash
# 单策略回测
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
  --result-json result.json \
  --report-html result.html

# 参数扫描
a4q backtest sweep \
  --provider demo \
  --symbol DEMO.SH \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --strategy sma_cross \
  --fast-range 5,10,15 \
  --slow-range 20,30,40 \
  --output-json sweep.json

# 策略对比
a4q backtest compare \
  --provider demo \
  --symbol DEMO.SH \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --strategies sma_cross,rsi,momentum \
  --output-json compare.json
```

## 支持的策略

| 策略 | 参数 | 说明 |
|------|------|------|
| sma_cross | fast, slow | 移动平均线交叉 |
| rsi | period, oversold, overbought | RSI 策略 |
| momentum | period | 动量策略 |

## 输出

- `result.json`：回测结果（含收益率、夏普比、最大回撤、交易明细）
- `result.html`：Plotly 可视化报告
- `sweep.json`：参数扫描排行榜
- `compare.json`：策略对比结果

## 交易成本模型

| 成本项 | 默认值 |
|--------|--------|
| 手续费 | 2 bps |
| 滑点 | 3 bps |
| 印花税 | 10 bps |

## 安全声明

- 默认在用户本地执行
- 联网仅用于用户显式选择的在线数据源调用
- 不托管策略、回测结果或研究数据
- 仅处理用户显式给定的参数、输入路径和输出路径

## 测试

```bash
pytest tests/
```

## 依赖

```
pandas
plotly
```
