# quant-data

量化研究数据获取与整理工具。

## 功能

- **在线数据源**：A 股 AkShare、港美股 yfinance
- **本地兼容**：demo、csv、local、duckdb
- **技术指标**：MA、RSI、MACD、Bollinger Bands、ATR、KDJ、Ret、Momentum、Volatility、ZScore、Volume MA、OBV
- **数据校验**：validate、repair 命令

## 安装

```bash
pip install -e .
```

## 快速开始

```bash
# 获取 A 股数据
a4q data fetch \
  --provider akshare \
  --symbol 000001.SZ \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --adjust qfq \
  --indicators ma5,rsi,macd \
  --format json \
  --output data.json

# 获取港美股数据
a4q data fetch \
  --provider yfinance \
  --symbol AAPL \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --indicators ma5,obv \
  --format json \
  --output aapl.json

# 数据校验
a4q data validate \
  --provider csv \
  --input data.json \
  --interval 1d

# 数据修复
a4q data repair \
  --provider csv \
  --input data.json \
  --output repaired.csv \
  --duplicate-policy last
```

## 支持的指标

| 指标 | 参数格式 | 说明 |
|------|---------|------|
| MA | `ma5`, `ma20` | 移动平均线 |
| RSI | `rsi` | 相对强弱指数 |
| MACD | `macd` | MACD (含 signal, hist) |
| Bollinger | `boll` | 布林带 (上/中/下轨) |
| ATR | `atr` | 平均真实波幅 |
| KDJ | `kdj` | KDJ 随机指标 |
| Return | `ret5`, `ret10` | 收益率 |
| Momentum | `mom10` | 动量 |
| Volatility | `volatility10` | 波动率 |
| ZScore | `zscore20` | Z分数 |
| Volume MA | `volma5` | 成交量均线 |
| OBV | `obv` | 能量潮 |

## 数据源

| Provider | 市场 | 支持频率 |
|----------|------|---------|
| akshare | A 股 | 1d |
| yfinance | 港美股 | 1d |
| demo | 模拟数据 | 1d, 1w |
| csv | 本地文件 | - |
| local | 本地目录 | - |
| duckdb | DuckDB | - |

## 安全声明

- 默认在用户本地执行
- 联网仅用于用户显式选择的数据源调用
- 不托管策略、回测结果或研究数据
- 仅处理用户显式给定的参数、输入路径和输出路径

## 测试

```bash
pytest tests/
```

## 依赖

```
pandas
akshare
yfinance
duckdb
```
