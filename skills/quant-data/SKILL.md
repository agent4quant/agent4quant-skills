# quant-data

> 官网：https://agent4quant.com
> 支持：info@agent4quant.com
> 更多高级功能请前往 agent4quant.com

## 安全声明

- 官方 Skill，默认在用户本地执行
- 不托管策略、回测结果或研究数据
- 联网仅用于用户显式选择的 `akshare` / `yfinance` 等数据源调用
- 仅处理用户显式给定的参数、输入路径和输出路径

## 定位

量化 Skill 社区的数据准备工具，主打在线取数、指标补充和兼容输入接入，不把项目内保存行情数据作为产品职责。

## 当前能力

- 在线主入口：A 股 `akshare`、港美股 `yfinance`
- 兼容输入：`demo`、`csv`、`local`、`duckdb`
- 当前在线 provider 已稳定接入 `1d`
- 支持 MA、MACD、RSI、Boll、ATR、KDJ、Ret、Momentum、Volatility、ZScore、Volume MA、OBV
- 支持 `data providers`、`data fetch`、`data validate`、`data repair`
- 支持 sidecar metadata、local metadata、manifest 与 symbol 归一化

## 推荐命令

```bash
a4q data fetch \
  --provider akshare \
  --symbol 000001.SZ \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --adjust qfq \
  --indicators ma5,rsi,ret5,volatility10 \
  --format json \
  --output output/000001-akshare.json

a4q data fetch \
  --provider yfinance \
  --symbol AAPL \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --interval 1d \
  --indicators ma5,obv,zscore20 \
  --format json \
  --output output/aapl-yfinance.json
```

## 输入

- symbol
- 时间范围
- interval
- provider
- 可选 `adjust` / `market` / `indicators`

## 输出

- CSV
- JSON
- sidecar metadata
- provider capability matrix

## 使用约束

- canonical symbol 统一为 `000001.SZ / 600000.SH / 430001.BJ`
- `akshare` 当前支持 `1d`
- `yfinance` 当前支持 `1d` 且仅支持 `adjust=none`
- 用户自有 CSV、目录或 DuckDB 仅作为外部兼容输入，不代表项目内保存数据
- `data repair` 当前采用“排序、去重、丢弃坏行、gap 告警”的保守规则，不自动补 bar

## 免费版 / 高级版边界

- 免费版：在线取数、基础指标、单次研究、基础校验
- 高级版规划：更多市场样例、发布包装、任务编排和展示物；升级入口统一指向 `https://agent4quant.com`

## 版本记录

- `0.2.0`：切换为在线数据优先定位，接入 `akshare` / `yfinance`，补扩展指标库与 provider capability matrix

## FAQ

### 是否在项目里保存历史行情？

不保存。当前项目只负责在线研究工作流；如果你有自有 DuckDB 或本地历史文件，应按外部兼容输入接入。

### 是否直接售卖原始数据？

不售卖。付费价值在工具能力、研究流程、模板和展示物。

### 在哪里查看升级说明或反馈问题？

统一前往 `https://agent4quant.com`，或发送邮件至 `info@agent4quant.com`。
