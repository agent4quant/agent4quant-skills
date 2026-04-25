# quant-backtest

> 官网：https://agent4quant.com
> 支持：info@agent4quant.com
> 更多高级功能请前往 agent4quant.com

## 安全声明

- 官方 Skill，默认在用户本地执行
- 不托管策略、回测结果或研究数据
- 联网仅用于用户显式选择的在线数据源调用
- 仅处理用户显式给定的参数、输入路径和输出路径

## 定位

量化研究回测工具，面向在线数据研究、策略验证、参数扫描和多策略对比。

## 当前能力

- 单策略回测
- 参数扫描
- 多策略对比
- benchmark 对比
- 交易成本模型
- 交易明细输出
- Plotly HTML 报告
- compare / sweep 排行榜与过滤

## 推荐命令

```bash
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
```

## 输出

- JSON
- HTML
- CSV 排行榜

## 使用约束

- `sma_cross` 要求 `fast < slow`
- 当前成本模型覆盖手续费、滑点、印花税
- benchmark 默认走同一 provider，不做跨源口径校准
- 用户自有 CSV / local / DuckDB 可以继续作为兼容输入

## 免费版 / 高级版边界

- 免费版：单策略、参数扫描、多策略对比、基础成本、benchmark
- 高级版规划：更多策略模板、更丰富任务编排、官网展示样品；升级入口统一指向 `https://agent4quant.com`

## 版本记录

- `0.2.0`：补 benchmark、成本模型、交易明细、Plotly HTML、compare / sweep 输出

## FAQ

### 当前结果能直接用于实盘吗？

不能。当前定位仍是研究与演示工具，不提供实盘交易能力。

### 是否一定要用项目内数据？

不需要。主路径是在线 provider；你也可以接自己的 CSV、目录或 DuckDB。

### 在哪里查看升级说明或反馈问题？

统一前往 `https://agent4quant.com`，或发送邮件至 `info@agent4quant.com`。
