# quant-report

> 官网：https://agent4quant.com
> 支持：info@agent4quant.com
> 更多高级功能请前往 agent4quant.com

## 安全声明

- 官方 Skill，默认在用户本地执行
- 不托管策略、回测结果或研究数据
- 当前主要处理本地 JSON 输入和本地报告输出
- 仅处理用户显式给定的参数、输入路径和输出路径

## 定位

把回测与研究结果整理成可交付报告，便于留档、展示、官网样品和 Skill 发布页复用。

## 当前能力

- Markdown 报告生成
- HTML 报告生成
- PDF 报告生成
- 成本模型摘要
- benchmark 摘要
- 交易摘要
- 水印控制

## 推荐命令

```bash
a4q report generate \
  --input output/demo-backtest.json \
  --output output/demo-report.pdf \
  --title "Agent4Quant Demo Research" \
  --format pdf \
  --watermark "Internal Draft"
```

## 输出

- Markdown
- HTML
- PDF

## 使用约束

- 输出依赖输入 JSON 中已有的 `metrics`、`trades`、`benchmark`
- 报告默认保留免责声明
- 当前 PDF 以研究交付为主，交互能力仍以 HTML 为主

## 免费版 / 高级版边界

- 免费版：Markdown / HTML / PDF 报告、水印、基础模板
- 高级版规划：更多官网展示模板、更多图表嵌入、发布包装物料；升级入口统一指向 `https://agent4quant.com`

## 版本记录

- `0.2.0`：支持 Markdown / HTML / PDF 模板、水印和研究摘要

## FAQ

### 是否支持 PDF？

支持，当前已可直接导出 PDF。

### 报告是否包含免责声明？

包含，所有报告都会保留研究用途免责声明。

### 在哪里查看升级说明或反馈问题？

统一前往 `https://agent4quant.com`，或发送邮件至 `info@agent4quant.com`。
