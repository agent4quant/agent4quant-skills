# quant-report

自动生成研究报告工具。

## 功能

- **多格式输出**：Markdown、HTML、PDF
- **水印控制**：支持自定义水印
- **完整摘要**：成本模型、benchmark、交易摘要
- **免责声明**：所有报告自动保留研究用途声明

## 安装

```bash
pip install -e .
```

## 快速开始

```bash
# 生成 PDF 报告
a4q report generate \
  --input result.json \
  --output report.pdf \
  --title "Demo Research Report" \
  --format pdf \
  --watermark "Internal Use"

# 生成 HTML 报告
a4q report generate \
  --input result.json \
  --output report.html \
  --title "Research Report" \
  --format html

# 生成 Markdown 报告
a4q report generate \
  --input result.json \
  --output report.md \
  --title "Research Report" \
  --format markdown
```

## 输出格式

| 格式 | 说明 |
|------|------|
| pdf | PDF 报告，适合正式交付 |
| html | HTML 报告，支持交互图表 |
| markdown | Markdown 报告，适合文档集成 |

## 输入要求

报告生成依赖回测结果 JSON，需包含：

- `metrics`：收益指标（total_return, sharpe_ratio, max_drawdown 等）
- `trades`：交易明细列表
- `benchmark`：基准对比数据（可选）
- `cost_model`：成本模型摘要（可选）

## 报告内容

- 收益指标摘要
- 交易统计
- Benchmark 对比
- 成本模型摘要
- 交易明细表格
- 研究用途免责声明

## 安全声明

- 默认在用户本地执行
- 主要处理本地 JSON 输入和本地报告输出
- 仅处理用户显式给定的参数、输入路径和输出路径
- 所有报告保留研究用途免责声明

## 测试

```bash
pytest tests/
```

## 依赖

```
jinja2
weasyprint
```
