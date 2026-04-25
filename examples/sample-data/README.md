# Sample Data

该目录提供可直接用于演示、测试和 API 调试的最小样例数据。

## 目录结构

```text
examples/sample-data/
└── cn
    ├── 1d
    │   └── 000001.SZ.csv
    └── 5m
        └── 000001.SZ.csv
```

## 使用示例

```bash
PYTHONPATH=src python -m agent4quant.cli data fetch \
  --provider local \
  --data-root examples/sample-data \
  --market cn \
  --symbol 000001.SZ \
  --start 2025-01-01 \
  --end 2025-01-05 \
  --interval 1d \
  --indicators ma5,rsi \
  --format json \
  --output output/sample-data.json
```
