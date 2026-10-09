# 元数据工艺场景可视化

本项目从 InfluxDB 按生产时段统计元数据的日级最小值、最大值和有效采样量，写入 MySQL，并通过 Streamlit + Flask 展示二维点位分布、三维上下限和逐日对比图。

## 目录结构

```text
metadata-visualization/
├─ app/
│  ├─ app.py
│  └─ chart.html
├─ pipeline/
│  ├─ daily_limits_pipeline.py
│  └─ threaded_daily_limits_pipeline.py
├─ config/
│  └─ config.example.yaml
├─ requirements.txt
├─ README.md
└─ .gitignore
```

## 安装

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 配置

1. 将 `config/config.example.yaml` 复制为 `config/config.yaml`。
2. 填写公司 MySQL、InfluxDB、结果 MySQL、元数据编码文件和生产明细文件路径。
3. 不要提交 `config/config.yaml`，该文件已被 `.gitignore` 排除。

也可以通过环境变量 `METADATA_CONFIG_FILE` 指定可视化应用使用的配置文件。

## 导出日级数据

```powershell
python pipeline/threaded_daily_limits_pipeline.py `
  --start 2025-01-01 `
  --end 2025-01-31 `
  --batch-size 10 `
  --retries 3
```

小范围测试可增加 `--code-limit 5`，或使用 `--code 元数据编码` 仅处理一个元数据。

流水线会按“日期＋元数据＋钢种＋宽度＋拉速”汇总实际最小值、实际最大值和有效秒级采样量，并写入结果 MySQL 的 `metadata_daily_limits` 表。

## 启动可视化

```powershell
streamlit run app/app.py --server.address 0.0.0.0 --server.port 8501
```

默认页面地址为 `http://本机IP:8501/`，同一局域网内可访问。Flask 数据接口默认监听 `8765` 端口。

