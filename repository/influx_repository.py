"""InfluxDB 数据访问和查询结果标准化。"""

import json
import time
from datetime import timezone

import pandas as pd
import pytz
from influxdb_client import InfluxDBClient


EMPTY_COLUMNS = ["_time", "metadata_code", "value"]


def create_client(config):
    """创建 InfluxDB 客户端。"""
    return InfluxDBClient(
        url=config["url"],
        token=config["token"],
        org=config["org"],
        timeout=120000,
    )


def to_utc_string(local_time):
    """将上海本地时间转换为 Flux 使用的 UTC 字符串。"""
    local = pytz.timezone("Asia/Shanghai").localize(
        pd.Timestamp(local_time).to_pydatetime()
    )
    return local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_frames(result):
    frames = result if isinstance(result, list) else [result]
    frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame(columns=EMPTY_COLUMNS)
    frame = pd.concat(frames, ignore_index=True)[["_time", "_field", "_value"]]
    frame = frame.rename(columns={"_field": "metadata_code", "_value": "value"})
    frame["_time"] = pd.to_datetime(frame["_time"], utc=True)
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    return frame.dropna(subset=EMPTY_COLUMNS)


def query_periods(client, config, periods, codes, storage_by_code):
    """一次查询多个精确生产时段和元数据编码。"""
    fields = json.dumps(codes, ensure_ascii=False)
    measurements = json.dumps(
        sorted({str(storage_by_code[code]) for code in codes}),
        ensure_ascii=False,
    )
    names, queries = [], []
    for index, period in enumerate(periods.to_dict("records")):
        name = f"period{index}"
        names.append(name)
        queries.append(
            f'''{name} = from(bucket: "{config["bucket"]}")
  |> range(start: {to_utc_string(period["start_time"])}, stop: {to_utc_string(period["end_time"])})
  |> filter(fn: (r) => contains(value: r["_measurement"], set: {measurements}))
  |> filter(fn: (r) => contains(value: r["_field"], set: {fields}))'''
        )
    query = "\n\n".join(queries) + f'''

union(tables: [{", ".join(names)}])
  |> aggregateWindow(every: 1s, fn: last, createEmpty: false)
  |> keep(columns: ["_value", "_time", "_field"])
'''
    return _normalize_frames(client.query_api().query_data_frame(query))


def query_code_range(client, config, start_time, end_time, code, storage):
    """查询一个元数据在连续时间范围内的每秒末值。"""
    field = json.dumps(code, ensure_ascii=False)
    measurement = json.dumps(str(storage), ensure_ascii=False)
    query = f'''from(bucket: "{config["bucket"]}")
  |> range(start: {to_utc_string(start_time)}, stop: {to_utc_string(end_time)})
  |> filter(fn: (r) => r["_measurement"] == {measurement})
  |> filter(fn: (r) => r["_field"] == {field})
  |> aggregateWindow(every: 1s, fn: last, createEmpty: false)
  |> keep(columns: ["_value", "_time", "_field", "_measurement"])
  |> yield(name: "last")
'''
    return _normalize_frames(client.query_api().query_data_frame(query))


def query_code_range_with_retry(
    client, config, start_time, end_time, code, storage, retries
):
    """查询一个元数据范围；瞬时错误按指数退避重试。"""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return query_code_range(
                client, config, start_time, end_time, code, storage
            )
        except Exception as error:
            last_error = error
            if attempt < retries:
                time.sleep(min(2 ** (attempt - 1), 8))
    raise RuntimeError(
        f"查询元数据 {code} 失败：{start_time} 至 {end_time}，"
        f"重试{retries}次仍失败；最后错误：{last_error}"
    ) from last_error
