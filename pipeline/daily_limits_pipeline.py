"""3ST浇铸时段日级上下限流水线（独立于原main.py）。"""

import argparse
import json
import logging
import time
from datetime import timezone
from pathlib import Path

import pandas as pd
import pymysql
import pytz
import yaml
from influxdb_client import InfluxDBClient
from influxdb_client.client.warnings import MissingPivotFunction
import warnings

warnings.simplefilter("ignore", MissingPivotFunction)

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
PROJECT_CONFIG = PROJECT_ROOT / "config" / "config.yaml"
PIPELINE_CONFIG = PROJECT_CONFIG
TABLE_NAME = "metadata_daily_limits"
LOGGER = logging.getLogger("daily_limits")


def read_yaml(path):
    with Path(path).open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def load_codes(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    result, seen = [], set()
    for group in data.values():
        for value in group:
            code = str(value).strip()
            if code and code not in seen:
                seen.add(code)
                result.append(code)
    return result


def mysql_connection(config, autocommit=False):
    return pymysql.connect(
        host=config["Host"], port=int(config.get("Port", 3306)),
        user=config["Username"], password=config["Password"],
        database=config["database"], charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor, autocommit=autocommit,
    )


def get_storage_locations(codes, company_mysql):
    placeholders = ",".join(["%s"] * len(codes))
    sql = f"""
        SELECT id, storage_location FROM kl_metadata
        WHERE organ_code = %s AND id IN ({placeholders})
    """
    connection = mysql_connection(company_mysql, autocommit=True)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, [company_mysql["organ_code"], *codes])
            return {row["id"]: row["storage_location"] for row in cursor.fetchall()}
    finally:
        connection.close()


def ensure_result_table(result_mysql):
    sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            stat_date DATE NOT NULL,
            metadata_code VARCHAR(64) NOT NULL,
            steel_grade VARCHAR(64) NOT NULL,
            width INT NOT NULL,
            casting_speed DECIMAL(8,3) NOT NULL,
            min_value DOUBLE NULL,
            max_value DOUBLE NULL,
            valid_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uk_daily_condition
                (stat_date, metadata_code, steel_grade, width, casting_speed),
            KEY idx_code_date (metadata_code, stat_date),
            KEY idx_date_condition
                (stat_date, steel_grade, width, casting_speed)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    connection = mysql_connection(result_mysql)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def write_results(rows, result_mysql, batch_size=1000):
    if not rows:
        return 0
    sql = f"""
        INSERT INTO {TABLE_NAME} (
            stat_date, metadata_code, steel_grade, width, casting_speed,
            min_value, max_value, valid_count
        ) VALUES (
            %(stat_date)s, %(metadata_code)s, %(steel_grade)s, %(width)s,
            %(casting_speed)s, %(min_value)s, %(max_value)s, %(valid_count)s
        ) ON DUPLICATE KEY UPDATE
            min_value=VALUES(min_value), max_value=VALUES(max_value),
            valid_count=VALUES(valid_count), updated_at=CURRENT_TIMESTAMP
    """
    connection = mysql_connection(result_mysql)
    try:
        with connection.cursor() as cursor:
            for offset in range(0, len(rows), batch_size):
                cursor.executemany(sql, rows[offset:offset + batch_size])
        connection.commit()
        return len(rows)
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def write_local_day(rows, output_dir, day):
    """按日期原子覆盖CSV，方便中断后安全重跑当天。"""
    if not rows:
        return 0
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{pd.Timestamp(day):%Y-%m-%d}.csv"
    temporary_path = output_path.with_suffix(".csv.tmp")
    frame = pd.DataFrame(rows).drop_duplicates([
        "stat_date", "metadata_code", "steel_grade", "width", "casting_speed"
    ], keep="last")
    frame.sort_values([
        "stat_date", "metadata_code", "steel_grade", "width", "casting_speed"
    ], inplace=True)
    frame.to_csv(temporary_path, index=False, encoding="utf-8-sig")
    temporary_path.replace(output_path)
    return len(frame)


def to_utc_string(local_time):
    local = pytz.timezone("Asia/Shanghai").localize(
        pd.Timestamp(local_time).to_pydatetime()
    )
    return local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def query_influx_batch(client, influx, start_time, end_time, codes, storage):
    fields = json.dumps(codes, ensure_ascii=False)
    measurement = json.dumps(str(storage), ensure_ascii=False)
    query = f'''
    from(bucket: "{influx["bucket"]}")
      |> range(start: {to_utc_string(start_time)}, stop: {to_utc_string(end_time)})
      |> filter(fn: (r) => r["_measurement"] == {measurement})
      |> filter(fn: (r) => contains(value: r["_field"], set: {fields}))
      |> aggregateWindow(every: 1s, fn: last, createEmpty: false)
      |> keep(columns: ["_value", "_time", "_field"])
    '''
    result = client.query_api().query_data_frame(query)
    frames = result if isinstance(result, list) else [result]
    frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame(columns=["_time", "metadata_code", "value"])
    frame = pd.concat(frames, ignore_index=True)[["_time", "_field", "_value"]]
    frame = frame.rename(columns={"_field": "metadata_code", "_value": "value"})
    frame["_time"] = pd.to_datetime(frame["_time"], utc=True)
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    return frame.dropna(subset=["_time", "metadata_code", "value"])


def query_influx_day_batch(client, influx, periods, codes, storage_by_code):
    """用一次Flux请求union当天全部精确生产时间段。"""
    fields = json.dumps(codes, ensure_ascii=False)
    measurements = json.dumps(
        sorted({str(storage_by_code[code]) for code in codes}), ensure_ascii=False
    )
    table_names = []
    table_queries = []
    for index, period in enumerate(periods.to_dict("records")):
        start_utc = to_utc_string(period["start_time"])
        end_utc = to_utc_string(period["end_time"])
        table_name = f"period{index}"
        table_names.append(table_name)
        table_queries.append(
            f'''{table_name} = from(bucket: "{influx["bucket"]}")
      |> range(start: {start_utc}, stop: {end_utc})
      |> filter(fn: (r) => contains(value: r["_measurement"], set: {measurements}))
      |> filter(fn: (r) => contains(value: r["_field"], set: {fields}))'''
        )
    query = "\n\n".join(table_queries) + f'''

    union(tables: [{", ".join(table_names)}])
      |> aggregateWindow(every: 1s, fn: last, createEmpty: false)
      |> keep(columns: ["_value", "_time", "_field"])
    '''
    result = client.query_api().query_data_frame(query)
    frames = result if isinstance(result, list) else [result]
    frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame(columns=["_time", "metadata_code", "value"])
    frame = pd.concat(frames, ignore_index=True)[["_time", "_field", "_value"]]
    frame = frame.rename(columns={"_field": "metadata_code", "_value": "value"})
    frame["_time"] = pd.to_datetime(frame["_time"], utc=True)
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    return frame.dropna(subset=["_time", "metadata_code", "value"])


def load_periods(excel_path, start_date=None, end_date=None):
    frame = pd.read_excel(excel_path, sheet_name="3ST生产明细")
    required = ["开始时间", "结束时间", "钢种组", "规格", "拉速"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"3ST生产明细缺少字段：{missing}")
    frame = frame[required].rename(columns={
        "开始时间": "start_time", "结束时间": "end_time",
        "钢种组": "steel_grade", "规格": "width", "拉速": "casting_speed",
    })
    frame["start_time"] = pd.to_datetime(frame["start_time"], errors="coerce")
    frame["end_time"] = pd.to_datetime(frame["end_time"], errors="coerce")
    frame["width"] = pd.to_numeric(frame["width"], errors="coerce")
    frame["casting_speed"] = pd.to_numeric(frame["casting_speed"], errors="coerce")
    frame.dropna(inplace=True)
    frame = frame[frame["end_time"] > frame["start_time"]].copy()
    if start_date:
        frame = frame[frame["start_time"] >= pd.Timestamp(start_date)]
    if end_date:
        frame = frame[frame["start_time"] < pd.Timestamp(end_date) + pd.Timedelta(days=1)]
    return frame.sort_values(["start_time", "end_time"]).reset_index(drop=True)


def merge_adjacent_periods(frame):
    rows = []
    conditions = ["steel_grade", "width", "casting_speed"]
    for record in frame.sort_values(["start_time", "end_time"]).to_dict("records"):
        if rows:
            previous = rows[-1]
            same = all(previous[column] == record[column] for column in conditions)
            touching = record["start_time"] <= previous["end_time"] + pd.Timedelta(seconds=1)
            if same and touching:
                previous["end_time"] = max(previous["end_time"], record["end_time"])
                continue
        rows.append(record)
    return pd.DataFrame(rows, columns=frame.columns)


def periods_for_day(periods, day):
    day_start = pd.Timestamp(day)
    day_end = day_start + pd.Timedelta(days=1)
    result = periods[
        (periods["start_time"] >= day_start) & (periods["start_time"] < day_end)
    ].copy()
    return result.sort_values("start_time")


def match_and_aggregate(raw, periods):
    if raw.empty:
        return []
    raw = raw.copy()
    raw["time_local"] = raw["_time"].dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    period_columns = [
        "start_time", "end_time", "steel_grade", "width", "casting_speed"
    ]
    matched = pd.merge_asof(
        raw.sort_values("time_local"), periods[period_columns].sort_values("start_time"),
        left_on="time_local", right_on="start_time", direction="backward",
    )
    matched = matched[matched["time_local"] < matched["end_time"]].copy()
    matched["stat_date"] = matched["time_local"].dt.date
    result = matched.groupby([
        "stat_date", "metadata_code", "steel_grade", "width", "casting_speed"
    ], as_index=False).agg(
        min_value=("value", "min"), max_value=("value", "max"),
        valid_count=("value", "count"),
    )
    result["width"] = result["width"].astype(int)
    result["casting_speed"] = result["casting_speed"].astype(float)
    result["min_value"] = result["min_value"].astype(float)
    result["max_value"] = result["max_value"].astype(float)
    result["valid_count"] = result["valid_count"].astype(int)
    return result.to_dict("records")


def aggregate_tagged(raw):
    """先算各生产时段上下限，再由时段结果汇总为日级上下限。"""
    if raw.empty:
        return []
    raw = raw.copy()
    raw["time_local"] = raw["_time"].dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    raw["stat_date"] = raw["time_local"].dt.date
    period_result = raw.groupby([
        "stat_date", "period_start", "period_end", "metadata_code",
        "steel_grade", "width", "casting_speed"
    ], as_index=False).agg(
        min_value=("value", "min"), max_value=("value", "max"),
        valid_count=("value", "count"),
    )
    result = period_result.groupby([
        "stat_date", "metadata_code", "steel_grade", "width", "casting_speed"
    ], as_index=False).agg(
        min_value=("min_value", "min"),
        max_value=("max_value", "max"),
        valid_count=("valid_count", "sum"),
    )
    result["width"] = result["width"].astype(int)
    result["casting_speed"] = result["casting_speed"].astype(float)
    result["min_value"] = result["min_value"].astype(float)
    result["max_value"] = result["max_value"].astype(float)
    result["valid_count"] = result["valid_count"].astype(int)
    return result.to_dict("records")


def chunks(items, size):
    for offset in range(0, len(items), size):
        yield items[offset:offset + size]


def format_duration(seconds):
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}时{minutes}分{seconds}秒"
    if minutes:
        return f"{minutes}分{seconds}秒"
    return f"{seconds}秒"


def run(args):
    project = read_yaml(PROJECT_CONFIG)
    pipeline = read_yaml(args.pipeline_config)
    codes = load_codes(pipeline["CodesFile"])
    if args.code_limit:
        codes = codes[:args.code_limit]
    storage_by_code = get_storage_locations(codes, project["Mysql"])
    missing = [code for code in codes if code not in storage_by_code]
    if missing:
        LOGGER.warning("%s个编码没有storage_location，将跳过", len(missing))
    codes = [code for code in codes if code in storage_by_code]
    if not codes:
        raise ValueError("没有可处理的变量编码")
    periods = load_periods(pipeline["ProductionExcel"], args.start, args.end)
    if periods.empty:
        raise ValueError("指定范围内没有3ST浇铸时间段")
    result_mysql = pipeline["ResultMysql"]
    if not args.dry_run and not args.local_only:
        ensure_result_table(result_mysql)
    influx = project["Influxdb"]
    client = InfluxDBClient(
        url=influx["url"], token=influx["token"], org=influx["org"],
        timeout=120000,
    )
    first_day = pd.Timestamp(args.start or periods["start_time"].min().date())
    last_day = pd.Timestamp(args.end or periods["end_time"].max().date())
    days = list(pd.date_range(first_day, last_day, freq="D"))
    code_batches = list(chunks(codes, args.batch_size))
    day_period_map = {day: periods_for_day(periods, day) for day in days}
    total_queries = sum(
        len(day_period_map[day]) * len(code_batches)
        for day in days if not day_period_map[day].empty
    )

    total = 0
    completed_queries = 0
    pipeline_started = time.perf_counter()
    try:
        for day_number, day in enumerate(days, 1):
            day_periods = day_period_map[day]
            if day_periods.empty:
                continue
            day_rows = []
            for batch_number, code_batch in enumerate(code_batches, 1):
                batch_started = time.perf_counter()
                frames = []
                period_records = day_periods.to_dict("records")
                for period_number, period in enumerate(period_records, 1):
                    query_started = time.perf_counter()
                    one_period = pd.DataFrame([period])
                    frame = query_influx_day_batch(
                        client, influx, one_period, code_batch, storage_by_code
                    )
                    query_seconds = time.perf_counter() - query_started
                    completed_queries += 1
                    elapsed = time.perf_counter() - pipeline_started
                    average = elapsed / completed_queries
                    remaining = average * (total_queries - completed_queries)
                    progress = (
                        f"日期 {day_number}/{len(days)} {day.date()} | "
                        f"批次 {batch_number}/{len(code_batches)} | "
                        f"时段 {period_number}/{len(period_records)} | "
                        f"查询 {query_seconds:.1f}秒 | "
                        f"总进度 {completed_queries}/{total_queries} "
                        f"({completed_queries / total_queries:.1%}) | "
                        f"预计剩余 {format_duration(remaining)}"
                    )
                    print(progress.ljust(150), end="\r", flush=True)
                    if frame.empty:
                        continue
                    frame["steel_grade"] = period["steel_grade"]
                    frame["width"] = period["width"]
                    frame["casting_speed"] = period["casting_speed"]
                    frame["period_start"] = period["start_time"]
                    frame["period_end"] = period["end_time"]
                    frames.append(frame)
                raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                rows = aggregate_tagged(raw)
                if args.local_only:
                    day_rows.extend(rows)
                elif not args.dry_run:
                    total += write_results(rows, result_mysql)
                print(" " * 150, end="\r", flush=True)
                LOGGER.info(
                    "日期=%s 批次=%s/%s 编码=%s 原始点=%s 结果=%s "
                    "批次耗时=%s dry_run=%s",
                    day.date(), batch_number, len(code_batches), len(code_batch),
                    len(raw), len(rows),
                    format_duration(time.perf_counter() - batch_started), args.dry_run,
                )
            if args.local_only and not args.dry_run:
                saved = write_local_day(day_rows, pipeline["OutputDir"], day)
                total += saved
                LOGGER.info(
                    "日期=%s 本地CSV已保存，记录=%s，目录=%s",
                    day.date(), saved, pipeline["OutputDir"],
                )
    finally:
        print()
        client.close()
    return total


def parse_args():
    parser = argparse.ArgumentParser(description="3ST日级上下限批处理")
    parser.add_argument("--pipeline-config", default=str(PIPELINE_CONFIG))
    parser.add_argument("--start", help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end", help="结束日期 YYYY-MM-DD")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--code-limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--local-only", action="store_true",
        help="只保存本地日级CSV，不创建或写入MySQL结果表",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    arguments = parse_args()
    written = run(arguments)
    LOGGER.info("完成，写入或更新%s条结果", written)
