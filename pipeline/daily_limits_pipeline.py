"""3ST浇铸时段日级上下限流水线（独立于原main.py）。"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import yaml
from influxdb_client.client.warnings import MissingPivotFunction
import warnings

warnings.simplefilter("ignore", MissingPivotFunction)

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from repository import influx_repository, mysql_repository

PROJECT_CONFIG = PROJECT_ROOT / "config" / "config.yaml"
PIPELINE_CONFIG = PROJECT_CONFIG
TABLE_NAME = "metadata_daily_limits"
LOGGER = logging.getLogger("daily_limits")


class TeeStream:
    """把终端输出同步复制到日志文件。"""

    def __init__(self, terminal, log_file):
        self.terminal = terminal
        self.log_file = log_file

    def write(self, message):
        self.terminal.write(message)
        self.log_file.write(message)
        self.log_file.flush()

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()

    def isatty(self):
        return self.terminal.isatty()


def configure_run_logging():
    """将普通日志和进度打印同时保存到 logs/daily_limits.log。"""
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = (log_dir / "daily_limits.log").open(
        "a", encoding="utf-8", buffering=1
    )
    sys.stdout = TeeStream(sys.__stdout__, log_file)
    sys.stderr = TeeStream(sys.__stderr__, log_file)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )
    LOGGER.info("日级上下限任务启动，日志=%s", log_file.name)


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
    storage_by_code = mysql_repository.get_storage_locations(
        codes, project["Mysql"]
    )
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
        mysql_repository.ensure_daily_limits_table(result_mysql)
    influx = project["Influxdb"]
    client = influx_repository.create_client(influx)
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
                    frame = influx_repository.query_periods(
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
                    total += mysql_repository.write_daily_limits(
                        rows, result_mysql
                    )
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
    configure_run_logging()
    arguments = parse_args()
    written = run(arguments)
    LOGGER.info("完成，写入或更新%s条结果", written)
