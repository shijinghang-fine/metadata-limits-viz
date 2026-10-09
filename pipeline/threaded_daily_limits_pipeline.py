"""3ST浇铸时段日级上下限多线程流水线。

直接查询公司的 InfluxDB；原始秒级数据只保留在内存中。
每天统计完成后立即将最小值、最大值和有效数量写入本地 MySQL。
"""

import argparse
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

import daily_limits_pipeline as base
from repository import influx_repository, mysql_repository


LOGGER = logging.getLogger("threaded_daily_limits")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "config.yaml"
THREAD_LOCAL = threading.local()
WORKER_COUNT = 5
TIMER_INTERVAL_SECONDS = 30


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
    """将线程进度、耗时和错误保存到 logs/threaded_daily_limits.log。"""
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = (log_dir / "threaded_daily_limits.log").open(
        "a", encoding="utf-8", buffering=1
    )
    sys.stdout = TeeStream(sys.__stdout__, log_file)
    sys.stderr = TeeStream(sys.__stderr__, log_file)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )
    LOGGER.info("多线程日级上下限任务启动，日志=%s", log_file.name)


def read_pipeline_config(path):
    with Path(path).open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def get_thread_client(influx):
    """每个工作线程复用自己的 InfluxDB 连接。"""
    client = getattr(THREAD_LOCAL, "influx_client", None)
    if client is None:
        client = influx_repository.create_client(influx)
        THREAD_LOCAL.influx_client = client
    return client


def process_day_batch(
    day,
    code_batch,
    day_periods,
    storage_by_code,
    influx,
    result_mysql,
    retries,
):
    """处理一个月中的一天；由5个线程并行领取不同日期。"""
    thread_name = threading.current_thread().name
    client = get_thread_client(influx)
    query_count = 0
    started_at = time.perf_counter()
    frames = []
    # 每天只发一次请求，但不查询完整00:00-24:00；
    # 只取当天首个生产时段开始至最后一个生产时段结束。
    query_start = day_periods["start_time"].min()
    query_end = day_periods["end_time"].max()
    print(
        f"[线程开始] {thread_name} | {pd.Timestamp(day):%Y-%m-%d} | "
        f"查询 {query_start:%H:%M:%S} 至 {query_end:%H:%M:%S}"
    )

    # 与备份程序一致：每个元数据整天只查询一次。
    for code in code_batch:
        frame = influx_repository.query_code_range_with_retry(
            client=client,
            config=influx,
            start_time=query_start,
            end_time=query_end,
            code=code,
            storage=storage_by_code[code],
            retries=retries,
        )
        query_count += 1

        if frame.empty:
            continue

        frames.append(frame)

    raw = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame()
    )
    # 查询结果在内存中匹配当天全部3ST生产时段，再计算日级上下限。
    rows = base.match_and_aggregate(raw, day_periods)
    written_rows = mysql_repository.write_daily_limits(rows, result_mysql)

    # 本日统计完成后立即释放原始秒级数据。
    del raw, frames, rows

    return {
        "thread_name": thread_name,
        "day": pd.Timestamp(day).strftime("%Y-%m-%d"),
        "code_count": len(code_batch),
        "query_count": query_count,
        "written_rows": written_rows,
        "seconds": time.perf_counter() - started_at,
    }


def live_timer(stop_event, started_at, progress, progress_lock, total_tasks):
    """每30秒显示一次总体耗时和预计剩余时间。"""
    while not stop_event.wait(TIMER_INTERVAL_SECONDS):
        elapsed = time.perf_counter() - started_at
        with progress_lock:
            completed = progress["completed"]

        if completed:
            average = elapsed / completed
            remaining = average * (total_tasks - completed)
            remaining_text = base.format_duration(remaining)
        else:
            remaining_text = "等待首个任务完成后估算"

        print(
            f"[实时计时] 已运行 {base.format_duration(elapsed)}，"
            f"已完成 {completed}/{total_tasks} 个任务，"
            f"预计剩余 {remaining_text}"
        )


def run(args):
    project = read_pipeline_config(args.pipeline_config)
    pipeline = project

    codes = base.load_codes(pipeline["CodesFile"])
    if args.code:
        codes = [args.code.strip()]
    elif args.code_limit:
        codes = codes[:args.code_limit]

    storage_by_code = mysql_repository.get_storage_locations(
        codes, project["Mysql"]
    )
    missing_codes = [code for code in codes if code not in storage_by_code]
    if missing_codes:
        print(f"有 {len(missing_codes):,} 个编码没有存储位置，已跳过。")

    codes = [code for code in codes if code in storage_by_code]
    if not codes:
        raise ValueError("没有可以处理的元数据编码")

    periods = base.load_periods(
        pipeline["ProductionExcel"],
        args.start,
        args.end,
    )
    if periods.empty:
        raise ValueError("指定日期范围内没有3ST浇铸生产时段")

    first_day = pd.Timestamp(args.start or periods["start_time"].min().date())
    last_day = pd.Timestamp(args.end or periods["end_time"].max().date())
    code_batches = list(base.chunks(codes, args.batch_size))
    result_mysql = pipeline["ResultMysql"]
    influx = project["Influxdb"]

    mysql_repository.ensure_daily_limits_table(result_mysql)
    mysql_repository.ensure_progress_table(result_mysql)

    month_periods = list(pd.period_range(first_day, last_day, freq="M"))
    completed_months = mysql_repository.load_completed_months(
        result_mysql,
        codes,
        first_day,
        last_day,
    )
    month_plans = []
    total_day_tasks = 0
    skipped_code_months = 0

    for month in month_periods:
        month_start = max(first_day, month.start_time.normalize())
        month_end = min(last_day, month.end_time.normalize())
        calendar_month_start = month.start_time.date()
        next_month = month_end + pd.Timedelta(days=1)
        current_month_periods = periods[
            (periods["start_time"] >= month_start)
            & (periods["start_time"] < next_month)
        ].copy()
        active_days = [
            day
            for day in pd.date_range(month_start, month_end, freq="D")
            if not base.periods_for_day(current_month_periods, day).empty
        ]
        pending_batches = []

        for code_batch in code_batches:
            pending_codes = [
                code for code in code_batch
                if (code, calendar_month_start) not in completed_months
            ]
            skipped_code_months += len(code_batch) - len(pending_codes)
            if not pending_codes:
                continue
            pending_batches.append(pending_codes)
            total_day_tasks += len(active_days)

        if pending_batches:
            month_plans.append({
                "month_start": month_start,
                "month_end": month_end,
                "progress_month_start": month.start_time.normalize(),
                "periods": current_month_periods,
                "active_days": active_days,
                "pending_batches": pending_batches,
            })

    print(f"日期范围：{first_day.date()} 至 {last_day.date()}")
    print(f"元数据：{len(codes):,} 个")
    print(f"每批元数据：{args.batch_size} 个")
    print(f"月份：{len(month_periods):,} 个")
    print(f"待处理月份：{len(month_plans):,} 个")
    print(f"月内日任务：{total_day_tasks:,} 个")
    print(f"已跳过完成项：{skipped_code_months:,} 个元数据月份")
    print(f"处理方式：月份顺序执行，每个月内部固定 {WORKER_COUNT} 个线程")
    print("原始数据不写入本地文件，只保存日级统计结果。\n")

    if not month_plans:
        print("指定范围内的月份均已完成，无需重复查询。")
        return 0

    started_clock = datetime.now()
    started_at = time.perf_counter()
    completed_batches = 0
    total_written = 0
    failures = []
    stop_timer = threading.Event()
    progress_lock = threading.Lock()
    progress = {"completed": 0}
    timer_thread = threading.Thread(
        target=live_timer,
        args=(
            stop_timer,
            started_at,
            progress,
            progress_lock,
            max(total_day_tasks, 1),
        ),
        name="elapsed-time-reporter",
        daemon=True,
    )
    timer_thread.start()

    print(f"开始时间：{started_clock:%Y-%m-%d %H:%M:%S}")

    try:
        with ThreadPoolExecutor(
            max_workers=WORKER_COUNT,
            thread_name_prefix="daily-limit",
        ) as executor:
            for month_index, plan in enumerate(month_plans, start=1):
                month_label = plan["month_start"].strftime("%Y-%m")
                month_started = time.perf_counter()
                print(
                    f"\n开始处理月份 {month_label} "
                    f"({month_index}/{len(month_plans)})，"
                    f"有效生产日 {len(plan['active_days'])} 天。"
                )

                if not plan["active_days"]:
                    for code_batch in plan["pending_batches"]:
                        mysql_repository.mark_month_completed(
                            result_mysql,
                            code_batch,
                            plan["progress_month_start"],
                            plan["month_end"],
                        )
                    print(f"月份 {month_label} 没有生产时段，已记录完成。")
                    continue

                futures = {}
                for batch_index, code_batch in enumerate(
                    plan["pending_batches"], start=1
                ):
                    for day in plan["active_days"]:
                        day_periods = base.periods_for_day(plan["periods"], day)
                        future = executor.submit(
                            process_day_batch,
                            day,
                            code_batch,
                            day_periods,
                            storage_by_code,
                            influx,
                            result_mysql,
                            args.retries,
                        )
                        futures[future] = (batch_index, code_batch, day)

                failed_batches = set()
                month_written = 0

                for future in as_completed(futures):
                    batch_index, code_batch, day = futures[future]
                    completed_batches += 1
                    with progress_lock:
                        progress["completed"] = completed_batches

                    try:
                        result = future.result()
                        total_written += result["written_rows"]
                        month_written += result["written_rows"]
                        state = (
                            f"{result['thread_name']} | {result['day']} 完成，"
                            f"编码 {result['code_count']} 个，"
                            f"写入或更新 {result['written_rows']:,} 条，"
                            f"耗时 {base.format_duration(result['seconds'])}"
                        )
                    except Exception as error:
                        failed_batches.add(batch_index)
                        failures.append(
                            (month_label, day, code_batch, str(error))
                        )
                        state = (
                            f"{day:%Y-%m-%d} 失败，"
                            f"编码 {len(code_batch)} 个：{error}"
                        )

                    elapsed = time.perf_counter() - started_at
                    average = elapsed / completed_batches
                    remaining = average * (
                        total_day_tasks - completed_batches
                    )
                    print(
                        f"[{completed_batches}/{total_day_tasks}] {state}；"
                        f"预计剩余 {base.format_duration(remaining)}"
                    )

                completed_code_batches = 0
                for batch_index, code_batch in enumerate(
                    plan["pending_batches"], start=1
                ):
                    if batch_index in failed_batches:
                        continue
                    mysql_repository.mark_month_completed(
                        result_mysql,
                        code_batch,
                        plan["progress_month_start"],
                        plan["month_end"],
                    )
                    completed_code_batches += 1

                print(
                    f"月份 {month_label} 处理结束："
                    f"写入或更新 {month_written:,} 条，"
                    f"成功编码批次 {completed_code_batches}/"
                    f"{len(plan['pending_batches'])}，"
                    f"月耗时 {base.format_duration(time.perf_counter() - month_started)}"
                )
    finally:
        stop_timer.set()
        timer_thread.join(timeout=2)

    elapsed = time.perf_counter() - started_at
    finished_clock = datetime.now()
    print("\n全部任务结束。")
    print(f"结束时间：{finished_clock:%Y-%m-%d %H:%M:%S}")
    print(f"写入或更新：{total_written:,} 条日级结果")
    print(f"失败日任务：{len(failures):,} 个")
    print(f"总耗时：{base.format_duration(elapsed)}")

    if failures:
        failure_log = PROJECT_ROOT / "logs" / "threaded_daily_limits_failures.txt"
        with failure_log.open("w", encoding="utf-8") as file:
            for month_label, day, code_batch, error in failures:
                file.write(
                    f"{month_label}\t{day:%Y-%m-%d}\t"
                    f"{','.join(code_batch)}\t{error}\n"
                )
        print(f"失败记录：{failure_log}")

    return total_written


def parse_args():
    parser = argparse.ArgumentParser(
        description="3ST浇铸时段日级上下限多线程统计"
    )
    parser.add_argument(
        "--pipeline-config",
        default=str(DEFAULT_CONFIG),
        help="流水线配置文件",
    )
    parser.add_argument("--start", required=True, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="结束日期 YYYY-MM-DD")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="每个线程任务包含的元数据数量",
    )
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--code",
        help="只处理指定的一个元数据编码，例如 ISA09d957c3f2ac4ef6bd",
    )
    selection.add_argument(
        "--code-limit",
        type=int,
        help="只取前N个元数据，用于小范围测试",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="单次远程查询失败后的最大尝试次数",
    )
    args = parser.parse_args()

    if args.batch_size < 1:
        parser.error("--batch-size必须大于0")
    if args.retries < 1:
        parser.error("--retries必须大于0")
    if pd.Timestamp(args.end) < pd.Timestamp(args.start):
        parser.error("结束日期不能早于开始日期")

    return args


if __name__ == "__main__":
    configure_run_logging()
    run(parse_args())
