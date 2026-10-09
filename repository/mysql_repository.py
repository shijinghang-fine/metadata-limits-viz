"""MySQL 数据访问。

本模块集中管理连接、查询、建表、事务和批量写入；上层只处理业务逻辑。
"""

from pathlib import Path

import pymysql
import yaml


DAILY_LIMITS_TABLE = "metadata_daily_limits"
PROGRESS_TABLE = "daily_limits_month_progress"


def load_result_mysql_config(config_file):
    """从项目配置文件读取结果库连接配置。"""
    config = yaml.safe_load(Path(config_file).read_text(encoding="utf-8"))
    return config["ResultMysql"]


def connect(config, autocommit=False):
    """创建独立 MySQL 连接；多线程任务不得共享该连接。"""
    return pymysql.connect(
        host=config["Host"],
        port=int(config.get("Port", 3306)),
        user=config["Username"],
        password=config["Password"],
        database=config["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=autocommit,
    )


def fetch_metadata(config):
    """查询存在浇铸模式标准的元数据及其可查询日期范围。"""
    sql = """
        SELECT daily.metadata_code,
               COALESCE(
                        (SELECT MAX(s.metadata_name)
                         FROM standard_limits AS s
                         WHERE s.metadata_code =
                               daily.metadata_code COLLATE utf8mb4_unicode_ci
                           AND s.casting_mode = '浇铸模式'),
                        variables.metadata_name,
                        daily.metadata_code) AS metadata_name,
               daily.min_date, daily.max_date
        FROM (
            SELECT metadata_code, MIN(stat_date) AS min_date,
                   MAX(stat_date) AS max_date
            FROM metadata_daily_limits GROUP BY metadata_code
        ) AS daily
        LEFT JOIN metadata_variable AS variables
          ON variables.metadata_code =
             daily.metadata_code COLLATE utf8mb4_unicode_ci
        WHERE EXISTS (
            SELECT 1 FROM standard_limits AS existing
            WHERE existing.metadata_code =
                  daily.metadata_code COLLATE utf8mb4_unicode_ci
              AND existing.casting_mode = '浇铸模式'
        )
        ORDER BY daily.metadata_code
    """
    with connect(config, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()


def fetch_standards(config, metadata_code):
    """查询一个元数据的浇铸模式标准上下限。"""
    sql = """
        SELECT metadata_code, metadata_name, condition_key, upper_limit,
               lower_limit, duration_seconds, steel_mark, casting_speed,
               width, casting_mode, limit_range
        FROM standard_limits
        WHERE metadata_code = %s AND casting_mode = '浇铸模式'
        ORDER BY steel_mark, width, casting_speed, condition_key
    """
    with connect(config, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, (metadata_code,))
            return cursor.fetchall()


def fetch_daily_limits(config, metadata_code, start_date, end_date):
    """查询一个元数据在日期范围内的每日实际上下限。"""
    sql = """
        SELECT stat_date, metadata_code, steel_grade, width,
               casting_speed, min_value, max_value, valid_count
        FROM metadata_daily_limits
        WHERE metadata_code = %s AND stat_date BETWEEN %s AND %s
        ORDER BY stat_date, steel_grade, width, casting_speed
    """
    with connect(config, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, (metadata_code, start_date, end_date))
            return cursor.fetchall()


def get_storage_locations(codes, company_mysql):
    """查询元数据编码对应的 InfluxDB measurement。"""
    if not codes:
        return {}
    placeholders = ",".join(["%s"] * len(codes))
    sql = f"""
        SELECT id, storage_location FROM kl_metadata
        WHERE organ_code = %s AND id IN ({placeholders})
    """
    with connect(company_mysql, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, [company_mysql["organ_code"], *codes])
            return {
                row["id"]: row["storage_location"]
                for row in cursor.fetchall()
            }


def ensure_daily_limits_table(result_mysql):
    """确保日级上下限结果表存在。"""
    sql = f"""
        CREATE TABLE IF NOT EXISTS {DAILY_LIMITS_TABLE} (
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
    connection = connect(result_mysql)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def write_daily_limits(rows, result_mysql, batch_size=1000):
    """批量新增或更新日级上下限，并以一次事务提交。"""
    if not rows:
        return 0
    sql = f"""
        INSERT INTO {DAILY_LIMITS_TABLE} (
            stat_date, metadata_code, steel_grade, width, casting_speed,
            min_value, max_value, valid_count
        ) VALUES (
            %(stat_date)s, %(metadata_code)s, %(steel_grade)s, %(width)s,
            %(casting_speed)s, %(min_value)s, %(max_value)s, %(valid_count)s
        ) ON DUPLICATE KEY UPDATE
            min_value=VALUES(min_value), max_value=VALUES(max_value),
            valid_count=VALUES(valid_count), updated_at=CURRENT_TIMESTAMP
    """
    connection = connect(result_mysql)
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


def ensure_progress_table(result_mysql):
    """确保月份级断点续跑进度表存在。"""
    sql = f"""
        CREATE TABLE IF NOT EXISTS {PROGRESS_TABLE} (
            metadata_code VARCHAR(64) NOT NULL,
            month_start DATE NOT NULL,
            month_end DATE NOT NULL,
            completed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (metadata_code, month_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    connection = connect(result_mysql)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def load_completed_months(result_mysql, codes, first_day, last_day):
    """读取日期范围内已经成功处理的元数据月份。"""
    if not codes:
        return set()
    placeholders = ",".join(["%s"] * len(codes))
    sql = f"""
        SELECT metadata_code, month_start
        FROM {PROGRESS_TABLE}
        WHERE metadata_code IN ({placeholders})
          AND month_start >= %s
          AND month_start <= %s
    """
    with connect(result_mysql, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, [*codes, first_day.date(), last_day.date()])
            return {
                (row["metadata_code"], row["month_start"])
                for row in cursor.fetchall()
            }


def mark_month_completed(result_mysql, codes, month_start, month_end):
    """在整批月任务成功后写入完成标记。"""
    sql = f"""
        INSERT INTO {PROGRESS_TABLE} (
            metadata_code, month_start, month_end, completed_at
        ) VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON DUPLICATE KEY UPDATE
            month_end = VALUES(month_end),
            completed_at = CURRENT_TIMESTAMP
    """
    rows = [
        (code, month_start.date(), month_end.date())
        for code in codes
    ]
    connection = connect(result_mysql)
    try:
        with connection.cursor() as cursor:
            cursor.executemany(sql, rows)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
