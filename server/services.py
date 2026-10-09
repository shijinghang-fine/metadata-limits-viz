"""接口业务服务。

负责把 repository 返回的数据库记录转换成前端需要的 JSON 结构，
不直接连接数据库，也不处理 HTTP 请求。
"""

from repository import mysql_repository


STEEL_BY_MARK = {
    "1": "超低碳",
    "2": "低碳",
    "3": "包晶钢",
    "4": "中碳",
    "6": "包晶合金",
    "7": "中碳合金",
    "8": "高碳",
    "11": "包晶优化",
    "12": "低碳优化",
}


def display_name(name):
    """移除元数据展示名称中的内部 TDC 前缀。"""
    value = str(name or "").strip()
    return value.split("#TDC_", 1)[1] if "#TDC_" in value else value


def list_metadata(mysql_config):
    """返回可视化可用的元数据及其最早、最晚数据日期。"""
    rows = mysql_repository.fetch_metadata(mysql_config)
    return [
        {
            "code": row["metadata_code"],
            "name": display_name(row["metadata_name"]),
            "minDate": row["min_date"].isoformat(),
            "maxDate": row["max_date"].isoformat(),
        }
        for row in rows
    ]


def list_standards(mysql_config, metadata_code):
    """返回标准上下限，并生成二维、三维图共用的工艺坐标。"""
    rows = mysql_repository.fetch_standards(mysql_config, metadata_code)
    result = []
    for row in rows:
        mark = str(row["steel_mark"] or "").strip()
        speed_raw = float(row["casting_speed"])
        speed = speed_raw / 100 if abs(speed_raw) >= 10 else speed_raw
        width = float(row["width"])
        steel = STEEL_BY_MARK.get(mark, f"钢种标记{mark or '未知'}")
        result.append(
            {
                "metadata": display_name(row["metadata_name"]),
                "metadataCode": row["metadata_code"],
                "combination": row["condition_key"],
                "upper": row["upper_limit"],
                "lower": row["lower_limit"],
                "duration": row["duration_seconds"],
                "mark": mark,
                "steel": steel,
                "speed": speed,
                "width": width,
                "mode": row["casting_mode"],
                "range": row["limit_range"],
                "y": 0,
                "yLabel": f"{steel}｜宽度{width:g}",
            }
        )

    labels = sorted({row["yLabel"] for row in result})
    y_by_label = {label: index for index, label in enumerate(labels)}
    for row in result:
        row["y"] = y_by_label[row["yLabel"]]
    return result


def list_daily_limits(mysql_config, metadata_code, start_date, end_date):
    """返回日期范围内每日实际上下限、工艺条件及有效数据量。"""
    rows = mysql_repository.fetch_daily_limits(
        mysql_config, metadata_code, start_date, end_date
    )
    return [
        {
            "d": row["stat_date"].isoformat(),
            "c": row["metadata_code"],
            "s": row["steel_grade"],
            "w": row["width"],
            "v": float(row["casting_speed"]),
            "l": row["min_value"],
            "u": row["max_value"],
            "n": row["valid_count"],
        }
        for row in rows
    ]
