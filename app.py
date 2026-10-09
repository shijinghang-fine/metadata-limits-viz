from pathlib import Path
import os
import socket
from threading import Thread

import pymysql
import streamlit as st
import yaml
from flask import Flask, Response, jsonify, request
from werkzeug.serving import make_server


ROOT = Path(__file__).resolve().parent
DB_CONFIG_FILE = Path(r"D:\数据查找方式\history\daily_limits_config.yaml")
API_BIND_HOST = "0.0.0.0"
API_PORT = 8765


def get_lan_host():
    """Return the address other computers should use for this machine."""
    configured = os.environ.get("METADATA_WEB_HOST", "").strip()
    if configured:
        return configured

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("192.168.50.1", 9))
            return sock.getsockname()[0]
    except OSError:
        return socket.gethostbyname(socket.gethostname())

STEEL_BY_MARK = {
    "1": "超低碳", "2": "低碳", "3": "包晶钢", "4": "中碳",
    "6": "包晶合金", "7": "中碳合金", "8": "高碳",
    "11": "包晶优化", "12": "低碳优化",
}


def load_mysql_config():
    config = yaml.safe_load(DB_CONFIG_FILE.read_text(encoding="utf-8"))
    return config["ResultMysql"]


def mysql_connection():
    config = load_mysql_config()
    return pymysql.connect(
        host=config["Host"], port=int(config.get("Port", 3306)),
        user=config["Username"], password=config["Password"],
        database=config["database"], charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor, autocommit=True,
    )


def display_name(name):
    value = str(name or "").strip()
    return value.split("#TDC_", 1)[1] if "#TDC_" in value else value


def create_api():
    api = Flask(__name__)

    @api.after_request
    def allow_local_page(response):
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Cache-Control"] = "no-store"
        return response

    @api.get("/chart")
    def chart_page():
        chart_html = (ROOT / "chart.html").read_text(encoding="utf-8")
        plotly_js = (ROOT / "plotly.min.js").read_text(encoding="utf-8")
        chart_html = chart_html.replace(
            '<script src="plotly.min.js"></script>',
            f"<script>{plotly_js}</script>",
            1,
        )
        return Response(chart_html, content_type="text/html; charset=utf-8")

    @api.get("/api/metadata")
    def metadata_list():
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
        with mysql_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
        return jsonify([{
            "code": row["metadata_code"],
            "name": display_name(row["metadata_name"]),
            "minDate": row["min_date"].isoformat(),
            "maxDate": row["max_date"].isoformat(),
        } for row in rows])

    @api.get("/api/standards")
    def standards():
        code = request.args.get("code", "").strip()
        if not code:
            return jsonify({"error": "缺少元数据编码"}), 400
        sql = """
            SELECT metadata_code, metadata_name, condition_key, upper_limit,
                   lower_limit, duration_seconds, steel_mark, casting_speed,
                   width, casting_mode, limit_range
            FROM standard_limits
            WHERE metadata_code = %s AND casting_mode = '浇铸模式'
            ORDER BY steel_mark, width, casting_speed, condition_key
        """
        with mysql_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (code,))
                rows = cursor.fetchall()
        result = []
        for row in rows:
            mark = str(row["steel_mark"] or "").strip()
            speed_raw = float(row["casting_speed"])
            speed = speed_raw / 100 if abs(speed_raw) >= 10 else speed_raw
            width = float(row["width"])
            steel = STEEL_BY_MARK.get(mark, f"钢种标记{mark or '未知'}")
            result.append({
                "metadata": display_name(row["metadata_name"]),
                "metadataCode": row["metadata_code"],
                "combination": row["condition_key"],
                "upper": row["upper_limit"], "lower": row["lower_limit"],
                "duration": row["duration_seconds"], "mark": mark,
                "steel": steel, "speed": speed, "width": width,
                "mode": row["casting_mode"], "range": row["limit_range"],
                "y": 0, "yLabel": f"{steel}｜宽度{width:g}",
            })
        labels = sorted({row["yLabel"] for row in result})
        y_by_label = {label: index for index, label in enumerate(labels)}
        for row in result:
            row["y"] = y_by_label[row["yLabel"]]
        return jsonify(result)

    @api.get("/api/daily")
    def daily():
        code = request.args.get("code", "").strip()
        start = request.args.get("start", "").strip()
        end = request.args.get("end", "").strip()
        if not code or not start or not end:
            return jsonify({"error": "缺少元数据编码或日期范围"}), 400
        sql = """
            SELECT stat_date, metadata_code, steel_grade, width,
                   casting_speed, min_value, max_value, valid_count
            FROM metadata_daily_limits
            WHERE metadata_code = %s AND stat_date BETWEEN %s AND %s
            ORDER BY stat_date, steel_grade, width, casting_speed
        """
        with mysql_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (code, start, end))
                rows = cursor.fetchall()
        return jsonify([{
            "d": row["stat_date"].isoformat(), "c": row["metadata_code"],
            "s": row["steel_grade"], "w": row["width"],
            "v": float(row["casting_speed"]), "l": row["min_value"],
            "u": row["max_value"], "n": row["valid_count"],
        } for row in rows])

    return api


@st.cache_resource
def start_api_server():
    server = make_server(API_BIND_HOST, API_PORT, create_api(), threaded=True)
    Thread(target=server.serve_forever, daemon=True).start()
    return server


st.set_page_config(page_title="元数据工艺场景上下限", page_icon="📊", layout="wide")
st.markdown("""
<style>
  .block-container {padding: 0 !important; max-width: 100% !important;}
  header[data-testid="stHeader"] {background: transparent;}
</style>
""", unsafe_allow_html=True)

try:
    start_api_server()
    st.iframe(f"http://{get_lan_host()}:{API_PORT}/chart", height=920)
except Exception as error:
    st.error(f"页面启动失败：{error}")
