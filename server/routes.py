"""Flask 路由定义。

仅负责读取 HTTP 参数、调用 services 并返回响应；数据库操作位于 repository。
"""

import logging
from pathlib import Path

from flask import Flask, Response, jsonify, request

from repository import mysql_repository
from server import services


LOGGER = logging.getLogger("metadata.server.routes")


def create_api(config_file, chart_file):
    """创建图表页面及数据接口应用。

    接口：
    - ``GET /chart``：返回前端可视化页面。
    - ``GET /api/metadata``：返回元数据列表和可查询日期范围。
    - ``GET /api/standards?code=...``：返回指定元数据标准上下限。
    - ``GET /api/daily?code=...&start=...&end=...``：返回每日实际上下限。
    """
    api = Flask(__name__)
    mysql_config = mysql_repository.load_result_mysql_config(config_file)
    chart_path = Path(chart_file)

    @api.after_request
    def add_response_headers(response):
        """允许同机页面访问接口，并禁用接口响应缓存。"""
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Cache-Control"] = "no-store"
        return response

    @api.get("/chart")
    def chart_page():
        """读取并返回二维、三维可视化页面。"""
        return Response(
            chart_path.read_text(encoding="utf-8"),
            content_type="text/html; charset=utf-8",
        )

    @api.get("/api/metadata")
    def metadata_list():
        """提供元数据下拉列表和可查询日期范围。"""
        return jsonify(services.list_metadata(mysql_config))

    @api.get("/api/standards")
    def standards():
        """提供指定元数据的标准上下限和工艺坐标。"""
        code = request.args.get("code", "").strip()
        if not code:
            return jsonify({"error": "缺少元数据编码"}), 400
        return jsonify(services.list_standards(mysql_config, code))

    @api.get("/api/daily")
    def daily():
        """提供指定日期范围内的每日实际上下限。"""
        code = request.args.get("code", "").strip()
        start = request.args.get("start", "").strip()
        end = request.args.get("end", "").strip()
        if not code or not start or not end:
            return jsonify({"error": "缺少元数据编码或日期范围"}), 400
        return jsonify(
            services.list_daily_limits(mysql_config, code, start, end)
        )

    LOGGER.info("数据接口已创建，配置=%s，页面=%s", config_file, chart_path)
    return api
