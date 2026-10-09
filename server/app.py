"""Streamlit 启动入口。

启动内嵌 Flask 数据服务，并在 Streamlit 中展示 frontend/chart.html。
运行方式：streamlit run server/app.py --server.port 8501
"""

import logging
import os
from pathlib import Path
import socket
import sys
from threading import Thread

import streamlit as st
from werkzeug.serving import make_server


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from server.routes import create_api


CONFIG_FILE = Path(
    os.environ.get(
        "METADATA_CONFIG_FILE",
        PROJECT_ROOT / "config" / "config.yaml",
    )
)
CHART_FILE = PROJECT_ROOT / "frontend" / "chart.html"
LOG_DIR = PROJECT_ROOT / "logs"
API_BIND_HOST = "0.0.0.0"
API_PORT = int(os.environ.get("METADATA_API_PORT", "8765"))


def configure_logging():
    """同时把服务运行信息输出到终端和 logs/server.log。"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("metadata")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    file_handler = logging.FileHandler(
        LOG_DIR / "server.log", encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


LOGGER = configure_logging()


def get_lan_host():
    """返回局域网其他电脑访问当前服务时应使用的本机地址。"""
    configured = os.environ.get("METADATA_WEB_HOST", "").strip()
    if configured:
        return configured
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("192.168.50.1", 9))
            return sock.getsockname()[0]
    except OSError:
        return socket.gethostbyname(socket.gethostname())


@st.cache_resource
def start_api_server():
    """后台启动 Flask API；Streamlit 重跑时复用同一个服务实例。"""
    api = create_api(CONFIG_FILE, CHART_FILE)
    server = make_server(API_BIND_HOST, API_PORT, api, threaded=True)
    Thread(
        target=server.serve_forever,
        name="metadata-api",
        daemon=True,
    ).start()
    LOGGER.info("Flask 数据接口已启动：%s:%s", API_BIND_HOST, API_PORT)
    return server


def render_page():
    """配置 Streamlit 页面并嵌入前端可视化。"""
    st.set_page_config(
        page_title="元数据工艺场景上下限",
        page_icon="📊",
        layout="wide",
    )
    st.markdown(
        """
        <style>
          .block-container {padding: 0 !important; max-width: 100% !important;}
          header[data-testid="stHeader"] {background: transparent;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    start_api_server()
    st.iframe(f"http://{get_lan_host()}:{API_PORT}/chart", height=920)


try:
    render_page()
except Exception as error:
    LOGGER.exception("页面启动失败")
    st.error(f"页面启动失败：{error}")
