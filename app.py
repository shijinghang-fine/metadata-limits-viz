from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components


ROOT = Path(__file__).resolve().parent

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

chart_html = (ROOT / "chart.html").read_text(encoding="utf-8")
plotly_js = (ROOT / "plotly.min.js").read_text(encoding="utf-8")
chart_html = chart_html.replace(
    '<script src="plotly.min.js"></script>',
    f"<script>{plotly_js}</script>",
    1,
)

components.html(chart_html, height=920, scrolling=True)
