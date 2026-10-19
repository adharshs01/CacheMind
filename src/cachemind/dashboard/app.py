"""CacheMind dashboard.  Run:  streamlit run src/cachemind/dashboard/app.py

Bound to localhost by .streamlit/config.toml. Reads bounded snapshots only and
never starts a Spark session. Every block states where its numbers come from.
"""

from __future__ import annotations

import streamlit as st

from cachemind.dashboard import views as V

st.set_page_config(page_title="CacheMind", page_icon=":material/memory:", layout="wide")

PAGES = [
    st.Page(V.page_overview, title="Project overview", icon=":material/home:", default=True),
    st.Page(V.page_live, title="Live request monitoring", icon=":material/monitoring:"),
    st.Page(V.page_simulator, title="Cache simulator", icon=":material/play_circle:"),
    st.Page(V.page_comparison, title="Algorithm comparison", icon=":material/compare:"),
    st.Page(V.page_models, title="ML model evaluation", icon=":material/model_training:"),
    st.Page(V.page_analytics, title="Big Data analytics", icon=":material/analytics:"),
    st.Page(V.page_health, title="System health", icon=":material/health_and_safety:"),
    st.Page(V.page_research, title="Research experiments", icon=":material/science:"),
]
st.navigation(PAGES).run()
