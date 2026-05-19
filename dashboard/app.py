"""
Tetouan Smart Energy Dashboard - Production-grade redesign.

Senior frontend pass:
- Dark theme with custom design system (tokens, spacing, semantic colors)
- Hero header with live status pulse
- KPI cards with glassmorphism + delta indicators
- Tab-based information architecture (overview / map / ML / NLP / quality)
- Consistent Plotly template across every chart
- Status pills, alert cards, branded empty states
- WordCloud (Section F) and Data Quality (Section E) sections
"""

import os
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh


# ============================================================
# MongoDB Configuration
# ============================================================

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "energy_project")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

spark_district_collection = db["spark_district_consumption"]
spark_alerts_collection = db["spark_alerts"]
spark_cycle_collection = db["spark_cycle_metadata"]

ml_metrics_collection = db["ml_model_metrics"]
ml_predictions_collection = db["ml_predictions"]
ml_comparisons_collection = db["ml_prediction_comparisons"]

nlp_reports_collection = db["nlp_reports"]
nlp_metrics_collection = db["nlp_model_metrics"]

quality_reports_collection = db["data_quality_reports"]
quality_alerts_collection = db["data_quality_alerts"]
quality_bias_collection = db["data_quality_bias_reports"]


# ============================================================
# Design System
# ============================================================

COLOR = {
    "bg": "#0B0F1A",
    "surface": "#121826",
    "surface_alt": "#1A2233",
    "border": "rgba(255,255,255,0.08)",
    "text": "#E6EAF2",
    "text_muted": "#94A3B8",
    "accent": "#00E5C7",          # mint / electric
    "accent_2": "#7C3AED",        # violet
    "warning": "#F59E0B",
    "danger": "#EF4444",
    "success": "#22C55E",
    "info": "#3B82F6",
}

STATUS_COLOR = {
    "normal": COLOR["success"],
    "voltage_risk": COLOR["warning"],
    "overload_risk": COLOR["danger"],
    "saturation_risk": COLOR["accent_2"],
    "unknown": COLOR["text_muted"],
}

ZONE_COLOR = {
    "Residential": "#3B82F6",
    "Commercial": "#F59E0B",
    "Infrastructure": "#7C3AED",
}


def chart_template():
    """Single Plotly layout template applied to every chart."""
    return dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(
            family="Inter, system-ui, -apple-system, Segoe UI, Roboto, sans-serif",
            size=13,
            color=COLOR["text"],
        ),
        title=dict(
            font=dict(size=16, color=COLOR["text"]),
            x=0,
            xanchor="left",
        ),
        margin=dict(l=10, r=10, t=50, b=10),
        xaxis=dict(
            gridcolor="rgba(255,255,255,0.05)",
            zerolinecolor="rgba(255,255,255,0.1)",
        ),
        yaxis=dict(
            gridcolor="rgba(255,255,255,0.05)",
            zerolinecolor="rgba(255,255,255,0.1)",
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor=COLOR["border"],
            borderwidth=1,
        ),
        hoverlabel=dict(
            bgcolor=COLOR["surface_alt"],
            bordercolor=COLOR["border"],
            font_size=12,
            font_color=COLOR["text"],
        ),
    )


def apply_chart_theme(fig):
    fig.update_layout(**chart_template())
    return fig


# ============================================================
# Streamlit Page Configuration
# ============================================================

st.set_page_config(
    page_title="Tetouan Smart Energy",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# Custom CSS
# ============================================================

st.markdown(
    f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    html, body, [class*="css"] {{
        font-family: 'Inter', system-ui, -apple-system, sans-serif !important;
    }}

    .stApp {{
        background:
            radial-gradient(1200px 600px at 10% -10%, rgba(0,229,199,0.08), transparent 60%),
            radial-gradient(900px 500px at 90% 0%, rgba(124,58,237,0.10), transparent 60%),
            {COLOR["bg"]};
        color: {COLOR["text"]};
    }}

    /* Hide Streamlit branding */
    #MainMenu, footer, header {{visibility: hidden;}}

    /* Sidebar */
    [data-testid="stSidebar"] {{
        background: {COLOR["surface"]};
        border-right: 1px solid {COLOR["border"]};
    }}
    [data-testid="stSidebar"] * {{
        color: {COLOR["text"]};
    }}

    /* Hero header */
    .tse-hero {{
        background: linear-gradient(135deg, rgba(0,229,199,0.10) 0%, rgba(124,58,237,0.10) 100%);
        border: 1px solid {COLOR["border"]};
        border-radius: 16px;
        padding: 22px 28px;
        margin-bottom: 18px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
        gap: 16px;
    }}
    .tse-hero h1 {{
        font-size: 26px;
        font-weight: 800;
        margin: 0;
        letter-spacing: -0.02em;
        background: linear-gradient(90deg, {COLOR["accent"]}, {COLOR["accent_2"]});
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }}
    .tse-hero p {{
        margin: 4px 0 0 0;
        color: {COLOR["text_muted"]};
        font-size: 13px;
    }}
    .tse-hero-right {{
        display: flex;
        align-items: center;
        gap: 12px;
    }}
    .tse-pulse {{
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 6px 12px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 600;
        background: rgba(34,197,94,0.12);
        color: {COLOR["success"]};
        border: 1px solid rgba(34,197,94,0.3);
    }}
    .tse-pulse-dot {{
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: {COLOR["success"]};
        box-shadow: 0 0 0 0 rgba(34,197,94,0.7);
        animation: tse-pulse 2s infinite;
    }}
    @keyframes tse-pulse {{
        0%   {{ box-shadow: 0 0 0 0 rgba(34,197,94,0.7); }}
        70%  {{ box-shadow: 0 0 0 10px rgba(34,197,94,0); }}
        100% {{ box-shadow: 0 0 0 0 rgba(34,197,94,0); }}
    }}

    /* KPI cards */
    .tse-kpi {{
        background: {COLOR["surface"]};
        border: 1px solid {COLOR["border"]};
        border-radius: 14px;
        padding: 16px 18px;
        height: 100%;
        position: relative;
        overflow: hidden;
        transition: transform 0.15s ease, border-color 0.15s ease;
    }}
    .tse-kpi:hover {{
        transform: translateY(-2px);
        border-color: rgba(0,229,199,0.4);
    }}
    .tse-kpi::before {{
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 2px;
        background: linear-gradient(90deg, {COLOR["accent"]}, {COLOR["accent_2"]});
    }}
    .tse-kpi-label {{
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: {COLOR["text_muted"]};
        font-weight: 600;
    }}
    .tse-kpi-value {{
        font-size: 26px;
        font-weight: 800;
        color: {COLOR["text"]};
        margin-top: 4px;
        letter-spacing: -0.02em;
    }}
    .tse-kpi-sub {{
        font-size: 12px;
        color: {COLOR["text_muted"]};
        margin-top: 6px;
    }}
    .tse-kpi-icon {{
        position: absolute;
        right: 14px;
        top: 14px;
        font-size: 22px;
        opacity: 0.45;
    }}

    /* Status pills */
    .tse-pill {{
        display: inline-block;
        padding: 3px 10px;
        border-radius: 999px;
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.04em;
    }}

    /* Alert cards */
    .tse-alert {{
        border-radius: 10px;
        padding: 10px 14px;
        margin: 6px 0;
        display: flex;
        align-items: center;
        gap: 12px;
        border: 1px solid;
        font-size: 13px;
    }}
    .tse-alert.warning {{
        background: rgba(245,158,11,0.08);
        border-color: rgba(245,158,11,0.35);
        color: #fcd34d;
    }}
    .tse-alert.danger {{
        background: rgba(239,68,68,0.08);
        border-color: rgba(239,68,68,0.35);
        color: #fca5a5;
    }}
    .tse-alert.info {{
        background: rgba(59,130,246,0.08);
        border-color: rgba(59,130,246,0.35);
        color: #93c5fd;
    }}

    /* Section headings */
    .tse-section-title {{
        font-size: 18px;
        font-weight: 700;
        color: {COLOR["text"]};
        margin: 22px 0 10px 0;
        display: flex;
        align-items: center;
        gap: 10px;
    }}
    .tse-section-title::before {{
        content: '';
        width: 4px;
        height: 18px;
        background: linear-gradient(180deg, {COLOR["accent"]}, {COLOR["accent_2"]});
        border-radius: 2px;
    }}
    .tse-section-sub {{
        font-size: 12px;
        color: {COLOR["text_muted"]};
        margin: 0 0 14px 14px;
    }}

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 4px;
        background: {COLOR["surface"]};
        padding: 4px;
        border-radius: 12px;
        border: 1px solid {COLOR["border"]};
    }}
    .stTabs [data-baseweb="tab"] {{
        background: transparent;
        border-radius: 8px;
        padding: 8px 16px;
        font-weight: 600;
        font-size: 13px;
        color: {COLOR["text_muted"]};
    }}
    .stTabs [aria-selected="true"] {{
        background: linear-gradient(135deg, rgba(0,229,199,0.18), rgba(124,58,237,0.18));
        color: {COLOR["text"]} !important;
        border: 1px solid rgba(0,229,199,0.35);
    }}

    /* Dataframes */
    [data-testid="stDataFrame"] {{
        border-radius: 10px;
        overflow: hidden;
        border: 1px solid {COLOR["border"]};
    }}

    /* Streamlit metric override */
    [data-testid="stMetricValue"] {{
        font-weight: 800;
        font-size: 24px !important;
    }}

    /* Empty hero illustration */
    .tse-empty {{
        background: {COLOR["surface"]};
        border: 1px dashed {COLOR["border"]};
        border-radius: 16px;
        padding: 40px 30px;
        text-align: center;
        margin: 20px 0;
    }}
    .tse-empty h3 {{
        margin: 0 0 8px 0;
        color: {COLOR["text"]};
    }}
    .tse-empty p {{
        margin: 0;
        color: {COLOR["text_muted"]};
        font-size: 13px;
    }}
    .tse-empty-icon {{
        font-size: 42px;
        margin-bottom: 12px;
        opacity: 0.5;
    }}

    /* Footer */
    .tse-footer {{
        margin-top: 36px;
        padding-top: 16px;
        border-top: 1px solid {COLOR["border"]};
        font-size: 11px;
        color: {COLOR["text_muted"]};
        display: flex;
        justify-content: space-between;
        flex-wrap: wrap;
        gap: 8px;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# UI Helpers
# ============================================================

def section_title(icon, title, subtitle=None):
    st.markdown(
        f'<div class="tse-section-title">{icon} {title}</div>',
        unsafe_allow_html=True,
    )
    if subtitle:
        st.markdown(
            f'<div class="tse-section-sub">{subtitle}</div>',
            unsafe_allow_html=True,
        )


def kpi_card(label, value, sub=None, icon=""):
    sub_html = f'<div class="tse-kpi-sub">{sub}</div>' if sub else ""
    icon_html = f'<div class="tse-kpi-icon">{icon}</div>' if icon else ""
    st.markdown(
        f"""
        <div class="tse-kpi">
            {icon_html}
            <div class="tse-kpi-label">{label}</div>
            <div class="tse-kpi-value">{value}</div>
            {sub_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def status_pill(status):
    color = STATUS_COLOR.get(status, COLOR["text_muted"])
    label = status.replace("_", " ").title()
    return (
        f'<span class="tse-pill" style="background:{color}22; color:{color}; '
        f'border:1px solid {color}55;">{label}</span>'
    )


def alert_card(level, content):
    return f'<div class="tse-alert {level}">{content}</div>'


def empty_state(icon, title, message):
    st.markdown(
        f"""
        <div class="tse-empty">
            <div class="tse-empty-icon">{icon}</div>
            <h3>{title}</h3>
            <p>{message}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# Data Loading
# ============================================================

@st.cache_data(ttl=5)
def load_collection(name, sort_field=None, limit=2000):
    coll = db[name]
    cur = coll.find()
    if sort_field:
        cur = cur.sort(sort_field, -1)
    data = list(cur.limit(limit))
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)
    return df


def to_datetime(df, *cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df


def to_numeric(df, *cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


district_df = load_collection("spark_district_consumption", "processed_at", 1500)
district_df = to_datetime(district_df, "processed_at")
district_df = to_numeric(district_df, "cycle_id")
if not district_df.empty:
    district_df = district_df.sort_values(["cycle_id", "district"])

alerts_df = load_collection("spark_alerts", "created_at", 300)
alerts_df = to_datetime(alerts_df, "created_at")
alerts_df = to_numeric(alerts_df, "cycle_id")

cycles_df = load_collection("spark_cycle_metadata", "cycle_id", 100)
cycles_df = to_numeric(cycles_df, "cycle_id")
if not cycles_df.empty:
    cycles_df = cycles_df.sort_values("cycle_id")

ml_metrics_df = load_collection("ml_model_metrics", "created_at", 300)
ml_metrics_df = to_datetime(ml_metrics_df, "created_at", "updated_at")

ml_predictions_df = load_collection("ml_predictions", "prediction_generated_at", 1000)
ml_predictions_df = to_datetime(ml_predictions_df, "prediction_generated_at")
ml_predictions_df = to_numeric(ml_predictions_df, "predicted_cycle_id", "based_on_cycle_id")

ml_comparisons_df = load_collection("ml_prediction_comparisons", "compared_at", 1000)
ml_comparisons_df = to_datetime(ml_comparisons_df, "compared_at")
ml_comparisons_df = to_numeric(ml_comparisons_df, "cycle_id")

nlp_reports_df = load_collection("nlp_reports", "analyzed_at", 300)
nlp_reports_df = to_datetime(nlp_reports_df, "analyzed_at")

nlp_metrics_df = load_collection("nlp_model_metrics", "updated_at", 50)
nlp_metrics_df = to_datetime(nlp_metrics_df, "updated_at")

quality_reports_df = load_collection("data_quality_reports", "processed_at", 200)
quality_reports_df = to_datetime(quality_reports_df, "processed_at")
quality_reports_df = to_numeric(quality_reports_df, "cycle_id")

quality_bias_df = load_collection("data_quality_bias_reports", "processed_at", 500)
quality_bias_df = to_datetime(quality_bias_df, "processed_at")
quality_bias_df = to_numeric(quality_bias_df, "cycle_id")


# ============================================================
# Sidebar
# ============================================================

st.sidebar.markdown(
    f"""
    <div style="padding: 14px 6px 18px 6px; border-bottom: 1px solid {COLOR["border"]}; margin-bottom: 14px;">
        <div style="font-size: 22px; font-weight: 800; letter-spacing: -0.02em;
                    background: linear-gradient(90deg, {COLOR["accent"]}, {COLOR["accent_2"]});
                    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
                    background-clip: text;">
            ⚡ Tetouan Smart Energy
        </div>
        <div style="font-size: 11px; color: {COLOR["text_muted"]}; margin-top: 4px;">
            Real-time grid analytics
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.sidebar.markdown("**⚙️ Settings**")
refresh_seconds = st.sidebar.slider(
    "Auto-refresh (seconds)",
    min_value=3,
    max_value=60,
    value=5,
)
st_autorefresh(interval=refresh_seconds * 1000, key="tse_refresh")


# ============================================================
# Empty State
# ============================================================

if district_df.empty:
    st.markdown(
        f"""
        <div class="tse-hero">
            <div>
                <h1>⚡ Tetouan Smart Energy Dashboard</h1>
                <p>Pipeline: Smart Meters → Kafka → Spark → MongoDB → Analytics → Dashboard</p>
            </div>
            <div class="tse-hero-right">
                <span class="tse-pulse" style="background:rgba(245,158,11,0.12); color:{COLOR["warning"]}; border-color:rgba(245,158,11,0.35);">
                    <span class="tse-pulse-dot" style="background:{COLOR["warning"]};"></span>
                    Waiting for first cycle
                </span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    empty_state(
        "📡",
        "Waiting for the first Spark cycle…",
        "The pipeline is running. Spark will write its first batch into MongoDB once "
        "the simulator finishes the first 15-minute cycle (or 2 minutes in demo mode).",
    )

    with st.expander("🛠️ Troubleshooting commands"):
        st.code(
            "docker logs -f tetouan-simulator\n"
            "docker logs -f spark-streaming-job\n"
            "docker logs -f analytics-job\n"
            "docker compose ps",
            language="bash",
        )

    st.stop()


# ============================================================
# Cycle Selection (sidebar)
# ============================================================

st.sidebar.markdown("**📅 Cycle**")

completed_cycles = (
    district_df["cycle_id"].dropna().astype(int).sort_values().unique().tolist()
)

selected_cycle = st.sidebar.selectbox(
    "Select cycle",
    completed_cycles,
    index=len(completed_cycles) - 1,
)

cycle_df = district_df[district_df["cycle_id"] == selected_cycle].copy()

available_districts = sorted(cycle_df["district"].dropna().unique().tolist())

st.sidebar.markdown("**🏘️ Districts**")
selected_districts = st.sidebar.multiselect(
    "Filter districts",
    available_districts,
    default=available_districts,
    label_visibility="collapsed",
)

cycle_df = cycle_df[cycle_df["district"].isin(selected_districts)]
history_df = district_df[district_df["district"].isin(selected_districts)].copy()


# ============================================================
# Hero Header
# ============================================================

cycle_status = "unknown"
if not cycles_df.empty:
    sel = cycles_df[cycles_df["cycle_id"] == selected_cycle]
    if not sel.empty and "status" in sel.columns:
        cycle_status = sel.iloc[0]["status"]

is_complete = cycle_status == "completed"
pulse_color = COLOR["success"] if is_complete else COLOR["warning"]
pulse_label = "Cycle complete" if is_complete else f"Cycle {cycle_status}"

st.markdown(
    f"""
    <div class="tse-hero">
        <div>
            <h1>⚡ Tetouan Smart Energy Dashboard</h1>
            <p>Pipeline · Smart Meters → Kafka → Spark → MongoDB → Analytics</p>
        </div>
        <div class="tse-hero-right">
            <span class="tse-pulse" style="background:{pulse_color}1a; color:{pulse_color}; border-color:{pulse_color}59;">
                <span class="tse-pulse-dot" style="background:{pulse_color};"></span>
                Cycle {selected_cycle} · {pulse_label}
            </span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# KPI Cards
# ============================================================

total_energy = float(cycle_df["total_energy_consumption"].sum())
avg_voltage = float(cycle_df["avg_voltage"].mean()) if not cycle_df.empty else 0.0
total_meters = int(cycle_df["total_meters"].sum())
risk_count = int((cycle_df["district_status"] != "normal").sum())
district_count = int(cycle_df["district"].nunique())
quality_score = float(cycle_df["quality_score"].mean()) if "quality_score" in cycle_df.columns else 0.0

# Compute deltas vs previous cycle
prev_energy = None
if len(completed_cycles) >= 2:
    prev_cycle_id = completed_cycles[-2]
    prev_cycle_df = district_df[district_df["cycle_id"] == prev_cycle_id]
    if not prev_cycle_df.empty:
        prev_energy = float(prev_cycle_df["total_energy_consumption"].sum())

delta_energy_html = ""
if prev_energy is not None and prev_energy > 0:
    delta_pct = ((total_energy - prev_energy) / prev_energy) * 100
    arrow = "▲" if delta_pct >= 0 else "▼"
    color = COLOR["danger"] if delta_pct >= 0 else COLOR["success"]
    delta_energy_html = (
        f'<span style="color:{color};">{arrow} {abs(delta_pct):.1f}% vs cycle {prev_cycle_id}</span>'
    )

k1, k2, k3, k4, k5, k6 = st.columns(6)
with k1:
    kpi_card(
        "Total energy",
        f"{total_energy:,.1f}",
        sub=delta_energy_html or "kWh this cycle",
        icon="⚡",
    )
with k2:
    kpi_card("Avg voltage", f"{avg_voltage:.1f} V", sub="Target ≈ 220 V", icon="🔌")
with k3:
    kpi_card("Smart meters", f"{total_meters:,}", sub="Aggregated", icon="📊")
with k4:
    kpi_card(
        "Districts",
        f"{district_count}/18",
        sub="Reporting now",
        icon="🏘️",
    )
with k5:
    risk_color = COLOR["danger"] if risk_count > 0 else COLOR["success"]
    kpi_card(
        "Districts at risk",
        f"<span style='color:{risk_color};'>{risk_count}</span>",
        sub="Active anomalies",
        icon="⚠️",
    )
with k6:
    q_color = (
        COLOR["success"] if quality_score >= 95
        else COLOR["warning"] if quality_score >= 90
        else COLOR["danger"]
    )
    kpi_card(
        "Data quality",
        f"<span style='color:{q_color};'>{quality_score:.1f}%</span>",
        sub="Spark validation",
        icon="✅",
    )


# ============================================================
# Tabs
# ============================================================

tab_overview, tab_districts, tab_ml, tab_nlp, tab_quality = st.tabs(
    [
        "📊 Overview",
        "🗺️ Districts & Map",
        "🧠 Machine Learning",
        "📰 NLP & Alerts",
        "✅ Data Quality",
    ]
)


# ============================================================
# TAB 1 — OVERVIEW
# ============================================================

with tab_overview:
    section_title("🗺️", "Tetouan consumption map", "Bubble size = energy · color = status")

    fig_map = px.scatter_mapbox(
        cycle_df,
        lat="latitude",
        lon="longitude",
        size="total_energy_consumption",
        color="district_status",
        color_discrete_map=STATUS_COLOR,
        hover_name="district",
        hover_data={
            "zone_type": True,
            "total_energy_consumption": ":.2f",
            "avg_voltage": ":.2f",
            "avg_current": ":.2f",
            "total_meters": True,
            "overload_count": True,
            "voltage_drop_count": True,
            "latitude": False,
            "longitude": False,
        },
        zoom=12.5,
        height=560,
    )
    fig_map.update_layout(
        mapbox_style="carto-darkmatter",
        mapbox=dict(center=dict(lat=35.57, lon=-5.37), zoom=12.5),
        margin=dict(l=0, r=0, t=10, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color=COLOR["text"]),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            x=0,
            bgcolor="rgba(0,0,0,0)",
        ),
    )
    st.plotly_chart(fig_map, use_container_width=True)

    col_left, col_right = st.columns([7, 5])

    with col_left:
        section_title("📊", "Energy by district")
        sorted_df = cycle_df.sort_values("total_energy_consumption", ascending=True)
        fig_bar = px.bar(
            sorted_df,
            y="district",
            x="total_energy_consumption",
            color="zone_type",
            color_discrete_map=ZONE_COLOR,
            orientation="h",
            text="total_energy_consumption",
        )
        fig_bar.update_traces(
            texttemplate="%{text:.0f}",
            textposition="outside",
            cliponaxis=False,
        )
        fig_bar.update_xaxes(title="Consumption (kWh)")
        fig_bar.update_yaxes(title="")
        fig_bar.update_layout(height=520)
        apply_chart_theme(fig_bar)
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_right:
        section_title("🥧", "Zone breakdown")
        zone_df = (
            cycle_df.groupby("zone_type", as_index=False)
            .agg(
                total_energy_consumption=("total_energy_consumption", "sum"),
                avg_voltage=("avg_voltage", "mean"),
                total_meters=("total_meters", "sum"),
            )
        )
        fig_zone = px.pie(
            zone_df,
            names="zone_type",
            values="total_energy_consumption",
            color="zone_type",
            color_discrete_map=ZONE_COLOR,
            hole=0.55,
        )
        fig_zone.update_traces(
            textposition="outside",
            textinfo="label+percent",
            marker=dict(line=dict(color=COLOR["bg"], width=2)),
        )
        fig_zone.update_layout(height=380, showlegend=False)
        apply_chart_theme(fig_zone)
        st.plotly_chart(fig_zone, use_container_width=True)

        section_title("🔌", "Voltage stability")
        fig_v = px.line(
            history_df.sort_values("cycle_id"),
            x="cycle_id",
            y="avg_voltage",
            color="district",
        )
        fig_v.add_hline(
            y=220, line_dash="dash", line_color=COLOR["text_muted"],
            annotation_text="220 V nominal", annotation_position="top right",
            annotation_font_color=COLOR["text_muted"],
        )
        fig_v.add_hline(
            y=210, line_dash="dot", line_color=COLOR["warning"],
            annotation_text="210 V threshold", annotation_position="bottom right",
            annotation_font_color=COLOR["warning"],
        )
        fig_v.update_xaxes(title="Cycle")
        fig_v.update_yaxes(title="Voltage (V)")
        fig_v.update_layout(height=320, showlegend=False)
        apply_chart_theme(fig_v)
        st.plotly_chart(fig_v, use_container_width=True)


# ============================================================
# TAB 2 — DISTRICTS & MAP
# ============================================================

with tab_districts:
    section_title("🌡️", "Real-time consumption heatmap", "Energy per district over time")

    heatmap_df = history_df.copy()
    if "processed_at" not in heatmap_df.columns or heatmap_df["processed_at"].isna().all():
        empty_state(
            "🌡️",
            "No timeline data yet",
            "The heatmap needs at least one processed cycle with timestamps.",
        )
    else:
        max_points = st.slider(
            "Recent aggregation points to display",
            20, 300, 100, 20,
        )
        heatmap_df = heatmap_df.dropna(subset=["processed_at"]).sort_values("processed_at").tail(max_points)
        heatmap_df["time_label"] = heatmap_df["processed_at"].dt.floor("min").dt.strftime("%H:%M")
        pivot_df = (
            heatmap_df.groupby(["district", "time_label"], as_index=False)
            .agg(total_energy_consumption=("total_energy_consumption", "sum"))
            .pivot(index="district", columns="time_label", values="total_energy_consumption")
            .fillna(0)
        )

        fig_hm = go.Figure(
            data=go.Heatmap(
                z=pivot_df.values,
                x=pivot_df.columns,
                y=pivot_df.index,
                colorscale=[
                    [0.0, "#0f3a2e"],
                    [0.3, "#0d8a4f"],
                    [0.6, "#f59e0b"],
                    [1.0, "#ef4444"],
                ],
                colorbar=dict(title="kWh", thickness=12, len=0.7),
                hovertemplate="<b>%{y}</b><br>%{x}<br>%{z:.2f} kWh<extra></extra>",
                xgap=2,
                ygap=2,
            )
        )
        fig_hm.update_layout(
            height=560,
            xaxis=dict(tickangle=-45, side="bottom"),
            yaxis=dict(autorange="reversed"),
        )
        apply_chart_theme(fig_hm)
        st.plotly_chart(fig_hm, use_container_width=True)

    section_title("📋", "District aggregations")

    table_columns = [
        "cycle_id", "district", "zone_type", "total_energy_consumption",
        "avg_voltage", "avg_current", "total_meters",
        "overload_count", "voltage_drop_count", "quality_score", "district_status",
    ]
    existing = [c for c in table_columns if c in cycle_df.columns]
    table_df = cycle_df[existing].sort_values("total_energy_consumption", ascending=False)

    st.dataframe(
        table_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "total_energy_consumption": st.column_config.NumberColumn(
                "Energy (kWh)", format="%.2f"
            ),
            "avg_voltage": st.column_config.NumberColumn(
                "Voltage (V)", format="%.2f"
            ),
            "avg_current": st.column_config.NumberColumn(
                "Current (A)", format="%.2f"
            ),
            "quality_score": st.column_config.ProgressColumn(
                "Quality",
                format="%.1f%%",
                min_value=0,
                max_value=100,
            ),
        },
    )

    with st.expander("📦 Cycle metadata"):
        if cycles_df.empty:
            st.info("No cycle metadata yet.")
        else:
            st.dataframe(cycles_df, use_container_width=True, hide_index=True)


# ============================================================
# TAB 3 — MACHINE LEARNING
# ============================================================

with tab_ml:
    section_title("🤖", "Model comparison", "3 regressors trained automatically every 2 minutes")

    if ml_metrics_df.empty:
        empty_state(
            "🧠",
            "ML models not trained yet",
            "Analytics-job needs ≥ 2 completed cycles before training. "
            "Keep the pipeline running.",
        )
    else:
        metrics_display = ml_metrics_df.copy()
        if "task" in metrics_display.columns:
            metrics_display = metrics_display[
                metrics_display["task"] == "energy_consumption_regression"
            ].copy()

        if metrics_display.empty:
            empty_state("📈", "No regression metrics yet", "Waiting for the first training round.")
        else:
            best = metrics_display.sort_values("rmse").iloc[0]
            mb1, mb2, mb3, mb4 = st.columns(4)
            with mb1:
                kpi_card(
                    "🏆 Best model",
                    str(best.get("model_name", "—")).replace("_", " ").title(),
                    sub="Lowest RMSE",
                    icon="🥇",
                )
            with mb2:
                kpi_card("RMSE", f"{float(best.get('rmse', 0)):.3f}", sub="Lower = better")
            with mb3:
                kpi_card("R²", f"{float(best.get('r2_score', 0)):.3f}", sub="Higher = better")
            with mb4:
                kpi_card(
                    "Inference",
                    f"{float(best.get('inference_time_seconds', 0)) * 1000:.1f} ms",
                    sub="Per batch",
                )

            col_a, col_b = st.columns(2)
            with col_a:
                fig_rmse = px.bar(
                    metrics_display.sort_values("rmse"),
                    x="model_name", y="rmse",
                    color="model_name",
                    text="rmse",
                    title="RMSE comparison",
                )
                fig_rmse.update_traces(texttemplate="%{text:.3f}", textposition="outside")
                fig_rmse.update_layout(showlegend=False, height=380)
                apply_chart_theme(fig_rmse)
                st.plotly_chart(fig_rmse, use_container_width=True)

            with col_b:
                fig_r2 = px.bar(
                    metrics_display.sort_values("r2_score", ascending=False),
                    x="model_name", y="r2_score",
                    color="model_name",
                    text="r2_score",
                    title="R² comparison",
                )
                fig_r2.update_traces(texttemplate="%{text:.3f}", textposition="outside")
                fig_r2.update_layout(showlegend=False, height=380)
                apply_chart_theme(fig_r2)
                st.plotly_chart(fig_r2, use_container_width=True)

            with st.expander("🔬 All metrics"):
                cols = [
                    "model_name", "mae", "rmse", "r2_score",
                    "training_time_seconds", "inference_time_seconds",
                    "train_rows", "test_rows", "created_at",
                ]
                existing_cols = [c for c in cols if c in metrics_display.columns]
                st.dataframe(
                    metrics_display[existing_cols],
                    use_container_width=True,
                    hide_index=True,
                )

    section_title("🎯", "Real vs predicted consumption", "Best model · per district")

    if ml_comparisons_df.empty:
        if ml_predictions_df.empty:
            empty_state(
                "🎯",
                "Awaiting first prediction",
                "The best model will predict the next cycle as soon as enough history is collected.",
            )
        else:
            latest_pc = int(ml_predictions_df["predicted_cycle_id"].max())
            preds = ml_predictions_df[ml_predictions_df["predicted_cycle_id"] == latest_pc].copy()
            st.markdown(
                alert_card(
                    "info",
                    f"📡 Predictions ready for upcoming cycle <b>{latest_pc}</b>. "
                    f"Real values not available yet.",
                ),
                unsafe_allow_html=True,
            )
            fig_pp = px.bar(
                preds.sort_values("predicted_consumption", ascending=False),
                x="district", y="predicted_consumption",
                color="zone_type",
                color_discrete_map=ZONE_COLOR,
            )
            fig_pp.update_layout(xaxis_tickangle=-45, height=380)
            apply_chart_theme(fig_pp)
            st.plotly_chart(fig_pp, use_container_width=True)
    else:
        comparison_districts = sorted(
            ml_comparisons_df["district"].dropna().unique().tolist()
        )
        sel_d = st.selectbox(
            "District",
            comparison_districts,
            key="best_model_prediction_district",
        )
        cdf = ml_comparisons_df[ml_comparisons_df["district"] == sel_d].sort_values("cycle_id")

        fig_p = go.Figure()
        fig_p.add_trace(go.Scatter(
            x=cdf["cycle_id"], y=cdf["actual_consumption"],
            mode="lines+markers", name="Real",
            line=dict(color=COLOR["accent"], width=3),
            marker=dict(size=8),
        ))
        fig_p.add_trace(go.Scatter(
            x=cdf["cycle_id"], y=cdf["predicted_consumption"],
            mode="lines+markers", name="Predicted",
            line=dict(color=COLOR["accent_2"], width=3, dash="dash"),
            marker=dict(size=8, symbol="diamond"),
        ))
        fig_p.update_layout(
            title=f"Real vs predicted — {sel_d}",
            xaxis_title="Cycle", yaxis_title="Consumption (kWh)",
            height=400,
        )
        apply_chart_theme(fig_p)
        st.plotly_chart(fig_p, use_container_width=True)

        last = cdf.tail(1)
        if not last.empty:
            row = last.iloc[0]
            ec1, ec2, ec3 = st.columns(3)
            with ec1:
                kpi_card(
                    "Absolute error",
                    f"{float(row.get('absolute_error', 0)):.2f} kWh",
                    icon="📏",
                )
            with ec2:
                pe = float(row.get("percentage_error", 0))
                pe_color = (
                    COLOR["success"] if pe < 5
                    else COLOR["warning"] if pe < 15
                    else COLOR["danger"]
                )
                kpi_card(
                    "Percentage error",
                    f"<span style='color:{pe_color};'>{pe:.2f} %</span>",
                    icon="📉",
                )
            with ec3:
                kpi_card("Cycle", f"{int(row.get('cycle_id', 0))}", icon="🔢")

    section_title("🔮", "Next-cycle predictions")
    if ml_predictions_df.empty:
        empty_state("🔮", "No predictions yet", "Waiting for the analytics job to train models.")
    else:
        latest_pc = int(ml_predictions_df["predicted_cycle_id"].max())
        preds = ml_predictions_df[ml_predictions_df["predicted_cycle_id"] == latest_pc].copy()
        st.markdown(
            alert_card("info", f"🔮 Predictions for cycle <b>{latest_pc}</b>"),
            unsafe_allow_html=True,
        )
        fig_np = px.bar(
            preds.sort_values("predicted_consumption", ascending=False),
            x="district", y="predicted_consumption",
            color="zone_type",
            color_discrete_map=ZONE_COLOR,
        )
        fig_np.update_layout(
            xaxis_tickangle=-45, height=380,
            yaxis_title="Predicted consumption (kWh)",
            xaxis_title="",
        )
        apply_chart_theme(fig_np)
        st.plotly_chart(fig_np, use_container_width=True)


# ============================================================
# TAB 4 — NLP & ALERTS
# ============================================================

with tab_nlp:
    section_title("🚨", "Spark alerts for current cycle")
    cycle_alerts = (
        alerts_df[alerts_df["cycle_id"] == selected_cycle]
        if not alerts_df.empty and "cycle_id" in alerts_df.columns
        else pd.DataFrame()
    )

    if cycle_alerts.empty:
        st.markdown(
            alert_card("info", f"✅ No alerts for cycle <b>{selected_cycle}</b>. Network is stable."),
            unsafe_allow_html=True,
        )
    else:
        for _, row in cycle_alerts.iterrows():
            atype = row.get("alert_type", "unknown")
            level = "danger" if "overload" in str(atype) or "saturation" in str(atype) else "warning"
            content = (
                f"<b>{row.get('district')}</b> &middot; {status_pill(atype)} &middot; "
                f"⚡ {row.get('total_energy_consumption', 0):.1f} kWh &middot; "
                f"🔌 {row.get('avg_voltage', 0):.1f} V"
            )
            st.markdown(alert_card(level, content), unsafe_allow_html=True)

    section_title("📰", "NLP technical reports", "TF-IDF + Logistic Regression on industrial RSS feeds")

    if nlp_metrics_df.empty:
        empty_state("📰", "No NLP metrics yet", "RSS producer needs more messages.")
    else:
        latest = nlp_metrics_df.head(1).iloc[0]
        nm1, nm2, nm3, nm4 = st.columns(4)
        with nm1:
            kpi_card(
                "Model",
                str(latest.get("model_name", "—")).replace("_", " ").title(),
                icon="🧠",
            )
        with nm2:
            acc = float(latest.get("accuracy", 0))
            kpi_card("Accuracy", f"{acc * 100:.1f} %", icon="🎯")
        with nm3:
            f1 = float(latest.get("f1_score", 0))
            kpi_card("F1 Score", f"{f1 * 100:.1f} %", icon="⚖️")
        with nm4:
            kpi_card("Reports analyzed", f"{int(latest.get('total_rows', 0)):,}", icon="📚")

    if nlp_reports_df.empty:
        empty_state("📰", "No NLP reports yet", "Waiting for RSS messages to be classified.")
    else:
        latest_nlp = nlp_reports_df.head(20)
        for _, row in latest_nlp.iterrows():
            correlated = bool(row.get("correlated_with_physical_anomaly", False))
            risk = float(row.get("risk_score", 0))
            cat = row.get("predicted_category", "?")
            level = "danger" if correlated else "info"
            badge = "🔴 Correlated with physical anomaly" if correlated else "ℹ️ No physical correlation"
            content = (
                f"<b>{row.get('district')}</b> &middot; {status_pill(cat)} &middot; "
                f"Risk <b>{risk:.2f}</b> &middot; {badge}<br>"
                f"<span style='color:{COLOR['text_muted']}; font-size:12px;'>{row.get('title', '')}</span>"
            )
            st.markdown(alert_card(level, content), unsafe_allow_html=True)

        # ============================================================
        # Word Cloud — Section F: Nuage de mots dynamique
        # ============================================================
        section_title(
            "☁️",
            "Dynamic word cloud — technical incidents",
            "Section F · keywords from classified RSS reports",
        )

        try:
            from wordcloud import WordCloud
            import matplotlib.pyplot as plt

            text_corpus = " ".join(
                (
                    nlp_reports_df["title"].fillna("").astype(str) + " "
                    + nlp_reports_df["description"].fillna("").astype(str)
                ).tolist()
            )

            if text_corpus.strip():
                french_stopwords = {
                    "le", "la", "les", "un", "une", "des", "de", "du", "et", "ou",
                    "à", "au", "aux", "ce", "cette", "ces", "dans", "pour", "par",
                    "avec", "sans", "sur", "sous", "est", "sont", "être", "avoir",
                    "que", "qui", "quoi", "dont", "où", "ne", "pas", "plus", "très",
                    "peuvent", "pourraient", "tels", "telles", "cela", "celui",
                    "celle", "ceux", "tous", "tout", "toute", "toutes", "leur",
                    "leurs", "son", "sa", "ses", "notre", "votre", "nos", "vos",
                    "il", "elle", "ils", "elles", "on", "nous", "vous",
                    "actuellement", "actuelle", "actuelles", "majeur", "majeure",
                    "près", "autour", "temporaire", "temporairement",
                }

                wc = WordCloud(
                    width=1100,
                    height=440,
                    background_color=COLOR["bg"],
                    colormap="cool",
                    stopwords=french_stopwords,
                    min_font_size=10,
                    max_words=80,
                    collocations=False,
                    relative_scaling=0.4,
                ).generate(text_corpus)

                fig_wc, ax_wc = plt.subplots(figsize=(11, 4.5), facecolor=COLOR["bg"])
                ax_wc.imshow(wc, interpolation="bilinear")
                ax_wc.axis("off")
                fig_wc.patch.set_facecolor(COLOR["bg"])
                st.pyplot(fig_wc, clear_figure=True)
            else:
                st.info("Empty text corpus — waiting for more RSS reports.")
        except ImportError:
            st.warning("`wordcloud` library missing — check `dashboard/requirements.txt`.")

        with st.expander("📋 Full NLP report table"):
            nlp_columns = [
                "district", "title", "original_category", "predicted_category",
                "severity", "risk_score", "correlated_with_physical_anomaly",
                "latest_avg_voltage", "latest_voltage_drop_count",
                "latest_district_status", "analyzed_at",
            ]
            existing_nlp = [c for c in nlp_columns if c in nlp_reports_df.columns]
            st.dataframe(
                nlp_reports_df[existing_nlp],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "risk_score": st.column_config.ProgressColumn(
                        "Risk", format="%.2f", min_value=0, max_value=1
                    ),
                },
            )


# ============================================================
# TAB 5 — DATA QUALITY
# ============================================================

with tab_quality:
    section_title("✅", "Data quality framework", "Section E · automatic Spark validation")

    if quality_reports_df.empty:
        empty_state(
            "✅",
            "Data quality reports not generated yet",
            "Spark writes a report at the end of each cycle.",
        )
    else:
        last_q = quality_reports_df.sort_values("cycle_id").tail(1).iloc[0]

        q1, q2, q3, q4 = st.columns(4)
        with q1:
            score = float(last_q.get("quality_score", 0))
            color = (
                COLOR["success"] if score >= 95
                else COLOR["warning"] if score >= 90
                else COLOR["danger"]
            )
            kpi_card(
                "Quality score",
                f"<span style='color:{color};'>{score:.2f}%</span>",
                sub=str(last_q.get("quality_status", "")).title(),
                icon="🎯",
            )
        with q2:
            kpi_card(
                "Records",
                f"{int(last_q.get('total_records', 0)):,}",
                sub="In current cycle",
                icon="📊",
            )
        with q3:
            kpi_card(
                "Coverage",
                f"{float(last_q.get('meter_coverage_rate', 0)):.1f}%",
                sub="Distinct meters / expected",
                icon="📡",
            )
        with q4:
            kpi_card(
                "Invalid",
                f"{float(last_q.get('invalid_rate', 0)):.2f}%",
                sub="Failed validation",
                icon="❌",
            )

        # Trend
        section_title("📈", "Quality score over time")
        fig_q = px.line(
            quality_reports_df.sort_values("cycle_id"),
            x="cycle_id",
            y="quality_score",
            markers=True,
        )
        fig_q.update_traces(line=dict(color=COLOR["accent"], width=3))
        fig_q.add_hline(
            y=95, line_dash="dash", line_color=COLOR["warning"],
            annotation_text="95% threshold",
            annotation_font_color=COLOR["warning"],
        )
        fig_q.update_layout(
            height=320,
            yaxis=dict(range=[80, 101], title="Quality (%)"),
            xaxis=dict(title="Cycle"),
        )
        apply_chart_theme(fig_q)
        st.plotly_chart(fig_q, use_container_width=True)

        # Anomaly breakdown for last cycle
        section_title("⚠️", "Anomaly breakdown · last cycle")
        breakdown = {
            "Missing values": int(last_q.get("missing_values_count", 0)),
            "Invalid voltage": int(last_q.get("invalid_voltage_count", 0)),
            "Invalid current": int(last_q.get("invalid_current_count", 0)),
            "Invalid energy": int(last_q.get("invalid_energy_count", 0)),
            "Inconsistent E=V·I·Δt": int(last_q.get("inconsistent_energy_count", 0)),
            "Duplicates": int(last_q.get("duplicate_count", 0)),
        }
        bd_df = pd.DataFrame({"type": list(breakdown.keys()), "count": list(breakdown.values())})

        fig_bd = px.bar(
            bd_df.sort_values("count", ascending=True),
            x="count", y="type",
            orientation="h",
            color="count",
            color_continuous_scale=[
                [0.0, COLOR["success"]],
                [0.5, COLOR["warning"]],
                [1.0, COLOR["danger"]],
            ],
            text="count",
        )
        fig_bd.update_traces(textposition="outside")
        fig_bd.update_layout(
            showlegend=False, height=320, yaxis_title="", xaxis_title="Count",
            coloraxis_showscale=False,
        )
        apply_chart_theme(fig_bd)
        st.plotly_chart(fig_bd, use_container_width=True)

    section_title(
        "🎚️",
        "Bias report by district",
        "Section E · meter share, overload rate, voltage drop rate",
    )
    if quality_bias_df.empty:
        st.info("No bias report yet.")
    else:
        last_cycle_bias = int(quality_bias_df["cycle_id"].max())
        bias_df = quality_bias_df[quality_bias_df["cycle_id"] == last_cycle_bias].copy()
        st.markdown(
            alert_card("info", f"📊 Bias snapshot · cycle <b>{last_cycle_bias}</b>"),
            unsafe_allow_html=True,
        )

        cb1, cb2 = st.columns(2)
        with cb1:
            fig_share = px.bar(
                bias_df.sort_values("meter_share_percent", ascending=True),
                x="meter_share_percent", y="district",
                orientation="h",
                color="zone_type",
                color_discrete_map=ZONE_COLOR,
                title="Meter share (%)",
            )
            fig_share.update_layout(height=460, xaxis_title="% of total meters")
            apply_chart_theme(fig_share)
            st.plotly_chart(fig_share, use_container_width=True)

        with cb2:
            fig_anom = go.Figure()
            fig_anom.add_trace(go.Bar(
                x=bias_df["district"], y=bias_df["overload_rate_percent"],
                name="Overload rate %", marker_color=COLOR["danger"],
            ))
            fig_anom.add_trace(go.Bar(
                x=bias_df["district"], y=bias_df["voltage_drop_rate_percent"],
                name="Voltage drop rate %", marker_color=COLOR["warning"],
            ))
            fig_anom.update_layout(
                barmode="group", height=460, title="Anomaly rate per district",
                xaxis=dict(tickangle=-45),
            )
            apply_chart_theme(fig_anom)
            st.plotly_chart(fig_anom, use_container_width=True)


# ============================================================
# Footer
# ============================================================

st.markdown(
    f"""
    <div class="tse-footer">
        <span>⚡ Tetouan Smart Energy · ENSA Tétouan · Module M126</span>
        <span>Auto-refresh every {refresh_seconds}s · MongoDB: <code>{DB_NAME}</code> · {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}</span>
    </div>
    """,
    unsafe_allow_html=True,
)
