"""Streamlit app — Tableau-style interactive dashboard."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from folium import Map, TileLayer
from folium.plugins import HeatMap
from streamlit_folium import st_folium

from boston_needle_map.cache import load_cached, save_cache
from boston_needle_map.cleaner import clean
from boston_needle_map.config import RESOURCE_IDS
from boston_needle_map.fetcher import fetch_year
from boston_needle_map.models import CleanedRecord

# -- Tableau palette --
BLUE = "#4e79a7"
ORANGE = "#f28e2b"
RED = "#e15759"
TEAL = "#76b7b2"
GREEN = "#59a14f"
ACCENT = "#e85a1b"
YEAR_COLORS = [BLUE, ORANGE, RED, TEAL, GREEN, "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac"]

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Source Sans 3, Segoe UI, Roboto, sans-serif", size=12, color="#1a1a1a"),
    margin=dict(l=0, r=8, t=0, b=0),
)

HEAT_GRADIENT = {
    0.00: "rgba(0,0,0,0)",
    0.12: "rgba(0,170,68,0.5)",
    0.30: "rgba(0,204,0,0.75)",
    0.50: "rgba(255,255,0,0.88)",
    0.70: "rgba(255,136,0,0.94)",
    0.88: "rgba(220,30,0,0.97)",
    1.00: "rgba(150,0,0,1)",
}

st.set_page_config(
    page_title="Boston 311 Sharps Dashboard",
    page_icon=":map:",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# -- CSS to match the old static version's look --
st.markdown(
    """
    <link href="https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
    /* Font */
    html, body, [class*="css"] {
        font-family: 'Source Sans 3', 'Segoe UI', system-ui, sans-serif;
    }
    /* Tight layout */
    .block-container { padding: 0.5rem 1rem 0; }
    /* Header bar */
    .dash-header {
        background: #fff; border-bottom: 1px solid #d0d0d0;
        padding: 8px 0; display: flex; align-items: baseline;
        gap: 12px; box-shadow: 0 1px 3px rgba(0,0,0,.08);
        margin: -0.5rem -1rem 8px -1rem; padding: 10px 16px;
    }
    .dash-title { font-size: 18px; font-weight: 700; color: #1a1a1a; }
    .dash-sub { font-size: 12px; color: #666; }
    .dash-links { margin-left: auto; font-size: 11px; color: #888; }
    .dash-links a { color: #4e79a7; text-decoration: none; }
    .dash-links a:hover { text-decoration: underline; }
    /* KPI row */
    div[data-testid="stMetric"] {
        background: #f8f9fa; border: 1px solid #e0e0e0;
        border-radius: 6px; padding: 10px 14px;
        box-shadow: 0 1px 2px rgba(0,0,0,.04);
    }
    div[data-testid="stMetric"] label {
        color: #666; font-size: 0.7rem; text-transform: uppercase;
        letter-spacing: .05em;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #1a1a1a; font-size: 1.5rem; font-weight: 700;
    }
    /* Panel borders */
    div[data-testid="column"] > div[data-testid="stVerticalBlockBorderWrapper"] {
        border: 1px solid #e0e0e0; border-radius: 6px;
        background: #fff;
    }
    /* Section titles like the old version */
    .chart-title {
        font-size: 13px; font-weight: 700; color: #222;
        margin-bottom: 6px; padding-bottom: 4px;
        border-bottom: 2px solid #4e79a7;
    }
    /* Legend strip */
    .legend-strip {
        display: flex; align-items: center; gap: 6px;
        font-size: 11px; color: #666; margin-top: 4px;
    }
    .legend-grad {
        height: 8px; flex: 1; border-radius: 4px;
        background: linear-gradient(90deg,
            transparent 0%, #00aa44 20%, #ffff00 50%, #ff8800 75%, #cc0000 100%);
    }
    /* Filter chip */
    .filter-chip {
        display: inline-block; background: #e85a1b; color: white;
        padding: 3px 12px; border-radius: 14px; font-size: 12px;
    }
    /* Pill-style filter buttons (Tableau look) */
    button[data-testid="stBaseButton-pills"] {
        border-radius: 14px !important;
        padding: 2px 14px !important;
        font-size: 12px !important;
        border: 1px solid #ccc !important;
        background: #fff !important;
        color: #333 !important;
        font-weight: 600 !important;
    }
    button[data-testid="stBaseButton-pills"][aria-pressed="true"],
    button[data-testid="stBaseButton-pills"][aria-checked="true"] {
        background: #e85a1b !important;
        color: #fff !important;
        border-color: #e85a1b !important;
    }
    /* Hide streamlit chrome */
    #MainMenu, footer, header { visibility: hidden; }
    /* Mobile */
    @media (max-width: 768px) {
        .block-container { padding: 0.3rem 0.5rem 0; }
        .dash-title { font-size: 15px; }
        div[data-testid="stMetric"] div[data-testid="stMetricValue"] { font-size: 1.2rem; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# -- Cross-filter state --
if "selected_hood" not in st.session_state:
    st.session_state.selected_hood = None
if "selected_zip" not in st.session_state:
    st.session_state.selected_zip = None


@st.cache_data(ttl=3600, show_spinner="Fetching data...")
def load_data(years: tuple[int, ...]) -> list[dict[str, object]]:
    """Fetch and clean records for the given years, using cache when available."""
    all_records: list[CleanedRecord] = []
    for year in years:
        cached = load_cached(year)
        if cached is not None:
            raw = cached
        else:
            raw = fetch_year(year)
            if raw:
                save_cache(year, raw)
        cleaned = [r for r in (clean(row) for row in raw) if r is not None]
        all_records.extend(cleaned)
    return [r.model_dump() for r in all_records]


# ━━━━ HEADER BAR ━━━━
st.markdown(
    """
    <div class="dash-header">
        <span class="dash-title">Boston 311 Sharps Collection Requests</span>
        <span class="dash-sub">Reported pickups &amp; cleanups</span>
        <span class="dash-links">
            Data: <a href="https://data.boston.gov/dataset/311-service-requests" target="_blank">data.boston.gov</a>
            &middot; <a href="https://github.com/urban-hazards/boston-needle-map" target="_blank">Source</a>
        </span>
    </div>
    """,
    unsafe_allow_html=True,
)

# ━━━━ FILTER BAR (Tableau-style pills) ━━━━
available_years = sorted(RESOURCE_IDS.keys(), reverse=True)
default_years = [y for y in available_years if y >= max(available_years) - 2]
months_list = [
    "All", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

yr_col, mo_col = st.columns([3, 2])
with yr_col:
    st.markdown('<span style="font-size:11px;font-weight:700;color:#444;text-transform:uppercase;letter-spacing:.05em">Year</span>', unsafe_allow_html=True)
    selected_years = st.pills("Year", available_years, default=default_years, selection_mode="multi", label_visibility="collapsed")
with mo_col:
    st.markdown('<span style="font-size:11px;font-weight:700;color:#444;text-transform:uppercase;letter-spacing:.05em">Month</span>', unsafe_allow_html=True)
    selected_month = st.pills("Month", months_list, default="All", selection_mode="single", label_visibility="collapsed")

if st.session_state.selected_hood or st.session_state.selected_zip:
    if st.button("✕ Clear filter"):
        st.session_state.selected_hood = None
        st.session_state.selected_zip = None
        st.rerun()

if not selected_years:
    st.warning("Select at least one year.")
    st.stop()

records = load_data(tuple(sorted(selected_years)))
if not records:
    st.error("No records found.")
    st.stop()

df = pd.DataFrame(records)
df["dt"] = pd.to_datetime(df["dt"], format="mixed")
# Ensure zip codes have leading zero (cached data may lack it)
df["zipcode"] = df["zipcode"].apply(lambda z: z.zfill(5) if z and z.isdigit() else z)

if selected_month and selected_month != "All":
    month_num = months_list.index(selected_month)
    df = df[df["month"] == month_num]

# Cross-filter
active_filter = None
if st.session_state.selected_hood:
    df = df[df["hood"] == st.session_state.selected_hood]
    active_filter = st.session_state.selected_hood
elif st.session_state.selected_zip:
    df = df[df["zipcode"] == st.session_state.selected_zip]
    active_filter = f"Zip {st.session_state.selected_zip}"

# ━━━━ KPI ROW ━━━━
top_hood = df["hood"].value_counts().index[0] if not df.empty else "—"
avg_mo = int(df.groupby([df["dt"].dt.year, df["dt"].dt.month]).size().mean()) if not df.empty else 0
peak_h = df["hour"].value_counts().index[0] if not df.empty else 0
peak_lbl = f"{peak_h % 12 or 12}{'AM' if peak_h < 12 else 'PM'}"

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Requests", f"{len(df):,}")
k2.metric("Avg / Month", f"{avg_mo:,}")
k3.metric("Top Neighborhood", top_hood)
k4.metric("Peak Hour", peak_lbl)

if active_filter:
    st.markdown(f'<span class="filter-chip">Filtered: {active_filter}</span>', unsafe_allow_html=True)

# ━━━━ ROW 1: Map (left) + Neighborhoods (right) ━━━━
map_col, side_col = st.columns([3, 1.3], border=True)

with map_col:
    st.markdown('<div class="chart-title">Heatmap</div>', unsafe_allow_html=True)
    m = Map(location=[42.332, -71.078], zoom_start=13, tiles=None)
    TileLayer(
        tiles="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
        attr="CARTO", subdomains="abcd", max_zoom=19,
    ).add_to(m)

    heat_data = df[["lat", "lng"]].values.tolist()
    if heat_data:
        HeatMap(
            heat_data, radius=18, blur=12, max_zoom=16, min_opacity=0.3,
            gradient=HEAT_GRADIENT,
        ).add_to(m)  # type: ignore[no-untyped-call]

    st.markdown(
        '<div class="legend-strip"><span>Low</span><div class="legend-grad"></div><span>High</span></div>',
        unsafe_allow_html=True,
    )
    st_folium(m, use_container_width=True, height=480)
    st.caption(f"Showing **{len(df):,}** requests")

with side_col:
    # Neighborhoods
    st.markdown('<div class="chart-title">Top Neighborhoods</div>', unsafe_allow_html=True)
    hood_counts = df["hood"].value_counts().head(12).reset_index()
    hood_counts.columns = ["Neighborhood", "Count"]
    fig_hoods = px.bar(
        hood_counts, x="Count", y="Neighborhood", orientation="h",
        color_discrete_sequence=[BLUE],
    )
    fig_hoods.update_layout(
        **PLOTLY_LAYOUT, height=260,
        yaxis=dict(categoryorder="total ascending", tickfont=dict(size=10)),
        xaxis=dict(showticklabels=False), showlegend=False,
    )
    fig_hoods.update_traces(
        marker_line_width=0, texttemplate="%{x}", textposition="outside",
        textfont=dict(size=10, color=ACCENT),
        hovertemplate="%{y}: <b>%{x}</b><extra></extra>",
    )
    hood_event = st.plotly_chart(fig_hoods, use_container_width=True, on_select="rerun", key="hood_chart")
    if hood_event and hood_event.selection and hood_event.selection.points:
        clicked = hood_event.selection.points[0]["y"]
        if clicked != st.session_state.selected_hood:
            st.session_state.selected_hood = clicked
            st.session_state.selected_zip = None
            st.rerun()

    # Zip codes
    st.markdown('<div class="chart-title">Top Zip Codes</div>', unsafe_allow_html=True)
    zip_counts = df[df["zipcode"] != ""]["zipcode"].value_counts().head(8).reset_index()
    zip_counts.columns = ["Zip", "Count"]
    fig_zips = px.bar(
        zip_counts, x="Count", y="Zip", orientation="h",
        color_discrete_sequence=[TEAL],
    )
    fig_zips.update_layout(
        **PLOTLY_LAYOUT, height=180,
        yaxis=dict(categoryorder="total ascending", tickfont=dict(size=10)),
        xaxis=dict(showticklabels=False), showlegend=False,
    )
    fig_zips.update_traces(
        marker_line_width=0, texttemplate="%{x}", textposition="outside",
        textfont=dict(size=10, color="#333"),
        hovertemplate="%{y}: <b>%{x}</b><extra></extra>",
    )
    zip_event = st.plotly_chart(fig_zips, use_container_width=True, on_select="rerun", key="zip_chart")
    if zip_event and zip_event.selection and zip_event.selection.points:
        clicked = zip_event.selection.points[0]["y"]
        if clicked != st.session_state.selected_zip:
            st.session_state.selected_zip = clicked
            st.session_state.selected_hood = None
            st.rerun()

# ━━━━ ROW 2: Trend (left) + Hourly (right) ━━━━
trend_col, hour_col = st.columns([3, 1.3], border=True)

with trend_col:
    st.markdown('<div class="chart-title">Monthly Trend</div>', unsafe_allow_html=True)
    monthly = (
        df.groupby([df["dt"].dt.year.rename("year"), df["dt"].dt.month.rename("mo")])
        .size().reset_index(name="count")
    )
    monthly = monthly[monthly["count"] > 0]
    if not monthly.empty:
        month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        monthly["month_name"] = monthly["mo"].apply(lambda x: month_names[x - 1])
        year_list = sorted(monthly["year"].unique())
        color_map = {y: YEAR_COLORS[i % len(YEAR_COLORS)] for i, y in enumerate(year_list)}
        fig_trend = px.line(
            monthly, x="month_name", y="count", color="year", markers=True,
            color_discrete_map=color_map,
            labels={"month_name": "", "count": "Cases", "year": ""},
        )
        fig_trend.update_layout(
            **PLOTLY_LAYOUT, height=220,
            legend=dict(orientation="h", y=1.12, font=dict(size=10)),
        )
        fig_trend.update_traces(line_width=2)
        st.plotly_chart(fig_trend, use_container_width=True)

with hour_col:
    st.markdown('<div class="chart-title">Requests by Hour</div>', unsafe_allow_html=True)
    hourly = df["hour"].value_counts().sort_index()
    hour_labels = [f"{h % 12 or 12}{'a' if h < 12 else 'p'}" for h in range(24)]
    max_h = max(hourly.values) if not hourly.empty else 1
    bar_colors = []
    for h in range(24):
        v = hourly.get(h, 0)
        t = v / max_h if max_h else 0
        if t > 0.7:
            bar_colors.append("#cc0000")
        elif t > 0.4:
            bar_colors.append("#ff8800")
        else:
            bar_colors.append(BLUE)
    fig_hour = go.Figure(go.Bar(
        x=hour_labels,
        y=[hourly.get(h, 0) for h in range(24)],
        marker_color=bar_colors, marker_line_width=0,
        hovertemplate="%{x}: <b>%{y}</b><extra></extra>",
    ))
    fig_hour.update_layout(
        **PLOTLY_LAYOUT, height=220,
        xaxis=dict(tickfont=dict(size=8)),
        yaxis=dict(tickfont=dict(size=9)),
    )
    st.plotly_chart(fig_hour, use_container_width=True)
