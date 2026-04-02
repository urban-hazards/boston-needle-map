"""Streamlit app for interactive data exploration."""

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

st.set_page_config(page_title="Boston 311 Needle Requests", layout="centered")

# Reduce top padding on mobile
st.markdown(
    """
    <style>
    @media (max-width: 768px) {
        .block-container { padding-top: 1rem; }
        h1 { font-size: 1.5rem !important; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Boston 311 Needle Requests")


@st.cache_data(ttl=3600, show_spinner="Fetching data...")
def load_data(years: tuple[int, ...]) -> list[dict[str, object]]:
    """Fetch and clean records for the given years, using cache when available."""
    all_records: list[CleanedRecord] = []
    for year in years:
        # Try cache first
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


# -- Filters in an expander (more discoverable on mobile than sidebar) --
available_years = sorted(RESOURCE_IDS.keys(), reverse=True)
default_years = [y for y in available_years if y >= max(available_years) - 2]

with st.expander("Filters", expanded=True):
    filter_cols = st.columns([2, 1])
    with filter_cols[0]:
        selected_years = st.multiselect("Years", available_years, default=default_years)
    with filter_cols[1]:
        months = [
            "All",
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ]
        selected_month = st.selectbox("Month", months, index=0)

if not selected_years:
    st.warning("Select at least one year.")
    st.stop()

records = load_data(tuple(sorted(selected_years)))
if not records:
    st.error("No records found for the selected years.")
    st.stop()

df = pd.DataFrame(records)
df["dt"] = pd.to_datetime(df["dt"], format="mixed")

if selected_month != "All":
    month_num = months.index(selected_month)
    df = df[df["month"] == month_num]

st.metric("Total Requests", f"{len(df):,}")

# -- Heatmap (full width, stacked on top) --
st.subheader("Heatmap")
m = Map(location=[42.332, -71.078], zoom_start=13, tiles=None)
TileLayer(
    tiles="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
    attr="CARTO",
    subdomains="abcd",
    max_zoom=19,
).add_to(m)

heat_data = df[["lat", "lng"]].values.tolist()
if heat_data:
    HeatMap(heat_data, radius=20, blur=15, max_zoom=16, min_opacity=0.4).add_to(m)  # type: ignore[no-untyped-call]
st_folium(m, use_container_width=True, height=500)

# -- Charts (stacked vertically below map) --
st.subheader("Monthly Trend")
monthly = df.groupby([df["dt"].dt.year.rename("year"), df["dt"].dt.month.rename("mo")]).size().reset_index(name="count")
if not monthly.empty:
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    monthly["month_name"] = monthly["mo"].apply(lambda x: month_names[x - 1])
    fig_trend = px.line(
        monthly,
        x="month_name",
        y="count",
        color="year",
        markers=True,
        labels={"month_name": "Month", "count": "Requests", "year": "Year"},
    )
    fig_trend.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig_trend, use_container_width=True)

st.subheader("Requests by Hour")
hourly = df["hour"].value_counts().sort_index()
hour_labels = [f"{h % 12 or 12}{'a' if h < 12 else 'p'}" for h in range(24)]
fig_hour = go.Figure(
    go.Bar(
        x=hour_labels,
        y=[hourly.get(h, 0) for h in range(24)],
        marker_color=["#cc0000" if hourly.get(h, 0) > hourly.quantile(0.7) else "#4e79a7" for h in range(24)],
    )
)
fig_hour.update_layout(height=250, margin=dict(l=0, r=0, t=10, b=0), xaxis_title="Hour", yaxis_title="Requests")
st.plotly_chart(fig_hour, use_container_width=True)

# -- Data tables in tabs (instead of side-by-side columns) --
tab_hoods, tab_zips = st.tabs(["Top Neighborhoods", "Top Zip Codes"])

with tab_hoods:
    hood_counts = df["hood"].value_counts().head(15).reset_index()
    hood_counts.columns = ["Neighborhood", "Count"]
    st.dataframe(hood_counts, use_container_width=True, hide_index=True)

with tab_zips:
    zip_counts = df[df["zipcode"] != ""]["zipcode"].value_counts().head(10).reset_index()
    zip_counts.columns = ["Zip Code", "Count"]
    st.dataframe(zip_counts, use_container_width=True, hide_index=True)
