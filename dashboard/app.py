import os
import streamlit as st
import pandas as pd
from pymongo import MongoClient
import plotly.express as px
import plotly.graph_objects as go
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


# ============================================================
# Streamlit Page Configuration
# ============================================================

st.set_page_config(
    page_title="Tetouan Smart Energy Dashboard",
    page_icon="⚡",
    layout="wide",
)

st.title("⚡ Tetouan Smart Energy Dashboard - Spark Streaming")

st.markdown(
    """
    Dashboard alimenté par **Spark Structured Streaming**.  
    Pipeline principal : Smart Meters → Kafka → Spark → MongoDB → Analytics → Dashboard.
    """
)


# ============================================================
# Sidebar Settings
# ============================================================

st.sidebar.header("Dashboard Settings")

refresh_seconds = st.sidebar.slider(
    "Refresh interval seconds",
    min_value=3,
    max_value=60,
    value=5,
)

st_autorefresh(
    interval=refresh_seconds * 1000,
    key="spark_dashboard_autorefresh",
)


# ============================================================
# Data Loading Functions
# ============================================================

@st.cache_data(ttl=5)
def load_spark_district_data():
    data = list(
        spark_district_collection
        .find()
        .sort("processed_at", -1)
        .limit(1500)
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    if "processed_at" in df.columns:
        df["processed_at"] = pd.to_datetime(df["processed_at"], errors="coerce")

    if "cycle_id" in df.columns:
        df["cycle_id"] = pd.to_numeric(df["cycle_id"], errors="coerce")

    return df.sort_values(["cycle_id", "district"])


@st.cache_data(ttl=5)
def load_spark_alerts():
    data = list(
        spark_alerts_collection
        .find()
        .sort("created_at", -1)
        .limit(300)
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    if "cycle_id" in df.columns:
        df["cycle_id"] = pd.to_numeric(df["cycle_id"], errors="coerce")

    return df


@st.cache_data(ttl=5)
def load_spark_cycles():
    data = list(
        spark_cycle_collection
        .find()
        .sort("cycle_id", -1)
        .limit(100)
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    if "cycle_id" in df.columns:
        df["cycle_id"] = pd.to_numeric(df["cycle_id"], errors="coerce")

    return df.sort_values("cycle_id")


@st.cache_data(ttl=10)
def load_ml_metrics():
    data = list(
        ml_metrics_collection
        .find()
        .sort("created_at", -1)
        .limit(300)
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    for col in ["created_at", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


@st.cache_data(ttl=10)
def load_ml_predictions():
    data = list(
        ml_predictions_collection
        .find()
        .sort("prediction_generated_at", -1)
        .limit(1000)
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    if "prediction_generated_at" in df.columns:
        df["prediction_generated_at"] = pd.to_datetime(
            df["prediction_generated_at"],
            errors="coerce",
        )

    if "predicted_cycle_id" in df.columns:
        df["predicted_cycle_id"] = pd.to_numeric(
            df["predicted_cycle_id"],
            errors="coerce",
        )

    if "based_on_cycle_id" in df.columns:
        df["based_on_cycle_id"] = pd.to_numeric(
            df["based_on_cycle_id"],
            errors="coerce",
        )

    return df


@st.cache_data(ttl=10)
def load_ml_comparisons():
    data = list(
        ml_comparisons_collection
        .find()
        .sort("compared_at", -1)
        .limit(1000)
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    if "compared_at" in df.columns:
        df["compared_at"] = pd.to_datetime(df["compared_at"], errors="coerce")

    if "cycle_id" in df.columns:
        df["cycle_id"] = pd.to_numeric(df["cycle_id"], errors="coerce")

    return df


@st.cache_data(ttl=10)
def load_nlp_reports():
    data = list(
        nlp_reports_collection
        .find()
        .sort("analyzed_at", -1)
        .limit(300)
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    if "analyzed_at" in df.columns:
        df["analyzed_at"] = pd.to_datetime(df["analyzed_at"], errors="coerce")

    return df


@st.cache_data(ttl=10)
def load_nlp_metrics():
    data = list(
        nlp_metrics_collection
        .find()
        .sort("updated_at", -1)
        .limit(50)
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    return df


district_df = load_spark_district_data()
alerts_df = load_spark_alerts()
cycles_df = load_spark_cycles()

ml_metrics_df = load_ml_metrics()
ml_predictions_df = load_ml_predictions()
ml_comparisons_df = load_ml_comparisons()

nlp_reports_df = load_nlp_reports()
nlp_metrics_df = load_nlp_metrics()


# ============================================================
# Empty State
# ============================================================

if district_df.empty:
    st.warning(
        "No Spark district data found yet. Wait until Spark receives data from Kafka."
    )

    st.info(
        "Check logs with:\n\n"
        "`sudo docker logs -f spark-streaming-job`\n\n"
        "`sudo docker logs -f tetouan-simulator`\n\n"
        "`sudo docker logs -f analytics-job`"
    )

    st.stop()


# ============================================================
# Cycle Selection
# ============================================================

st.sidebar.header("Cycle Selection")

completed_cycles = (
    district_df["cycle_id"]
    .dropna()
    .astype(int)
    .sort_values()
    .unique()
    .tolist()
)

selected_cycle = st.sidebar.selectbox(
    "Select cycle",
    completed_cycles,
    index=len(completed_cycles) - 1,
)

cycle_df = district_df[district_df["cycle_id"] == selected_cycle].copy()

available_districts = sorted(cycle_df["district"].dropna().unique().tolist())

selected_districts = st.sidebar.multiselect(
    "Districts",
    available_districts,
    default=available_districts,
)

cycle_df = cycle_df[cycle_df["district"].isin(selected_districts)]

history_df = district_df[
    district_df["district"].isin(selected_districts)
].copy()


# ============================================================
# Cycle Status
# ============================================================

cycle_status = "unknown"

if not cycles_df.empty:
    selected_cycle_status = cycles_df[cycles_df["cycle_id"] == selected_cycle]

    if not selected_cycle_status.empty and "status" in selected_cycle_status.columns:
        cycle_status = selected_cycle_status.iloc[0]["status"]

st.info(
    f"Showing Spark aggregation for cycle {selected_cycle}. "
    f"Cycle status: {cycle_status}. "
    f"Districts received: {cycle_df['district'].nunique()}/18."
)


# ============================================================
# KPI Section
# ============================================================

st.subheader(f"Spark Aggregated Consumption - Cycle {selected_cycle}")

total_energy = cycle_df["total_energy_consumption"].sum()
avg_voltage = cycle_df["avg_voltage"].mean()
total_meters = cycle_df["total_meters"].sum()
risk_count = cycle_df[cycle_df["district_status"] != "normal"].shape[0]
district_count = cycle_df["district"].nunique()

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Total Energy", f"{total_energy:,.2f} kWh")
col2.metric("Average Voltage", f"{avg_voltage:.2f} V")
col3.metric("Smart Meters Processed", f"{int(total_meters):,}")
col4.metric("Districts Received", f"{district_count}/18")
col5.metric("Districts With Risk", risk_count)


# ============================================================
# Map
# ============================================================

st.subheader("Tetouan District Consumption Map - Spark")

fig_map = px.scatter_mapbox(
    cycle_df,
    lat="latitude",
    lon="longitude",
    size="total_energy_consumption",
    color="district_status",
    hover_name="district",
    hover_data={
        "zone_type": True,
        "total_energy_consumption": True,
        "avg_voltage": True,
        "avg_current": True,
        "total_meters": True,
        "overload_count": True,
        "voltage_drop_count": True,
        "latitude": False,
        "longitude": False,
    },
    zoom=12.5,
    height=700,
    title=f"Tetouan Energy Consumption Map - Cycle {selected_cycle}",
)

fig_map.update_layout(
    mapbox_style="open-street-map",
    mapbox=dict(
        center=dict(
            lat=35.5700,
            lon=-5.3700,
        ),
        zoom=12.5,
    ),
    margin={
        "r": 0,
        "t": 40,
        "l": 0,
        "b": 0,
    },
)

st.plotly_chart(fig_map, use_container_width=True)


# ============================================================
# Energy Consumption by District
# ============================================================

st.subheader("Energy Consumption by District")

fig_bar = px.bar(
    cycle_df.sort_values("total_energy_consumption", ascending=False),
    x="district",
    y="total_energy_consumption",
    color="zone_type",
    title=f"Spark District Consumption - Cycle {selected_cycle}",
    labels={
        "district": "District",
        "total_energy_consumption": "Energy Consumption kWh",
        "zone_type": "Zone Type",
    },
)

fig_bar.update_layout(xaxis_tickangle=-45)

st.plotly_chart(fig_bar, use_container_width=True)


# ============================================================
# Real-Time Consumption Grid Heatmap
# ============================================================

st.subheader("Real-Time Consumption Grid")

heatmap_df = history_df.copy()

if "processed_at" not in heatmap_df.columns:
    st.warning("No processed_at column found for real-time heatmap.")

else:
    heatmap_df["processed_at"] = pd.to_datetime(
        heatmap_df["processed_at"],
        errors="coerce",
    )

    heatmap_df = heatmap_df.dropna(subset=["processed_at"])

    if heatmap_df.empty:
        st.warning("No valid timestamp values found for the heatmap.")

    else:
        max_points = st.slider(
            "Number of recent aggregation points to display",
            min_value=20,
            max_value=300,
            value=100,
            step=20,
        )

        heatmap_df = heatmap_df.sort_values("processed_at").tail(max_points)

        heatmap_df["time_bucket"] = heatmap_df["processed_at"].dt.floor("min")
        heatmap_df["time_label"] = heatmap_df["time_bucket"].dt.strftime("%H:%M")

        heatmap_grouped_df = (
            heatmap_df
            .groupby(["district", "time_label"], as_index=False)
            .agg(
                total_energy_consumption=("total_energy_consumption", "sum")
            )
        )

        heatmap_pivot = heatmap_grouped_df.pivot(
            index="district",
            columns="time_label",
            values="total_energy_consumption",
        )

        heatmap_pivot = heatmap_pivot.fillna(0)

        fig_heatmap = go.Figure(
            data=go.Heatmap(
                z=heatmap_pivot.values,
                x=heatmap_pivot.columns,
                y=heatmap_pivot.index,
                colorscale=[
                    [0.0, "green"],
                    [0.5, "yellow"],
                    [1.0, "red"],
                ],
                colorbar=dict(
                    title="kWh"
                ),
                hovertemplate=(
                    "Time: %{x}<br>"
                    "District: %{y}<br>"
                    "Consumption: %{z:.2f} kWh"
                    "<extra></extra>"
                ),
                xgap=2,
                ygap=2,
            )
        )

        fig_heatmap.update_layout(
            title="Real-Time Energy Consumption Grid by District",
            xaxis_title="Time",
            yaxis_title="District",
            height=700,
            xaxis=dict(
                tickangle=-45,
                tickmode="auto",
                nticks=12,
                showgrid=True,
                gridcolor="lightgray",
                side="bottom",
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="lightgray",
                autorange="reversed",
            ),
            plot_bgcolor="white",
        )

        st.plotly_chart(fig_heatmap, use_container_width=True)


# ============================================================
# Real Consumption vs Best ML Model Prediction
# ============================================================

st.subheader("Real Consumption vs Best ML Model Prediction - Section F")

if ml_comparisons_df.empty:
    st.info(
        "No ML prediction comparison available yet. "
        "This section appears after the predicted cycle becomes a real completed cycle."
    )

    if not ml_predictions_df.empty:
        latest_predicted_cycle = int(ml_predictions_df["predicted_cycle_id"].max())

        latest_predictions = ml_predictions_df[
            ml_predictions_df["predicted_cycle_id"] == latest_predicted_cycle
        ].copy()

        st.info(
            f"Next-cycle predictions are already available for cycle "
            f"{latest_predicted_cycle}, but real values for this cycle are not available yet."
        )

        fig_pending_predictions = px.bar(
            latest_predictions.sort_values("predicted_consumption", ascending=False),
            x="district",
            y="predicted_consumption",
            color="zone_type",
            title=f"Best Model Predictions for Future Cycle {latest_predicted_cycle}",
            labels={
                "district": "District",
                "predicted_consumption": "Predicted Consumption kWh",
                "zone_type": "Zone Type",
            },
        )

        fig_pending_predictions.update_layout(xaxis_tickangle=-45)

        st.plotly_chart(fig_pending_predictions, use_container_width=True)

        pending_columns = [
            "predicted_cycle_id",
            "based_on_cycle_id",
            "district",
            "zone_type",
            "predicted_consumption",
            "latest_real_consumption",
            "avg_voltage",
            "avg_current",
            "total_meters",
            "prediction_generated_at",
        ]

        existing_pending_columns = [
            col for col in pending_columns
            if col in latest_predictions.columns
        ]

        st.dataframe(
            latest_predictions[existing_pending_columns],
            use_container_width=True,
        )

else:
    comparison_districts = sorted(
        ml_comparisons_df["district"]
        .dropna()
        .unique()
        .tolist()
    )

    if not comparison_districts:
        st.info("No district available in ML comparison data.")

    else:
        selected_ml_district = st.selectbox(
            "Select district for best model prediction comparison",
            comparison_districts,
            key="best_model_prediction_district",
        )

        comparison_df = ml_comparisons_df[
            ml_comparisons_df["district"] == selected_ml_district
        ].copy()

        comparison_df = comparison_df.sort_values("cycle_id")

        fig_best_model_prediction = go.Figure()

        fig_best_model_prediction.add_trace(
            go.Scatter(
                x=comparison_df["cycle_id"],
                y=comparison_df["actual_consumption"],
                mode="lines+markers",
                name="Real Consumption",
            )
        )

        fig_best_model_prediction.add_trace(
            go.Scatter(
                x=comparison_df["cycle_id"],
                y=comparison_df["predicted_consumption"],
                mode="lines+markers",
                name="Best ML Model Prediction",
            )
        )

        fig_best_model_prediction.update_layout(
            title=f"Real vs Best ML Model Prediction - {selected_ml_district}",
            xaxis_title="Cycle",
            yaxis_title="Energy Consumption kWh",
        )

        st.plotly_chart(fig_best_model_prediction, use_container_width=True)

        latest_error_df = comparison_df.tail(1)

        if not latest_error_df.empty:
            latest_error = latest_error_df.iloc[0]

            ecol1, ecol2, ecol3 = st.columns(3)

            ecol1.metric(
                "Absolute Error",
                f"{latest_error.get('absolute_error', 0):.2f} kWh"
            )

            ecol2.metric(
                "Percentage Error",
                f"{latest_error.get('percentage_error', 0):.2f} %"
            )

            ecol3.metric(
                "Compared Cycle",
                int(latest_error.get("cycle_id", 0))
            )

        comparison_columns = [
            "cycle_id",
            "district",
            "actual_consumption",
            "predicted_consumption",
            "absolute_error",
            "percentage_error",
            "compared_at",
        ]

        existing_comparison_columns = [
            col for col in comparison_columns
            if col in comparison_df.columns
        ]

        st.dataframe(
            comparison_df[existing_comparison_columns],
            use_container_width=True,
        )


# ============================================================
# Spark Alerts
# ============================================================

st.subheader("Spark Alerts")

cycle_alerts = (
    alerts_df[alerts_df["cycle_id"] == selected_cycle]
    if not alerts_df.empty and "cycle_id" in alerts_df.columns
    else pd.DataFrame()
)

if cycle_alerts.empty:
    st.success("No Spark alert detected for this cycle.")
else:
    for _, row in cycle_alerts.iterrows():
        st.error(
            f"{row['district']} | {row['alert_type']} | "
            f"Consumption={row['total_energy_consumption']:.2f} kWh | "
            f"Voltage={row['avg_voltage']:.2f} V"
        )

    st.dataframe(cycle_alerts, use_container_width=True)


# ============================================================
# Voltage Monitoring
# ============================================================

st.subheader("Average Voltage by District")

fig_voltage = px.line(
    history_df.sort_values("cycle_id"),
    x="cycle_id",
    y="avg_voltage",
    color="district",
    title="Average Voltage Evolution by Cycle",
    labels={
        "cycle_id": "Cycle",
        "avg_voltage": "Average Voltage V",
        "district": "District",
    },
)

st.plotly_chart(fig_voltage, use_container_width=True)


# ============================================================
# Zone Type Analysis
# ============================================================

st.subheader("Consumption by Zone Type")

zone_type_df = (
    cycle_df
    .groupby("zone_type", as_index=False)
    .agg(
        total_energy_consumption=("total_energy_consumption", "sum"),
        avg_voltage=("avg_voltage", "mean"),
        total_meters=("total_meters", "sum"),
    )
)

fig_zone = px.pie(
    zone_type_df,
    names="zone_type",
    values="total_energy_consumption",
    title=f"Energy Consumption Share by Zone Type - Cycle {selected_cycle}",
)

st.plotly_chart(fig_zone, use_container_width=True)


# ============================================================
# ML Model Comparison
# ============================================================

st.subheader("ML Model Comparison - Section D")

if ml_metrics_df.empty:
    st.info(
        "No ML metrics found yet. Wait until analytics-job has enough data. "
        "Minimum is usually 2 completed cycles."
    )
else:
    metrics_display = ml_metrics_df.copy()

    if "task" in metrics_display.columns:
        metrics_display = metrics_display[
            metrics_display["task"] == "energy_consumption_regression"
        ].copy()

    if metrics_display.empty:
        st.info("No regression model metrics found yet.")
    else:
        columns = [
            "model_name",
            "mae",
            "rmse",
            "r2_score",
            "training_time_seconds",
            "inference_time_seconds",
            "train_rows",
            "test_rows",
            "created_at",
        ]

        existing_columns = [
            col for col in columns
            if col in metrics_display.columns
        ]

        st.dataframe(
            metrics_display[existing_columns],
            use_container_width=True,
        )

        fig_rmse = px.bar(
            metrics_display,
            x="model_name",
            y="rmse",
            title="Model Comparison by RMSE",
            labels={
                "model_name": "Model",
                "rmse": "RMSE",
            },
        )

        st.plotly_chart(fig_rmse, use_container_width=True)

        fig_r2 = px.bar(
            metrics_display,
            x="model_name",
            y="r2_score",
            title="Model Comparison by R² Score",
            labels={
                "model_name": "Model",
                "r2_score": "R² Score",
            },
        )

        st.plotly_chart(fig_r2, use_container_width=True)


# ============================================================
# ML Next Cycle Predictions
# ============================================================

st.subheader("Next Cycle ML Predictions")

if ml_predictions_df.empty:
    st.info(
        "No next-cycle predictions found yet. "
        "Wait until analytics-job trains the ML models."
    )
else:
    latest_predicted_cycle = int(ml_predictions_df["predicted_cycle_id"].max())

    latest_predictions = ml_predictions_df[
        ml_predictions_df["predicted_cycle_id"] == latest_predicted_cycle
    ].copy()

    st.info(f"Predictions for next cycle: {latest_predicted_cycle}")

    fig_next_pred = px.bar(
        latest_predictions.sort_values("predicted_consumption", ascending=False),
        x="district",
        y="predicted_consumption",
        color="zone_type",
        title=f"Predicted Consumption by District - Cycle {latest_predicted_cycle}",
        labels={
            "district": "District",
            "predicted_consumption": "Predicted Consumption kWh",
            "zone_type": "Zone Type",
        },
    )

    fig_next_pred.update_layout(xaxis_tickangle=-45)

    st.plotly_chart(fig_next_pred, use_container_width=True)

    pred_columns = [
        "predicted_cycle_id",
        "based_on_cycle_id",
        "district",
        "zone_type",
        "predicted_consumption",
        "latest_real_consumption",
        "avg_voltage",
        "avg_current",
        "total_meters",
        "prediction_generated_at",
    ]

    existing_pred_columns = [
        col for col in pred_columns
        if col in latest_predictions.columns
    ]

    st.dataframe(
        latest_predictions[existing_pred_columns],
        use_container_width=True,
    )


# ============================================================
# NLP Technical Reports
# ============================================================

st.subheader("NLP Technical Reports Analysis - Section B")

if nlp_metrics_df.empty:
    st.info("No NLP metrics found yet.")
else:
    latest_nlp_metric = nlp_metrics_df.head(1).iloc[0]

    ncol1, ncol2, ncol3, ncol4 = st.columns(4)

    ncol1.metric(
        "NLP Model",
        str(latest_nlp_metric.get("model_name", "unknown")),
    )

    ncol2.metric(
        "Accuracy",
        f"{float(latest_nlp_metric.get('accuracy', 0)):.2f}",
    )

    ncol3.metric(
        "F1 Score",
        f"{float(latest_nlp_metric.get('f1_score', 0)):.2f}",
    )

    ncol4.metric(
        "Rows",
        f"{int(latest_nlp_metric.get('total_rows', 0))}",
    )

if nlp_reports_df.empty:
    st.info(
        "No NLP reports found yet. Wait until rss-producer generates enough messages "
        "and analytics-job trains the NLP model."
    )
else:
    latest_nlp = nlp_reports_df.head(20)

    for _, row in latest_nlp.iterrows():
        correlated = bool(row.get("correlated_with_physical_anomaly", False))
        risk_score = float(row.get("risk_score", 0))

        message = (
            f"{row.get('district')} | "
            f"{row.get('predicted_category')} | "
            f"Risk={risk_score:.2f} | "
            f"{row.get('title')}"
        )

        if correlated:
            st.warning(message)
        else:
            st.info(message)

    with st.expander("NLP Reports Table"):
        nlp_columns = [
            "district",
            "title",
            "original_category",
            "predicted_category",
            "severity",
            "risk_score",
            "correlated_with_physical_anomaly",
            "latest_avg_voltage",
            "latest_voltage_drop_count",
            "latest_district_status",
            "analyzed_at",
        ]

        existing_nlp_columns = [
            col for col in nlp_columns
            if col in nlp_reports_df.columns
        ]

        st.dataframe(
            nlp_reports_df[existing_nlp_columns],
            use_container_width=True,
        )


# ============================================================
# Spark District Aggregations Table
# ============================================================

st.subheader("Spark District Aggregations")

columns = [
    "cycle_id",
    "district",
    "zone_type",
    "total_energy_consumption",
    "avg_voltage",
    "avg_current",
    "total_meters",
    "overload_count",
    "voltage_drop_count",
    "quality_score",
    "district_status",
    "processed_at",
]

existing_columns = [c for c in columns if c in cycle_df.columns]

st.dataframe(
    cycle_df[existing_columns].sort_values("total_energy_consumption", ascending=False),
    use_container_width=True,
)


with st.expander("Spark Cycle Metadata"):
    if cycles_df.empty:
        st.info("No Spark cycle metadata yet.")
    else:
        st.dataframe(cycles_df, use_container_width=True)


# ============================================================
# Footer
# ============================================================

st.caption(
    f"Auto-refresh every {refresh_seconds} seconds. "
    f"Source: Spark Structured Streaming → MongoDB → Analytics. "
    f"MongoDB: {MONGO_URI} | Database: {DB_NAME}"
)
