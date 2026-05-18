import os
import time
import math
import traceback
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pymongo import MongoClient

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, accuracy_score, f1_score
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.feature_extraction.text import TfidfVectorizer


# ============================================================
# Configuration
# ============================================================

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
DB_NAME = os.getenv("DB_NAME", "energy_project")

RUN_INTERVAL_SECONDS = int(os.getenv("ANALYTICS_INTERVAL_SECONDS", "120"))
MIN_ML_ROWS = int(os.getenv("MIN_ML_ROWS", "36"))
MIN_NLP_ROWS = int(os.getenv("MIN_NLP_ROWS", "10"))

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

district_collection = db["spark_district_consumption"]
weather_collection = db["weather_data"]
rss_collection = db["rss_industrial_news"]

ml_metrics_collection = db["ml_model_metrics"]
ml_predictions_collection = db["ml_predictions"]
ml_comparison_collection = db["ml_prediction_comparisons"]

nlp_reports_collection = db["nlp_reports"]
nlp_metrics_collection = db["nlp_model_metrics"]


# ============================================================
# Helpers
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def safe_float(value, default=0.0):
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        if value is None or pd.isna(value):
            return default
        return int(value)
    except Exception:
        return default


def load_collection(collection, limit=5000, sort_field=None):
    query = collection.find()

    if sort_field:
        query = query.sort(sort_field, -1)

    data = list(query.limit(limit))

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    return df


def prepare_time_features(df):
    if "processed_at" in df.columns:
        df["processed_at"] = pd.to_datetime(df["processed_at"], errors="coerce")
    else:
        df["processed_at"] = pd.NaT

    df["hour"] = df["processed_at"].dt.hour.fillna(0).astype(int)
    df["dayofweek"] = df["processed_at"].dt.dayofweek.fillna(0).astype(int)

    return df


# ============================================================
# ML: 3 Models for Energy Consumption Prediction
# ============================================================

def build_ml_dataset():
    df = load_collection(
        district_collection,
        limit=10000,
        sort_field="processed_at"
    )

    if df.empty:
        return pd.DataFrame()

    required_columns = [
        "cycle_id",
        "district",
        "zone_type",
        "total_energy_consumption",
        "avg_voltage",
        "avg_current",
        "total_meters",
        "overload_count",
        "voltage_drop_count",
        "processed_at",
    ]

    for col in required_columns:
        if col not in df.columns:
            df[col] = np.nan

    df = df.dropna(subset=["cycle_id", "district", "total_energy_consumption"])
    df = prepare_time_features(df)

    numeric_defaults = {
        "avg_voltage": 220.0,
        "avg_current": 0.0,
        "total_meters": 0,
        "overload_count": 0,
        "voltage_drop_count": 0,
    }

    for col, default in numeric_defaults.items():
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)

    df["cycle_id"] = pd.to_numeric(df["cycle_id"], errors="coerce").fillna(0).astype(int)
    df["total_energy_consumption"] = pd.to_numeric(
        df["total_energy_consumption"],
        errors="coerce"
    )

    df = df.dropna(subset=["total_energy_consumption"])

    return df


def train_and_compare_models(df):
    features = [
        "cycle_id",
        "district",
        "zone_type",
        "avg_voltage",
        "avg_current",
        "total_meters",
        "overload_count",
        "voltage_drop_count",
        "hour",
        "dayofweek",
    ]

    target = "total_energy_consumption"

    X = df[features]
    y = df[target]

    categorical_features = ["district", "zone_type"]
    numeric_features = [
        "cycle_id",
        "avg_voltage",
        "avg_current",
        "total_meters",
        "overload_count",
        "voltage_drop_count",
        "hour",
        "dayofweek",
    ]

    preprocessor = ColumnTransformer(
        transformers=[
            ("categorical", OneHotEncoder(handle_unknown="ignore"), categorical_features),
            ("numeric", "passthrough", numeric_features),
        ]
    )

    models = {
        "linear_regression": LinearRegression(),
        "random_forest": RandomForestRegressor(
            n_estimators=80,
            random_state=42,
            n_jobs=-1
        ),
        "gradient_boosting": GradientBoostingRegressor(
            random_state=42
        ),
    }

    if len(df) < 5:
        return None, []

    test_size = 0.25 if len(df) >= 40 else 0.30

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=test_size,
        random_state=42
    )

    trained_models = {}
    metrics = []

    for model_name, model in models.items():
        start_time = time.time()

        pipeline = Pipeline(
            steps=[
                ("preprocessor", preprocessor),
                ("model", model),
            ]
        )

        pipeline.fit(X_train, y_train)

        inference_start = time.time()
        predictions = pipeline.predict(X_test)
        inference_time = time.time() - inference_start

        training_time = time.time() - start_time

        mae = mean_absolute_error(y_test, predictions)
        rmse = math.sqrt(mean_squared_error(y_test, predictions))
        r2 = r2_score(y_test, predictions)

        metric_doc = {
            "model_name": model_name,
            "task": "energy_consumption_regression",
            "mae": float(mae),
            "rmse": float(rmse),
            "r2_score": float(r2),
            "training_time_seconds": float(training_time),
            "inference_time_seconds": float(inference_time),
            "train_rows": int(len(X_train)),
            "test_rows": int(len(X_test)),
            "created_at": now_utc(),
        }

        metrics.append(metric_doc)
        trained_models[model_name] = pipeline

    best_metric = sorted(metrics, key=lambda item: item["rmse"])[0]
    best_model_name = best_metric["model_name"]
    best_model = trained_models[best_model_name]

    return best_model, metrics


def generate_next_cycle_predictions(df, best_model):
    latest_cycle = int(df["cycle_id"].max())
    next_cycle = latest_cycle + 1

    latest_df = (
        df.sort_values("cycle_id")
        .groupby("district", as_index=False)
        .tail(1)
        .copy()
    )

    latest_df["cycle_id"] = next_cycle

    prediction_features = [
        "cycle_id",
        "district",
        "zone_type",
        "avg_voltage",
        "avg_current",
        "total_meters",
        "overload_count",
        "voltage_drop_count",
        "hour",
        "dayofweek",
    ]

    predicted_values = best_model.predict(latest_df[prediction_features])

    prediction_docs = []

    for index, row in latest_df.reset_index(drop=True).iterrows():
        prediction_docs.append({
            "prediction_generated_at": now_utc(),
            "source": "analytics_job",
            "predicted_cycle_id": int(next_cycle),
            "based_on_cycle_id": int(latest_cycle),
            "district": row["district"],
            "zone_type": row.get("zone_type"),
            "predicted_consumption": float(max(predicted_values[index], 0)),
            "latest_real_consumption": safe_float(row.get("total_energy_consumption")),
            "avg_voltage": safe_float(row.get("avg_voltage")),
            "avg_current": safe_float(row.get("avg_current")),
            "total_meters": safe_int(row.get("total_meters")),
        })

    return prediction_docs


def update_prediction_comparisons():
    predictions = list(ml_predictions_collection.find().limit(5000))

    if not predictions:
        return

    for pred in predictions:
        predicted_cycle_id = pred.get("predicted_cycle_id")
        district = pred.get("district")

        actual = district_collection.find_one({
            "cycle_id": predicted_cycle_id,
            "district": district,
        })

        if not actual:
            continue

        predicted_value = safe_float(pred.get("predicted_consumption"))
        actual_value = safe_float(actual.get("total_energy_consumption"))

        absolute_error = abs(actual_value - predicted_value)

        percentage_error = 0.0
        if actual_value != 0:
            percentage_error = (absolute_error / actual_value) * 100

        comparison_doc = {
            "cycle_id": int(predicted_cycle_id),
            "district": district,
            "zone_type": actual.get("zone_type"),
            "actual_consumption": actual_value,
            "predicted_consumption": predicted_value,
            "absolute_error": float(absolute_error),
            "percentage_error": float(percentage_error),
            "compared_at": now_utc(),
            "source": "analytics_job",
        }

        ml_comparison_collection.update_one(
            {
                "cycle_id": int(predicted_cycle_id),
                "district": district,
            },
            {
                "$set": comparison_doc,
            },
            upsert=True,
        )


def run_ml_pipeline():
    df = build_ml_dataset()

    if df.empty:
        print("ML: no district data available yet.", flush=True)
        return

    if len(df) < MIN_ML_ROWS:
        print(f"ML: waiting for more rows. Current={len(df)}, minimum={MIN_ML_ROWS}", flush=True)
        return

    best_model, metrics = train_and_compare_models(df)

    if best_model is None:
        print("ML: model training skipped.", flush=True)
        return

    for metric in metrics:
        ml_metrics_collection.update_one(
            {
                "model_name": metric["model_name"],
                "task": metric["task"],
                "created_at": metric["created_at"],
            },
            {
                "$set": metric,
            },
            upsert=True,
        )

    best_metric = sorted(metrics, key=lambda item: item["rmse"])[0]

    ml_metrics_collection.update_one(
        {
            "task": "best_model_current",
        },
        {
            "$set": {
                "task": "best_model_current",
                "best_model_name": best_metric["model_name"],
                "mae": best_metric["mae"],
                "rmse": best_metric["rmse"],
                "r2_score": best_metric["r2_score"],
                "updated_at": now_utc(),
            }
        },
        upsert=True,
    )

    prediction_docs = generate_next_cycle_predictions(df, best_model)

    for doc in prediction_docs:
        ml_predictions_collection.update_one(
            {
                "predicted_cycle_id": doc["predicted_cycle_id"],
                "district": doc["district"],
            },
            {
                "$set": doc,
            },
            upsert=True,
        )

    update_prediction_comparisons()

    print(
        f"ML: trained 3 models. Best={best_metric['model_name']} "
        f"RMSE={best_metric['rmse']:.4f}. "
        f"Generated {len(prediction_docs)} predictions.",
        flush=True,
    )


# ============================================================
# NLP: Technical Report / RSS Analysis
# ============================================================

def build_nlp_dataset():
    df = load_collection(
        rss_collection,
        limit=3000,
        sort_field="processed_at"
    )

    if df.empty:
        return pd.DataFrame()

    for col in ["title", "description", "category", "severity", "district"]:
        if col not in df.columns:
            df[col] = ""

    df["text"] = (
        df["title"].fillna("").astype(str)
        + " "
        + df["description"].fillna("").astype(str)
    )

    df["category"] = df["category"].fillna("unknown").astype(str)
    df["severity"] = df["severity"].fillna("low").astype(str)
    df["district"] = df["district"].fillna("unknown").astype(str)

    df = df[df["text"].str.strip() != ""]

    return df


def calculate_risk_score(category, severity):
    category_scores = {
        "incident": 0.95,
        "maintenance": 0.70,
        "weather_risk": 0.65,
        "industrial_activity": 0.60,
        "public_event": 0.50,
        "normal_news": 0.10,
        "unknown": 0.30,
    }

    severity_multiplier = {
        "high": 1.00,
        "medium": 0.75,
        "low": 0.40,
    }

    base = category_scores.get(category, 0.30)
    multiplier = severity_multiplier.get(severity, 0.50)

    return min(base * multiplier, 1.0)


def correlate_with_voltage_drop(district):
    latest = district_collection.find_one(
        {"district": district},
        sort=[("cycle_id", -1)]
    )

    if not latest:
        return {
            "correlated_with_physical_anomaly": False,
            "latest_avg_voltage": None,
            "latest_voltage_drop_count": 0,
            "latest_district_status": "unknown",
        }

    avg_voltage = safe_float(latest.get("avg_voltage"))
    voltage_drop_count = safe_int(latest.get("voltage_drop_count"))
    district_status = latest.get("district_status", "normal")

    correlated = (
        avg_voltage < 210
        or voltage_drop_count > 0
        or district_status in ["voltage_risk", "overload_risk", "saturation_risk"]
    )

    return {
        "correlated_with_physical_anomaly": correlated,
        "latest_avg_voltage": avg_voltage,
        "latest_voltage_drop_count": voltage_drop_count,
        "latest_district_status": district_status,
    }


def run_nlp_pipeline():
    df = build_nlp_dataset()

    if df.empty:
        print("NLP: no RSS data available yet.", flush=True)
        return

    if len(df) < MIN_NLP_ROWS:
        print(f"NLP: waiting for more rows. Current={len(df)}, minimum={MIN_NLP_ROWS}", flush=True)
        return

    X = df["text"]
    y = df["category"]

    if y.nunique() < 2:
        print("NLP: waiting for at least two categories.", flush=True)
        return

    test_size = 0.25 if len(df) >= 40 else 0.30

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=test_size,
        random_state=42,
        stratify=y if y.value_counts().min() >= 2 else None,
    )

    nlp_model = Pipeline(
        steps=[
            ("tfidf", TfidfVectorizer(max_features=3000, ngram_range=(1, 2))),
            ("classifier", LogisticRegression(max_iter=1000)),
        ]
    )

    start_time = time.time()
    nlp_model.fit(X_train, y_train)
    training_time = time.time() - start_time

    inference_start = time.time()
    y_pred = nlp_model.predict(X_test)
    inference_time = time.time() - inference_start

    accuracy = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred, average="weighted")

    nlp_metrics_collection.update_one(
        {
            "task": "rss_technical_report_classification",
        },
        {
            "$set": {
                "task": "rss_technical_report_classification",
                "model_name": "tfidf_logistic_regression",
                "accuracy": float(accuracy),
                "f1_score": float(f1),
                "training_time_seconds": float(training_time),
                "inference_time_seconds": float(inference_time),
                "total_rows": int(len(df)),
                "updated_at": now_utc(),
            }
        },
        upsert=True,
    )

    latest_rows = df.sort_values("processed_at" if "processed_at" in df.columns else "timestamp").tail(100)

    predicted_categories = nlp_model.predict(latest_rows["text"])

    for index, (_, row) in enumerate(latest_rows.iterrows()):
        predicted_category = predicted_categories[index]
        severity = row.get("severity", "low")
        district = row.get("district", "unknown")

        risk_score = calculate_risk_score(predicted_category, severity)
        correlation = correlate_with_voltage_drop(district)

        doc = {
            "source": "analytics_job",
            "district": district,
            "title": row.get("title"),
            "description": row.get("description"),
            "text": row.get("text"),
            "original_category": row.get("category"),
            "predicted_category": predicted_category,
            "severity": severity,
            "risk_score": float(risk_score),
            "correlated_with_physical_anomaly": correlation["correlated_with_physical_anomaly"],
            "latest_avg_voltage": correlation["latest_avg_voltage"],
            "latest_voltage_drop_count": correlation["latest_voltage_drop_count"],
            "latest_district_status": correlation["latest_district_status"],
            "analyzed_at": now_utc(),
        }

        nlp_reports_collection.update_one(
            {
                "title": doc["title"],
                "district": doc["district"],
            },
            {
                "$set": doc,
            },
            upsert=True,
        )

    print(
        f"NLP: trained TF-IDF Logistic Regression. "
        f"Accuracy={accuracy:.4f}, F1={f1:.4f}. "
        f"Analyzed {len(latest_rows)} reports.",
        flush=True,
    )


# ============================================================
# Main Loop
# ============================================================

def run():
    print("Analytics job started.", flush=True)
    print(f"MongoDB: {MONGO_URI} / {DB_NAME}", flush=True)

    while True:
        try:
            run_ml_pipeline()
            run_nlp_pipeline()

        except Exception as error:
            print("Analytics job error:", error, flush=True)
            traceback.print_exc()

        print(f"Analytics job sleeping for {RUN_INTERVAL_SECONDS} seconds.", flush=True)
        time.sleep(RUN_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
