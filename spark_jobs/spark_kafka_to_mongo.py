import os
from datetime import datetime, timezone

from pymongo import MongoClient

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    sum as spark_sum,
    avg,
    count,
    approx_count_distinct,
    max as spark_max,
    when,
    lit,
    current_timestamp,
    abs as spark_abs,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:29092")
KAFKA_TOPIC_METERS = os.getenv("KAFKA_TOPIC_METERS", "smart_meter_readings")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
RSS_TOPIC = os.getenv("RSS_TOPIC", "rss_industrial_news")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
DB_NAME = os.getenv("DB_NAME", "energy_project")

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

EXPECTED_DISTRICTS = int(os.getenv("EXPECTED_DISTRICTS", "18"))
EXPECTED_METERS = int(os.getenv("EXPECTED_METERS", "91000"))


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("TetouanEnergySparkStreamingFullIngestion")
    .master(SPARK_MASTER_URL)
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Schemas
# ============================================================

meter_schema = StructType([
    StructField("level", StringType(), True),
    StructField("cycle_id", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("meter_id", StringType(), True),
    StructField("distributor_id", StringType(), True),
    StructField("concentrator_id", StringType(), True),
    StructField("district", StringType(), True),
    StructField("zone_type", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("energy_consumption", DoubleType(), True),
    StructField("voltage", DoubleType(), True),
    StructField("current", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True),
])

weather_schema = StructType([
    StructField("source", StringType(), True),
    StructField("city", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("cloudiness", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("cooling_risk", BooleanType(), True),
    StructField("heating_risk", BooleanType(), True),
    StructField("solar_variability_risk", BooleanType(), True),
    StructField("wind_risk", BooleanType(), True),
])

rss_schema = StructType([
    StructField("source", StringType(), True),
    StructField("city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("language", StringType(), True),
])


# ============================================================
# Helper Functions
# ============================================================

def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def safe_bool(value, default=False):
    try:
        if value is None:
            return default
        return bool(value)
    except Exception:
        return default


def safe_datetime(value):
    if value is None:
        return datetime.now(timezone.utc)

    try:
        return value.to_pydatetime()
    except Exception:
        return datetime.now(timezone.utc)


def safe_timestamp_string(value):
    if value is None:
        return None
    return str(value)


# ============================================================
# Read Smart Meter Kafka Stream
# ============================================================

raw_meter_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", KAFKA_TOPIC_METERS)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

meter_df = (
    raw_meter_kafka_df
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), meter_schema).alias("data"))
    .select("data.*")
    .filter(col("level") == "meter")
)


# ============================================================
# Data Quality Logic
# ============================================================

quality_df = (
    meter_df
    .withColumn(
        "missing_required_field",
        when(
            col("cycle_id").isNull()
            | col("meter_id").isNull()
            | col("distributor_id").isNull()
            | col("concentrator_id").isNull()
            | col("district").isNull()
            | col("zone_type").isNull()
            | col("energy_consumption").isNull()
            | col("voltage").isNull()
            | col("current").isNull()
            | col("timestamp").isNull(),
            lit(1),
        ).otherwise(lit(0))
    )
    .withColumn(
        "invalid_voltage",
        when(
            col("voltage").isNull()
            | (col("voltage") < 170)
            | (col("voltage") > 250),
            lit(1),
        ).otherwise(lit(0))
    )
    .withColumn(
        "invalid_current",
        when(
            col("current").isNull()
            | (col("current") < 0)
            | (col("current") > 200),
            lit(1),
        ).otherwise(lit(0))
    )
    .withColumn(
        "invalid_energy",
        when(
            col("energy_consumption").isNull()
            | (col("energy_consumption") < 0)
            | (col("energy_consumption") > 10),
            lit(1),
        ).otherwise(lit(0))
    )
    .withColumn(
        "expected_energy",
        ((col("voltage") * col("current")) / lit(1000)) * lit(0.25)
    )
    .withColumn(
        "energy_consistency_error",
        spark_abs(col("energy_consumption") - col("expected_energy"))
    )
    .withColumn(
        "inconsistent_energy",
        when(
            col("expected_energy").isNull()
            | col("energy_consumption").isNull(),
            lit(1),
        )
        .when(
            col("energy_consistency_error") > 0.5,
            lit(1),
        )
        .otherwise(lit(0))
    )
    .withColumn(
        "invalid_record",
        when(
            (col("missing_required_field") == 1)
            | (col("invalid_voltage") == 1)
            | (col("invalid_current") == 1)
            | (col("invalid_energy") == 1)
            | (col("inconsistent_energy") == 1),
            lit(1),
        ).otherwise(lit(0))
    )
)

valid_meter_df = (
    quality_df
    .filter(col("missing_required_field") == 0)
    .filter(col("cycle_id").isNotNull())
    .filter(col("district").isNotNull())
)


# ============================================================
# District Aggregation
# ============================================================

district_agg_df = (
    valid_meter_df
    .groupBy(
        "cycle_id",
        "city",
        "district",
        "zone_type",
        "latitude",
        "longitude",
        "concentrator_id",
    )
    .agg(
        spark_sum("energy_consumption").alias("total_energy_consumption"),
        avg("voltage").alias("avg_voltage"),
        avg("current").alias("avg_current"),
        count("meter_id").alias("total_meters"),
        spark_sum(when(col("status") == "overload", 1).otherwise(0)).alias("overload_count"),
        spark_sum(when(col("status") == "voltage_drop", 1).otherwise(0)).alias("voltage_drop_count"),
        spark_sum("invalid_record").alias("invalid_record_count"),
        spark_sum("missing_required_field").alias("missing_values_count"),
        spark_sum("invalid_voltage").alias("invalid_voltage_count"),
        spark_sum("invalid_current").alias("invalid_current_count"),
        spark_sum("invalid_energy").alias("invalid_energy_count"),
        spark_sum("inconsistent_energy").alias("inconsistent_energy_count"),
        spark_max("timestamp").alias("last_meter_timestamp"),
    )
    .withColumn(
        "district_status",
        when(col("voltage_drop_count") > col("total_meters") * lit(0.02), lit("voltage_risk"))
        .when(col("overload_count") > col("total_meters") * lit(0.02), lit("overload_risk"))
        .otherwise(lit("normal"))
    )
    .withColumn(
        "quality_score",
        lit(100) - ((col("invalid_record_count") / col("total_meters")) * lit(100))
    )
    .withColumn("processed_at", current_timestamp())
)


# ============================================================
# Quality Reports
# ============================================================

quality_report_df = (
    quality_df
    .filter(col("cycle_id").isNotNull())
    .groupBy("cycle_id")
    .agg(
        count("*").alias("total_records"),
        approx_count_distinct("meter_id").alias("distinct_meters"),
        approx_count_distinct("district").alias("district_count"),
        approx_count_distinct("zone_type").alias("zone_type_count"),
        spark_sum("missing_required_field").alias("missing_values_count"),
        spark_sum("invalid_voltage").alias("invalid_voltage_count"),
        spark_sum("invalid_current").alias("invalid_current_count"),
        spark_sum("invalid_energy").alias("invalid_energy_count"),
        spark_sum("inconsistent_energy").alias("inconsistent_energy_count"),
        spark_sum("invalid_record").alias("invalid_records_count"),
    )
    .withColumn(
        "duplicate_count",
        col("total_records") - col("distinct_meters")
    )
    .withColumn(
        "missing_districts_count",
        lit(EXPECTED_DISTRICTS) - col("district_count")
    )
    .withColumn(
        "meter_coverage_rate",
        (col("distinct_meters") / lit(EXPECTED_METERS)) * lit(100)
    )
    .withColumn(
        "invalid_rate",
        (col("invalid_records_count") / col("total_records")) * lit(100)
    )
    .withColumn(
        "quality_score",
        lit(100) - col("invalid_rate")
    )
    .withColumn(
        "quality_status",
        when(col("quality_score") >= 98, lit("excellent"))
        .when(col("quality_score") >= 95, lit("good"))
        .when(col("quality_score") >= 90, lit("acceptable"))
        .otherwise(lit("poor"))
    )
    .withColumn("processed_at", current_timestamp())
)


# ============================================================
# Bias Reports
# ============================================================

bias_report_df = (
    valid_meter_df
    .groupBy(
        "cycle_id",
        "district",
        "zone_type",
    )
    .agg(
        count("meter_id").alias("meter_count"),
        avg("energy_consumption").alias("avg_energy_per_meter"),
        spark_sum(when(col("status") == "overload", 1).otherwise(0)).alias("overload_count"),
        spark_sum(when(col("status") == "voltage_drop", 1).otherwise(0)).alias("voltage_drop_count"),
    )
    .withColumn(
        "meter_share_percent",
        (col("meter_count") / lit(EXPECTED_METERS)) * lit(100)
    )
    .withColumn(
        "overload_rate_percent",
        (col("overload_count") / col("meter_count")) * lit(100)
    )
    .withColumn(
        "voltage_drop_rate_percent",
        (col("voltage_drop_count") / col("meter_count")) * lit(100)
    )
    .withColumn("processed_at", current_timestamp())
)


# ============================================================
# Read Weather Kafka Stream
# ============================================================

raw_weather_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", WEATHER_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

weather_df = (
    raw_weather_kafka_df
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), weather_schema).alias("data"))
    .select("data.*")
    .withColumn("processed_at", current_timestamp())
)


# ============================================================
# Read RSS Kafka Stream
# ============================================================

raw_rss_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", RSS_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

rss_df = (
    raw_rss_kafka_df
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), rss_schema).alias("data"))
    .select("data.*")
    .withColumn("processed_at", current_timestamp())
)


# ============================================================
# Mongo Writers
# ============================================================

def write_district_batch_to_mongo(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    records = batch_df.toPandas().to_dict("records")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    district_collection = db["spark_district_consumption"]
    alerts_collection = db["spark_alerts"]
    cycle_collection = db["spark_cycle_metadata"]

    for record in records:
        cycle_id = safe_int(record.get("cycle_id"))
        district = record.get("district")

        document = {
            "cycle_id": cycle_id,
            "city": record.get("city"),
            "district": district,
            "zone_type": record.get("zone_type"),
            "latitude": safe_float(record.get("latitude")),
            "longitude": safe_float(record.get("longitude")),
            "concentrator_id": record.get("concentrator_id"),
            "total_energy_consumption": safe_float(record.get("total_energy_consumption")),
            "avg_voltage": safe_float(record.get("avg_voltage")),
            "avg_current": safe_float(record.get("avg_current")),
            "total_meters": safe_int(record.get("total_meters")),
            "overload_count": safe_int(record.get("overload_count")),
            "voltage_drop_count": safe_int(record.get("voltage_drop_count")),
            "invalid_record_count": safe_int(record.get("invalid_record_count")),
            "missing_values_count": safe_int(record.get("missing_values_count")),
            "invalid_voltage_count": safe_int(record.get("invalid_voltage_count")),
            "invalid_current_count": safe_int(record.get("invalid_current_count")),
            "invalid_energy_count": safe_int(record.get("invalid_energy_count")),
            "inconsistent_energy_count": safe_int(record.get("inconsistent_energy_count")),
            "quality_score": safe_float(record.get("quality_score")),
            "district_status": record.get("district_status", "normal"),
            "last_meter_timestamp": record.get("last_meter_timestamp"),
            "processed_at": safe_datetime(record.get("processed_at")),
            "source": "spark_structured_streaming",
        }

        district_collection.update_one(
            {"cycle_id": cycle_id, "district": district},
            {"$set": document},
            upsert=True,
        )

        if document["district_status"] != "normal":
            alerts_collection.update_one(
                {
                    "cycle_id": cycle_id,
                    "district": district,
                    "alert_type": document["district_status"],
                },
                {
                    "$set": {
                        "cycle_id": cycle_id,
                        "district": district,
                        "zone_type": document["zone_type"],
                        "alert_type": document["district_status"],
                        "total_energy_consumption": document["total_energy_consumption"],
                        "avg_voltage": document["avg_voltage"],
                        "overload_count": document["overload_count"],
                        "voltage_drop_count": document["voltage_drop_count"],
                        "created_at": datetime.now(timezone.utc),
                        "source": "spark_structured_streaming",
                    }
                },
                upsert=True,
            )

    cycle_ids = sorted(set(safe_int(record.get("cycle_id")) for record in records))

    for cycle_id in cycle_ids:
        district_count = district_collection.count_documents({"cycle_id": cycle_id})

        status = "processing"
        if district_count >= EXPECTED_DISTRICTS:
            status = "completed"

        cycle_collection.update_one(
            {"cycle_id": cycle_id},
            {
                "$set": {
                    "cycle_id": cycle_id,
                    "district_count": district_count,
                    "status": status,
                    "updated_at": datetime.now(timezone.utc),
                    "source": "spark_structured_streaming",
                }
            },
            upsert=True,
        )

    print(f"Spark batch {batch_id}: wrote {len(records)} district records", flush=True)

    client.close()


def write_quality_report_batch_to_mongo(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    records = batch_df.toPandas().to_dict("records")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    report_collection = db["data_quality_reports"]
    alert_collection = db["data_quality_alerts"]

    for record in records:
        cycle_id = safe_int(record.get("cycle_id"))

        document = {
            "cycle_id": cycle_id,
            "total_records": safe_int(record.get("total_records")),
            "distinct_meters": safe_int(record.get("distinct_meters")),
            "district_count": safe_int(record.get("district_count")),
            "zone_type_count": safe_int(record.get("zone_type_count")),
            "missing_values_count": safe_int(record.get("missing_values_count")),
            "invalid_voltage_count": safe_int(record.get("invalid_voltage_count")),
            "invalid_current_count": safe_int(record.get("invalid_current_count")),
            "invalid_energy_count": safe_int(record.get("invalid_energy_count")),
            "inconsistent_energy_count": safe_int(record.get("inconsistent_energy_count")),
            "invalid_records_count": safe_int(record.get("invalid_records_count")),
            "duplicate_count": safe_int(record.get("duplicate_count")),
            "missing_districts_count": safe_int(record.get("missing_districts_count")),
            "meter_coverage_rate": safe_float(record.get("meter_coverage_rate")),
            "invalid_rate": safe_float(record.get("invalid_rate")),
            "quality_score": safe_float(record.get("quality_score")),
            "quality_status": record.get("quality_status"),
            "processed_at": safe_datetime(record.get("processed_at")),
            "source": "spark_structured_streaming",
        }

        report_collection.update_one(
            {"cycle_id": cycle_id},
            {"$set": document},
            upsert=True,
        )

        alerts = []

        if document["missing_values_count"] > 0:
            alerts.append(("missing_values", "Missing required fields detected."))

        if document["invalid_voltage_count"] > 0:
            alerts.append(("invalid_voltage", "Voltage values outside valid range detected."))

        if document["invalid_current_count"] > 0:
            alerts.append(("invalid_current", "Current values outside valid range detected."))

        if document["invalid_energy_count"] > 0:
            alerts.append(("invalid_energy", "Energy consumption values outside valid range detected."))

        if document["inconsistent_energy_count"] > 0:
            alerts.append(("inconsistent_energy", "Energy/current/voltage inconsistency detected."))

        if document["duplicate_count"] > 0:
            alerts.append(("duplicates", "Duplicate meter readings detected for the cycle."))

        if document["missing_districts_count"] > 0:
            alerts.append(("district_coverage", "Not all Tetouan districts are represented."))

        if document["quality_score"] < 95:
            alerts.append(("low_quality_score", "Global data quality score is below 95%."))

        for alert_type, message in alerts:
            alert_collection.update_one(
                {"cycle_id": cycle_id, "alert_type": alert_type},
                {
                    "$set": {
                        "cycle_id": cycle_id,
                        "alert_type": alert_type,
                        "message": message,
                        "quality_score": document["quality_score"],
                        "created_at": datetime.now(timezone.utc),
                        "source": "spark_structured_streaming",
                    }
                },
                upsert=True,
            )

    print(f"Spark batch {batch_id}: wrote {len(records)} data quality reports", flush=True)

    client.close()


def write_bias_report_batch_to_mongo(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    records = batch_df.toPandas().to_dict("records")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    bias_collection = db["data_quality_bias_reports"]

    for record in records:
        cycle_id = safe_int(record.get("cycle_id"))
        district = record.get("district")

        document = {
            "cycle_id": cycle_id,
            "district": district,
            "zone_type": record.get("zone_type"),
            "meter_count": safe_int(record.get("meter_count")),
            "avg_energy_per_meter": safe_float(record.get("avg_energy_per_meter")),
            "overload_count": safe_int(record.get("overload_count")),
            "voltage_drop_count": safe_int(record.get("voltage_drop_count")),
            "meter_share_percent": safe_float(record.get("meter_share_percent")),
            "overload_rate_percent": safe_float(record.get("overload_rate_percent")),
            "voltage_drop_rate_percent": safe_float(record.get("voltage_drop_rate_percent")),
            "processed_at": safe_datetime(record.get("processed_at")),
            "source": "spark_structured_streaming",
        }

        bias_collection.update_one(
            {"cycle_id": cycle_id, "district": district},
            {"$set": document},
            upsert=True,
        )

    print(f"Spark batch {batch_id}: wrote {len(records)} bias reports", flush=True)

    client.close()


def write_weather_batch_to_mongo(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    records = batch_df.toPandas().to_dict("records")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    weather_collection = db["weather_data"]

    for record in records:
        document = {
            "source": record.get("source"),
            "city": record.get("city"),
            "timestamp": safe_timestamp_string(record.get("timestamp")),
            "temperature": safe_float(record.get("temperature")),
            "humidity": safe_float(record.get("humidity")),
            "cloudiness": safe_float(record.get("cloudiness")),
            "wind_speed": safe_float(record.get("wind_speed")),
            "weather_condition": record.get("weather_condition"),
            "cooling_risk": safe_bool(record.get("cooling_risk")),
            "heating_risk": safe_bool(record.get("heating_risk")),
            "solar_variability_risk": safe_bool(record.get("solar_variability_risk")),
            "wind_risk": safe_bool(record.get("wind_risk")),
            "processed_at": safe_datetime(record.get("processed_at")),
        }

        weather_collection.insert_one(document)

    print(f"Spark batch {batch_id}: wrote {len(records)} weather records", flush=True)

    client.close()


def write_rss_batch_to_mongo(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    records = batch_df.toPandas().to_dict("records")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    rss_collection = db["rss_industrial_news"]

    for record in records:
        document = {
            "source": record.get("source"),
            "city": record.get("city"),
            "district": record.get("district"),
            "timestamp": safe_timestamp_string(record.get("timestamp")),
            "title": record.get("title"),
            "description": record.get("description"),
            "category": record.get("category"),
            "severity": record.get("severity"),
            "language": record.get("language"),
            "processed_at": safe_datetime(record.get("processed_at")),
        }

        rss_collection.insert_one(document)

    print(f"Spark batch {batch_id}: wrote {len(records)} RSS records", flush=True)

    client.close()


# ============================================================
# Start Streams
# ============================================================

district_query = (
    district_agg_df
    .writeStream
    .foreachBatch(write_district_batch_to_mongo)
    .outputMode("update")
    .option("checkpointLocation", "/tmp/spark-checkpoints/tetouan-energy-district")
    .trigger(processingTime="30 seconds")
    .start()
)

quality_query = (
    quality_report_df
    .writeStream
    .foreachBatch(write_quality_report_batch_to_mongo)
    .outputMode("update")
    .option("checkpointLocation", "/tmp/spark-checkpoints/tetouan-energy-quality")
    .trigger(processingTime="30 seconds")
    .start()
)

bias_query = (
    bias_report_df
    .writeStream
    .foreachBatch(write_bias_report_batch_to_mongo)
    .outputMode("update")
    .option("checkpointLocation", "/tmp/spark-checkpoints/tetouan-energy-bias")
    .trigger(processingTime="30 seconds")
    .start()
)

weather_query = (
    weather_df
    .writeStream
    .foreachBatch(write_weather_batch_to_mongo)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/spark-checkpoints/tetouan-weather")
    .trigger(processingTime="30 seconds")
    .start()
)

rss_query = (
    rss_df
    .writeStream
    .foreachBatch(write_rss_batch_to_mongo)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/spark-checkpoints/tetouan-rss")
    .trigger(processingTime="30 seconds")
    .start()
)

print("Spark Structured Streaming job started with full ingestion.", flush=True)
print("Reading Kafka topics:", flush=True)
print(f"- {KAFKA_TOPIC_METERS}", flush=True)
print(f"- {WEATHER_TOPIC}", flush=True)
print(f"- {RSS_TOPIC}", flush=True)
print(f"Writing to MongoDB: {MONGO_URI} / {DB_NAME}", flush=True)

spark.streams.awaitAnyTermination()
