import os
import json
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "energy_project")

TOPIC_METERS = os.getenv("TOPIC_METERS", "smart_meter_readings")
TOPIC_DISTRIBUTORS = os.getenv("TOPIC_DISTRIBUTORS", "distributor_aggregations")
TOPIC_CONCENTRATORS = os.getenv("TOPIC_CONCENTRATORS", "district_aggregations")

CONCENTRATORS_PER_CYCLE = int(os.getenv("CONCENTRATORS_PER_CYCLE", "18"))
DELETE_RAW_AFTER_COMPLETION = os.getenv("DELETE_RAW_AFTER_COMPLETION", "true").lower() == "true"
CLEANUP_DELAY_SECONDS = int(os.getenv("CLEANUP_DELAY_SECONDS", "10"))


# ============================================================
# Connections
# ============================================================

def connect_mongo():
    while True:
        try:
            client = MongoClient(MONGO_URI)
            client.admin.command("ping")
            print(f"Connected to MongoDB: {MONGO_URI}", flush=True)
            return client
        except Exception as error:
            print(f"MongoDB not ready yet: {error}", flush=True)
            time.sleep(5)


def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_METERS,
                TOPIC_DISTRIBUTORS,
                TOPIC_CONCENTRATORS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="mongo_writer_group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )

            print(f"Connected to Kafka: {KAFKA_BOOTSTRAP_SERVER}", flush=True)
            return consumer

        except Exception as error:
            print(f"Kafka not ready yet: {error}", flush=True)
            time.sleep(5)


mongo_client = connect_mongo()
db = mongo_client[DB_NAME]

meter_collection = db["smart_meter_readings"]
distributor_collection = db["distributor_aggregations"]
district_collection = db["district_aggregations"]
cycle_collection = db["cycle_metadata"]

collections = {
    TOPIC_METERS: meter_collection,
    TOPIC_DISTRIBUTORS: distributor_collection,
    TOPIC_CONCENTRATORS: district_collection,
}


# ============================================================
# Indexes
# ============================================================

def create_indexes():
    meter_collection.create_index([("cycle_id", ASCENDING)])
    meter_collection.create_index([("timestamp", ASCENDING)])
    meter_collection.create_index([("district", ASCENDING)])

    distributor_collection.create_index([("cycle_id", ASCENDING)])
    distributor_collection.create_index([("distributor_id", ASCENDING)])
    distributor_collection.create_index([("district", ASCENDING)])

    district_collection.create_index([("cycle_id", ASCENDING)])
    district_collection.create_index([("concentrator_id", ASCENDING)])
    district_collection.create_index([("district", ASCENDING)])

    cycle_collection.create_index([("cycle_id", ASCENDING)], unique=True)

    print("MongoDB indexes are ready", flush=True)


create_indexes()


# ============================================================
# Helpers
# ============================================================

def parse_timestamp(data):
    if "timestamp" not in data:
        data["timestamp"] = datetime.now(timezone.utc)
        return data

    try:
        data["timestamp"] = datetime.fromisoformat(
            str(data["timestamp"]).replace("Z", "+00:00")
        )
    except Exception:
        data["timestamp"] = datetime.now(timezone.utc)

    return data


def count_concentrators_for_cycle(cycle_id):
    concentrator_ids = district_collection.distinct(
        "concentrator_id",
        {"cycle_id": cycle_id}
    )

    return len(concentrator_ids)


def is_cycle_already_completed(cycle_id):
    doc = cycle_collection.find_one({"cycle_id": cycle_id})
    return bool(doc and doc.get("status") == "completed")


def mark_cycle_completed(cycle_id):
    district_count = district_collection.count_documents({"cycle_id": cycle_id})
    distributor_count = distributor_collection.count_documents({"cycle_id": cycle_id})
    meter_count_before_delete = meter_collection.count_documents({"cycle_id": cycle_id})

    cycle_collection.update_one(
        {"cycle_id": cycle_id},
        {
            "$set": {
                "cycle_id": cycle_id,
                "status": "completed",
                "completed_at": datetime.now(timezone.utc),
                "district_aggregation_count": district_count,
                "distributor_aggregation_count": distributor_count,
                "meter_count_before_delete": meter_count_before_delete,
                "raw_meter_deleted": False,
            }
        },
        upsert=True,
    )

    print(
        f"Cycle {cycle_id} marked as completed | "
        f"district_docs={district_count} | "
        f"distributor_docs={distributor_count} | "
        f"meter_docs_before_delete={meter_count_before_delete}",
        flush=True,
    )


def delete_raw_meter_data_for_cycle(cycle_id):
    if not DELETE_RAW_AFTER_COMPLETION:
        print(
            f"Raw meter deletion disabled. Cycle {cycle_id} raw data kept.",
            flush=True,
        )
        return

    print(
        f"Waiting {CLEANUP_DELAY_SECONDS} seconds before deleting raw meter data "
        f"for cycle {cycle_id}...",
        flush=True,
    )

    time.sleep(CLEANUP_DELAY_SECONDS)

    result = meter_collection.delete_many({"cycle_id": cycle_id})

    cycle_collection.update_one(
        {"cycle_id": cycle_id},
        {
            "$set": {
                "raw_meter_deleted": True,
                "raw_meter_deleted_at": datetime.now(timezone.utc),
                "deleted_meter_documents": result.deleted_count,
            }
        },
    )

    print(
        f"Deleted {result.deleted_count} raw smart meter documents "
        f"from cycle {cycle_id}",
        flush=True,
    )


def try_complete_cycle(cycle_id):
    if cycle_id is None:
        return

    if is_cycle_already_completed(cycle_id):
        return

    concentrator_count = count_concentrators_for_cycle(cycle_id)

    print(
        f"Cycle {cycle_id}: received {concentrator_count}/"
        f"{CONCENTRATORS_PER_CYCLE} concentrator aggregations",
        flush=True,
    )

    if concentrator_count >= CONCENTRATORS_PER_CYCLE:
        mark_cycle_completed(cycle_id)
        delete_raw_meter_data_for_cycle(cycle_id)


# ============================================================
# Main Consumer Loop
# ============================================================

consumer = create_consumer()

print("Kafka -> MongoDB consumer started", flush=True)
print(f"Topics: {TOPIC_METERS}, {TOPIC_DISTRIBUTORS}, {TOPIC_CONCENTRATORS}", flush=True)
print(f"Delete raw meter data after completion: {DELETE_RAW_AFTER_COMPLETION}", flush=True)


for message in consumer:
    topic = message.topic
    data = message.value

    data = parse_timestamp(data)

    collection = collections.get(topic)

    if collection is None:
        print(f"Unknown topic ignored: {topic}", flush=True)
        continue

    collection.insert_one(data)

    print(
        f"Inserted into {topic} | "
        f"level={data.get('level')} | "
        f"cycle_id={data.get('cycle_id')} | "
        f"district={data.get('district')}",
        flush=True,
    )

    if topic == TOPIC_CONCENTRATORS:
        try_complete_cycle(data.get("cycle_id"))
