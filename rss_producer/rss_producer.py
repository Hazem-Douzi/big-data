import os
import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
RSS_TOPIC = os.getenv("RSS_TOPIC", "rss_industrial_news")
CITY = os.getenv("CITY", "Tetouan")
INTERVAL_SECONDS = int(os.getenv("RSS_INTERVAL_SECONDS", "90"))


DISTRICTS = [
    "Medina",
    "Ensanche",
    "Saniat Rmel",
    "Touilaa",
    "Touta",
    "Ziana",
    "Mhannech",
    "Barrio Malaga",
    "Dersa",
    "Semsa",
    "Touibla",
    "Taffaline",
    "El Matar",
    "Souani",
    "Ain Melloul",
    "Wilaya",
    "Coelma",
    "Boujarrah",
]


NEWS_TEMPLATES = [
    {
        "category": "maintenance",
        "severity": "medium",
        "title": "Maintenance électrique programmée à {district}",
        "description": "Une intervention technique est prévue dans le quartier {district}, pouvant réduire temporairement la capacité du réseau.",
    },
    {
        "category": "incident",
        "severity": "high",
        "title": "Incident technique signalé à {district}",
        "description": "Des perturbations électriques ont été signalées dans le quartier {district}, avec risque de baisse de tension.",
    },
    {
        "category": "industrial_activity",
        "severity": "medium",
        "title": "Activité industrielle élevée près de {district}",
        "description": "Une hausse d'activité industrielle peut augmenter la demande énergétique autour de {district}.",
    },
    {
        "category": "weather_risk",
        "severity": "medium",
        "title": "Conditions météorologiques pouvant impacter le réseau",
        "description": "Les conditions météorologiques actuelles peuvent influencer la consommation et la stabilité du réseau électrique.",
    },
    {
        "category": "normal_news",
        "severity": "low",
        "title": "Situation normale du réseau à {district}",
        "description": "Aucun incident majeur n'est signalé actuellement dans le quartier {district}.",
    },
    {
        "category": "public_event",
        "severity": "medium",
        "title": "Événement public dans la zone {district}",
        "description": "Un événement local peut provoquer une augmentation temporaire de la consommation électrique dans la zone {district}.",
    },
]


def create_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                value_serializer=lambda value: json.dumps(
                    value,
                    ensure_ascii=False
                ).encode("utf-8"),
            )
            print(f"RSS producer connected to Kafka: {KAFKA_BOOTSTRAP_SERVER}", flush=True)
            return producer
        except Exception as error:
            print(f"Kafka not ready for RSS producer: {error}", flush=True)
            time.sleep(5)


producer = create_kafka_producer()


def generate_rss_event():
    now = datetime.now(timezone.utc)
    district = random.choice(DISTRICTS)
    template = random.choice(NEWS_TEMPLATES)

    title = template["title"].format(district=district)
    description = template["description"].format(district=district)

    event = {
        "source": "synthetic_rss_connector",
        "city": CITY,
        "district": district,
        "timestamp": now.isoformat(),
        "title": title,
        "description": description,
        "category": template["category"],
        "severity": template["severity"],
        "language": "fr",
    }

    return event


def run():
    print(f"RSS producer started. Topic={RSS_TOPIC}", flush=True)

    while True:
        event = generate_rss_event()

        producer.send(RSS_TOPIC, event)
        producer.flush()

        print(f"RSS sent: {event}", flush=True)

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("RSS producer stopped.", flush=True)
        producer.close()
