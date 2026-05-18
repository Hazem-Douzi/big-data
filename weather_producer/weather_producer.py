import os
import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
CITY = os.getenv("CITY", "Tetouan")
INTERVAL_SECONDS = int(os.getenv("WEATHER_INTERVAL_SECONDS", "60"))


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
            print(f"Weather producer connected to Kafka: {KAFKA_BOOTSTRAP_SERVER}", flush=True)
            return producer
        except Exception as error:
            print(f"Kafka not ready for weather producer: {error}", flush=True)
            time.sleep(5)


producer = create_kafka_producer()


def get_weather_condition(temperature, cloudiness, humidity, wind_speed):
    if cloudiness > 75 and humidity > 70:
        return "cloudy"
    if wind_speed > 35:
        return "windy"
    if temperature > 32:
        return "hot"
    if temperature < 10:
        return "cold"
    if cloudiness < 25:
        return "clear"
    return "partly_cloudy"


def generate_weather():
    now = datetime.now(timezone.utc)
    hour = now.hour

    # Tétouan-like synthetic weather profile.
    if 6 <= hour < 12:
        base_temperature = random.uniform(16, 24)
    elif 12 <= hour < 18:
        base_temperature = random.uniform(23, 34)
    elif 18 <= hour < 23:
        base_temperature = random.uniform(18, 27)
    else:
        base_temperature = random.uniform(12, 20)

    humidity = random.uniform(45, 85)
    cloudiness = random.uniform(5, 90)
    wind_speed = random.uniform(2, 40)

    temperature = round(base_temperature + random.uniform(-1.5, 1.5), 2)
    humidity = round(humidity, 2)
    cloudiness = round(cloudiness, 2)
    wind_speed = round(wind_speed, 2)

    condition = get_weather_condition(
        temperature=temperature,
        cloudiness=cloudiness,
        humidity=humidity,
        wind_speed=wind_speed,
    )

    cooling_risk = temperature >= 30
    heating_risk = temperature <= 10
    solar_variability_risk = cloudiness >= 70
    wind_risk = wind_speed >= 35

    weather_event = {
        "source": "synthetic_weather_connector",
        "city": CITY,
        "timestamp": now.isoformat(),
        "temperature": temperature,
        "humidity": humidity,
        "cloudiness": cloudiness,
        "wind_speed": wind_speed,
        "weather_condition": condition,
        "cooling_risk": cooling_risk,
        "heating_risk": heating_risk,
        "solar_variability_risk": solar_variability_risk,
        "wind_risk": wind_risk,
    }

    return weather_event


def run():
    print(f"Weather producer started. Topic={WEATHER_TOPIC}", flush=True)

    while True:
        event = generate_weather()

        producer.send(WEATHER_TOPIC, event)
        producer.flush()

        print(f"Weather sent: {event}", flush=True)

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("Weather producer stopped.", flush=True)
        producer.close()
