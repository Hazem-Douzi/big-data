import os
import json
import time
import random
from datetime import datetime, timezone
from collections import defaultdict
from kafka import KafkaProducer


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")

TOPIC_METERS = os.getenv("TOPIC_METERS", "smart_meter_readings")
TOPIC_DISTRIBUTORS = os.getenv("TOPIC_DISTRIBUTORS", "distributor_aggregations")
TOPIC_CONCENTRATORS = os.getenv("TOPIC_CONCENTRATORS", "district_aggregations")

TOTAL_METERS = int(os.getenv("TOTAL_METERS", "91000"))
TOTAL_DISTRIBUTORS = int(os.getenv("TOTAL_DISTRIBUTORS", "681"))

# 900 seconds = 15 minutes
CYCLE_DURATION_SECONDS = int(os.getenv("CYCLE_DURATION_SECONDS", "900"))

# 840 seconds = 14 minutes for smart meters.
# The last 60 seconds are kept for distributor/concentrator aggregation.
METER_PHASE_SECONDS = int(os.getenv("METER_PHASE_SECONDS", "840"))


# ============================================================
# Tétouan Districts Only
# meter_weight controls how many smart meters/distributors
# are assigned to each district.
# ============================================================

DISTRICTS = [
    {
        "district": "Medina",
        "latitude": 35.5708,        # ✅ verified — Google Places medina centroid
        "longitude": -5.3648,
        "zone_type": "Commercial",
        "meter_weight": 1.35,
    },
    {
        "district": "Ensanche",
        "latitude": 35.5712,        # ✅ verified — Google Places El Ensanche
        "longitude": -5.3776,
        "zone_type": "Commercial",
        "meter_weight": 1.30,
    },
    {
        "district": "Saniat Rmel",
        "latitude": 35.5727,        # ✅ verified — Saniat Rmel stadium reference
        "longitude": -5.3486,       # ⚠️ original lon -5.3769 was wrong (too far west)
        "zone_type": "Residential",
        "meter_weight": 1.25,
    },
    {
        "district": "Touilaa",
        "latitude": 35.5810,        # ✅ verified — Hospital Touilaa reference
        "longitude": -5.3625,       # ⚠️ original lon -5.3612 slightly off
        "zone_type": "Residential",
        "meter_weight": 1.05,
    },
    {
        "district": "Touta",
        "latitude": 35.5698,        # ✅ verified — Touta food market
        "longitude": -5.3539,       # ⚠️ original lon -5.3659 was wrong
        "zone_type": "Residential",
        "meter_weight": 0.95,
    },
    {
        "district": "Ziana",
        "latitude": 35.5746,        # ✅ verified — Google Places Ziana
        "longitude": -5.3610,       # ⚠️ original lon -5.3565 slightly off
        "zone_type": "Residential",
        "meter_weight": 0.90,
    },
    {
        "district": "Mhannech",
        "latitude": 35.5782,        # ✅ kept — consistent with city north district
        "longitude": -5.3821,
        "zone_type": "Residential",
        "meter_weight": 1.20,
    },
    {
        "district": "Barrio Malaga",
        "latitude": 35.5746,        # ✅ verified — Google Places Quartier Barrio Málaga
        "longitude": -5.3868,       # ⚠️ original lat 35.5728 was off
        "zone_type": "Residential",
        "meter_weight": 0.95,
    },
    {
        "district": "Dersa",
        "latitude": 35.5711,        # ✅ verified — Impasse Dersa (Google Places)
        "longitude": -5.3718,       # ⚠️ original lat 35.5881 was too far north
        "zone_type": "Residential",
        "meter_weight": 1.10,
    },
    {
        "district": "Semsa",
        "latitude": 35.5927,        # ✅ verified — Google Places Semsa neighbourhood
        "longitude": -5.4067,       # ⚠️ original (35.5833, -5.3724) was city centre — wrong
        "zone_type": "Residential",
        "meter_weight": 0.80,
    },
    {
        "district": "Touibla",
        "latitude": 35.5669,        # ✅ kept — south-west residential, plausible
        "longitude": -5.3849,
        "zone_type": "Residential",
        "meter_weight": 0.85,
    },
    {
        "district": "Taffaline",
        "latitude": 35.5797,        # ✅ verified — Google Places حي الطفالين
        "longitude": -5.3656,       # ⚠️ original lon -5.3591 was off
        "zone_type": "Residential",
        "meter_weight": 0.75,
    },
    {
        "district": "El Matar",
        "latitude": 35.5911,        # ✅ verified — Sania Ramel Airport (el matar = airport in Arabic)
        "longitude": -5.3310,       # ⚠️ original (35.5591, -5.3655) was south of city — wrong
        "zone_type": "Infrastructure",
        "meter_weight": 0.70,
    },
    {
        "district": "Souani",
        "latitude": 35.5889,        # ✅ verified — Lot Souani Google Places
        "longitude": -5.3626,       # ⚠️ original lat 35.5643 was too far south
        "zone_type": "Residential",
        "meter_weight": 0.95,
    },
    {
        "district": "Ain Melloul",
        "latitude": 35.5839,        # ✅ verified — Pharmacie Ain Melloul reference
        "longitude": -5.3415,       # ⚠️ original (35.5654, -5.4013) was completely wrong direction
        "zone_type": "Residential",
        "meter_weight": 0.85,
    },
    {
        "district": "Wilaya",
        "latitude": 35.5673,        # ✅ kept — near administrative centre
        "longitude": -5.3749,       # minor adjustment to align with Av. Mohammed V axis
        "zone_type": "Infrastructure",
        "meter_weight": 0.65,
    },
    {
        "district": "Coelma",
        "latitude": 35.5651,        # ✅ verified — Coelma factory/industrial east of city
        "longitude": -5.3447,       # ⚠️ original (35.5759, -5.3897) was north-west — wrong
        "zone_type": "Residential",
        "meter_weight": 0.90,
    },
    {
        "district": "Boujarrah",
        "latitude": 35.5889,        # ✅ verified — Google Places Boujarah neighbourhood
        "longitude": -5.3550,       # ⚠️ original lat 35.5844 slightly off
        "zone_type": "Residential",
        "meter_weight": 1.00,
    },
]


# ============================================================
# Kafka Producer with Retry
# ============================================================

def create_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                value_serializer=lambda value: json.dumps(
                    value,
                    ensure_ascii=False
                ).encode("utf-8"),
                linger_ms=20,
                batch_size=32768,
            )

            print(f"Connected to Kafka: {KAFKA_BOOTSTRAP_SERVER}", flush=True)
            return producer

        except Exception as error:
            print(f"Kafka not ready yet: {error}", flush=True)
            time.sleep(5)


producer = create_kafka_producer()


# ============================================================
# Allocation Helpers
# ============================================================

def allocate_counts(total, number_of_groups):
    """
    Equal distribution.
    Kept as helper for distributing meters inside distributors.
    """
    base = total // number_of_groups
    remainder = total % number_of_groups

    counts = [base] * number_of_groups

    for i in range(remainder):
        counts[i] += 1

    return counts


def allocate_weighted_counts(total, weights):
    """
    Weighted distribution.
    Example:
        total = 91000
        weights = [1.35, 1.30, 1.25, ...]
    Higher weight => more meters/distributors.
    """

    weight_sum = sum(weights)

    counts = [
        int((weight / weight_sum) * total)
        for weight in weights
    ]

    difference = total - sum(counts)

    # Adjust rounding difference while keeping total exact.
    if difference > 0:
        for i in range(difference):
            index = i % len(counts)
            counts[index] += 1

    elif difference < 0:
        for i in range(abs(difference)):
            index = i % len(counts)
            if counts[index] > 1:
                counts[index] -= 1

    return counts


# ============================================================
# Infrastructure Generation
# ============================================================

def build_infrastructure():
    """
    Build:
        - 18 districts
        - 18 concentrators
        - 681 distributors distributed by district weight
        - 91,000 smart meters distributed by district weight
    """

    district_weights = [
        district["meter_weight"]
        for district in DISTRICTS
    ]

    distributor_counts = allocate_weighted_counts(
        TOTAL_DISTRIBUTORS,
        district_weights
    )

    meter_counts_by_district = allocate_weighted_counts(
        TOTAL_METERS,
        district_weights
    )

    infrastructure = []

    global_distributor_index = 1
    global_meter_index = 1

    for district_index, district_info in enumerate(DISTRICTS):
        district_name = district_info["district"]
        zone_type = district_info["zone_type"]

        concentrator_id = f"CONC-TET-{district_index + 1:02d}"

        district_distributor_count = distributor_counts[district_index]
        district_meter_count = meter_counts_by_district[district_index]

        meters_per_distributor = allocate_counts(
            district_meter_count,
            district_distributor_count,
        )

        distributors = []

        for local_distributor_index in range(district_distributor_count):
            distributor_id = f"DIST-TET-{global_distributor_index:04d}"
            meter_count = meters_per_distributor[local_distributor_index]

            meter_ids = [
                f"SM-TET-{global_meter_index + i:06d}"
                for i in range(meter_count)
            ]

            global_meter_index += meter_count
            global_distributor_index += 1

            distributors.append({
                "distributor_id": distributor_id,
                "meter_ids": meter_ids,
            })

        infrastructure.append({
            "district": district_name,
            "latitude": district_info["latitude"],
            "longitude": district_info["longitude"],
            "zone_type": zone_type,
            "meter_weight": district_info["meter_weight"],
            "concentrator_id": concentrator_id,
            "distributors": distributors,
        })

    return infrastructure


INFRASTRUCTURE = build_infrastructure()


# ============================================================
# Energy Consumption Profile
# ============================================================

def get_hour_factor(zone_type, hour):
    """
    Realistic load profile:
        Residential:
            morning and evening peaks
        Commercial:
            day and evening peaks
        Infrastructure:
            stable load, higher during working hours
    """

    if zone_type == "Residential":
        if 6 <= hour < 9:
            return 1.25
        if 18 <= hour < 23:
            return 1.75
        if 0 <= hour < 5:
            return 0.65
        return 1.0

    if zone_type == "Commercial":
        if 9 <= hour < 17:
            return 1.65
        if 17 <= hour < 22:
            return 1.85
        if 0 <= hour < 6:
            return 0.45
        return 0.95

    if zone_type == "Infrastructure":
        if 8 <= hour < 18:
            return 1.35
        if 18 <= hour < 23:
            return 1.10
        if 0 <= hour < 5:
            return 0.70
        return 1.0

    return 1.0


def get_district_factor(district_name):
    """
    Adds realistic differences between districts even if they have the same zone type.
    """

    district_factors = {
        "Medina": 1.25,
        "Ensanche": 1.20,
        "Saniat Rmel": 1.15,
        "Touilaa": 1.05,
        "Touta": 0.95,
        "Ziana": 0.90,
        "Mhannech": 1.12,
        "Barrio Malaga": 0.98,
        "Dersa": 1.08,
        "Semsa": 0.85,
        "Touibla": 0.88,
        "Taffaline": 0.82,
        "El Matar": 1.10,
        "Souani": 0.96,
        "Ain Melloul": 0.90,
        "Wilaya": 1.18,
        "Coelma": 0.92,
        "Boujarrah": 1.00,
    }

    return district_factors.get(district_name, 1.0)


def generate_meter_consumption(zone_type, district_name, timestamp):
    hour = timestamp.hour

    hour_factor = get_hour_factor(zone_type, hour)
    district_factor = get_district_factor(district_name)

    if zone_type == "Residential":
        base_kwh = random.uniform(0.04, 0.30)

    elif zone_type == "Commercial":
        base_kwh = random.uniform(0.10, 0.70)

    elif zone_type == "Infrastructure":
        base_kwh = random.uniform(0.08, 0.45)

    else:
        base_kwh = random.uniform(0.05, 0.40)

    energy_consumption = (
        base_kwh
        * hour_factor
        * district_factor
        * random.uniform(0.85, 1.20)
    )

    # Energy is for a 15-minute interval.
    # power_kw = energy_kwh / 0.25h
    power_kw = energy_consumption / 0.25

    voltage = random.normalvariate(220, 5)
    status = "normal"

    # Voltage drop simulation
    if random.random() < 0.015:
        voltage = random.uniform(175, 198)
        status = "voltage_drop"

    # Overload simulation
    if random.random() < 0.01:
        energy_consumption *= random.uniform(1.8, 2.8)
        power_kw = energy_consumption / 0.25
        status = "overload"

    # current = power / voltage
    current = (power_kw * 1000) / max(voltage, 1)

    return (
        round(energy_consumption, 4),
        round(voltage, 2),
        round(current, 2),
        status,
    )


# ============================================================
# Kafka Helpers
# ============================================================

def send_to_kafka(topic, data):
    producer.send(topic, data)


def flush_kafka():
    producer.flush()


# ============================================================
# Level 1: Smart Meters -> Distributors
# ============================================================

def simulate_meter_to_distributor(
    cycle_id,
    meter_phase_start_time,
    meter_phase_end_time,
):
    print("\nLEVEL 1: Smart Meters -> Distributors", flush=True)
    print(f"Meter phase duration: {METER_PHASE_SECONDS} seconds", flush=True)

    distributor_buffer = {}

    total_sent = 0
    total_meters = TOTAL_METERS

    available_duration = meter_phase_end_time - meter_phase_start_time

    for district in INFRASTRUCTURE:
        district_name = district["district"]
        zone_type = district["zone_type"]
        concentrator_id = district["concentrator_id"]

        for distributor in district["distributors"]:
            distributor_id = distributor["distributor_id"]

            distributor_buffer[distributor_id] = {
                "cycle_id": cycle_id,
                "distributor_id": distributor_id,
                "district": district_name,
                "zone_type": zone_type,
                "concentrator_id": concentrator_id,
                "latitude": district["latitude"],
                "longitude": district["longitude"],
                "readings": [],
            }

            for meter_id in distributor["meter_ids"]:
                now = datetime.now(timezone.utc)

                energy_consumption, voltage, current, status = generate_meter_consumption(
                    zone_type=zone_type,
                    district_name=district_name,
                    timestamp=now,
                )

                meter_event = {
                    "level": "meter",
                    "cycle_id": cycle_id,
                    "city": "Tetouan",
                    "meter_id": meter_id,
                    "distributor_id": distributor_id,
                    "concentrator_id": concentrator_id,
                    "district": district_name,
                    "zone_type": zone_type,
                    "latitude": district["latitude"],
                    "longitude": district["longitude"],
                    "energy_consumption": energy_consumption,
                    "voltage": voltage,
                    "current": current,
                    "status": status,
                    "timestamp": now.isoformat(),
                }

                send_to_kafka(TOPIC_METERS, meter_event)
                distributor_buffer[distributor_id]["readings"].append(meter_event)

                total_sent += 1

                if total_sent % 5000 == 0:
                    print(
                        f"Sent {total_sent}/{total_meters} meter readings",
                        flush=True,
                    )
                    flush_kafka()

                expected_time = meter_phase_start_time + (
                    total_sent / total_meters
                ) * available_duration

                now_time = time.time()

                if expected_time > now_time:
                    time.sleep(expected_time - now_time)

    flush_kafka()

    print(f"LEVEL 1 completed: {total_sent} readings sent", flush=True)

    return distributor_buffer


# ============================================================
# Level 2: Distributors -> Concentrators
# ============================================================

def aggregate_distributors(cycle_id, distributor_buffer):
    print("\nLEVEL 2: Distributors -> Concentrators", flush=True)

    concentrator_buffer = defaultdict(list)

    for distributor_id, distributor_data in distributor_buffer.items():
        readings = distributor_data["readings"]

        if not readings:
            continue

        total_energy = sum(r["energy_consumption"] for r in readings)
        avg_voltage = sum(r["voltage"] for r in readings) / len(readings)
        avg_current = sum(r["current"] for r in readings) / len(readings)
        total_meters = len(readings)

        overload_count = sum(1 for r in readings if r["status"] == "overload")
        voltage_drop_count = sum(1 for r in readings if r["status"] == "voltage_drop")

        distributor_event = {
            "level": "distributor",
            "cycle_id": cycle_id,
            "city": "Tetouan",
            "distributor_id": distributor_id,
            "concentrator_id": distributor_data["concentrator_id"],
            "district": distributor_data["district"],
            "zone_type": distributor_data["zone_type"],
            "latitude": distributor_data["latitude"],
            "longitude": distributor_data["longitude"],
            "total_energy_consumption": round(total_energy, 4),
            "avg_voltage": round(avg_voltage, 2),
            "avg_current": round(avg_current, 2),
            "total_meters": total_meters,
            "overload_count": overload_count,
            "voltage_drop_count": voltage_drop_count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        send_to_kafka(TOPIC_DISTRIBUTORS, distributor_event)

        concentrator_buffer[
            distributor_data["concentrator_id"]
        ].append(distributor_event)

    flush_kafka()

    print(
        f"LEVEL 2 completed: {len(distributor_buffer)} distributor events sent",
        flush=True,
    )

    return concentrator_buffer


# ============================================================
# Level 3: Concentrators -> Central Processing
# ============================================================

def estimate_district_energy_threshold(zone_type, total_meters, district_name):
    district_factor = get_district_factor(district_name)

    if zone_type == "Residential":
        threshold_per_meter = 0.55

    elif zone_type == "Commercial":
        threshold_per_meter = 0.95

    elif zone_type == "Infrastructure":
        threshold_per_meter = 0.75

    else:
        threshold_per_meter = 0.75

    return total_meters * threshold_per_meter * district_factor


def aggregate_concentrators(cycle_id, concentrator_buffer):
    print("\nLEVEL 3: Concentrators -> Central Processing", flush=True)

    sent_count = 0

    for concentrator_id, distributor_events in concentrator_buffer.items():
        if not distributor_events:
            continue

        district = distributor_events[0]["district"]
        zone_type = distributor_events[0]["zone_type"]
        latitude = distributor_events[0]["latitude"]
        longitude = distributor_events[0]["longitude"]

        total_energy = sum(e["total_energy_consumption"] for e in distributor_events)
        total_meters = sum(e["total_meters"] for e in distributor_events)
        total_distributors = len(distributor_events)

        avg_voltage = (
            sum(e["avg_voltage"] * e["total_meters"] for e in distributor_events)
            / total_meters
        )

        avg_current = (
            sum(e["avg_current"] * e["total_meters"] for e in distributor_events)
            / total_meters
        )

        overload_count = sum(e["overload_count"] for e in distributor_events)
        voltage_drop_count = sum(e["voltage_drop_count"] for e in distributor_events)

        district_status = "normal"

        if voltage_drop_count > total_meters * 0.02:
            district_status = "voltage_risk"

        if overload_count > total_meters * 0.02:
            district_status = "overload_risk"

        if total_energy > estimate_district_energy_threshold(
            zone_type,
            total_meters,
            district,
        ):
            district_status = "saturation_risk"

        concentrator_event = {
            "level": "concentrator",
            "cycle_id": cycle_id,
            "city": "Tetouan",
            "concentrator_id": concentrator_id,
            "district": district,
            "zone_type": zone_type,
            "latitude": latitude,
            "longitude": longitude,
            "total_energy_consumption": round(total_energy, 4),
            "avg_voltage": round(avg_voltage, 2),
            "avg_current": round(avg_current, 2),
            "total_meters": total_meters,
            "total_distributors": total_distributors,
            "overload_count": overload_count,
            "voltage_drop_count": voltage_drop_count,
            "district_status": district_status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        send_to_kafka(TOPIC_CONCENTRATORS, concentrator_event)
        sent_count += 1

    flush_kafka()

    print(f"LEVEL 3 completed: {sent_count} district events sent", flush=True)


# ============================================================
# Infrastructure Summary
# ============================================================

def print_infrastructure_summary():
    print("\n================ TETOUAN ENERGY INFRASTRUCTURE ================", flush=True)
    print(f"Districts: {len(INFRASTRUCTURE)}", flush=True)
    print(f"Smart meters: {TOTAL_METERS}", flush=True)
    print(f"Distributors: {TOTAL_DISTRIBUTORS}", flush=True)
    print(f"Concentrators: {len(INFRASTRUCTURE)}", flush=True)
    print(f"Cycle duration: {CYCLE_DURATION_SECONDS} seconds", flush=True)
    print(f"Meter phase duration: {METER_PHASE_SECONDS} seconds", flush=True)
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVER}", flush=True)
    print("================================================================", flush=True)

    total_meters_check = 0
    total_distributors_check = 0

    for district in INFRASTRUCTURE:
        meter_count = sum(len(d["meter_ids"]) for d in district["distributors"])
        distributor_count = len(district["distributors"])

        total_meters_check += meter_count
        total_distributors_check += distributor_count

        print(
            f"{district['district']} | "
            f"type={district['zone_type']} | "
            f"weight={district['meter_weight']} | "
            f"meters={meter_count} | "
            f"distributors={distributor_count} | "
            f"concentrator={district['concentrator_id']}",
            flush=True,
        )

    print("----------------------------------------------------------------", flush=True)
    print(f"Total meters check: {total_meters_check}", flush=True)
    print(f"Total distributors check: {total_distributors_check}", flush=True)
    print("================================================================", flush=True)


# ============================================================
# Main Loop: Exact 15-Minute Cycle
# ============================================================

def run_simulation():
    print_infrastructure_summary()

    if METER_PHASE_SECONDS >= CYCLE_DURATION_SECONDS:
        raise ValueError(
            "METER_PHASE_SECONDS must be smaller than CYCLE_DURATION_SECONDS. "
            "Example: CYCLE_DURATION_SECONDS=900 and METER_PHASE_SECONDS=840"
        )

    cycle_id = 1

    while True:
        cycle_start_time = time.time()
        cycle_end_time = cycle_start_time + CYCLE_DURATION_SECONDS

        meter_phase_start_time = cycle_start_time
        meter_phase_end_time = cycle_start_time + METER_PHASE_SECONDS

        cycle_start_datetime = datetime.now(timezone.utc)

        print("\n================================================", flush=True)
        print(f"START CYCLE {cycle_id}", flush=True)
        print(f"Cycle start UTC: {cycle_start_datetime.isoformat()}", flush=True)
        print(f"Cycle duration: {CYCLE_DURATION_SECONDS} seconds", flush=True)
        print("================================================", flush=True)

        # 00:00 -> 14:00
        distributor_buffer = simulate_meter_to_distributor(
            cycle_id=cycle_id,
            meter_phase_start_time=meter_phase_start_time,
            meter_phase_end_time=meter_phase_end_time,
        )

        # 14:00 -> 15:00
        concentrator_buffer = aggregate_distributors(
            cycle_id=cycle_id,
            distributor_buffer=distributor_buffer,
        )

        aggregate_concentrators(
            cycle_id=cycle_id,
            concentrator_buffer=concentrator_buffer,
        )

        elapsed = time.time() - cycle_start_time
        remaining_time = cycle_end_time - time.time()

        if remaining_time > 0:
            print(
                f"Cycle {cycle_id} processing finished in "
                f"{round(elapsed, 2)} seconds",
                flush=True,
            )
            print(
                f"Waiting {round(remaining_time, 2)} seconds "
                f"to complete exactly 15 minutes",
                flush=True,
            )
            time.sleep(remaining_time)

        else:
            print(
                f"WARNING: Cycle {cycle_id} exceeded 15 minutes by "
                f"{round(abs(remaining_time), 2)} seconds",
                flush=True,
            )

        final_elapsed = time.time() - cycle_start_time

        print("================================================", flush=True)
        print(f"END CYCLE {cycle_id}", flush=True)
        print(f"Final cycle duration: {round(final_elapsed, 2)} seconds", flush=True)
        print("================================================", flush=True)

        cycle_id += 1


if __name__ == "__main__":
    try:
        run_simulation()

    except KeyboardInterrupt:
        print("Simulation stopped by user", flush=True)
        producer.flush()
        producer.close()
