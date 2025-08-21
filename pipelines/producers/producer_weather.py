# producer_weather.py
"""
Synthetic weather producer for the Redpanda/Kafka "weather" topic.

It picks a random station (kept in sync with the bike producer), jitters a few
weather fields around city-specific baselines, and pushes one JSON message per
tick. Environment variables let you point it at a different broker/topic and
control how fast it emits.
"""

import json
import os
import random
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

# --- config (env) ---
# Broker, topic and cadence come from the environment so this can run the same
# way locally and in a container.
BROKER = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC_WEATHER", "weather")
INTERVAL_SEC = float(os.getenv("WEATHER_INTERVAL_SEC", "1.0"))


def now_iso_z() -> str:
    """Current UTC time in ISO-8601 with a 'Z' suffix (e.g., 2025-08-20T12:34:56Z)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- station set (MATCHES BIKE) ---
# If the bike mapping changes, mirror it here so joins are straightforward.
STATIONS = [
    ("S001", "Boston"),
    ("S002", "Cambridge"),
    ("S003", "Somerville"),
    ("S004", "Worcester"),
    ("S005", "Springfield"),
    ("S006", "Lowell"),
    ("S007", "Quincy"),
    ("S008", "Newton"),
    ("S009", "Brockton"),
    ("S010", "Lynn"),
    ("S011", "Fall River"),
    ("S012", "New Bedford"),
    ("S013", "Framingham"),
    ("S014", "Waltham"),
    ("S015", "Haverhill"),
    ("S016", "Malden"),
    ("S017", "Medford"),
    ("S018", "Taunton"),
    ("S019", "Chicopee"),
    ("S020", "Everett"),
    ("S021", "Revere"),
    ("S022", "Peabody"),
    ("S023", "Arlington"),
    ("S024", "Attleboro"),
    ("S025", "Methuen"),
    ("S026", "Barnstable"),
    ("S027", "Pittsfield"),
    ("S028", "Leominster"),
    ("S029", "Westfield"),
    ("S030", "Salem"),
]

# City-specific baselines: temp_c, wind_kph, humidity%, pressure_mb.
BASELINES = {
    "S001": (18.0, 14.0, 70, 1015),  # Boston
    "S002": (17.5, 12.0, 68, 1015),
    "S003": (17.5, 12.0, 66, 1015),
    "S004": (16.0, 10.0, 72, 1013),
    "S005": (17.0, 9.0, 67, 1014),
    "S006": (17.0, 10.0, 66, 1014),
    "S007": (17.5, 13.0, 71, 1015),
    "S008": (17.2, 11.0, 67, 1015),
    "S009": (17.0, 11.0, 69, 1014),
    "S010": (17.8, 14.0, 73, 1015),
    "S011": (17.2, 13.0, 74, 1015),
    "S012": (17.0, 14.0, 75, 1015),
    "S013": (16.8, 10.0, 68, 1014),
    "S014": (16.9, 10.0, 67, 1015),
    "S015": (16.6, 10.0, 66, 1014),
    "S016": (17.2, 12.0, 69, 1015),
    "S017": (17.1, 11.0, 68, 1015),
    "S018": (16.9, 11.0, 70, 1015),
    "S019": (16.5, 9.0, 67, 1014),
    "S020": (17.2, 12.0, 70, 1015),
    "S021": (17.8, 15.0, 74, 1015),
    "S022": (17.0, 12.0, 69, 1015),
    "S023": (16.9, 11.0, 68, 1015),
    "S024": (16.8, 11.0, 69, 1015),
    "S025": (16.5, 10.0, 66, 1014),
    "S026": (17.0, 17.0, 76, 1016),
    "S027": (15.0, 9.0, 72, 1012),
    "S028": (16.2, 9.0, 68, 1013),
    "S029": (16.4, 9.0, 67, 1014),
    "S030": (17.6, 13.0, 72, 1015),
}


def classify_condition(
    temp_c: float,
    precip_mm: float,
    wind_kph: float,
    humidity: int,
) -> str:
    """A tiny rule-based label that reads nicely on a dashboard."""
    if precip_mm >= 2.0:
        return "Rain"
    if precip_mm > 0.0:
        return "Light rain"
    if wind_kph >= 30:
        return "Windy"
    if humidity >= 85 and temp_c < 20:
        return "Cloudy"
    return "Clear"


_running = True  # flipped to False when we get SIGINT/SIGTERM


def _handle_stop(*_) -> None:
    """Signal handler to stop the loop cleanly."""
    global _running
    _running = False


def _delivery(err, msg) -> None:
    """Print a short delivery report; handy when testing locally."""
    if err:
        print(f"delivery failed: {err}")
    else:
        # comment this if you want quieter logs
        print(f"delivered {msg.topic()}[{msg.partition()}]@{msg.offset()}")


def main() -> None:
    """Wire up the Kafka producer and emit one record per interval."""
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    p = Producer(
        {
            "bootstrap.servers": BROKER,
            "linger.ms": 5,
            "batch.num.messages": 1000,
            "queue.buffering.max.messages": 100000,
        }
    )

    rng = random.Random()

    while _running:
        station_id, city = rng.choice(STATIONS)
        base_t, base_w, base_h, base_p = BASELINES[station_id]

        # small, realistic jitter around each baseline
        temp_c = round(base_t + rng.uniform(-4.0, 4.0), 1)
        precip_mm = round(max(0.0, rng.gauss(0.2, 0.6)), 1)
        if rng.random() < 0.65:
            precip_mm = 0.0  # most ticks are dry

        wind_kph = round(max(0.0, base_w + rng.uniform(-6, 10)), 1)
        humidity = int(min(100, max(25, base_h + rng.randint(-10, 12))))
        pressure_mb = int(base_p + rng.randint(-8, 8))

        condition = classify_condition(temp_c, precip_mm, wind_kph, humidity)

        value = {
            "station_id": station_id,
            "weather_city": city,
            "temp_c": temp_c,
            "precip_mm": precip_mm,
            "humidity": humidity,
            "wind_kph": wind_kph,
            "pressure_mb": pressure_mb,
            "condition": condition,
            "event_time": now_iso_z(),
        }

        p.produce(
            TOPIC,
            key=station_id.encode("utf-8"),
            value=json.dumps(value).encode("utf-8"),
            on_delivery=_delivery,
        )
        p.poll(0)
        print(f"produced -> {value}", flush=True)

        time.sleep(INTERVAL_SEC)

    print("flushing…")
    p.flush(10)


if __name__ == "__main__":
    main()
