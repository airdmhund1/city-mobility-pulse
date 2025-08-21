"""
Bike events producer.

Publishes synthetic bike availability for fixed MA stations to Kafka/Redpanda.
Keeps payload stable for downstream consumers—no schema changes here.
"""

from __future__ import annotations

import json
import os
import random
import signal
import time
from datetime import datetime, timezone
from typing import Dict, Tuple

from confluent_kafka import Producer

# --- env / config ---
BROKER: str = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC: str = os.getenv("BIKE_TOPIC", "bike")
INTERVAL_SEC: float = float(os.getenv("BIKE_INTERVAL_SEC", "1.0"))


def now_iso_z() -> str:
    """UTC ISO8601 timestamp with trailing Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# city -> (station_id, docks_total)  # fixed capacity per station
CITY_CFG: Dict[str, Tuple[str, int]] = {
    "Boston": ("S001", 28),
    "Cambridge": ("S002", 24),
    "Somerville": ("S003", 20),
    "Worcester": ("S004", 26),
    "Springfield": ("S005", 22),
    "Lowell": ("S006", 20),
    "Quincy": ("S007", 18),
    "Newton": ("S008", 20),
    "Brockton": ("S009", 18),
    "Lynn": ("S010", 16),
    "Fall River": ("S011", 22),
    "New Bedford": ("S012", 22),
    "Framingham": ("S013", 18),
    "Waltham": ("S014", 18),
    "Haverhill": ("S015", 16),
    "Malden": ("S016", 16),
    "Medford": ("S017", 18),
    "Taunton": ("S018", 16),
    "Chicopee": ("S019", 16),
    "Everett": ("S020", 14),
    "Revere": ("S021", 16),
    "Peabody": ("S022", 16),
    "Arlington": ("S023", 14),
    "Attleboro": ("S024", 14),
    "Methuen": ("S025", 14),
    "Barnstable": ("S026", 12),
    "Pittsfield": ("S027", 12),
    "Leominster": ("S028", 12),
    "Westfield": ("S029", 12),
    "Salem": ("S030", 16),
}
CITIES = list(CITY_CFG.keys())

_running = True


def _handle_stop(*_: object) -> None:
    """Graceful shutdown on SIGINT/SIGTERM."""
    global _running
    _running = False


def _delivery(err, msg) -> None:
    """Per-message delivery callback (quiet on success)."""
    if err:
        print(f"delivery failed: {err}")
    else:
        print(f"delivered {msg.topic()}[{msg.partition()}]@{msg.offset()}")


def main() -> None:
    """Produce bike messages until interrupted."""
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    producer = Producer(
        {
            "bootstrap.servers": BROKER,
            "linger.ms": 5,
            "batch.num.messages": 1000,
            "queue.buffering.max.messages": 100_000,
        }
    )

    while _running:
        city = random.choice(CITIES)
        station_id, docks_total = CITY_CFG[city]

        # Bikes currently available  or ready in racks.
        bikes_available = random.randint(0, docks_total)

        # Bikes in use or occupied by users now.
        bikes_occupied = docks_total - bikes_available

        # Empty docks ready to accept a bike (equals bikes_available).
        docks_available = docks_total - bikes_occupied

        payload = {
            "bike_city": city,
            "station_id": station_id,
            "docks_total": docks_total,
            "bikes_available": bikes_available,
            "bikes_occupied": bikes_occupied,  # aka bikes_taken
            "docks_available": docks_available,
            "event_time": now_iso_z(),
        }

        producer.produce(
            TOPIC,
            key=station_id.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            on_delivery=_delivery,
        )
        producer.poll(0)  # serve delivery callbacks

        print(f"produced -> {payload}", flush=True)
        time.sleep(INTERVAL_SEC)

    print("flushing…")
    producer.flush(10)


if __name__ == "__main__":
    main()
