import os
import json
import time
import random
import signal
from datetime import datetime, timezone
from confluent_kafka import Producer

# --- env / config ---
BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC = os.getenv("BIKE_TOPIC", "bike")
INTERVAL_SEC = float(os.getenv("BIKE_INTERVAL_SEC", "1.0"))

def now_iso_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# city -> (station_id, docks_total)  # fixed capacity per station
CITY_CFG = {
    "Boston":        ("S001", 28),
    "Cambridge":     ("S002", 24),
    "Somerville":    ("S003", 20),
    "Worcester":     ("S004", 26),
    "Springfield":   ("S005", 22),
    "Lowell":        ("S006", 20),
    "Quincy":        ("S007", 18),
    "Newton":        ("S008", 20),
    "Brockton":      ("S009", 18),
    "Lynn":          ("S010", 16),
    "Fall River":    ("S011", 22),
    "New Bedford":   ("S012", 22),
    "Framingham":    ("S013", 18),
    "Waltham":       ("S014", 18),
    "Haverhill":     ("S015", 16),
    "Malden":        ("S016", 16),
    "Medford":       ("S017", 18),
    "Taunton":       ("S018", 16),
    "Chicopee":      ("S019", 16),
    "Everett":       ("S020", 14),
    "Revere":        ("S021", 16),
    "Peabody":       ("S022", 16),
    "Arlington":     ("S023", 14),
    "Attleboro":     ("S024", 14),
    "Methuen":       ("S025", 14),
    "Barnstable":    ("S026", 12),
    "Pittsfield":    ("S027", 12),
    "Leominster":    ("S028", 12),
    "Westfield":     ("S029", 12),
    "Salem":         ("S030", 16),
}
CITIES = list(CITY_CFG.keys())

_running = True
def _handle_stop(*_):
    global _running
    _running = False

def _delivery(err, msg):
    if err:
        print(f"delivery failed: {err}")
    else:
        # comment to quiet logs
        print(f"delivered {msg.topic()}[{msg.partition()}]@{msg.offset()}")

def main():
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    p = Producer({
        "bootstrap.servers": BROKER,
        "linger.ms": 5,
        "batch.num.messages": 1000,
        "queue.buffering.max.messages": 100000,
    })

    while _running:
        city = random.choice(CITIES)
        station_id, docks_total = CITY_CFG[city]

        # choose how many bikes are physically at the station
        bikes_available = random.randint(0, docks_total)

        # bikes occupying docks right now (taken/parked)
        bikes_occupied = docks_total - bikes_available

        # docks that are empty and ready to accept a bike
        docks_available = docks_total - bikes_occupied  # == bikes_available

        payload = {
            "bike_city": city,
            "station_id": station_id,
            "docks_total": docks_total,
            "bikes_available": bikes_available,
            "bikes_occupied": bikes_occupied,   # a.k.a. bikes_taken
            "docks_available": docks_available,
            "event_time": now_iso_z(),
        }

        p.produce(
            TOPIC,
            key=station_id.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            on_delivery=_delivery,
        )
        p.poll(0)

        print(f"produced -> {payload}", flush=True)
        time.sleep(INTERVAL_SEC)

    print("flushing…")
    p.flush(10)

if __name__ == "__main__":
    main()