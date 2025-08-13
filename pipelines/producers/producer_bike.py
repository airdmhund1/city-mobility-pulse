import os, json, time, random, datetime
from confluent_kafka import Producer

BROKERS = os.getenv("REDPANDA_BROKERS","redpanda:9092")
TOPIC   = os.getenv("KAFKA_TOPIC_BIKE","bike_status")
p = Producer({"bootstrap.servers": BROKERS})

STATIONS = [f"S{str(i).zfill(3)}" for i in range(1, 31)]

def now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"

while True:
    st = random.choice(STATIONS)
    bikes = max(0, int(random.gauss(8, 3)))
    docks = max(0, 20 - bikes)
    payload = {
        "station_id": st,
        "bikes_available": bikes,
        "docks_available": docks,
        "event_time": now_iso()
    }
    p.produce(TOPIC, json.dumps(payload).encode("utf-8"))
    p.poll(0)
    time.sleep(0.2)
