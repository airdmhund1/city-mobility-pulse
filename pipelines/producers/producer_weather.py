import os, json, time, datetime, math
from confluent_kafka import Producer

BROKERS = os.getenv("REDPANDA_BROKERS","redpanda:9092")
TOPIC   = os.getenv("KAFKA_TOPIC_WEATHER","weather")
p = Producer({"bootstrap.servers": BROKERS})

def now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"

t = 0.0
while True:
    # toy sinusoid temp + sporadic precip
    temp = 12 + 8*math.sin(t/60.0)
    precip = 0.0 if int(t) % 97 else 3.0
    payload = {"temp_c": round(temp,2), "precip_mm": precip, "event_time": now_iso()}
    p.produce(TOPIC, json.dumps(payload).encode("utf-8"))
    p.poll(0)
    t += 1
    time.sleep(1.0)
