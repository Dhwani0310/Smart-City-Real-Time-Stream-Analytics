
import json
import random
import sys
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Load project configuration
with open("config/student_config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

TOPIC = cfg["topic_name"]
STUDENT = cfg["student_id"]
LIMIT = cfg["temperature_threshold"]

# Default event count is 2000; an optional command-line argument overrides it.
TOTAL = int(sys.argv[1]) if len(sys.argv) > 1 else 2000

producer = KafkaProducer(bootstrap_servers="localhost:9092")

# Generate 12 sensors distributed across four zones.
zones = {
    f"S{i:02d}": f"ZONE_{'ABCD'[i % 4]}"
    for i in range(1, 13)
}

# Store previous events for duplicate generation.
sent = []

# Simulated event-time clock.
clock = datetime.now()

for n in range(1, TOTAL + 1):

    time.sleep(0.05)
    clock += timedelta(seconds=1)

    sensor = random.choice(list(zones))
    temp = round(random.gauss(30, 4), 1)

    # Generate an abnormal temperature reading with 10% probability.
    if random.random() < 0.10:
        temp = round(random.uniform(LIMIT + 1, LIMIT + 15), 1)

    event = {
        "event_id": f"EVT_{n}",
        "sensor_id": sensor,
        "zone_id": zones[sensor],
        "event_time": clock.strftime("%Y-%m-%dT%H:%M:%S"),
        "temperature": temp,
        "humidity": round(random.uniform(20, 70), 1),
        "traffic_count": random.randint(20, 200),
        "status": random.choice(["ACTIVE"] * 9 + ["FAULTY"]),
        "student_id": STUDENT
    }

    # Inject different data-quality scenarios.
    r = random.random()

    if r < 0.05 and sent:
        # Duplicate an earlier event.
        event = dict(random.choice(sent))

    elif r < 0.08:
        # Missing temperature field.
        del event["temperature"]

    elif r < 0.11:
        # Invalid timestamp.
        event["event_time"] = "not-a-time"

    elif r < 0.14:
        # Malformed JSON.
        producer.send(
            TOPIC,
            key=sensor.encode("utf-8"),
            value=b"{broken json"
        )
        continue

    elif r < 0.22:
        # Out-of-order event: 1-5 minutes in the past.
        event["event_time"] = (
            clock - timedelta(minutes=random.randint(1, 5))
        ).strftime("%Y-%m-%dT%H:%M:%S")

    elif r < 0.24:
        # Very late event: 20-30 minutes in the past.
        event["event_time"] = (
            clock - timedelta(minutes=random.randint(20, 30))
        ).strftime("%Y-%m-%dT%H:%M:%S")

    sent.append(event)

    # Use the sensor ID from the actual event as its Kafka key.
    producer.send(
        TOPIC,
        key=event["sensor_id"].encode("utf-8"),
        value=json.dumps(event).encode("utf-8")
    )

    if n % 100 == 0:
        print(f"[{STUDENT}] sent {n} events", flush=True)

producer.flush()
producer.close()

print(f"[{STUDENT}] done, {TOTAL} events published")
