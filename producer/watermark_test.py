
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

with open("config/student_config.json") as f:
    cfg = json.load(f)

producer = KafkaProducer(bootstrap_servers="localhost:9092")

TOPIC = cfg["topic_name"]
STUDENT = cfg["student_id"]

# Start with the current event time; TEST 3 advances it by 20 minutes.
test_time = datetime.now().replace(microsecond=0)


def make_event(event_id, event_time):
    return {
        "event_id": event_id,
        "sensor_id": "S01",
        "zone_id": "ZONE_B",
        "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "temperature": 45.0,
        "humidity": 55.0,
        "traffic_count": 150,
        "status": "ACTIVE",
        "student_id": STUDENT
    }


original = make_event("WATERMARK_TEST_001", test_time)

print("TEST 1: Sending original event")
producer.send(TOPIC, value=json.dumps(original).encode())
producer.flush()
time.sleep(15)

print("TEST 2: Sending duplicate within watermark period")
producer.send(TOPIC, value=json.dumps(original).encode())
producer.flush()
time.sleep(15)

print("TEST 3: Advancing event-time watermark")
advance = make_event(
    "WATERMARK_ADVANCE_001",
    test_time + timedelta(minutes=20)
)
producer.send(TOPIC, value=json.dumps(advance).encode())
producer.flush()
time.sleep(15)

print("TEST 4: Sending original event again with old timestamp")
producer.send(TOPIC, value=json.dumps(original).encode())
producer.flush()
time.sleep(15)

print("All controlled watermark tests have been sent.")
producer.close()
