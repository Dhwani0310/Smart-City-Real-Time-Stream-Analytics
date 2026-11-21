
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Load project configuration
with open("config/student_config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

# Kafka configuration
producer = KafkaProducer(bootstrap_servers="localhost:9092")
topic = cfg["topic_name"]
student = cfg["student_id"]

# Base event timestamp
base_time = datetime.now().replace(microsecond=0)

# Test readings for each personalized sensor.
# Thresholds are loaded from student_config.json.
test_cases = {
    "E10_SENSOR_A": {
        "zone": "ZONE_A",
        "temperatures": [43.0, 41.0, 44.0, 40.0, 45.0]
    },
    "E10_SENSOR_B": {
        "zone": "ZONE_B",
        "temperatures": [39.0, 37.0, 40.0, 36.0, 41.0]
    },
    "E10_SENSOR_C": {
        "zone": "ZONE_C",
        "temperatures": [41.0, 44.0, 43.0, 42.0, 40.0]
    }
}

event_number = 1

for sensor_id, details in test_cases.items():

    # Get the personalized threshold from the configuration
    threshold = cfg["sensor_thresholds"].get(
        sensor_id,
        cfg["temperature_threshold"]
    )

    print(f"\nTesting {sensor_id}")
    print(f"Personalized threshold: {threshold} C")

    for temperature in details["temperatures"]:

        event = {
            "event_id": f"PERSONAL_TEST_{event_number}",
            "sensor_id": sensor_id,
            "zone_id": details["zone"],
            "event_time": (
                base_time + timedelta(seconds=event_number)
            ).strftime("%Y-%m-%dT%H:%M:%S"),
            "temperature": temperature,
            "humidity": 55.0,
            "traffic_count": 100,
            "status": "ACTIVE",
            "student_id": student
        }

        # Publish event to Kafka
        producer.send(
            topic,
            value=json.dumps(event).encode("utf-8")
        )
        producer.flush()

        print(f"Sent {sensor_id}: {temperature} C")

        event_number += 1
        time.sleep(3)

# Close producer connection
producer.close()

print("\nAll personalized threshold test events sent.")
