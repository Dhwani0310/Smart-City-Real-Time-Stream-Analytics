import json                                          # to read the config file
from kafka import KafkaConsumer, TopicPartition      # the consumer class, and a helper that names one partition

with open("config/student_config.json") as f:        # open the config so the topic name is not hardcoded
    cfg = json.load(f)
TOPIC = cfg["topic_name"]

consumer = KafkaConsumer(bootstrap_servers="localhost:9092")   # no group_id, so no group joining (the step that stalls)
parts = [TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)]   # the 2 lanes of your topic
consumer.assign(parts)                               # pick the lanes ourselves instead of asking Kafka to assign them
consumer.seek_to_end(*parts)                         # jump to the newest message, so we only show live events

print(f"Watching {TOPIC} on partitions 0 and 1...", flush=True)
for msg in consumer:
    key = msg.key.decode("utf-8") if msg.key is not None else "None"
    value = msg.value.decode("utf-8") if msg.value is not None else "None"

    print(
        f"P{msg.partition} off={msg.offset} key={key} {value}",
        flush=True
    )