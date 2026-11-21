import json                                            # to read the config file
from pyspark.sql import SparkSession                   # the entry point to everything in Spark

with open("config/student_config.json") as f:          # same config file as the producer
    cfg = json.load(f)
TOPIC = cfg["topic_name"]                              # never hardcode the topic

spark = (SparkSession.builder                          # start building a Spark session
         .appName("kafka_smoke_test")                  # a name that shows up in Spark's UI
         .master("local[2]")                           # run on this machine using 2 CPU threads
         .getOrCreate())                               # create it (or reuse an existing one)
spark.sparkContext.setLogLevel("WARN")                 # hide the noisy INFO logs

raw = (spark.read.format("kafka")                      # spark.read = one-time (batch) read of the topic
       .option("kafka.bootstrap.servers", "localhost:9092")   # where Kafka is listening
       .option("subscribe", TOPIC)                     # which topic to read
       .option("startingOffsets", "earliest")          # start from the first message
       .load())                                        # run it: gives a table with key, value, partition, offset...

print("Total messages in topic:", raw.count())         # count every message

raw.selectExpr(                                        # selectExpr lets us write small SQL expressions
    "CAST(key AS STRING) AS key",                      # Kafka stores bytes, so cast to text
    "CAST(value AS STRING) AS value",
    "partition", "offset"
).show(5, truncate=False)                              # print 5 rows without cutting the text short

spark.stop()                                           # shut Spark down cleanly