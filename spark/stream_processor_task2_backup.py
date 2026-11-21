import json                                              # to read the config file
import os                                                # for creating the output folder
from pyspark.sql import SparkSession                     # entry point to Spark
from pyspark.sql.functions import col, from_json, expr, when, lit, try_to_timestamp
                                                         # col = refer to a column; from_json = parse JSON text;
                                                         # expr = write a SQL condition; when = if/else for columns;
                                                         # lit = a fixed value; try_to_timestamp = convert text to time, null on failure
from pyspark.sql.types import (StructType, StructField,  # building blocks to describe the expected fields
                               StringType, DoubleType, IntegerType)

with open("config/student_config.json") as f:            # same config file as the producer
    cfg = json.load(f)
TOPIC = cfg["topic_name"]                                # never hardcode the topic
STUDENT = cfg["student_id"]                              # printed in every batch line as the student marker

spark = (SparkSession.builder
         .appName("stream_processor")                    # name shown in the Spark UI
         .master("local[2]")                             # run locally with 2 CPU threads
         .config("spark.sql.shuffle.partitions", "4")    # fewer internal partitions = faster on a laptop
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")                   # hide the noisy INFO logs

# The explicit schema: one StructField(name, type, nullable) per JSON field.
# event_time stays a STRING for now; we convert it to a real timestamp below,
# so a bad value like "not-a-time" becomes null there instead of breaking the parse.
schema = StructType([
    StructField("event_id", StringType(), True),         # True = this field is allowed to be null
    StructField("sensor_id", StringType(), True),
    StructField("zone_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("traffic_count", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("student_id", StringType(), True),
])

raw = (spark.readStream.format("kafka")                  # readStream = continuous read, not a one-time read
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")            # start from the first message
       .option("maxOffsetsPerTrigger", 500)              # at most 500 messages per micro-batch, so we see many batches
       .load())

text = raw.selectExpr("CAST(value AS STRING) AS raw_json")   # bytes -> text, keep the original as raw_json

parsed = text.select(                                    # try to fit each text into the schema
    col("raw_json"),
    from_json(col("raw_json"), schema).alias("e"))       # "e" is a struct holding all parsed fields

REQUIRED = ["event_id", "sensor_id", "zone_id", "event_time",
            "temperature", "humidity", "traffic_count", "status"]   # fields every valid record must have
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"                      # the exact timestamp format the producer uses
missing_cond = " OR ".join(f"{c} IS NULL" for c in REQUIRED)   # builds: event_id IS NULL OR sensor_id IS NULL OR ...

os.makedirs("output/quarantine", exist_ok=True)          # create the quarantine folder (no error if it exists)
QUARANTINE_FILE = "output/quarantine/rejected.jsonl"
open(QUARANTINE_FILE, "w").close()                       # "w" empties the file at start, so reruns don't pile up duplicates

# json_failed = True when nothing at all could be read from the JSON text.
# We test four key fields instead of the whole struct, so it works whether from_json
# gives back a null struct or a struct full of nulls for broken JSON.
flat = parsed.select(
    "raw_json",
    (col("e.event_id").isNull() & col("e.sensor_id").isNull() &
     col("e.zone_id").isNull() & col("e.event_time").isNull()).alias("json_failed"),
    "e.*")                                               # "e.*" spreads the struct into separate columns

checked = (flat
    .withColumn("ts", try_to_timestamp(col("event_time"), lit(TS_FORMAT)))   # real timestamp, or null if unparseable
    .withColumn("reason",                                # why a record is rejected (null = valid)
        when(col("json_failed"), "malformed_json")       # 1st check: could not parse at all
        .when(expr(missing_cond), "missing_field")       # 2nd check: some required field is null
        .when(col("ts").isNull(), "bad_timestamp")       # 3rd check: event_time is not a real time
        .otherwise(None)))                               # passed all checks

def process_batch(batch_df, batch_id):                   # Spark calls this once per micro-batch
    batch_df.persist()                                   # keep it in memory, we use it several times below
    valid = batch_df.filter(col("reason").isNull())      # records that passed
    rejected = batch_df.filter(col("reason").isNotNull())   # records that failed
    n_valid = valid.count()
    n_rejected = rejected.count()
    reasons = {r["reason"]: r["count"]                   # e.g. {'missing_field': 14, 'bad_timestamp': 12}
               for r in rejected.groupBy("reason").count().collect()}
    print(f"[{STUDENT}] batch {batch_id}: valid={n_valid} rejected={n_rejected} reasons={reasons}", flush=True)
    valid.select("event_id", "sensor_id", "zone_id", "ts", "temperature").show(5, truncate=False)   # sample of parsed records

    rows = (rejected.select("raw_json", "reason")
            .withColumn("batch_id", lit(batch_id))       # remember which batch rejected it
            .toJSON().collect())                         # each row becomes one JSON text line
    if rows:
        with open(QUARANTINE_FILE, "a", encoding="utf-8") as q:   # "a" = append to the end
            for line in rows:
                q.write(line + "\n")
    batch_df.unpersist()                                 # free the memory

query = (checked.writeStream
         .foreachBatch(process_batch)                    # run our function on every micro-batch
         .outputMode("append")
         .trigger(processingTime="5 seconds")            # one micro-batch every 5 seconds
         .start())

query.awaitTermination()                                 # keep running until Ctrl+C