
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, try_to_timestamp, lit,
    window, avg, max as spark_max, count
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)

# --------------------------------------------------
# 1. LOAD CONFIGURATION
# --------------------------------------------------

with open("config/student_config.json") as f:
    cfg = json.load(f)

TOPIC = cfg["topic_name"]
STUDENT = cfg["student_id"]
WATERMARK = cfg["watermark_delay"]

# --------------------------------------------------
# 2. CREATE SPARK SESSION
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Task3_Task4_EventTime_Deduplication")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print(f"STUDENT ID: {STUDENT}")
print(f"KAFKA TOPIC: {TOPIC}")
print(f"WATERMARK: {WATERMARK}")
print("TASK 3 AND TASK 4 STARTED")
print("=" * 60)

# --------------------------------------------------
# 3. DEFINE EXPLICIT JSON SCHEMA
# --------------------------------------------------

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("zone_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("traffic_count", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("student_id", StringType(), True)
])

# --------------------------------------------------
# 4. READ LIVE KAFKA STREAM
# --------------------------------------------------

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 500)
    .option("failOnDataLoss", "false")
    .load()
)

# Convert Kafka binary value into readable JSON text.

text = raw.selectExpr(
    "CAST(value AS STRING) AS raw_json"
)

# --------------------------------------------------
# 5. PARSE AND VALIDATE RECORDS
# --------------------------------------------------

parsed = text.select(
    col("raw_json"),
    from_json(col("raw_json"), schema).alias("e")
)

flat = parsed.select(
    "raw_json",
    "e.*"
)

valid = (
    flat
    .withColumn(
        "event_ts",
        try_to_timestamp(
            col("event_time"),
            lit("yyyy-MM-dd'T'HH:mm:ss")
        )
    )
    .filter(
        col("event_id").isNotNull() &
        col("sensor_id").isNotNull() &
        col("zone_id").isNotNull() &
        col("event_ts").isNotNull() &
        col("temperature").isNotNull() &
        col("traffic_count").isNotNull() &
        col("student_id").isNotNull() &
        (col("student_id") == STUDENT)
    )
)

# --------------------------------------------------
# 6. TASK 3 AND TASK 4: WATERMARK + DEDUPLICATION
# --------------------------------------------------

deduplicated = (
    valid
    .withWatermark("event_ts", WATERMARK)
    .dropDuplicatesWithinWatermark(["event_id"])
)

# --------------------------------------------------
# 7. TASK 4: DISPLAY UNIQUE EVENTS
# --------------------------------------------------

dedup_query = (
    deduplicated
    .select(
        "event_id",
        "sensor_id",
        "zone_id",
        "event_ts",
        "temperature",
        "traffic_count"
    )
    .writeStream
    .queryName("deduplication_output")
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 10)
    .trigger(processingTime="5 seconds")
    .start()
)

# --------------------------------------------------
# 8. TASK 3: SLIDING EVENT-TIME WINDOWS
# --------------------------------------------------

zone_statistics = (
    deduplicated
    .groupBy(
        window(
            col("event_ts"),
            "5 minutes",
            "1 minute"
        ),
        col("zone_id")
    )
    .agg(
        avg("temperature").alias("average_temperature"),
        spark_max("traffic_count").alias("maximum_traffic"),
        count("*").alias("event_count")
    )
)

# --------------------------------------------------
# 9. DISPLAY ZONE STATISTICS
# --------------------------------------------------

stats_query = (
    zone_statistics
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("zone_id"),
        col("average_temperature"),
        col("maximum_traffic"),
        col("event_count")
    )
    .writeStream
    .queryName("zone_sliding_window_statistics")
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 20)
    .trigger(processingTime="5 seconds")
    .start()
)

print("Deduplication query running:", dedup_query.isActive)
print("Zone statistics query running:", stats_query.isActive)

# --------------------------------------------------
# 10. KEEP STREAMING APPLICATION RUNNING
# --------------------------------------------------

spark.streams.awaitAnyTermination()
