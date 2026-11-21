
import json
from typing import Iterator

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, try_to_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
    BooleanType, TimestampType
)

# --------------------------------------------------
# 1. LOAD CONFIGURATION
# --------------------------------------------------

with open("config/student_config.json", "r") as f:
    cfg = json.load(f)

TOPIC = cfg["topic_name"]
STUDENT = cfg["student_id"]

# Global fallback threshold
THRESHOLD = float(cfg["temperature_threshold"])

# Individual sensor thresholds
SENSOR_THRESHOLDS = {
    sensor_id: float(value)
    for sensor_id, value in cfg.get("sensor_thresholds", {}).items()
}

CONSECUTIVE_RULE = int(cfg["consecutive_rule"])
BREACHES_NEEDED = int(cfg["breaches_needed"])
WATERMARK = cfg["watermark_delay"]

# --------------------------------------------------
# 2. CREATE SPARK SESSION
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Task5_Stateful_Anomaly_Detection")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("TASK 5: STATEFUL ANOMALY DETECTION")
print("STUDENT:", STUDENT)
print("KAFKA TOPIC:", TOPIC)
print("GLOBAL FALLBACK THRESHOLD:", THRESHOLD)
print("PERSONALIZED SENSOR THRESHOLDS:", SENSOR_THRESHOLDS)
print("CONSECUTIVE EVENTS:", CONSECUTIVE_RULE)
print("BREACHES REQUIRED:", BREACHES_NEEDED)
print("=" * 60)

# --------------------------------------------------
# 3. DEFINE INPUT SCHEMA
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
# 4. READ KAFKA STREAM
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

# --------------------------------------------------
# 5. PARSE AND VALIDATE EVENTS
# --------------------------------------------------

parsed = raw.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("e")
)

events = (
    parsed.select("e.*")
    .withColumn(
        "event_ts",
        try_to_timestamp(
            col("event_time"),
            lit("yyyy-MM-dd'T'HH:mm:ss")
        )
    )
    .filter(
        col("event_id").isNotNull()
        & col("sensor_id").isNotNull()
        & col("zone_id").isNotNull()
        & col("event_ts").isNotNull()
        & col("temperature").isNotNull()
        & col("student_id").isNotNull()
        & (col("student_id") == STUDENT)
    )
)

# --------------------------------------------------
# 6. WATERMARK AND DUPLICATE REMOVAL
# --------------------------------------------------

unique_events = (
    events
    .withWatermark("event_ts", WATERMARK)
    .dropDuplicatesWithinWatermark(["event_id"])
)

# --------------------------------------------------
# 7. DEFINE OUTPUT AND STATE SCHEMAS
# --------------------------------------------------

output_schema = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("zone_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("breach_count", IntegerType(), False),
    StructField("severity", StringType(), False),
    StructField("temperature", DoubleType(), False)
])

state_schema = StructType([
    StructField("history_json", StringType(), True),
    StructField("alerted", BooleanType(), True)
])

# --------------------------------------------------
# 8. STATEFUL ANOMALY DETECTION
# --------------------------------------------------

def detect_anomalies(
    key: tuple,
    pdf_iterator: Iterator[pd.DataFrame],
    state
) -> Iterator[pd.DataFrame]:

    sensor_id = key[0]

    # Select personalized threshold for this sensor.
    # Use global threshold if sensor is not configured.
    sensor_threshold = SENSOR_THRESHOLDS.get(
        sensor_id,
        THRESHOLD
    )

    # Restore previous sensor state.
    if state.exists:
        history_json, alerted = state.get
        history = json.loads(history_json)
    else:
        history = []
        alerted = False

    # Collect this microbatch's events.
    batches = list(pdf_iterator)

    if not batches:
        return

    pdf = pd.concat(batches, ignore_index=True)

    # Sort events by event time within this microbatch.
    pdf = pdf.sort_values("event_ts")

    alerts = []

    for _, row in pdf.iterrows():

        event_time = pd.Timestamp(row["event_ts"])
        temperature = float(row["temperature"])
        zone_id = str(row["zone_id"])

        # Compare temperature with this sensor's threshold.
        breach = temperature > sensor_threshold

        history.append({
            "event_time": event_time.isoformat(),
            "breach": breach,
            "zone_id": zone_id,
            "temperature": temperature
        })

        # Keep only the latest N events by event time.
        history.sort(key=lambda item: item["event_time"])
        history = history[-CONSECUTIVE_RULE:]

        breach_count = sum(
            1 for item in history if item["breach"]
        )

        # Generate a critical alert when the rule is met.
        if (
            len(history) == CONSECUTIVE_RULE
            and breach_count >= BREACHES_NEEDED
            and not alerted
        ):
            alerts.append({
                "sensor_id": sensor_id,
                "zone_id": zone_id,
                "event_time": event_time.to_pydatetime(),
                "breach_count": breach_count,
                "severity": "CRITICAL",
                "temperature": temperature
            })

            alerted = True

        # Allow another alert after the rolling window
        # falls below the required breach count.
        elif breach_count < BREACHES_NEEDED:
            alerted = False

    # Save updated state independently for each sensor.
    state.update((
        json.dumps(history),
        alerted
    ))

    if alerts:
        yield pd.DataFrame(alerts, columns=[
            "sensor_id",
            "zone_id",
            "event_time",
            "breach_count",
            "severity",
            "temperature"
        ])

# --------------------------------------------------
# 9. GROUP EVENTS BY SENSOR AND APPLY STATE
# --------------------------------------------------

sensor_alerts = (
    unique_events
    .select(
        "sensor_id",
        "zone_id",
        "event_ts",
        "temperature"
    )
    .groupBy("sensor_id")
    .applyInPandasWithState(
        detect_anomalies,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="Append",
        timeoutConf="NoTimeout"
    )
)

# --------------------------------------------------
# 10. SAVE CRITICAL ALERTS AS JSON
# --------------------------------------------------

alert_query = (
    sensor_alerts
    .writeStream
    .queryName("stateful_sensor_anomaly_alerts")
    .format("json")
    .outputMode("append")
    .option("path", "output/critical_alerts")
    .option(
        "checkpointLocation",
        "checkpoint/task5_anomaly_detector"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

print("Anomaly detection query started.")
print("Query active:", alert_query.isActive)
print("Critical alerts will be saved in output/critical_alerts")

# --------------------------------------------------
# 11. KEEP STREAMING APPLICATION RUNNING
# --------------------------------------------------

spark.streams.awaitAnyTermination()
