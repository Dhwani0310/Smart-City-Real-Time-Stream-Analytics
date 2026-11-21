# Smart City Real-Time Stream Analytics

**Real-Time Intelligent Stream Analytics Using Apache Kafka and Apache Spark Structured Streaming**

**Student:** Dhwani Chandrakant Haridwari  
**Student ID:** 202126940028  
**Institution:** Gujarat University  
**Programme:** Center For Professional Courses - Data Management and Visual Insights  
**Project Guide:** Monica Gahlawat  
**Semester:** 9

---

## 1. Project Overview

This project implements a real-time smart-city sensor monitoring and analytics pipeline using Apache Kafka and Apache Spark Structured Streaming.

A Python producer generates simulated sensor events and publishes them to a Kafka topic. Spark consumes these events and performs data-quality validation, watermark-based deduplication, event-time window aggregation, stateful anomaly detection and zone-level risk ranking.

The project also demonstrates checkpoint-based recovery for selected streaming queries and includes a personalized sensor-threshold test.

## 2. Features

- Generates simulated sensor events from 12 sensors distributed across 4 zones.
- Produces normal and abnormal temperature readings.
- Injects duplicate events, missing fields, malformed JSON and invalid timestamps.
- Simulates out-of-order and very late events.
- Uses an explicit Spark schema to parse Kafka JSON messages.
- Separates valid and rejected records in the Task 2 validation workflow.
- Stores rejected records in a JSON Lines quarantine file.
- Performs event-time window aggregation with a 10-minute watermark.
- Uses watermark-based duplicate removal.
- Maintains per-sensor state for anomaly detection.
- Generates critical alerts when at least 3 of the latest 5 readings exceed the applicable threshold.
- Calculates weighted risk scores for different zones.
- Demonstrates recovery using persistent Spark checkpoints for Tasks 5 and 6.

## 3. System Architecture

```text
+--------------------------+
| Python Sensor Producer   |
| 12 sensors / 4 zones     |
+------------+-------------+
             |
             v
+------------------------------+
| Apache Kafka                 |
| 202126940028_sensor_events   |
| 2 partitions                 |
+------------+-----------------+
             |
             v
+------------------------------------------+
| Apache Spark Structured Streaming        |
+------------------------------------------+
       |              |              |
       v              v              v
+--------------+ +-------------+ +-------------+
| Data Quality | | Event-Time  | | Stateful    |
| Validation   | | Aggregation | | Anomaly     |
| Quarantine   | | Dedup       | | Detection   |
+--------------+ +-------------+ +-------------+
                                      |
                                      v
                               +-------------+
                               | Zone Risk   |
                               | Ranking     |
                               +-------------+
                                      |
                                      v
                         +--------------------------+
                         | Output and Checkpoints   |
                         +--------------------------+
```

## 4. Technologies Used

| Technology | Purpose |
|---|---|
| Python 3.11 | Producer and test scripts |
| Apache Kafka | Event ingestion and message broker |
| Apache Spark 4.0.3 | Distributed stream processing |
| PySpark | Spark programming interface |
| Pandas | Batch-local processing in stateful anomaly detection |
| JSON / JSON Lines | Event payloads, configuration and quarantine |
| PowerShell | Windows command-line environment |
| Windows 11 | Development environment |

## 5. Project Structure

```text
Smart-City-Real-Time-Stream-Analytics/
|
|-- README.md
|-- requirements.txt
|-- Smart-City-Real-Time-Stream-Analytics.pdf
|-- .gitignore
|
|-- config/
|   |-- student_config.json
|   |-- comment.txt
|
|-- producer/
|   |-- sensor_producer.py
|   |-- watch_stream.py
|   |-- watermark_test.py
|   |-- anomaly_test_producer.py
|
|-- spark/
|   |-- read_kafka_test.py
|   |-- stream_processor.py
|   |-- stream_processor_task2_backup.py
|   |-- anomaly_detector.py
|   |-- risk_ranker.py
|
|-- tests/
|   |-- test_scenarios.md
|
|-- screenshots/
|
|-- output/                 (generated at runtime; excluded from Git)
|
|-- checkpoint/             (generated at runtime; excluded from Git)
```

The output and checkpoint directories contain generated runtime data and are excluded from the GitHub repository using `.gitignore`.

## 6. Configuration

All project settings are stored in:

`config/student_config.json`

### General configuration

| Parameter | Value |
|---|---|
| Student ID | 202126940028 |
| Kafka topic | `202126940028_sensor_events` |
| Number of partitions | 2 |
| Global temperature threshold | 40.0 °C |
| Watermark delay | 10 minutes |
| Consecutive-event rule | 5 readings |
| Required breaches | 3 |

### Personalized sensor thresholds

| Sensor | Threshold |
|---|---:|
| E10_SENSOR_A | 42.0 °C |
| E10_SENSOR_B | 38.0 °C |
| E10_SENSOR_C | 45.0 °C |

### Risk-ranking weights

| Component | Weight |
|---|---:|
| Anomaly percentage | 0.4 |
| Temperature excess | 0.3 |
| Critical alert count | 0.3 |

The weights total 1.0 (100%).


## 7. Environment Setup

### Prerequisites

- Python 3.11
- Java 17 or later
- Apache Kafka installed (example location: `C:\kafka`)
- Apache Spark 4.0.3 installed (example location: `C:\spark`)
- Windows 11 with PowerShell

Apache Spark 4.0.3 is installed separately and includes the PySpark runtime. Do not install a separate PySpark version through pip for this project setup.

### Create a Python virtual environment

Run these commands from the project root directory:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Upgrade pip and install the project dependencies:

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Configure Spark and Python

Ensure Java 17 or later is installed and `JAVA_HOME` is configured.

Verify Java:

```powershell
java -version
```

Set the Spark installation path:

```powershell
$env:SPARK_HOME = "C:\spark"
```

Set the Python interpreter used by Spark:

```powershell
$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source
```

Verify Spark:

```powershell
& "$env:SPARK_HOME\bin\spark-submit.cmd" --version
```

### Start Kafka

Start ZooKeeper and the Kafka broker using your Kafka installation.

Kafka should be accessible at:

`localhost:9092`

### Create and verify the Kafka topic

Run these commands in PowerShell:

```powershell
cd C:\kafka

.\bin\windows\kafka-topics.bat --create --topic 202126940028_sensor_events --partitions 2 --replication-factor 1 --bootstrap-server localhost:9092
```

Verify the topic:

```powershell
.\bin\windows\kafka-topics.bat --describe --topic 202126940028_sensor_events --bootstrap-server localhost:9092
```

If the topic already exists, use the describe command to verify it instead of trying to create it again.

## 8. Running the Project

Run project commands from the project root directory:

`Smart-City-Real-Time-Stream-Analytics`

The streaming jobs are long-running applications. Use separate PowerShell windows for the different Spark jobs.

### Task 1: Generate sensor events

```powershell
python .\producer\sensor_producer.py
```

The default producer generates 2,000 events. To specify a count:

```powershell
python .\producer\sensor_producer.py 2000
```

The producer generates events from 12 sensors across 4 zones. It includes normal readings, abnormal readings, duplicate IDs, missing temperature fields, malformed JSON, invalid timestamps, out-of-order events and very late events.

To monitor incoming Kafka messages:

```powershell
python .\producer\watch_stream.py
```

### Task 2: Data validation and quarantine

Run the separate validation script:

```powershell
& "C:\spark\bin\spark-submit.cmd" --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.3 .\spark\stream_processor_task2_backup.py
```

This script:

- Reads Kafka messages using an explicit schema.
- Parses JSON and timestamps.
- Identifies malformed JSON.
- Identifies missing required fields.
- Identifies invalid timestamps.
- Calculates valid and rejected record counts per micro-batch.
- Writes rejected records to `output/quarantine/rejected.jsonl`.

Each quarantined entry includes the raw JSON payload, rejection reason and batch ID.

### Tasks 3 and 4: Event-time aggregation and deduplication

Run:

```powershell
& "C:\spark\bin\spark-submit.cmd" --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.3 .\spark\stream_processor.py
```

This job:

- Reads events from Kafka.
- Parses event timestamps.
- Filters records with missing required fields and mismatched student IDs.
- Applies watermark-based duplicate handling using `event_id`.
- Calculates event-time sliding-window statistics by zone.

Aggregation uses:

- Window duration: 5 minutes
- Sliding interval: 1 minute
- Average temperature
- Maximum traffic count
- Event count

The reviewed console queries use a 5-second trigger.

### Task 5: Stateful anomaly detection

Run:

```powershell
& "C:\spark\bin\spark-submit.cmd" --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.3 .\spark\anomaly_detector.py
```

The anomaly detector maintains state grouped by sensor ID.

The rule uses the latest 5 readings and generates a critical alert when at least 3 readings exceed the applicable temperature threshold.

It uses personalized thresholds when available and the global threshold as a fallback.

Generated alerts are stored in:

`output/critical_alerts`

Checkpoint location:

`checkpoint/task5_anomaly_detector`

### Task 6: Zone risk ranking

Run:

```powershell
& "C:\spark\bin\spark-submit.cmd" --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.3 .\spark\risk_ranker.py
```

The ranking component calculates zone-level statistics using:

- Anomaly percentage
- Average temperature excess above the global threshold
- Critical alert count

The normalized components are combined using the configured weights:

```text
Risk Score =
    0.4 × Normalized Anomaly Percentage
  + 0.3 × Normalized Temperature Excess
  + 0.3 × Normalized Critical Alert Count
```

Zones are displayed in descending risk-score order.

Checkpoint location:

`checkpoint/task6_risk_ranker`

**Note:** Task 6 uses the global temperature threshold for its anomaly percentage calculation. Its zone statistics are cumulative rather than event-time windowed.

## 9. Testing

### Watermark and duplicate test

Run:

```powershell
python .\producer\watermark_test.py
```

This producer script is used for testing watermark and duplicate-handling scenarios.

### Personalized threshold test

Run:

```powershell
python .\producer\anomaly_test_producer.py
```

The script publishes 5 readings for each of 3 personalized test sensors, for a total of 15 readings.

Expected results:

| Sensor | Threshold | Expected result |
|---|---:|---|
| E10_SENSOR_A | 42 °C | Critical alert after 3 breaches |
| E10_SENSOR_B | 38 °C | Critical alert after 3 breaches |
| E10_SENSOR_C | 45 °C | No critical alert |

Check the generated alerts:

```powershell
Get-ChildItem .\output\critical_alerts -Recurse -File |
    Select-String "E10_SENSOR_A|E10_SENSOR_B|E10_SENSOR_C"
```

The observed demonstration results showed critical alerts for E10_SENSOR_A and E10_SENSOR_B, each with a breach count of 3, and no matching alert for E10_SENSOR_C.

Additional test scenarios are documented in:

`tests/test_scenarios.md`

The personalized threshold test is a custom demonstration. It does not replace an instructor-provided unseen test.

## 10. Checkpoint Recovery

Persistent checkpoint directories are configured for the following jobs:

| Job | Checkpoint directory |
|---|---|
| Task 5: Anomaly detector | `checkpoint/task5_anomaly_detector` |
| Task 6: Risk ranker | `checkpoint/task6_risk_ranker` |

When restarting either query, use the same checkpoint location and a compatible query configuration.

Checkpointing restores Spark's recorded streaming progress and state. It does not, by itself, guarantee duplicate-free writes to every external sink.

The console queries in `stream_processor.py` and the validation query in `stream_processor_task2_backup.py` do not configure checkpoint locations in the reviewed source.

## 11. Output and Evidence

Generated results are written to the `output/` directory.

| Output location | Purpose |
|---|---|
| `output/quarantine/rejected.jsonl` | Rejected records from Task 2 |
| `output/critical_alerts/` | Critical anomaly alerts from Task 5 |

Evidence screenshots are stored in `screenshots/`.

They cover:

- Environment setup
- Kafka topic creation
- Live Kafka stream
- Spark execution
- Data-quality validation
- Window aggregation and watermark testing
- Stateful anomaly detection
- Zone risk ranking
- Checkpoint restart and recovery
- Personalized threshold configuration and testing

## 12. Limitations

- Sensor events are simulated rather than collected from physical IoT devices.
- Personalized thresholds are configured demonstration values.
- Task 5 sorts records within each micro-batch, not globally across all batches. Out-of-order events arriving in different batches may therefore affect state history.
- Task 6 uses the global threshold for anomaly percentage rather than the personalized thresholds used by Task 5.
- Task 6 uses cumulative zone statistics rather than a bounded event-time window.
- Task 6 reads critical-alert JSON files from the output directory, so existing historical alert files can influence alert counts.
- The Task 2 validation script does not configure checkpoint recovery.
- The Task 3 and Task 4 console queries do not configure checkpoint recovery.
- End-to-end exactly-once behaviour depends on the external sink and its write semantics, not on checkpointing alone.
- The personalized demonstration does not replace the instructor-provided unseen test.

## 13. Conclusion

This project demonstrates the use of Apache Kafka and Apache Spark Structured Streaming for real-time smart-city sensor analytics.

It includes simulated event generation, Kafka ingestion, data-quality validation, quarantine handling, event-time analysis, watermark-based deduplication, stateful anomaly detection, weighted zone risk ranking and checkpoint-based recovery for selected streaming queries.

The personalized threshold demonstration successfully showed critical alerts for sensors E10_SENSOR_A and E10_SENSOR_B, while E10_SENSOR_C generated no alert, matching the expected test results.

The project provides practical experience with distributed event ingestion, stream processing, event-time operations, state management and real-time analytics.
