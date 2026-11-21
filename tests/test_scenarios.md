# Test Scenarios and Results

## 1. Purpose

This document describes the test scenarios used to verify the Real-Time Intelligent Stream Analytics system using Apache Kafka and Apache Spark Structured Streaming.

## 2. Test Scenarios

| Test ID | Test scenario | Expected behaviour | Observed result / evidence |
|---|---|---|---|
| T01 | Normal sensor events | Valid events are published to Kafka and processed by Spark. | Producer and live-stream execution were demonstrated; see E1-E3. |
| T02 | Malformed JSON | Invalid JSON records are identified by the Task 2 validation workflow and classified as `malformed_json`. | Malformed-input handling is implemented in the validation script; see E4-E5. |
| T03 | Missing required fields | Records missing fields listed in the Task 2 `REQUIRED` list are rejected and classified as `missing_field`. | Missing-field validation is implemented in the validation script; see E5. |
| T04 | Invalid timestamps | Records whose `event_time` cannot be parsed using the configured timestamp format are rejected and classified as `bad_timestamp`. | Timestamp validation is implemented in the validation script; see E5. |
| T05 | Duplicate event IDs | Duplicate events are subject to watermark-based deduplication using `event_id`. | Deduplication is implemented in the streaming jobs. Use the captured watermark/deduplication evidence for the specific demonstrated outcome; see E6. |
| T06 | Watermark and late-timestamp sequence | The test sends an original event, repeats it, sends another event timestamped 20 minutes later, and then sends the original event again with its earlier timestamp. This sequence is intended to exercise watermark advancement and old-event handling. | Controlled test events were generated. The precise handling of each event must be interpreted from the corresponding Spark output; see E6 watermark screenshots. |
| T07 | Event-time window aggregation | Spark calculates average temperature, maximum traffic count and event count by zone using a 5-minute window that slides every 1 minute. | Window aggregation output was captured; see E6. |
| T08 | Stateful anomaly detection | A sensor generates a critical alert when at least 3 of its latest 5 readings are strictly above its applicable threshold. | Stateful anomaly detection and critical-alert output were captured; see E7. |
| T09 | Zone risk ranking | Zones are ranked using normalized anomaly percentage, average temperature excess and critical-alert count, with weights 0.4, 0.3 and 0.3. | Ranked zone output was captured; see E8. Task 6 uses the global 40 °C threshold and cumulative zone statistics. |
| T10 | Checkpoint recovery | A supported streaming query is restarted using the same checkpoint directory and a compatible query configuration. | Task 6 risk-ranking restart and recovered output were captured; see E9. Task 5 also has a configured checkpoint path, but the cited restart evidence is for Task 6. |
| T11 | Personalized threshold test | Each test sensor uses its configured individual threshold. | E10_SENSOR_A and E10_SENSOR_B generated critical alerts with breach_count = 3; E10_SENSOR_C generated no alert. See E10 evidence. |

## 3. Personalized Threshold Test Details

The personalized test uses the following configuration:

| Sensor ID | Threshold | Temperature readings (°C) | Breaches |
|---|---:|---|---:|
| E10_SENSOR_A | 42 | 43, 41, 44, 40, 45 | 3 |
| E10_SENSOR_B | 38 | 39, 37, 40, 36, 41 | 3 |
| E10_SENSOR_C | 45 | 41, 44, 43, 42, 40 | 0 |

The anomaly rule requires at least three readings strictly greater than the applicable threshold within the latest five readings.

### Observed results

- E10_SENSOR_A: A critical alert was observed with `breach_count = 3`.
- E10_SENSOR_B: A critical alert was observed with `breach_count = 3`.
- E10_SENSOR_C: No matching critical-alert record was observed; all five readings are below its 45 °C threshold.

The personalized threshold test is a project demonstration using configured test values. It does not replace the instructor-provided unseen test, which must be completed during the individual demonstration if required.

## 4. Checkpoint Recovery Test

The Task 5 and Task 6 Spark streaming queries declare persistent checkpoint directories.

Checkpoint locations:

- Task 5: `checkpoint/task5_anomaly_detector`
- Task 6: `checkpoint/task6_risk_ranker`

The recovery demonstration involved stopping and restarting the Task 6 risk-ranking query using its existing checkpoint and checking the recovered output.

A checkpoint stores streaming progress and state needed for supported query recovery. Checkpointing alone does not guarantee exactly-once writes to every external sink. External sink write and idempotency semantics also matter.

The Task 2 validation query and the two console queries in `stream_processor.py` do not configure checkpoint locations in the reviewed source.

## 5. Evidence Files

Screenshots are stored in the `screenshots/` directory.

- E1: Environment setup (`E1_environment.png`)
- E2: Kafka topic (`E2_kafka_topic.png`)
- E3: Live stream (`E3_live_stream.png`)
- E4: Spark execution (`E4_spark_execution.png`)
- E5: Data quality (`E5_data_quality.png`)
- E6: Streaming/window aggregation and watermark testing (`E6_streaming_and_window_aggregation.png`, `E6_watermark_test.png`, `E6_watermark_test (2).png`)
- E7: Stateful anomaly detection (`E7_stateful_anomaly_detection.png`, `E7_stateful_anomaly_detection (2).png`)
- E8: Risk ranking (`E8_risk_ranking.png`, `E8_risk_ranking (2).png`)
- E9: Checkpoint recovery (`E9a_checkpoint_restart.png`, `E9b_checkpoint_recovered_output.png`)
- E10: Personalized threshold demonstration (`E10_1_Spark_Threshold_Configuration.png`, `E10_2_Personalized_Test_Producer.png`)

## 6. Limitations and Pending Verification

- The sensor events are simulated rather than collected from physical IoT devices.
- Personalized thresholds are configured demonstration values.
- Task 5 sorts events within each micro-batch, not globally across all micro-batches. Out-of-order events arriving in different batches may affect the rolling sensor history.
- Task 6 uses the global threshold for its anomaly-percentage calculation rather than the personalized thresholds used by Task 5.
- Task 6 uses cumulative zone statistics, and its critical-alert count can be affected by historical JSON files present in the alert output directory.
- The watermark test generates a controlled event-time sequence; the actual late-event and duplicate outcomes should be described from the observed Spark output, not inferred solely from successful producer execution.
- The instructor-provided unseen test must still be performed during the individual demonstration if required.
- End-to-end exactly-once behaviour depends on the external sink's write semantics and is not guaranteed by checkpointing alone.
