# Project Architecture & Custom Files

This document describes the primary custom Python files and directories created for this streaming pipeline.

## 1. Custom Python Pipeline

The core streaming logic is implemented in the following custom files located in the `src/streaming/` directory:

- **`kafka_producer_beaderstadt.py`**: The custom producer that simulates and sends validated sales messages to the Kafka topic.
- **`kafka_consumer_beaderstadt.py`**: The custom consumer that reads the Kafka topic, validates incoming records, computes derived fields, updates live charts, and maintains a real-time analytics aggregation summary.
- **`data_validation/data_contract_beaderstadt.py`**: Defines the data schema and rules required for a valid sales message.
- **`data_engineering/derived_fields_beaderstadt.py`**: Computes derived fields such as regional sales percentages and top-performing metrics.

## 2. Directory Structure

- **`data/`**: Contains input reference data and generated output files (such as DuckDB databases and JSON summary analytics).
- **`docs/`**: The documentation outlining the project's technical architecture, APIs, and operational runbook.
- **`src/`**: All importable source code.

## 3. Storage and Output

The pipeline stores valid and rejected messages into a local DuckDB file, while the custom aggregated business intelligence metrics are emitted into `data/output/analytics_summary_beaderstadt.json`.
