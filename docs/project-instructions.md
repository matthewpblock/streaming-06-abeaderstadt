# Runbook & Pipeline Operation

This runbook outlines how to operate the custom streaming analytics pipeline.
The project uses Kafka to transport sales messages, DuckDB for storage, and Python for live stream processing and custom analytics aggregation.

## Topic Configuration

The pipeline communicates over the following Kafka topic defined in your `.env` file:
```ini
KAFKA_TOPIC=streaming-06-scenarios-beaderstadt
PRODUCER_MESSAGE_COUNT=30
```

## Running the Pipeline

To successfully execute the streaming pipeline, use four separate terminals running concurrently. Follow these steps in order:

### Terminal 1: Start Kafka Broker (`kafka`)

Start the local Kafka KRaft cluster. Assuming Kafka is installed in `~/kafka`:

```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

### Terminal 2: Manage Kafka Topics (`topics`)

Create the required topic before running the producer or consumer:

```bash
cd ~/kafka
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --topic streaming-06-scenarios-beaderstadt
```

### Terminal 3: Run the Producer (`producer`)

Start the custom data producer. The producer will begin pushing sales transaction records onto the Kafka topic.

```shell
uv run python -m streaming.kafka_producer_beaderstadt
```

### Terminal 4: Run the Consumer (`consumer`)

Run the custom consumer to process the stream in real-time. The consumer will validate messages, enrich them with derived metrics, and output a live business intelligence summary.

```shell
uv run python -m streaming.kafka_consumer_beaderstadt
```

## Real-Time Analytics Capabilities

As the consumer processes the stream, it performs live aggregations to track:
- Revenue totals by customer.
- Revenue totals by region.
- Revenue contribution by customer type (new vs. returning).
- Regional revenue percentages and rankings.

The final summary is exported into `data/output/analytics_summary_beaderstadt.json`. Wait for the consumer to finish processing the messages to review the finalized analytics file.
