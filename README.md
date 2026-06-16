# streaming-06-scenarios

Alissa Beaderstadt - Graduate Student - Data Analytics

[![API Reference](https://img.shields.io/badge/API--Utils-datafun--streaming-purple)](https://denisecase.github.io/datafun-streaming/api/)
[![Workflow Guide](https://img.shields.io/badge/Pro--Guide-pro--analytics--02-green)](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
[![Python 3.14](https://img.shields.io/badge/python-3.14%2B-blue?logo=python)](./pyproject.toml)
[![MIT](https://img.shields.io/badge/license-see%20LICENSE-yellow.svg)](./LICENSE)

> Streaming data analytics: complete pipeline.

## Project Overview

This project extends the example Kafka streaming pipeline by adding real-time business
analytics during message consumption.

As sales transactions stream through Kafka, the consumer tracks:

- Top customers by revenue
- Top regions by revenue
- Revenue from new vs. returning customers
- Regional revenue performance rankings

The consumer exports these analytics to a custom JSON summary file in addition to the standard CSV,
DuckDB database, and live visualization outputs.

## Key Findings

In my sample run:

- US-MO generated 53.81% of total revenue and was the top-performing region.
- US-MO and US-TX together accounted for more than 72% of total sales.
- Returning customers generated approximately 79% of total revenue.
- The analytics summary identified the highest-value customers and regional sales patterns live.

This project shows how streaming data can be transformed into business intelligence while
transactions are actively being processed.

## Custom Enhancements

### Phase 4

Added real-time aggregation logic to track:

- Customer revenue totals
- Regional revenue totals
- Revenue by customer type (new vs. returning)

The results are exported to analytics_summary_beaderstadt.json.

### Phase 5

Extended the analytics pipeline by calculating:

- Regional revenue contribution percentages
- Top-performing region
- Ranked regional performance metrics

These additions help answer business questions about geographic sales performance and customer behavior.

## How the Project Works

This project brings the full streaming analytics workflow together.

The project uses Kafka to move sales messages from a producer to a consumer.
The producer sends validated sales messages to a Kafka topic.
The consumer reads each message, validates required fields, computes derived values,
updates a live chart, writes processed records to CSV, and stores results in DuckDB.

This module combines the major skills from the course:

- producing messages
- consuming messages
- validating message structure
- computing derived fields
- visualizing the stream
- storing processed data

The goal is to see how the parts work together in one complete scenario.

Custom files include:

- [data_contract_beaderstadt.py](src/streaming/data_validation/data_contract_beaderstadt.py)
  - Extends the message validation process by applying custom validation checks for fields
  such as quantity and unit price before records are processed by the consumer.

- [kafka_consumer_beaderstadt.py](src/streaming/kafka_consumer_beaderstadt.py)
  - Adds real-time aggregation logic for customer revenue totals, region revenue totals, and new
  vs. returning customer revenue.
  - Exports the final analytics summary to analytics_summary_beaderstadt.json.


## Working Files

You'll work with just these areas:

- **data/** - input data and generated output files
- **docs/** - the project narrative and documentation
- **src/streaming/** - producer, consumer, and supporting code
- **pyproject.toml** - update authorship & links
- **zensical.toml** - update authorship & links

## Instructions

Follow the
[step-by-step workflow guide](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to complete:

1. Phase 1. **Start & Run**
2. Phase 2. **Change Authorship**
3. Phase 3. **Read & Understand**
4. Phase 4. **Modify**
5. Phase 5. **Apply**

## Success

After completing Phase 1. **Start & Run**, you'll have your own GitHub project
running with Kafka.

Use four named terminals:

1. **kafka** - keep the Kafka message broker running
2. **topics** - create, list, or reset Kafka topics
3. **producer** - run the project and producer
4. **consumer** - run the consumer

After the producer and consumer run successfully, you should see:

```shell
========================
Consumer executed successfully!
========================
```

A new file `project.log` will appear in the root project folder
and processed data will appear in data/output/.

## Command Reference

The commands below are used in the workflow guide above.
They are provided here for convenience.

**Important:** Follow the workflow guide the first time you run this project.
It includes instructions for running the case example and then switching to the custom project.

<details>
<summary>Show command reference</summary>

### In a machine terminal (open in your `Repos` folder)

After you get a copy of this repo in your own GitHub account,
open a machine terminal in your `Repos` folder:

```bash
git clone https://github.com/abeaderstadt/streaming-06-scenarios

cd streaming-06-scenarios
code .
```

## Part A: Run the Case Example

Follow these steps first to verify that Kafka and the example project are
working correctly on your machine.

### In VS Code Terminal 1: Start Kafka (kafka)

Begin by running the case example. After verifying the example works,
follow the instructions below to run the custom Beaderstadt project.

For full instructions see
[**start kafka**](https://denisecase.github.io/pro-analytics-02/kafka/start-kafka/).

If any command fails,
repeat the steps at
[**install kafka**](https://denisecase.github.io/pro-analytics-02/kafka/install-kafka/)
until starting up is reliable.

Open a new VS Code terminal. Rename it `kafka`.
If running Windows, specify the terminal type as **wsl** or
type `wsl`.
Run the commands one at a time.

Step 1. Verify Java and PATH

```bash
echo "$JAVA_HOME"

"$JAVA_HOME/bin/java" --version
```

Step 2. Rebuild ClusterID (as needed)

```bash
cd ~/kafka

rm -rf /tmp/kraft-combined-logs

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

echo "Cluster ID: $KAFKA_CLUSTER_ID"

bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
```

Step 3. Start kafka server (keep running)

```bash
cd ~/kafka

bin/kafka-server-start.sh config/server.properties
```

### In VS Code terminal 2: Create the Case Topic (topics)

Create the example topic.

For full instructions see
[**create topic**](https://denisecase.github.io/pro-analytics-02/kafka/create-topic/).

The topic name must match the name defined in your
`.env` file (copy `.env.example` to `.env`).

Open another VS Code terminal. Rename it `topics`.
If running Windows, specify the terminal type as **wsl** or
type `wsl`.
Run the commands one at a time starting with the case example.

```bash
cd ~/kafka

bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --topic streaming-06-scenarios-case
```

### In VS Code Terminal 3: Run the Case Producer (producer)

Open another VS Code terminal. Rename it `producer`.
If running Windows, use **PowerShell**.
Run the commands one at a time.

```shell
uv self update
uv python pin 3.14
uv sync --extra dev --extra docs --upgrade
```

**Optional (recommended for VS Code users):**

- Open the Command Palette (menu: View/Command Palette, or Ctrl+Shift+P)
- Type and choose: Python: Select Interpreter
- Choose the interpreter inside this project's .venv folder (usually .venv\Scripts\python.exe)
- Open the Command Palette again (same as before)
- Type or choose: Developer: Reload Window

Then use the following to run the example producer:

```shell
uv run python -m streaming.kafka_producer_case
```

### In VS Code Terminal 4: Run the Case Consumer

Open another VS Code terminal. Rename it `consumer`.
If running Windows, use **PowerShell**.
Run the commands one at a time.
Start with the example consumer.

```shell
uv run python -m streaming.kafka_consumer_case
```

## Part B: Run the Custom Beaderstadt Project

After the case example is working, stop the case producer and consumer
and switch to the custom project.

### Step 1: Update the `.env` File

Update the topic name in your .env file:

KAFKA_TOPIC=streaming-06-scenarios-beaderstadt

Update the producer message count to 30:

PRODUCER_MESSAGE_COUNT=30

### Step 2: Create the Custom Topic (topics)

You do not need to delete the example topic.
Run the commands one at a time to create the custom topic:

```bash
cd ~/kafka

bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --topic streaming-06-scenarios-beaderstadt
```

### Step 3: Run the Custom Producer (producer)

Clear the terminal (if needed) and run the custom producer using:

```shell
clear
uv run python -m streaming.kafka_producer_beaderstadt
```

### Step 4: Run the Custom Consumer (consumer)

Clear the terminal (if needed) and run the custom consumer using:

```shell
clear
uv run python -m streaming.kafka_consumer_beaderstadt
```

### Optional Developer Commands

These commands are useful if you are modifying the project but are not required for peer review.

```bash
uvx pre-commit install
uvx pre-commit run --all-files

uv run ruff format .
uv run ruff check . --fix
uv run python -m pyright
uv run python -m pytest
uv run python -m zensical build
```

To start fresh, see
[manage topics](https://denisecase.github.io/pro-analytics-02/kafka/manage-topics/)
to delete the topic and recreate it.

</details>

## Notes

- Use the **UP ARROW** and **DOWN ARROW** in the terminal to scroll through past commands.
- Use `CTRL+f` to find (and replace) text within a file.
- You do not need to add to or modify `tests/`. They are provided for example only.
- Many files are silent helpers. Explore as you like, but nothing is required.
- You do NOT not to understand everything; understanding builds naturally over time.

## Troubleshooting >>> or

If you see something like this in your terminal: `>>>` or `...`
You accidentally started Python interactive mode.
It happens.
Press `Ctrl+c` (both keys together) or `Ctrl+Z` then `Enter` on Windows.
