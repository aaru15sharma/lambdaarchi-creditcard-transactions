# Project-4

## Table of Contents

- [Project Overview](#project-overview)
- [Project Structure](#project-structure)
- [Dataset Overview](#dataset-overview)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Setting up the Project](#setting-up-the-project)
- [Running the Project](#running-the-project)
- [Environment Variables](#environment-variables)
- [Output Files](#output-files)
- [License and Usage Restrictions](#license-and-usage-restrictions)


## Project Overview

This project implements a credit card transaction processing system using the Lambda Architecture, designed to process, validate, and analyze customer transactions in both real-time and batch modes.

## Project Structure

```bash
.
├── README.md
├── data
│   ├── dataset_10
│   │   ├── cards.csv
│   │   ├── credit_card_types.csv
│   │   ├── customers.csv
│   │   └── transactions.csv
│   └── output
│       ├── batch_transactions.csv
│       ├── cards_updated.csv
│       ├── customers_updated.csv
│       └── stream_transactions.csv
├── requirements.txt
└── src
    ├── __pycache__
    │   ├── data_processor.cpython-310.pyc
    │   └── helper.cpython-310.pyc
    ├── batch_processor.py
    ├── consumer.py
    ├── data_processor.py
    ├── helper.py
    ├── main.py
    └── producer.py
```

## Dataset Overview

The dataset spans transactions between April 1st to 4th, 2025, and includes:

| File | Description |
|------|-------------|
| `customers.csv` | Customer info with addresses, credit scores, income |
| `cards.csv` | Credit card info including credit limits and balances |
| `credit_card_types.csv` | Metadata on card types |
| `transactions.csv` | Raw card transaction logs over 4 days |

## Features

### Stream Layer (Apache Kafka)
Kafka Producer: Sends daily transaction batches to the Kafka topic.

Kafka Consumer:

-  Processes transactions in time order (not in batch).

- Declines transactions based on validation rules:

        - 50% credit limit per transaction.

        - Merchant too far from user’s ZIP code.

        - Exceeding credit limit with pending balance.

- Logs declined transactions to terminal in real time.

- Tracks pending balances and outputs stream_transactions.csv.

### Batch Layer
Processes stream_transactions.csv to:

- Approve pending transactions.

- Reflect refunds and cancellations.

- Output batch_transactions.csv.

- Updates:

    cards.csv → cards_updated.csv with new balances and reduced limits if necessary.

    customers.csv → customers_updated.csv with recalculated credit scores.

### Serving Layer (MySQL)
- Initial data load from CSVs into MySQL.

- Kafka consumer reads card/customer data directly from MySQL.

- Final processed data is written back to MySQL.

- Supports live user queries for demo purposes.

## Tech Stack

        Python

        Apache Kafka – Stream processing

        MySQL – Relational data store

## Setting up the project

### 1. Download and Setup Kafka

```bash
# Create a directory for Kafka
mkdir -p ~/kafka
cd ~/kafka

# Download Kafka 3.8.1
wget https://downloads.apache.org/kafka/3.8.1/kafka_2.13-3.8.1.tgz

# Extract the archive
tar -xzf kafka_2.13-3.8.1.tgz
```

### 2. Start Kafka Server

```bash
# Start Zookeeper (in a separate terminal)
cd ~/kafka/kafka_2.13-3.8.1
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka server (in another terminal)
cd ~/kafka/kafka_2.13-3.8.1
bin/kafka-server-start.sh config/server.properties
```
### 3. Create Kafka Topic

```bash
# Create the 'project4' topic
cd ~/kafka/kafka_2.13-3.8.1
bin/kafka-topics.sh --create --topic project4 --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Running the project

Terminal 1 (Consumer):
```bash
python src/consumer.py
```

Terminal 2 (Producer):
```bash
python src/producer.py
```

Terminal 3 (Batch Processing):
```bash
python src/batch_processor.py
```

## Environment Variables
All sensitive credentials and paths should be configured in a `.env` file.

| Variable Name           | Description |
|-------------------------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address (default: `localhost:9092`) |
| `KAFKA_TOPIC`           | Kafka topic name used for streaming transactions (`project4`) |
| `MYSQL_URL`             | Full JDBC URL for MySQL (`jdbc:mysql://localhost:3306/project4`) |
| `MYSQL_HOST`            | MySQL host (typically `localhost`) |
| `MYSQL_PORT`            | MySQL port (`3306`) |
| `MYSQL_DB`              | MySQL database name (`project4`) |
| `MYSQL_USER`            | MySQL username (`root`) |
| `MYSQL_PASSWORD`        | MySQL password (`Pushpendra@10`) |
| `MYSQL_JAR_PATH`        | Path to MySQL JDBC connector JAR file |
| `OUTPUT_DIR`            | Directory to store processed output files (`data/output`) |
| `DATASET_DIR`           | Directory containing input CSV datasets (`data/dataset_10`) |

## Output Files

| Output File | Description |
|-------------|-------------|
| `output/stream_transactions.csv` | Real-time processed transactions (pending/declined) |
| `output/batch_transactions.csv` | Finalized transactions post batch processing |
| `output/cards_updated.csv` | Updated balances and limits |
| `putput/customers_updated.csv` | Updated credit scores |

## License and Usage Restrictions

Copyright © 2025 Zimeng Lyu. All rights reserved.

This project was developed by Zimeng Lyu for the RIT DSCI-644 course, originally designed for GitLab CI/CD integration. It is shared publicly for portfolio and learning purposes.

**Usage Restriction:**  
This project **may not** be used as teaching material in any form (including lectures, assignments, or coursework) without explicit written permission from the author.

For inquiries regarding usage permissions, please contact Zimeng Lyu at zimenglyu@mail.rit.edu.

