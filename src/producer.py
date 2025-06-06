import os
import json
import argparse
from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import col, to_timestamp, date_format  # type: ignore
from kafka import KafkaProducer  # type: ignore
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
MYSQL_URL = os.getenv("MYSQL_URL")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_TABLE = os.getenv("MYSQL_TABLE", "transactions")
MYSQL_JAR_PATH = os.getenv("MYSQL_JAR_PATH")


# load transactions from MySql
def load_transactions_from_mysql(spark, table, target_date=None):
    # loads the dataframe
    df = (
        spark.read.format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", table)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
    )
    # timestamp processing
    if "timestamp" in df.columns:
        df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    # filtering transactions by date if the date is provided in the command line
    if target_date:
        date_fmt = date_format(col("timestamp"), "M/d/yy")
        df = df.filter(date_fmt == target_date)
    # sorting time, asc
    df = df.orderBy(col("timestamp").asc())
    return df


# send transactions to kafka
def send_transactions_to_kafka_spark(df, producer, topic):
    # converting all spark rows into dictionary
    for row in df.collect():
        message = row.asDict()
        # timestamp converted to string
        if "timestamp" in message and message["timestamp"] is not None:
            message["timestamp"] = str(message["timestamp"])
        # send message to kafka topic
        producer.send(topic, value=message)
        print(
            f"Sent transaction_id {message.get('transaction_id')} at {message.get('timestamp')}"
        )


# main function running the producer.py
def run_producer(target_date=None):
    spark = (
        SparkSession.builder.appName("KafkaProducerProject4")
        .config("spark.jars", MYSQL_JAR_PATH)
        .getOrCreate()
    )

    df = load_transactions_from_mysql(spark, MYSQL_TABLE, target_date)
    if df.count() == 0:
        print(
            "No transactions found for the given date."
            if target_date
            else "No transactions found."
        )
        spark.stop()
        return
    # creats kafka producer, serialising py dic into json
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        linger_ms=10,
    )

    send_transactions_to_kafka_spark(df, producer, KAFKA_TOPIC)
    producer.flush()
    producer.close()
    print(
        f"All transactions{' for ' + target_date if target_date else ''} sent to Kafka topic '{KAFKA_TOPIC}'."
    )
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to stream, format m/d/yy, e.g., '4/1/25'. Streams ALL if not specified.",
    )
    args = parser.parse_args()
    run_producer(args.date)
