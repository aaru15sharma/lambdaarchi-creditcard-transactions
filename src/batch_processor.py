import csv
import copy
import os
import mysql.connector  # type: ignore
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
from helper import calculate_credit_score_adjustment, calculate_new_credit_limit
from pyspark.sql import SparkSession, Row  # type: ignore
from pyspark.sql.types import StructType, StructField, StringType  # type: ignore
from pyspark.sql.functions import to_date, to_timestamp  # type: ignore

load_dotenv()

MYSQL_URL = os.getenv("MYSQL_URL")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")


def create_spark_session():
    return (
        SparkSession.builder.appName("BatchProcessor")
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .getOrCreate()
    )

def normalize_expiration_date(date_str):
    try:
        return datetime.strptime(date_str, "%m/%y").strftime("%Y-%m-01")
    except Exception:
        return None

#disables foreign key checks and clears all data from transactions, cards, and customers.
def truncate_mysql_tables():
    conn = mysql.connector.connect(
        host="localhost", user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB
    )
    cursor = conn.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    cursor.execute("TRUNCATE TABLE transactions;")
    cursor.execute("TRUNCATE TABLE cards;")
    cursor.execute("TRUNCATE TABLE customers;")
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
    conn.commit()
    cursor.close()
    conn.close()
    print("MySQL tables truncated successfully.")

#read csv
def read_csv(path):
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)

#write to csv
def write_csv(path, data, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

#approve all pending transactions
def approve_pending_transactions(transactions):
    count = 0
    for txn in transactions:
        if txn["status"] == "pending":
            txn["status"] = "approved"
            count += 1
    print(f"Approved {count} pending transactions")
    return transactions

#update card balances
def update_card_balances(transactions, cards):
    card_adjustments = defaultdict(float)
    for txn in transactions:
        card_id = txn["card_id"]
        status = txn["status"]
        txn_type = txn.get("transaction_type", "").lower()
        amount = float(txn["amount"])
        if status == "approved":
            if txn_type in ("refund", "cancel"):
                card_adjustments[card_id] -= amount
            else:
                card_adjustments[card_id] += amount

    for card in cards:
        orig_balance = float(card.get("current_balance", 0.0))
        adj = card_adjustments[card["card_id"]]
        card["current_balance"] = f"{orig_balance + adj:.2f}"
    print(f"Updated balances for {len(cards)} cards")
    return cards

#updated credit scores
def update_customer_credit_scores(cards, customers):
    customer_cards = defaultdict(list)
    #collect all cards
    for card in cards:
        customer_cards[card["customer_id"]].append(card)
    # compute total usage %, score adjustment and then update the scores
    for cust in customers:
        cust_id = cust["customer_id"]
        cards_of_cust = customer_cards.get(cust_id, [])
        total_limit = sum(float(card["credit_limit"]) for card in cards_of_cust)
        total_balance = sum(float(card["current_balance"]) for card in cards_of_cust)
        usage_pct = (total_balance / total_limit) * 100 if total_limit else 0.0

        score_change = calculate_credit_score_adjustment(usage_pct)
        old_score = int(cust.get("credit_score", 0))
        new_score = max(0, old_score + score_change)
        cust["credit_score"] = str(new_score)
        cust["credit_score_change"] = str(score_change)
    print(f"Recalculated credit scores for {len(customers)} customers")
    return customers

#reduce credit limits (iff score dropped)
def reduce_credit_limits_if_needed(cards, customers):
    customers_map = {row["customer_id"]: row for row in customers}
    reduced = 0
    for card in cards:
        cust_id = card["customer_id"]
        cust = customers_map[cust_id]
        score_change = int(cust.get("credit_score_change", 0))
        #in score_Change<0, recalculate credit limit, lower
        if score_change < 0:
            old_limit = float(card["credit_limit"])
            new_limit = calculate_new_credit_limit(old_limit, score_change)
            card["credit_limit"] = f"{new_limit:.2f}"
            reduced += 1
    print(f"Reduced limits for {reduced} cards due to score drop")
    return cards

#convert dictionary to DFs
def convert_to_clean_df(
    spark, records, required_fields, date_columns=None, timestamp_columns=None
):
    cleaned = []
    for rec in records:
        clean_row = {k: rec.get(k, None) for k in required_fields}
        cleaned.append(clean_row)

    schema = StructType([StructField(k, StringType(), True) for k in required_fields])
    df = spark.createDataFrame([Row(**row) for row in cleaned], schema=schema)

    if date_columns:
        for col_name in date_columns:
            df = df.withColumn(col_name, to_date(col_name))
    if timestamp_columns:
        for col_name in timestamp_columns:
            df = df.withColumn(col_name, to_timestamp(col_name))

    print(
        f"Created Spark DataFrame with {df.count()} rows and {len(df.columns)} columns: {required_fields}"
    )
    return df

#df to sql
def write_to_mysql(df, table_name):
    row_count = df.count()
    df.write.format("jdbc").option("url", MYSQL_URL).option(
        "dbtable", table_name
    ).option("user", MYSQL_USER).option("password", MYSQL_PASSWORD).option(
        "driver", "com.mysql.cj.jdbc.Driver"
    ).mode(
        "append"
    ).save()
    print(f"Wrote {row_count} rows to MySQL table `{table_name}`")


def main():
    spark = create_spark_session()

    stream_transactions = read_csv("data/output/stream_transactions.csv")
    cards = read_csv("data/dataset_10/cards.csv")
    customers = read_csv("data/dataset_10/customers.csv")

    batch_transactions = copy.deepcopy(stream_transactions)
    batch_transactions = approve_pending_transactions(batch_transactions)
    write_csv(
        "data/output/batch_transactions.csv",
        batch_transactions,
        fieldnames=batch_transactions[0].keys(),
    )

    cards = update_card_balances(batch_transactions, cards)
    write_csv("data/output/cards_updated.csv", cards, fieldnames=cards[0].keys())
    customers = update_customer_credit_scores(cards, customers)
    write_csv(
        "data/output/customers_updated.csv", customers, fieldnames=customers[0].keys()
    )
    cards = reduce_credit_limits_if_needed(cards, customers)
    write_csv("data/output/cards_updated.csv", cards, fieldnames=cards[0].keys())

    truncate_mysql_tables()

    df_customers = convert_to_clean_df(
        spark,
        customers,
        required_fields=[
            "customer_id",
            "name",
            "phone_number",
            "address",
            "email",
            "credit_score",
            "annual_income",
        ],
    )

    df_cards = convert_to_clean_df(
        spark,
        cards,
        required_fields=[
            "card_id",
            "customer_id",
            "card_type_id",
            "card_number",
            "expiration_date",
            "credit_limit",
            "current_balance",
            "issue_date",
        ],
        date_columns=["expiration_date", "issue_date"],
    )

    df_transactions = convert_to_clean_df(
        spark,
        batch_transactions,
        required_fields=[
            "transaction_id",
            "card_id",
            "merchant_name",
            "timestamp",
            "amount",
            "location",
            "transaction_type",
            "related_transaction_id",
        ],
        timestamp_columns=["timestamp"],
    )

    write_to_mysql(df_customers, "customers")
    write_to_mysql(df_cards, "cards")
    write_to_mysql(df_transactions, "transactions")

    spark.stop()
    print("Batch process completed and MySQL updated.")


if __name__ == "__main__":
    main()
