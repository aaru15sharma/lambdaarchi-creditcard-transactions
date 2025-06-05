import os
import json
import csv
from kafka import KafkaConsumer  # type: ignore
from dotenv import load_dotenv
from helper import is_location_close_enough
import mysql.connector  # type: ignore


# load customer data from mysql
def load_customers_mysql():
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT customer_id, address FROM customers")
    # bulding zip_code dictionary
    cust_map = {}
    for row in cursor.fetchall():
        address = row["address"]
        zip_code = address.strip().split()[-1] if address else ""
        cust_map[row["customer_id"]] = zip_code
    cursor.close()
    conn.close()
    return cust_map


# loading cards and builds card_state
def load_cards_mysql(cust_map):
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT card_id, credit_limit, customer_id, current_balance FROM cards"
    )
    card_map = {}
    for row in cursor.fetchall():
        card_id = row["card_id"]
        credit_limit = float(row["credit_limit"])
        customer_id = row["customer_id"]
        pending_balance = float(row.get("current_balance", 0.0))
        user_zip = cust_map.get(customer_id, "")
        card_map[card_id] = {
            "credit_limit": credit_limit,
            "customer_id": customer_id,
            "pending_balance": pending_balance,
            "user_zip": user_zip,
        }
    cursor.close()
    conn.close()
    return card_map


# validates a transaction against rules, updates card's pending_balance, returns structured output
def process_transaction(tx, card_state):
    """Validate and process a single transaction, updating the card state."""
    card_id = tx.get("card_id", "")
    card_info = card_state.get(card_id)

    if not card_info:
        status = "declined"
        decline_reason = "Unknown card_id"
        pending_balance = 0.0
    else:
        limit = card_info["credit_limit"]
        pending = card_info["pending_balance"]
        customer_id = card_info["customer_id"]
        amount = float(tx.get("amount", 0.0))
        transaction_type = tx.get("transaction_type", "").lower()
        merchant_location = tx.get("location", "")
        merchant_zip = (
            merchant_location.strip().split()[-1] if merchant_location else ""
        )
        user_zip = card_info["user_zip"]

        declined = False
        decline_reason = ""
        # refunds decrease pending balance
        if transaction_type in ["refund", "cancellation"]:
            card_info["pending_balance"] -= abs(amount)
            status = "pending"
            print(
                f"Processed transaction_id {tx['transaction_id']} - REFUND/CANCELLATION | Pending balance updated"
            )
        else:
            # rule1
            if amount >= 0.5 * limit:
                status = "declined"
                decline_reason = "Transaction ≥ 50% of credit limit"
                declined = True
            # rule2
            elif not is_location_close_enough(user_zip, merchant_zip):
                status = "declined"
                decline_reason = "Merchant location too far from user"
                declined = True
            # rule3
            elif amount > 0 and (pending + amount) > limit:
                status = "declined"
                decline_reason = "Pending balance exceeds limit"
                declined = True
            # if all rules passed, mark pending
            else:
                status = "pending"
            # add to balance
            if not declined and status == "pending":
                card_info["pending_balance"] += amount

        pending_balance = round(card_info["pending_balance"], 2)

    tx_out = {
        "transaction_id": tx.get("transaction_id", ""),
        "card_id": tx.get("card_id", ""),
        "merchant_name": tx.get("merchant_name", ""),
        "timestamp": tx.get("timestamp", ""),
        "amount": tx.get("amount", ""),
        "location": tx.get("location", ""),
        "transaction_type": tx.get("transaction_type", ""),
        "related_transaction_id": tx.get("related_transaction_id", ""),
        "status": status,
        "decline_reason": decline_reason,
        "pending_balance": str(pending_balance),
    }
    return tx_out, status, transaction_type


# write to csv
def write_transaction(writer, tx_out, output_fields):
    """Write a transaction row to CSV."""
    writer.writerow({f: tx_out.get(f, "") for f in output_fields})


# main function running consumer
def run_consumer(
    card_state, output_fields, stream_output_path, kafka_topic, kafka_bootstrap_servers
):
    """Run the Kafka consumer, process transactions, and write output CSV."""
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="project4-consumer-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    total_count = 0
    declined_count = 0
    refund_cancel_count = 0

    with open(stream_output_path, "w", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=output_fields)
        writer.writeheader()
        # reads each kafka message, processes it, writes to CSV, and logs results
        for message in consumer:
            tx = message.value
            tx_out, status, transaction_type = process_transaction(tx, card_state)
            write_transaction(writer, tx_out, output_fields)
            fout.flush()
            total_count += 1

            if status == "declined":
                declined_count += 1
            if transaction_type in ["refund", "cancellation"]:
                refund_cancel_count += 1

            print(
                f"Processed transaction_id {tx['transaction_id']} - {status.upper()}"
                + (
                    f" | REASON: {tx_out['decline_reason']}"
                    if tx_out["decline_reason"]
                    else ""
                )
            )

        print(f"---\nTotal processed: {total_count}")
        print(f"Declined: {declined_count}")
        print(f"Refund/Cancellation: {refund_cancel_count}")


def get_mysql_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB"),
    )


def initialize_consumer_config():
    """Load environment variables and construct required paths and config objects."""
    load_dotenv()
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
    OUTPUT_DIR = os.getenv("OUTPUT_DIR")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    STREAM_OUTPUT_PATH = os.path.join(OUTPUT_DIR, "stream_transactions.csv")

    customer_zips = load_customers_mysql()
    card_state = load_cards_mysql(customer_zips)

    output_fields = [
        "transaction_id",
        "card_id",
        "merchant_name",
        "timestamp",
        "amount",
        "location",
        "transaction_type",
        "related_transaction_id",
        "status",
        "pending_balance",
    ]

    return {
        "card_state": card_state,
        "output_fields": output_fields,
        "stream_output_path": STREAM_OUTPUT_PATH,
        "kafka_topic": KAFKA_TOPIC,
        "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    }


def main():
    config = initialize_consumer_config()
    print(f"Starting Kafka consumer on topic '{config['kafka_topic']}'...")
    run_consumer(
        config["card_state"],
        config["output_fields"],
        config["stream_output_path"],
        config["kafka_topic"],
        config["kafka_bootstrap_servers"],
    )


if __name__ == "__main__":
    main()
