import os
from pyspark.sql import SparkSession  # type: ignore
from dotenv import load_dotenv
from mysql.connector import connect  # type: ignore
from pyspark.sql.functions import to_date  # type: ignore
from pyspark.sql.functions import to_timestamp  # type: ignore


class DataProcessor:
    def __init__(self, spark: SparkSession):
        load_dotenv()
        self.spark = spark
        self.mysql_url = f"jdbc:mysql://{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
        self.mysql_user = os.getenv("MYSQL_USER")
        self.mysql_password = os.getenv("MYSQL_PASSWORD")
        self.dataset_dir = os.getenv("DATASET_DIR")
        self.db_name = os.getenv("MYSQL_DB")
        print(f"Initialized DataProcessor with DB: {self.mysql_url}")

    def create_mysql_tables(self):
        print("Dropping and creating database and tables in MySQL...")
        conn = connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            autocommit=True,
        )
        cursor = conn.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {self.db_name};")
        print(f"Database `{self.db_name}` dropped (if existed).")
        cursor.execute(f"CREATE DATABASE {self.db_name};")
        print(f"Database `{self.db_name}` created.")
        conn.close()
        conn = connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=self.db_name,
            autocommit=True,
        )
        cursor = conn.cursor()
        table_sqls = [
            """
            CREATE TABLE IF NOT EXISTS customers (
                customer_id VARCHAR(64) PRIMARY KEY,
                name VARCHAR(255),
                phone_number VARCHAR(32),
                address VARCHAR(255),
                email VARCHAR(255),
                credit_score INT,
                annual_income FLOAT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS credit_card_types (
                card_type_id VARCHAR(64) PRIMARY KEY,
                name VARCHAR(255),
                credit_score_min INT,
                credit_score_max INT,
                credit_limit_min FLOAT,
                credit_limit_max FLOAT,
                annual_fee FLOAT,
                rewards_rate FLOAT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS cards (
                card_id VARCHAR(64) PRIMARY KEY,
                customer_id VARCHAR(64),
                card_type_id VARCHAR(64),
                card_number VARCHAR(64),
                expiration_date DATE,
                credit_limit FLOAT,
                current_balance FLOAT,
                issue_date DATE,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
                FOREIGN KEY (card_type_id) REFERENCES credit_card_types(card_type_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR(64) PRIMARY KEY,
                card_id VARCHAR(64),
                merchant_name VARCHAR(255),
                timestamp DATETIME,
                amount FLOAT,
                location VARCHAR(255),
                transaction_type VARCHAR(32),
                related_transaction_id VARCHAR(64),
                FOREIGN KEY (card_id) REFERENCES cards(card_id)
            );
            """,
        ]
        for sql in table_sqls:
            cursor.execute(sql)
            print(f"Table creation executed:\n{sql.split('(')[0].strip()}")
        conn.close()
        print("All tables created/ensured.")

    def load_csvs_to_mysql(self):
        """Load all CSVs in the dataset dir to their respective MySQL tables."""
        csv_table_map = {
            "customers.csv": "customers",
            "credit_card_types.csv": "credit_card_types",
            "cards.csv": "cards",
            "transactions.csv": "transactions",
        }
        for csv_file, table in csv_table_map.items():
            path = os.path.join(self.dataset_dir, csv_file)
            print(f"Loading {csv_file} to MySQL table `{table}`...")
            df = self.spark.read.csv(path, header=True, inferSchema=True)
            if table == "cards":
                df = df.withColumn(
                    "expiration_date", to_date("expiration_date")
                ).withColumn("issue_date", to_date("issue_date"))
            elif table == "transactions":
                df = df.withColumn("timestamp", to_timestamp("timestamp"))

            df.write.format("jdbc").option("url", self.mysql_url).option(
                "dbtable", table
            ).option("user", self.mysql_user).option(
                "password", self.mysql_password
            ).option(
                "driver", "com.mysql.cj.jdbc.Driver"
            ).mode(
                "append"
            ).save()
            print(f"{csv_file} loaded into `{table}` ({df.count()} rows).")

    def run(self):
        print("Beginning MySQL database/table setup and data load process...")
        self.create_mysql_tables()
        self.load_csvs_to_mysql()
        print("Data loading process complete.")
