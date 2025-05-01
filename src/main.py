import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession  # type: ignore
from data_processor import DataProcessor


def main():
    load_dotenv()
    print("[INFO] Starting Spark session...")
    jar_path = os.getenv("MYSQL_JAR_PATH")
    if not jar_path or not os.path.exists(jar_path):
        raise FileNotFoundError(f"MySQL connector JAR not found at {jar_path}")
    spark = (
        SparkSession.builder.appName("Project4 Data Loader")
        .config("spark.jars", jar_path)
        .getOrCreate()
    )
    print("[INFO] Spark session created.")
    dp = DataProcessor(spark)
    dp.run()
    spark.stop()
    print("[INFO] Spark session stopped.")


if __name__ == "__main__":
    main()
