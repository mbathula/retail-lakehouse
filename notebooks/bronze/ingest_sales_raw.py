# notebooks/bronze/ingest_sales_raw.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from utils.config import BRONZE_STORAGE_PATH, BRONZE_SALES_TABLE, SCHEMA_BRONZE

def ingest_sales_raw(spark: SparkSession, source_path: str):
    """
    Ingests raw sales data into the Bronze layer.
    Assumes data is in CSV format.
    """
    print(f"Ingesting raw sales data from: {source_path}")

    df_raw = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(source_path)

    # Add ingestion metadata
    df_raw = df_raw.withColumn("ingestion_timestamp", current_timestamp())

    # Create schema if not exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_BRONZE}")

    # Write to Delta Lake Bronze layer
    df_raw.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(BRONZE_SALES_TABLE)

    print(f"Raw sales data successfully ingested to: {BRONZE_SALES_TABLE}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("IngestSalesRaw").getOrCreate()

    # Example usage: replace with your actual source path
    # For a real scenario, this would likely be an external cloud storage path
    # or a streaming source.
    sample_source_path = "/databricks-datasets/retail-org/sales_day/2023_sales_daily.csv"
    ingest_sales_raw(spark, sample_source_path)

    spark.stop()
