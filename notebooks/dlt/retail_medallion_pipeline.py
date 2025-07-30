# notebooks/dlt/retail_medallion_pipeline.py
import dlt
from pyspark.sql.functions import col, current_timestamp, sum, count, date_trunc

# Configuration for storage paths and catalog
BRONZE_STORAGE_PATH = "abfss://bronze@<your_storage_account>.dfs.core.windows.net/"
SILVER_STORAGE_PATH = "abfss://silver@<your_storage_account>.dfs.core.windows.net/"
GOLD_STORAGE_PATH = "abfss://gold@<your_storage_account>.dfs.core.windows.net/"

CATALOG_NAME = "retail_catalog"
SCHEMA_BRONZE = f"{CATALOG_NAME}.bronze"
SCHEMA_SILVER = f"{CATALOG_NAME}.silver"
SCHEMA_GOLD = f"{CATALOG_NAME}.gold"

# Set Unity Catalog schema for DLT (Optional, but good practice)
dlt.set_catalog(CATALOG_NAME)
dlt.set_target(SCHEMA_BRONZE) # Default target for bronze tables in this pipeline

@dlt.table(
    comment="Raw sales data from external source, including ingestion metadata.",
    table_properties={"quality": "bronze"}
)
def sales_raw():
    # Using Autoloader for incremental ingestion from cloud storage
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{BRONZE_STORAGE_PATH}/sales_raw_schema")
        .option("header", "true")
        .option("inferSchema", "true") # For initial schema inference
        .load(f"{BRONZE_STORAGE_PATH}/raw_sales_landing_zone/") # Path where raw CSVs land
        .withColumn("ingestion_timestamp", current_timestamp())
    )

# Set target for silver tables
dlt.set_target(SCHEMA_SILVER)

@dlt.table(
    comment="Cleaned and validated sales data.",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("positive_sale_amount", "sale_amount > 0")
def sales_cleansed():
    return (
        dlt.read_stream("sales_raw") # Read from the DLT bronze table
        .select(
            col("transaction_id").cast("string"),
            to_date(col("transaction_date"), "yyyy-MM-dd").alias("transaction_date"),
            col("product_id").cast("string"),
            col("customer_id").cast("string"),
            col("store_id").cast("string"),
            col("quantity").cast("integer"),
            col("sale_amount").cast("decimal(10, 2)").alias("sale_amount"),
            col("ingestion_timestamp")
        )
        .dropDuplicates(["transaction_id"]) # Deduplicate
    )

# Set target for gold tables
dlt.set_target(SCHEMA_GOLD)

@dlt.table(
    comment="Daily aggregated sales summary for reporting.",
    table_properties={"quality": "gold"}
)
def daily_sales_summary():
    return (
        dlt.read("sales_cleansed") # Read from the DLT silver table (batch read for aggregation)
        .groupBy(
            date_trunc("day", col("transaction_date")).alias("sale_date"),
            col("store_id")
        ).agg(
            sum("sale_amount").alias("total_sales_amount"),
            sum("quantity").alias("total_quantity_sold"),
            count("transaction_id").alias("number_of_transactions")
        )
    )
