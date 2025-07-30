# notebooks/silver/transform_sales.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, when
from utils.config import BRONZE_SALES_TABLE, SILVER_SALES_TABLE, SCHEMA_SILVER
from utils.data_quality_checks import check_nulls, check_duplicates

def transform_sales(spark: SparkSession):
    """
    Transforms raw sales data from Bronze to Silver layer.
    Performs cleansing, type conversions, and basic validations.
    """
    print(f"Reading raw sales data from: {BRONZE_SALES_TABLE}")
    df_bronze = spark.read.table(BRONZE_SALES_TABLE)

    # Apply data quality checks (example)
    check_nulls(df_bronze, ["transaction_id", "product_id", "customer_id", "sale_amount"])
    df_bronze = df_bronze.dropDuplicates(["transaction_id"]) # Deduplicate based on primary key

    # Transformations
    df_silver = df_bronze.select(
        col("transaction_id").cast("string"),
        to_date(col("transaction_date"), "yyyy-MM-dd").alias("transaction_date"),
        col("product_id").cast("string"),
        col("customer_id").cast("string"),
        col("store_id").cast("string"),
        col("quantity").cast("integer"),
        col("sale_amount").cast("decimal(10, 2)"),
        col("ingestion_timestamp") # Keep for lineage
    ).filter(
        # Basic data validation: sale_amount must be positive
        col("sale_amount") > 0
    )

    # Example: Handle invalid quantity (e.g., set to 0 or quarantine)
    df_silver = df_silver.withColumn(
        "quantity",
        when(col("quantity").isNull() | (col("quantity") < 0), lit(0)).otherwise(col("quantity"))
    )

    # Create schema if not exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_SILVER}")

    # Write to Delta Lake Silver layer (overwrite based on partition if applicable, or merge)
    # For simplicity, using overwrite for daily batch processing. For incremental, use merge.
    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(SILVER_SALES_TABLE)

    print(f"Sales data successfully transformed and written to: {SILVER_SALES_TABLE}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TransformSales").getOrCreate()
    transform_sales(spark)
    spark.stop()
