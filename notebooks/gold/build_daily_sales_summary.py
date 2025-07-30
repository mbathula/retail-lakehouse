# notebooks/gold/build_daily_sales_summary.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, date_trunc
from utils.config import SILVER_SALES_TABLE, GOLD_DAILY_SALES_SUMMARY, SCHEMA_GOLD

def build_daily_sales_summary(spark: SparkSession):
    """
    Aggregates cleansed sales data to create a daily sales summary for the Gold layer.
    """
    print(f"Reading cleansed sales data from: {SILVER_SALES_TABLE}")
    df_silver_sales = spark.read.table(SILVER_SALES_TABLE)

    df_gold = df_silver_sales.groupBy(
        date_trunc("day", col("transaction_date")).alias("sale_date"),
        col("store_id")
    ).agg(
        sum("sale_amount").alias("total_sales_amount"),
        sum("quantity").alias("total_quantity_sold"),
        count("transaction_id").alias("number_of_transactions")
    )

    # Create schema if not exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}")

    # Write to Delta Lake Gold layer
    # This table is typically a full refresh or an incremental merge based on date
    df_gold.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(GOLD_DAILY_SALES_SUMMARY)

    print(f"Daily sales summary successfully built and written to: {GOLD_DAILY_SALES_SUMMARY}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BuildDailySalesSummary").getOrCreate()
    build_daily_sales_summary(spark)
    spark.stop()
