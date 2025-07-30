# utils/data_quality_checks.py
from pyspark.sql import DataFrame

def check_nulls(df: DataFrame, columns: list):
    """
    Prints a warning if specified columns contain null values.
    In a real scenario, this might log to a monitoring system or quarantine records.
    """
    for col_name in columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            print(f"WARNING: Column '{col_name}' has {null_count} null values.")

def check_duplicates(df: DataFrame, primary_keys: list):
    """
    Prints a warning if duplicates are found based on primary key columns.
    """
    duplicate_count = df.groupBy(primary_keys).count().filter(col("count") > 1).count()
    if duplicate_count > 0:
        print(f"WARNING: Found {duplicate_count} duplicate records based on primary keys: {primary_keys}.")
