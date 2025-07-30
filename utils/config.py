# utils/config.py

BRONZE_STORAGE_PATH = "abfss://bronze@<your_storage_account>.dfs.core.windows.net/"
SILVER_STORAGE_PATH = "abfss://silver@<your_storage_account>.dfs.core.windows.net/"
GOLD_STORAGE_PATH = "abfss://gold@<your_storage_account>.dfs.core.windows.net/"

CATALOG_NAME = "retail_catalog" # Using Unity Catalog
SCHEMA_BRONZE = f"{CATALOG_NAME}.bronze"
SCHEMA_SILVER = f"{CATALOG_NAME}.silver"
SCHEMA_GOLD = f"{CATALOG_NAME}.gold"

# Table Names
BRONZE_SALES_TABLE = f"{SCHEMA_BRONZE}.sales_raw"
BRONZE_CUSTOMERS_TABLE = f"{SCHEMA_BRONZE}.customers_raw"
BRONZE_PRODUCTS_TABLE = f"{SCHEMA_BRONZE}.products_raw"

SILVER_SALES_TABLE = f"{SCHEMA_SILVER}.sales_cleansed"
SILVER_CUSTOMERS_TABLE = f"{SCHEMA_SILVER}.customers_cleansed"
SILVER_PRODUCTS_TABLE = f"{SCHEMA_SILVER}.products_conformed"

GOLD_DAILY_SALES_SUMMARY = f"{SCHEMA_GOLD}.daily_sales_summary"
GOLD_CUSTOMER_LTV = f"{SCHEMA_GOLD}.customer_lifetime_value"
GOLD_PRODUCT_PERFORMANCE = f"{SCHEMA_GOLD}.product_performance"
