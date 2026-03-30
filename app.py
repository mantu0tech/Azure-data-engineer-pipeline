# =============================================================================
# app.py - Azure Retail Data Pipeline: Optimized PySpark Commands
# Project: End-to-End Multi-Source Retail Data Pipeline
# Stack: Azure Databricks | PySpark | Delta Lake | ADLS Gen2
# GitHub: https://github.com/manish040596/azure-data-engineer---multi-source
# =============================================================================

# =============================================================================
# SECTION 1: VERIFY EXTERNAL LOCATION & ACCESS
# Run these in Databricks Notebook cells before anything else
# =============================================================================

# Cell 1: Verify external locations registered in Unity Catalog
# %sql
# SHOW EXTERNAL LOCATIONS;

# Cell 2: Verify catalogs available
# %sql
# SHOW CATALOGS;

# Cell 3: Verify grants on external location
# %sql
# SHOW GRANTS ON EXTERNAL LOCATION my_location;

# Cell 4: List Bronze folder to confirm ADF pipeline ran successfully
dbutils.fs.ls("abfss://retailer@mystg0poc.dfs.core.windows.net/Bronze")


# =============================================================================
# SECTION 2: READ BRONZE LAYER (Raw Parquet files from ADF)
# =============================================================================

# Read Transactions table from Azure SQL DB (via ADF -> Parquet)
df_transactions = spark.read.parquet(
    "abfss://retailer@mystg0poc.dfs.core.windows.net/Bronze/transaction/dbo.transactions.parquet"
)

# Read Products table
df_products = spark.read.parquet(
    "abfss://retailer@mystg0poc.dfs.core.windows.net/Bronze/transaction/dbo.products.parquet"
)

# Read Stores table
df_stores = spark.read.parquet(
    "abfss://retailer@mystg0poc.dfs.core.windows.net/Bronze/store/dbo.stores.parquet"
)

# Read Customers (from GitHub JSON API via ADF -> Parquet in customer folder)
df_customers = spark.read.parquet(
    "abfss://retailer@mystg0poc.dfs.core.windows.net/Bronze/customer/"
)

# Preview data
display(df_transactions)
display(df_products)
display(df_stores)
display(df_customers)


# =============================================================================
# SECTION 3: TYPE CASTING & SCHEMA ENFORCEMENT
# Optimization: Explicit casting prevents silent type errors in aggregations
# =============================================================================

from pyspark.sql.functions import col

# Cast Transactions columns to correct types
df_transactions = df_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")     # date type enables time-based groupBy
)

# Cast Products columns
df_products = df_products.select(
    col("product_id").cast("int"),
    col("product_name"),                      # string - no cast needed
    col("category"),                          # string - no cast needed
    col("price").cast("double")              # double for precise financial calculations
)

# Cast Stores columns
df_stores = df_stores.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

# Optimization: Deduplicate on product_id to prevent join fan-out
# (customers file had product_id duplicates causing row multiplication)
df_customers = df_customers.select(
    "product_id", "product_name", "category", "price"
).dropDuplicates(["product_id"])


# =============================================================================
# SECTION 4: BUILD SILVER TABLE (Join + Enrichment)
# Silver = Cleaned, joined, enriched data with business logic applied
# =============================================================================

df_silver = df_transactions \
    .join(df_products, "product_id") \
    .join(df_stores, "store_id") \
    .withColumn("total_amount", col("quantity") * col("price"))   # Derived KPI column

# Preview Silver DataFrame schema and data
display(df_silver)


# =============================================================================
# SECTION 5: WRITE SILVER TO ADLS (Delta Format)
# Optimization: Delta Lake enables ACID transactions, time travel, schema enforcement
# mode("overwrite") = idempotent - safe to re-run without duplication
# =============================================================================

silver_path = "abfss://retailer@mystg0poc.dfs.core.windows.net/Silver"
df_silver.write.mode("overwrite").format("delta").save(silver_path)

# Register as Spark SQL table (IF NOT EXISTS = idempotent re-runs)
spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_silver_cleaned
    USING DELTA
    LOCATION 'abfss://retailer@mystg0poc.dfs.core.windows.net/Silver'
""")

# Verify Silver table
# %sql
# select * from retail_silver_cleaned


# =============================================================================
# SECTION 6: READ SILVER & BUILD GOLD AGGREGATIONS
# Gold = Business-ready aggregates for Power BI / reporting consumption
# =============================================================================

silver_df = spark.read.format("delta").load(
    "abfss://retailer@mystg0poc.dfs.core.windows.net/Silver"
)

from pyspark.sql.functions import sum, countDistinct, avg

# Aggregate at grain: date x product x store
# Optimization: Pushes groupBy + agg to Spark engine (not Python row iteration)
gold_df = silver_df.groupBy(
    "transaction_date",
    "product_id", "product_name", "category",
    "store_id", "store_name", "location"
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("total_amount").alias("total_sales_amount"),
    countDistinct("transaction_id").alias("number_of_transactions"),
    avg("total_amount").alias("average_transaction_value")
)

display(gold_df)


# =============================================================================
# SECTION 7: WRITE GOLD TO ADLS (Delta Format)
# =============================================================================

gold_path = "abfss://retailer@mystg0poc.dfs.core.windows.net/Gold"
gold_df.write.mode("overwrite").format("delta").save(gold_path)

# Register Gold table
spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_gold_sales_summary
    USING DELTA
    LOCATION 'abfss://retailer@mystg0poc.dfs.core.windows.net/Gold'
""")

# Verify Gold table
# %sql
# select * from retail_gold_sales_summary


# =============================================================================
# SECTION 8: OPTIONAL - ADDITIONAL QUERIES FOR VALIDATION
# =============================================================================

# Check row counts
print("Transactions:", df_transactions.count())
print("Products:", df_products.count())
print("Stores:", df_stores.count())
print("Silver rows:", df_silver.count())
print("Gold rows:", gold_df.count())

# Quick null check (data quality)
from pyspark.sql.functions import count, when, isnan

null_check = df_silver.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_silver.columns
])
display(null_check)

# Check total sales
from pyspark.sql.functions import round as spark_round
total_sales = gold_df.agg(
    sum("total_sales_amount").alias("grand_total_sales"),
    sum("total_quantity_sold").alias("grand_total_qty"),
    sum("number_of_transactions").alias("grand_total_txns")
)
display(total_sales)


# =============================================================================
# OPTIMIZATION NOTES
# =============================================================================
# 1.  TYPE CASTING:        Ensures correct Spark physical plan — avoids StringType
#                          aggregation errors and implicit cast overhead
# 2.  dropDuplicates():    Prevents join fan-out on product_id in customer file
# 3.  DELTA FORMAT:        ACID compliance, time travel, schema evolution vs Parquet
# 4.  mode("overwrite"):   Makes pipeline idempotent — safe daily reruns
# 5.  IF NOT EXISTS:       Table registration won't fail on re-run
# 6.  groupBy + agg():     Catalyst optimizer pushes to partition-level execution
# 7.  abfss:// protocol:   Azure Data Lake Gen2 secure endpoint (Unity Catalog compat)
# 8.  withColumn():        Single-pass column addition (no extra shuffle)
# =============================================================================
