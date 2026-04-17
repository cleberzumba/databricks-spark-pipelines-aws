# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 5: Reading, Writing, and Partitioning Data
# MAGIC
# MAGIC ## Key Functions You Must Know:
# MAGIC - `spark.read` - Read data
# MAGIC - `df.write` - Write data
# MAGIC - `.mode()` - (append, overwrite, error, ignore)
# MAGIC - `.partitionBy()` 
# MAGIC - `.option()` - Set read/write options
# MAGIC - `.schema()` - Define schema
# MAGIC - File formats: parquet, csv, json, orc, delta, text

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Sample Data and Paths

# COMMAND ----------

# MAGIC %run ./00_Create_Unity_Catalog_Objects

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA default;

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

# Sample data
data = [
    ("John", "Doe", "USA", "Engineering", 75000),
    ("Jane", "Smith", "UK", "Marketing", 65000),
    ("Bob", "Johnson", "USA", "Engineering", 80000),
    ("Alice", "Williams", "Canada", "Sales", 70000),
    ("Charlie", "Brown", "USA", "Marketing", 60000),
    ("Eve", "Davis", "UK", "Engineering", 85000),
]

schema = ["firstName", "lastName", "country", "department", "salary"]
df = spark.createDataFrame(data, schema)

# Define paths (adjust for your environment)
base_path = "/Volumes/workspace/default/spark_cert"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Write Modes
# MAGIC
# MAGIC | Mode | Behavior | Use Case |
# MAGIC |------|----------|----------|
# MAGIC | **append** | Add to existing data | Incremental loads |
# MAGIC | **overwrite** | **Replace all existing data** | Full refresh |
# MAGIC | **error** / **errorifexists** | Fail if data exists (DEFAULT) | Prevent accidents |
# MAGIC | **ignore** | Do nothing if data exists | Skip if exists |

# COMMAND ----------

# MODE 1: overwrite
# Deletes existing data and writes new data
df.write.mode("overwrite").parquet(f"{base_path}/overwrite_test")

# MODE 2: append - Add to existing data
df.write.mode("append").parquet(f"{base_path}/append_test")

# MODE 3: error (default) - Fails if path exists
try:
    df.write.mode("error").parquet(f"{base_path}/overwrite_test")  # Will fail!
except Exception as e:
    print(f"Error as expected: {type(e).__name__}")

# MODE 4: ignore - Silently skip if exists
df.write.mode("ignore").parquet(f"{base_path}/overwrite_test")  # Does nothing

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. partitionBy() - EXAM QUESTION 1!
# MAGIC
# MAGIC ### What partitionBy() Does:
# MAGIC - Creates subdirectories for each partition value
# MAGIC - Example: `country=USA/`, `country=UK/`, `country=Canada/`
# MAGIC - Improves query performance when filtering by partition column
# MAGIC - Commonly used with date columns for time-based partitioning

# COMMAND ----------

df.write.mode("overwrite").partitionBy("country").parquet(f"{base_path}/partitioned_by_country")

# Check the directory structure
dbutils.fs.ls(f"{base_path}/partitioned_by_country")

# COMMAND ----------

# Multiple partition columns
df.write.mode("overwrite").partitionBy("country", "department").parquet(f"{base_path}/multi_partition")

# Creates structure like: country=USA/department=Engineering/
dbutils.fs.ls(f"{base_path}/multi_partition")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reading Partitioned Data
# MAGIC
# MAGIC ### Benefits:
# MAGIC - Partition pruning - only reads relevant partitions
# MAGIC - Faster queries when filtering by partition column

# COMMAND ----------

# Read partitioned data
partitioned_df = spark.read.parquet(f"{base_path}/partitioned_by_country")
partitioned_df.show()
partitioned_df.printSchema()

# Partition pruning - only reads country=USA partition
usa_only = spark.read.parquet(f"{base_path}/partitioned_by_country").filter(col("country") == "USA")
usa_only.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reading Different File Formats

# COMMAND ----------

# === PARQUET (Most common in production) ===
# Columnar format, compressed, schema included
df.write.mode("overwrite").parquet(f"{base_path}/data.parquet")
parquet_df = spark.read.parquet(f"{base_path}/data.parquet")

# === CSV ===
# Write CSV with header
df.write.mode("overwrite").option("header", "true").csv(f"{base_path}/data.csv")

# Read CSV with options
csv_df = (spark.read
    .option("header", "true")      # First row is header
    .option("inferSchema", "true")  # Auto-detect types
    .csv(f"{base_path}/data.csv"))

# CSV with explicit schema (recommended)
schema = StructType([
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("country", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", StringType(), True)
])
csv_df_schema = (spark.read
    .option("header", "true")
    .schema(schema)  # Provide schema instead of inferring
    .csv(f"{base_path}/data.csv"))

# === JSON ===
# Write JSON
df.write.mode("overwrite").json(f"{base_path}/data.json")

# Read JSON
json_df = spark.read.json(f"{base_path}/data.json")

# === ORC ===
# Optimized Row Columnar format
df.write.mode("overwrite").orc(f"{base_path}/data.orc")
orc_df = spark.read.orc(f"{base_path}/data.orc")

# === TEXT (single column) ===
# Writes all columns as single string column
df.write.mode("overwrite").text(f"{base_path}/data.txt")
text_df = spark.read.text(f"{base_path}/data.txt")

# It will fail because of the salary type. Fix it!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Reading Files with Schema
# MAGIC
# MAGIC ### Best Practice:
# MAGIC - Always provide schema instead of inferring
# MAGIC - Faster (no schema inference pass)
# MAGIC - More reliable (no type guessing)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define explicit schema
explicit_schema = StructType([
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("country", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Read with schema (RECOMMENDED)
df_with_schema = (spark.read
    .schema(explicit_schema)
    .option("header", "true")
    .csv(f"{base_path}/data.csv"))

df_with_schema.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. SQL Queries Directly on Files
# MAGIC
# MAGIC - Query files without creating tables
# MAGIC - Works with parquet, json, csv, orc, delta

# COMMAND ----------

# Query Parquet files directly
result = spark.sql(f"""
    SELECT country, COUNT(*) as employee_count
    FROM parquet.`{base_path}/data.parquet`
    GROUP BY country
""")
result.show()

# Query CSV files directly
result = spark.sql(f"""
    SELECT *
    FROM csv.`{base_path}/data.csv`
""")
result.show()

# Query JSON files directly
result = spark.sql(f"""
    SELECT department, AVG(salary) as avg_salary
    FROM json.`{base_path}/data.json`
    GROUP BY department
""")
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Save to Persistent Tables
# MAGIC
# MAGIC ### Table Types:
# MAGIC - Managed tables: Spark manages both data and metadata
# MAGIC - External tables: Spark manages metadata only

# COMMAND ----------

# Create managed table
df.write.mode("overwrite").saveAsTable("employees")

# Create table with partitioning
df.write.mode("overwrite") \
    .partitionBy("country") \
    .saveAsTable("employees_partitioned")

# Read from table
spark.table("employees").show()

# Query with SQL
spark.sql("SELECT * FROM employees WHERE country = 'USA'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Temporary Views
# MAGIC
# MAGIC ### View Types:
# MAGIC - `createOrReplaceTempView()` - Session-scoped
# MAGIC - `createGlobalTempView()` - Application-scoped
# MAGIC - `createOrReplaceGlobalTempView()` - Replace if exists

# COMMAND ----------

# Create temporary view
df.createOrReplaceTempView("employees_temp")

# Query the view
spark.sql("SELECT * FROM employees_temp WHERE salary > 70000").show()

# Create global temp view (accessible across sessions)
# df.createGlobalTempView("employees_global") # Not supported on Serverless compute

# Query global temp view (must use global_temp database)
# spark.sql("SELECT * FROM global_temp.employees_global").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Common Read Options for CSV

# COMMAND ----------

# Comprehensive CSV read with options
csv_with_options = (spark.read
    .option("header", "true")           # First row is header
    .option("inferSchema", "true")      # Auto-detect data types
    .option("delimiter", ",")           # Column delimiter (default is comma)
    .option("quote", "\"")              # Quote character
    .option("escape", "\\")             # Escape character
    .option("nullValue", "NULL")        # String representing null
    .option("dateFormat", "yyyy-MM-dd") # Date format
    .option("mode", "PERMISSIVE")       # Error handling mode
    .csv(f"{base_path}/data.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Writing with Partitioning and Sorting
# MAGIC
# MAGIC ### Performance Optimization:

# COMMAND ----------

# Partition by country, sort by salary within each partition
# (
# df.write
# .mode("overwrite")
# .partitionBy("country") # Physically partitions by country (folders)
# .bucketBy(8, "country") # Create 8 buckets based on country
# .sortBy("salary") # Sorts WITHIN each bucket
# .parquet(f"{base_path}/optimized_output")
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Repartition vs Coalesce
# MAGIC
# MAGIC - `repartition()` - Full shuffle, can increase/decrease partitions
# MAGIC - `coalesce()` - No shuffle, only decrease partitions
# MAGIC
# MAGIC WARNING: rdd functions are not available in Serverless Compute.

# COMMAND ----------

# Check current partitions
# print(f"Original partitions: {df.rdd.getNumPartitions()}")

# Repartition (with shuffle)
df_repartitioned = df.repartition(10)
# print(f"After repartition: {df_repartitioned.rdd.getNumPartitions()}")

# Coalesce (no shuffle, only reduce)
df_coalesced = df.coalesce(2)
# print(f"After coalesce: {df_coalesced.rdd.getNumPartitions()}")

# Repartition by column (hash partitioning)
df_repart_by_col = df.repartition("country")
# print(f"After repartition by country: {df_repart_by_col.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# EXAMAMPLE:
# "Write a DataFrame to a Parquet file, partitioned by the column country,
# and overwrite any existing data at the destination path."

df.write.mode("overwrite").partitionBy("country").parquet(f"{base_path}/exam_answer")

# Verify the result
result = spark.read.parquet(f"{base_path}/exam_answer")
result.show()

# Check partition structure
dbutils.fs.ls(f"{base_path}/exam_answer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Delta Lake (If using Databricks)
# MAGIC
# MAGIC ### Benefits:
# MAGIC - ACID transactions
# MAGIC - Time travel
# MAGIC - Schema enforcement
# MAGIC - Upserts (merge operations)

# COMMAND ----------

# Write as Delta
df.write.mode("overwrite").format("delta").save(f"{base_path}/delta_table")

# Read Delta
delta_df = spark.read.format("delta").load(f"{base_path}/delta_table")

# Save as Delta table
df.write.mode("overwrite").format("delta").saveAsTable("employees_delta")

# Time travel (read older version)
# old_version = spark.read.format("delta").option("versionAsOf", 0).load(path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ```python
# MAGIC # Write with overwrite and partitioning:
# MAGIC df.write.mode("overwrite").partitionBy("country").parquet("/data/output")
# MAGIC ```
# MAGIC
# MAGIC ### Write Modes:
# MAGIC | Mode | What It Does |
# MAGIC |------|--------------|
# MAGIC | **append** | Add to existing data |
# MAGIC | **overwrite** | Replace all data |
# MAGIC | **error** | Fail if exists (DEFAULT) |
# MAGIC | **ignore** | Skip if exists |
# MAGIC
# MAGIC ### File Formats:
# MAGIC - **Parquet** - Columnar, compressed, most common in production
# MAGIC - **CSV** - Text, human-readable
# MAGIC - **JSON** - Nested structures
# MAGIC - **ORC** - Optimized Row Columnar
# MAGIC - **Delta** - ACID transactions (Databricks)
# MAGIC
# MAGIC ### Reading Patterns:
# MAGIC ```python
# MAGIC # With schema (recommended)
# MAGIC spark.read.schema(schema).option("header", "true").csv(path)
# MAGIC
# MAGIC # Infer schema (slower)
# MAGIC spark.read.option("header", "true").option("inferSchema", "true").csv(path)
# MAGIC
# MAGIC # Query files directly
# MAGIC spark.sql("SELECT * FROM parquet.`/path/to/data`")
# MAGIC ```
# MAGIC
# MAGIC ### Partitioning:
# MAGIC ```python
# MAGIC # Single partition column
# MAGIC df.write.partitionBy("country").parquet(path)
# MAGIC
# MAGIC # Multiple partition columns
# MAGIC df.write.partitionBy("year", "month", "day").parquet(path)
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC # Overwrite with partitioning
# MAGIC df.write.mode("overwrite").partitionBy("col").parquet(path)
# MAGIC
# MAGIC # Append new data
# MAGIC df.write.mode("append").parquet(path)
# MAGIC
# MAGIC # Save as table
# MAGIC df.write.mode("overwrite").saveAsTable("table_name")
# MAGIC
# MAGIC # Create temp view for SQL
# MAGIC df.createOrReplaceTempView("view_name")
# MAGIC spark.sql("SELECT * FROM view_name")
# MAGIC ```
