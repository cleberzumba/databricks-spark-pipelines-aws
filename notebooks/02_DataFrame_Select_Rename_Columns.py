# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1: DataFrame Basics - Selecting, Renaming, and Manipulating Columns
# MAGIC
# MAGIC ## Key Functions:
# MAGIC - `select()` - Select specific columns
# MAGIC - `selectExpr()` - Select with SQL expressions
# MAGIC - `col()` / `column()` - Reference columns
# MAGIC - `withColumn()` - Add or replace a column
# MAGIC - `withColumnRenamed()` - Rename a single column
# MAGIC - `drop()` - Remove columns
# MAGIC - `alias()` - Rename columns in expressions
# MAGIC - `cast()` - Change data types
# MAGIC - `lit()` - Create literal values

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Sample DataFrame

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, concat, upper, lower, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Create sample employee data
data = [
    ("John", "Doe", "Engineering", 75000, "USA"),
    ("Jane", "Smith", "Marketing", 65000, "UK"),
    ("Bob", "Johnson", "Engineering", 80000, "USA"),
    ("Alice", "Williams", "Sales", 70000, "Canada"),
    ("Charlie", "Brown", "Marketing", 60000, "USA")
]

# Define schema
schema = StructType([
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("country", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Display the original DataFrame
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Selecting Columns
# MAGIC
# MAGIC - Most basic DataFrame operation
# MAGIC - Multiple syntax variations
# MAGIC - Foundation for more complex transformations

# COMMAND ----------

# METHOD 1: Using string column names (simplest)
df.select("firstName", "department", "salary").show()

# METHOD 2: Using col() function (recommended - more flexible)
df.select(col("firstName"), col("department"), col("salary")).show()

# METHOD 3: Using DataFrame column reference
df.select(df.firstName, df.department, df.salary).show()

# METHOD 4: Using select with wildcard (select all)
df.select("*").show()

# METHOD 5: Selecting columns with transformations
df.select(
    col("firstName"),
    col("salary"),
    (col("salary") * 1.1).alias("salary_with_bonus")  # Calculate new column inline
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. selectExpr() - SQL Expressions in Select
# MAGIC
# MAGIC - Allows SQL syntax directly in select
# MAGIC - Very powerful for calculations and transformations

# COMMAND ----------

# Using SQL expressions directly
df.selectExpr(
    "firstName",
    "salary",
    "salary * 1.1 AS salary_with_bonus",  # SQL-style calculation
    "CONCAT(firstName, ' ', lastName) AS fullName",  # SQL CONCAT
    "UPPER(department) AS dept_upper"  # SQL UPPER
).show()

# Equivalent using functions (both are valid)
df.select(
    col("firstName"),
    col("salary"),
    (col("salary") * 1.1).alias("salary_with_bonus"),
    concat(col("firstName"), lit(" "), col("lastName")).alias("fullName"),
    upper(col("department")).alias("dept_upper")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. withColumn() - Add or Replace Columns
# MAGIC
# MAGIC - **REPLACES** the column if it already exists
# MAGIC - **ADDS** a new column if it doesn't exist
# MAGIC - Returns a NEW DataFrame (immutability!)
# MAGIC - Can chain multiple withColumn() calls

# COMMAND ----------

# Add a new column
df_with_bonus = df.withColumn("bonus", col("salary") * 0.1)
display(df_with_bonus)

# Replace an existing column
df_salary_updated = df.withColumn("salary", col("salary") * 1.05)  # 5% raise
display(df_salary_updated)

# Chain multiple withColumn operations
df_transformed = (df
    .withColumn("fullName", concat(col("firstName"), lit(" "), col("lastName")))
    .withColumn("annual_bonus", col("salary") * 0.15)
    .withColumn("total_compensation", col("salary") + col("annual_bonus"))
)
display(df_transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. withColumnRenamed() - Rename Columns
# MAGIC
# MAGIC - Can chain multiple renames
# MAGIC - First parameter: existing name, Second parameter: new name

# COMMAND ----------

# Rename a single column
df_renamed = df.withColumnRenamed("firstName", "first_name")
display(df_renamed)

# Rename multiple columns (chaining)
df_all_renamed = (df
    .withColumnRenamed("firstName", "first_name")
    .withColumnRenamed("lastName", "last_name")
    .withColumnRenamed("department", "dept")
)
display(df_all_renamed)

# Rename and transform together
df_complex = (df
    .withColumnRenamed("firstName", "first_name")
    .withColumn("salary_category",
                when(col("salary") > 70000, "High")
                .when(col("salary") > 60000, "Medium")
                .otherwise("Low"))
)
display(df_complex)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. alias() - Rename Columns in Expressions
# MAGIC
# MAGIC ### Key Difference from withColumnRenamed():
# MAGIC - `alias()` is used DURING selection/transformation
# MAGIC - `withColumnRenamed()` is used AFTER the DataFrame exists
# MAGIC - Both achieve renaming but in different contexts

# COMMAND ----------

# Using alias in select
df.select(
    col("firstName").alias("first_name"),
    col("lastName").alias("last_name"),
    col("salary").alias("annual_salary")
).show()

# Using alias with calculations
df.select(
    col("firstName"),
    (col("salary") * 12).alias("yearly_salary"),
    (col("salary") / 12).alias("monthly_salary")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. drop() - Remove Columns
# MAGIC
# MAGIC - Drop single or multiple columns
# MAGIC - Can use string names or Column objects
# MAGIC - Returns new DataFrame

# COMMAND ----------

# Drop a single column
df.drop("country").show()

# Drop multiple columns
df.drop("firstName", "lastName").show()

# Drop using Column object
df.drop(col("country")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. cast() - Change Data Types
# MAGIC
# MAGIC - Converting between data types is frequently used
# MAGIC - Two syntax options: `.cast()` method or `cast()` function
# MAGIC - Common types: StringType, IntegerType, DoubleType, DateType, TimestampType

# COMMAND ----------

# Cast salary to Double
df_casted = df.withColumn("salary", col("salary").cast("double"))
df_casted.printSchema()

# Alternative syntax using DoubleType
from pyspark.sql.types import DoubleType
df_casted2 = df.withColumn("salary", col("salary").cast(DoubleType()))
df_casted2.printSchema()

# Multiple casts
df_multi_cast = df.select(
    col("firstName"),
    col("salary").cast("string").alias("salary_str"),  # Int to String
    col("salary").cast("double").alias("salary_double")  # Int to Double
)
display(df_multi_cast)
df_multi_cast.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. lit() - Create Literal/Constant Values
# MAGIC
# MAGIC - Adding constant columns
# MAGIC - Concatenating strings with literals
# MAGIC - Default values in conditional logic

# COMMAND ----------

# Add a constant column
df.withColumn("company", lit("TechCorp")).show()

# Use lit in concatenation
df.select(
    concat(col("firstName"), lit(" "), col("lastName")).alias("fullName"),
    col("salary")
).show()

# Use lit in conditional logic
df.withColumn("status",
    when(col("salary") > 70000, lit("Senior"))
    .otherwise(lit("Junior"))
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.
# MAGIC
# MAGIC Replace the `firstName` column with uppercase version named `first_name`,
# MAGIC and simultaneously rename `department` to `dept` in the DataFrame.

# COMMAND ----------

# ANSWER - This is the correct pattern:
result = (df
    .withColumn("first_name", upper(col("firstName")))  # Create new column with uppercase
    .drop("firstName")  # Remove old column
    .withColumnRenamed("department", "dept")  # Rename department
)
display(result)

# Alternative approach using select (also correct):
result2 = df.select(
    upper(col("firstName")).alias("first_name"),
    col("lastName"),
    col("department").alias("dept"),
    col("salary"),
    col("country")
)
display(result2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Combined Operations
# MAGIC
# MAGIC - Chaining multiple operations
# MAGIC - Renaming + Transforming simultaneously
# MAGIC - Understanding immutability (each operation returns NEW DataFrame)

# COMMAND ----------

# Complex transformation chain
final_df = (df
    .withColumn("fullName", concat(col("firstName"), lit(" "), col("lastName")))
    .withColumn("salary_category",
        when(col("salary") > 70000, "High")
        .when(col("salary") > 60000, "Medium")
        .otherwise("Low"))
    .withColumnRenamed("department", "dept")
    .drop("firstName", "lastName")
    .select("fullName", "dept", "salary", "salary_category", "country")
)
display(final_df)

# COMMAND ----------
