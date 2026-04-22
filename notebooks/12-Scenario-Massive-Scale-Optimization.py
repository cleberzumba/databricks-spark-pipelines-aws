# Notebook 12: Realistic Interview Scenario - Massive Scale Spark/PySpark Optimization (600TB)

- Realistic large-scale Spark optimization scenario
- Join strategy analysis
- Broadcast vs shuffle
- Pre-aggregation before joins
- Column pruning
- Early filtering
- Partitioning strategy
- Skew awareness
- `explain()` for execution plan validation

---

## Scenario

You are working on a financial data pipeline running on Spark in a distributed environment such as EMR or Databricks.

The pipeline processes around **600TB** of historical and incremental data.

The goal is to generate an analytical dataset with:
- contract information
- customer information
- institution information
- product category
- total transaction amount
- transaction count
- ticket classification
- aggregated indicators by economic group

---

## Input Tables

### 1. `contracts`
Main contracts table.

**Volume:** 8 billion rows

Columns:
- `contract_id`
- `customer_id`
- `institution_id`
- `product_id`
- `contract_date`
- `status`
- `amount`
- `region`

### 2. `customers`
Customer master data.

**Volume:** 500 million rows

Columns:
- `customer_id`
- `customer_name`
- `customer_type`
- `city`
- `state`
- `birth_date`

### 3. `institutions`
Financial institutions dimension.

**Volume:** 5 thousand rows

Columns:
- `institution_id`
- `institution_name`
- `group_name`
- `tier`

### 4. `products`
Products dimension.

**Volume:** 20 thousand rows

Columns:
- `product_id`
- `product_name`
- `category`
- `subcategory`

### 5. `transactions`
Financial transactions by contract.

**Volume:** 30 billion rows

Columns:
- `transaction_id`
- `contract_id`
- `transaction_date`
- `transaction_type`
- `transaction_amount`

---

## Problem Statement

The original Spark job is too slow and suffers from:
- massive shuffle
- disk spill
- long-running stages
- poor join ordering
- unnecessary columns
- possible skew

The task is to optimize the job.

---

## Bad Initial Version

from pyspark.sql.functions import *

df = contracts.join(customers, contracts.customer_id == customers.customer_id, "left") \
    .join(institutions, contracts.institution_id == institutions.institution_id, "left") \
    .join(products, contracts.product_id == products.product_id, "left") \
    .join(transactions, contracts.contract_id == transactions.contract_id, "left")

df = df.withColumn(
    "ticket_class",
    when(col("transaction_amount") > 100000, "HIGH")
    .when(col("transaction_amount") > 50000, "MEDIUM")
    .otherwise("LOW")
)

result = df.groupBy(
    "group_name",
    "institution_name",
    "category",
    "customer_type",
    "state"
).agg(
    sum("transaction_amount").alias("total_amount"),
    count("transaction_id").alias("transaction_count"),
    countDistinct("contract_id").alias("unique_contracts")
)

result.write.mode("overwrite").parquet("/final/output/")
