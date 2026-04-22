# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 12: Realistic Interview Scenario - Massive Scale Spark/PySpark Optimization (600TB)
# MAGIC
# MAGIC - Realistic large-scale Spark optimization scenario
# MAGIC - Join strategy analysis
# MAGIC - Broadcast vs shuffle
# MAGIC - Pre-aggregation before joins
# MAGIC - Column pruning
# MAGIC - Early filtering
# MAGIC - Partitioning strategy
# MAGIC - Skew awareness
# MAGIC - `explain()` for execution plan validation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario
# MAGIC
# MAGIC You are working on a financial data pipeline running on Spark in a distributed environment such as EMR or Databricks.
# MAGIC
# MAGIC The pipeline processes around **600TB** of historical and incremental data.
# MAGIC
# MAGIC The goal is to generate an analytical dataset with:
# MAGIC - contract information
# MAGIC - customer information
# MAGIC - institution information
# MAGIC - product category
# MAGIC - total transaction amount
# MAGIC - transaction count
# MAGIC - ticket classification
# MAGIC - aggregated indicators by economic group

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input Tables
# MAGIC
# MAGIC ### 1. `contracts`
# MAGIC Main contracts table.
# MAGIC
# MAGIC **Volume:** 8 billion rows
# MAGIC
# MAGIC Columns:
# MAGIC - `contract_id`
# MAGIC - `customer_id`
# MAGIC - `institution_id`
# MAGIC - `product_id`
# MAGIC - `contract_date`
# MAGIC - `status`
# MAGIC - `amount`
# MAGIC - `region`
# MAGIC
# MAGIC ### 2. `customers`
# MAGIC Customer master data.
# MAGIC
# MAGIC **Volume:** 500 million rows
# MAGIC
# MAGIC Columns:
# MAGIC - `customer_id`
# MAGIC - `customer_name`
# MAGIC - `customer_type`
# MAGIC - `city`
# MAGIC - `state`
# MAGIC - `birth_date`
# MAGIC
# MAGIC ### 3. `institutions`
# MAGIC Financial institutions dimension.
# MAGIC
# MAGIC **Volume:** 5 thousand rows
# MAGIC
# MAGIC Columns:
# MAGIC - `institution_id`
# MAGIC - `institution_name`
# MAGIC - `group_name`
# MAGIC - `tier`
# MAGIC
# MAGIC ### 4. `products`
# MAGIC Products dimension.
# MAGIC
# MAGIC **Volume:** 20 thousand rows
# MAGIC
# MAGIC Columns:
# MAGIC - `product_id`
# MAGIC - `product_name`
# MAGIC - `category`
# MAGIC - `subcategory`
# MAGIC
# MAGIC ### 5. `transactions`
# MAGIC Financial transactions by contract.
# MAGIC
# MAGIC **Volume:** 30 billion rows
# MAGIC
# MAGIC Columns:
# MAGIC - `transaction_id`
# MAGIC - `contract_id`
# MAGIC - `transaction_date`
# MAGIC - `transaction_type`
# MAGIC - `transaction_amount`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Problem Statement
# MAGIC
# MAGIC The original Spark job is too slow and suffers from:
# MAGIC - massive shuffle
# MAGIC - disk spill
# MAGIC - long-running stages
# MAGIC - poor join ordering
# MAGIC - unnecessary columns
# MAGIC - possible skew
# MAGIC
# MAGIC The task is to optimize the job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bad Initial Version

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Problems in the Initial Version
# MAGIC
# MAGIC - Implicit `select *` behavior
# MAGIC - Joins are performed before reducing data volume
# MAGIC - `transactions` is joined too early
# MAGIC - Small dimension tables are not broadcast
# MAGIC - High shuffle cost
# MAGIC - Expensive `countDistinct`
# MAGIC - Possible skew on business keys
# MAGIC - No explicit write partitioning strategy

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimized Version

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    sum,
    count,
    countDistinct,
    when,
    broadcast
)

# 1. Select only required columns (column pruning)
contracts_sel = contracts.select(
    "contract_id",
    "customer_id",
    "institution_id",
    "product_id",
    "status",
    "amount",
    "region"
)

customers_sel = customers.select(
    "customer_id",
    "customer_type",
    "state"
)

institutions_sel = institutions.select(
    "institution_id",
    "institution_name",
    "group_name"
)

products_sel = products.select(
    "product_id",
    "category"
)

transactions_sel = transactions.select(
    "contract_id",
    "transaction_id",
    "transaction_amount"
)

# 2. Early filter (reduces dataset before joins)
contracts_filt = contracts_sel.filter(col("status") == "ACTIVE")

# 3. Pre-aggregation (critical optimization)
transactions_agg = transactions_sel.groupBy("contract_id").agg(
    sum("transaction_amount").alias("total_transaction_amount"),
    count("transaction_id").alias("transaction_count")
)

# 4. Optimized joins
df = (
    contracts_filt
    .join(customers_sel, "customer_id", "left")  # no broadcast (large table)
    .join(broadcast(institutions_sel), "institution_id", "left")  # small dimension
    .join(broadcast(products_sel), "product_id", "left")  # small dimension
    .join(transactions_agg, "contract_id", "left")  # already reduced
)

# 5. Derive after reduction
df = df.withColumn(
    "ticket_class",
    when(col("total_transaction_amount") > 100000, "HIGH")
    .when(col("total_transaction_amount") > 50000, "MEDIUM")
    .otherwise("LOW")
)

# 6. Final aggregation
result = df.groupBy(
    "group_name",
    "institution_name",
    "category",
    "customer_type",
    "state",
    "ticket_class"
).agg(
    sum("total_transaction_amount").alias("total_amount"),
    sum("transaction_count").alias("transaction_count"),
    countDistinct("contract_id").alias("unique_contracts")
)

# 7. Controlled write (avoid small files / improve parallelism)
result.repartition("group_name") \
    .write \
    .mode("overwrite") \
    .parquet("/final/output/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why This Version Is Better
# MAGIC
# MAGIC ### 1. Column Pruning
# MAGIC Only required columns are selected before joins.
# MAGIC
# MAGIC ### 2. Early Filtering
# MAGIC `contracts` is filtered before joining, reducing the dataset early.
# MAGIC
# MAGIC ### 3. Pre-Aggregation of `transactions`
# MAGIC This is one of the biggest improvements.
# MAGIC
# MAGIC Instead of joining:
# MAGIC - `contracts` (8B rows)
# MAGIC with
# MAGIC - `transactions` (30B rows)
# MAGIC
# MAGIC we first aggregate `transactions` by `contract_id`, drastically reducing join volume.
# MAGIC
# MAGIC ### 4. Broadcast Join on Small Dimensions
# MAGIC `institutions` and `products` are small enough to broadcast.
# MAGIC
# MAGIC This avoids expensive shuffle for those joins.
# MAGIC
# MAGIC ### 5. Better Transformation Order
# MAGIC Derived columns are created after reducing data volume.
# MAGIC
# MAGIC ### 6. Write Partitioning
# MAGIC Repartitioning before writing helps control file layout and downstream performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why `customers` Should Not Be Broadcast

# COMMAND ----------

# Do NOT do this
# broadcast(customers_sel)

# customers has 500 million rows, so it is not a small dimension table.
# Broadcasting it would be unsafe and inefficient.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative for Expensive Distinct Count
# MAGIC
# MAGIC If exact precision is not mandatory, consider `approx_count_distinct()`.

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct

result_approx = df.groupBy(
    "group_name",
    "institution_name",
    "category",
    "customer_type",
    "state",
    "ticket_class"
).agg(
    sum("total_transaction_amount").alias("total_amount"),
    sum("transaction_count").alias("transaction_count"),
    approx_count_distinct("contract_id").alias("unique_contracts_approx")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution Plan Validation
# MAGIC
# MAGIC Use `explain()` to inspect whether the optimization is actually working.
# MAGIC
# MAGIC Look for:
# MAGIC - `BroadcastHashJoin`
# MAGIC - `SortMergeJoin`
# MAGIC - `Exchange`
# MAGIC - `HashAggregate`
# MAGIC
# MAGIC Interpretation:
# MAGIC - `BroadcastHashJoin` -> broadcast is being used
# MAGIC - `Exchange` -> shuffle is happening
# MAGIC - Too many `Exchange` nodes -> possible performance problem
# MAGIC - Heavy `SortMergeJoin` on huge datasets -> expensive operation

# COMMAND ----------

result.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Possible Skew Problem
# MAGIC
# MAGIC A real production issue may happen if a key such as `institution_id` is highly imbalanced.
# MAGIC
# MAGIC For example:
# MAGIC - one institution may concentrate 40% of all contracts
# MAGIC
# MAGIC This can lead to:
# MAGIC - skewed partitions
# MAGIC - slow tasks
# MAGIC - executor imbalance
# MAGIC - long tails in Spark stages

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to Do If There Is Skew
# MAGIC
# MAGIC Possible strategies:
# MAGIC - broadcast small side if possible
# MAGIC - filter before join
# MAGIC - salting for skewed keys
# MAGIC - Adaptive Query Execution (AQE)
# MAGIC - skew join optimization
# MAGIC - repartition more intelligently

# COMMAND ----------

from pyspark.sql.functions import rand

# Example only: add salt to distribute skewed keys
contracts_salted = contracts_filt.withColumn(
    "salt",
    (rand() * 10).cast("int")
)

# This is a simplified pattern.
# In real projects, salting must be coordinated on both sides of the join.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview-Style Explanation
# MAGIC
# MAGIC A strong answer in an interview would be:
# MAGIC
# MAGIC > I would first reduce the data volume before joining by selecting only required columns and filtering contracts as early as possible. 
# MAGIC Then I would pre-aggregate the transactions table before joining it to the main dataset, since joining 8 billion rows with 30 billion rows directly would create a massive shuffle. 
# MAGIC I would broadcast only truly small dimensions such as institutions and products to avoid unnecessary shuffle, but I would not broadcast customers because it is too large. 
# MAGIC Finally, I would validate the physical plan with `explain()` and inspect Spark UI for `BroadcastHashJoin`, `Exchange`, skew symptoms, and expensive `SortMergeJoin` operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Optimization Takeaways
# MAGIC
# MAGIC - Select only necessary columns
# MAGIC - Filter as early as possible
# MAGIC - Aggregate huge fact tables before joining
# MAGIC - Broadcast only truly small tables
# MAGIC - Inspect `explain()` and Spark UI
# MAGIC - Watch for skew on hot keys
# MAGIC - Control write partitioning
# MAGIC - Avoid blindly joining huge tables first

# COMMAND ----------
