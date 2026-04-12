# Delta Lake

## What is Delta Lake?

Delta Lake is an **open-source storage layer** that runs on top of existing data lakes (such as S3, ADLS, or DBFS). It brings **reliability and performance** to big data workloads by adding a transaction log on top of Parquet files.

In simple terms: it transforms a regular folder of files into a **reliable, versioned, and queryable table**.

---

## Why Delta Lake?

Traditional data lakes store raw files (CSV, JSON, Parquet) without any control over concurrent writes, schema changes, or failed jobs. Delta Lake solves these problems by introducing:

- **ACID Transactions** — guarantees that operations are complete and consistent
- **Schema Enforcement** — rejects data that doesn't match the table schema
- **Schema Evolution** — allows adding new columns safely over time
- **Time Travel** — lets you query previous versions of the data
- **Unified Batch and Streaming** — the same table can be used for both

---

## How It Works

Every Delta table has a `_delta_log/` folder that stores a **transaction log** — a history of every change made to the table (inserts, updates, deletes, schema changes).

```
my_table/
├── _delta_log/
│   ├── 00000000000000000000.json
│   ├── 00000000000000000001.json
│   └── ...
├── part-00000.parquet
├── part-00001.parquet
└── ...
```

When you read a Delta table, Spark reads the transaction log first to understand the current state of the data.

---

## ACID Transactions

| Property | Description |
|---|---|
| **Atomicity** | An operation either completes fully or not at all |
| **Consistency** | Data always remains in a valid state |
| **Isolation** | Concurrent operations don't interfere with each other |
| **Durability** | Once committed, changes are permanent |

---

## Time Travel

Delta Lake keeps a history of all changes. You can query any previous version of a table:

```python
# Query by version number
df = spark.read.format("delta").option("versionAsOf", 2).load("/path/to/table")

# Query by timestamp
df = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load("/path/to/table")
```

---

## Key Operations

```python
# Read a Delta table
df = spark.read.format("delta").load("/Volumes/workspace/default/spark_dev/my_table")

# Write (overwrite)
df.write.format("delta").mode("overwrite").save("/path/to/table")

# Write (append)
df.write.format("delta").mode("append").save("/path/to/table")

# Read table history
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "/path/to/table")
dt.history().show()
```

---

## Delta Lake vs Plain Parquet

| Feature | Parquet | Delta Lake |
|---|---|---|
| ACID Transactions | No | Yes |
| Schema Enforcement | No | Yes |
| Time Travel | No | Yes |
| Upserts (MERGE) | No | Yes |
| Streaming Support | Limited | Yes |
| Transaction Log | No | Yes |

---

## Where Delta Lake Fits in This Project

In this project, Delta Lake is used as the **storage format for all pipeline layers**:

- **Bronze** → raw data stored as Delta
- **Silver** → cleaned data stored as Delta
- **Gold** → aggregated data stored as Delta

This ensures data reliability and enables rollback if something goes wrong during processing.
