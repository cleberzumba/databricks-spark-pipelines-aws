# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 09: Spark Architecture & Components
# MAGIC
# MAGIC - Section 1: Apache Spark Architecture and Components
# MAGIC   - Identify advantages and challenges of implementing Spark
# MAGIC   - Identify role of core components
# MAGIC   - Describe architecture including DataFrame, Dataset, SparkSession
# MAGIC   - Explain execution hierarchy
# MAGIC   - Configure Spark partitioning
# MAGIC   - Describe execution patterns (actions, transformations, lazy evaluation)
# MAGIC   - Identify features of Spark modules
# MAGIC - Section 6: Spark Connect deployment modes
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **Cluster Roles**: Driver, Executor, Cluster Manager, Worker Node
# MAGIC - **Execution Hierarchy**: Job → Stage → Task
# MAGIC - **Deployment Modes**: Client, Cluster, Local
# MAGIC - **Transformations**: Narrow vs Wide
# MAGIC - **Actions**: What triggers execution
# MAGIC - **Lazy Evaluation**: How Spark optimizes
# MAGIC - **Spark Connect**: Architecture and benefits

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 1: Cluster Roles & Architecture
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Spark Cluster Components
# MAGIC
# MAGIC ### The Four Key Components:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │                    CLUSTER MANAGER                      │
# MAGIC │         (YARN, Kubernetes, Standalone, Mesos)           │
# MAGIC │              Allocates Resources                        │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC                           │
# MAGIC                           │ requests resources
# MAGIC                           ▼
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │                    DRIVER NODE                          │
# MAGIC │  ┌─────────────────────────────────────────────┐        │
# MAGIC │  │         SparkContext / SparkSession         │        │
# MAGIC │  │  • Maintains metadata                       │        │
# MAGIC │  │  • Schedules jobs                           │        │
# MAGIC │  │  • Creates DAG (execution plan)             │        │
# MAGIC │  │  • Coordinates executors                    │        │
# MAGIC │  │  • Does NOT execute tasks (unless Local)    │        │
# MAGIC │  └─────────────────────────────────────────────┘        │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC                           │
# MAGIC                           │ sends tasks
# MAGIC                           ▼
# MAGIC ┌──────────────────┬──────────────────┬──────────────────┐
# MAGIC │  WORKER NODE 1   │  WORKER NODE 2   │  WORKER NODE 3   │
# MAGIC │  ┌────────────┐  │  ┌────────────┐  │  ┌────────────┐  │
# MAGIC │  │ Executor 1 │  │  │ Executor 2 │  │  │ Executor 3 │  │
# MAGIC │  │ • Runs     │  │  │ • Runs     │  │  │ • Runs     │  │
# MAGIC │  │   tasks    │  │  │   tasks    │  │  │   tasks    │  │
# MAGIC │  │ • Stores   │  │  │ • Stores   │  │  │ • Stores   │  │
# MAGIC │  │   data     │  │  │   data     │  │  │   data     │  │
# MAGIC │  └────────────┘  │  └────────────┘  │  └────────────┘  │
# MAGIC └──────────────────┴──────────────────┴──────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Component Responsibilities 
# MAGIC
# MAGIC ### Driver Node:
# MAGIC - **Maintains SparkContext/SparkSession**
# MAGIC - **Schedules jobs and creates DAG**
# MAGIC - **Holds metadata** (schema, partition info)
# MAGIC - **Coordinates executors**
# MAGIC - **Collects results** from executors
# MAGIC - **Does NOT execute tasks** (except Local mode)
# MAGIC
# MAGIC ### Executor:
# MAGIC - **Executes tasks** assigned by driver
# MAGIC - **Stores data** in memory/disk (caching)
# MAGIC - **Returns results** to driver
# MAGIC - **Runs on worker nodes**
# MAGIC - **Cannot schedule jobs**
# MAGIC
# MAGIC ### Cluster Manager:
# MAGIC - **Allocates resources** (CPU, memory)
# MAGIC - **Manages worker nodes**
# MAGIC - **Types**: YARN, Kubernetes, Standalone, Mesos
# MAGIC
# MAGIC ### Worker Node:
# MAGIC - **Physical/virtual machine**
# MAGIC - **Hosts executors**
# MAGIC - **Provides compute resources**

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 2: Execution Hierarchy
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Execution Hierarchy - Job → Stage → Task
# MAGIC
# MAGIC ```
# MAGIC ACTION (e.g., count(), show(), write())
# MAGIC    │
# MAGIC    ▼
# MAGIC ┌─────────────────────────────────────────┐
# MAGIC │              JOB                        │  ← Triggered by Action
# MAGIC │  "Count all records in DataFrame"       │
# MAGIC └─────────────────────────────────────────┘
# MAGIC    │
# MAGIC    ▼ (divided by shuffle boundaries)
# MAGIC ┌─────────────────────────────────────────┐
# MAGIC │          STAGE 1                        │  ← Narrow transformations
# MAGIC │  Read → Filter → Map                    │     (no shuffle)
# MAGIC └─────────────────────────────────────────┘
# MAGIC    │
# MAGIC    ▼ SHUFFLE (wide transformation)
# MAGIC ┌─────────────────────────────────────────┐
# MAGIC │          STAGE 2                        │  ← Next stage after shuffle
# MAGIC │  GroupBy → Aggregate                    │
# MAGIC └─────────────────────────────────────────┘
# MAGIC    │
# MAGIC    ▼ (divided by partitions)
# MAGIC ┌─────────┬─────────┬─────────┬─────────┐
# MAGIC │ TASK 1  │ TASK 2  │ TASK 3  │ TASK 4  │  ← One task per partition
# MAGIC │ Part 1  │ Part 2  │ Part 3  │ Part 4  │     Smallest unit of work
# MAGIC └─────────┴─────────┴─────────┴─────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Facts:
# MAGIC - **Job**: Triggered by an **Action** (count, show, write, collect)
# MAGIC - **Stage**: Divided by **shuffle boundaries** (wide transformations)
# MAGIC - **Task**: Smallest unit of work, **one per partition**
# MAGIC - **Each executor can run multiple tasks** (based on cores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Understanding Stages - What Causes Stage Boundaries?
# MAGIC
# MAGIC ### Wide Transformations = New Stage:
# MAGIC - `groupBy()` / `groupByKey()`
# MAGIC - `reduceByKey()`
# MAGIC - `join()` (usually)
# MAGIC - `repartition()`
# MAGIC - `distinct()`
# MAGIC - `sortBy()` / `orderBy()`
# MAGIC
# MAGIC ### Narrow Transformations = Same Stage:
# MAGIC - `map()` / `flatMap()`
# MAGIC - `filter()` / `where()`
# MAGIC - `select()`
# MAGIC - `withColumn()`
# MAGIC - `drop()`

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg

# Create sample data
data = [
    ("Alice", "Engineering", 75000),
    ("Bob", "Engineering", 80000),
    ("Charlie", "Marketing", 65000),
    ("David", "Marketing", 70000),
    ("Eve", "Sales", 72000),
]

df = spark.createDataFrame(data, ["name", "department", "salary"])

# EXAMPLE: Analyze the execution plan
# This will show stages and shuffles
result = (df
    .filter(col("salary") > 65000)     # Narrow - Stage 1
    .groupBy("department")              # Wide - NEW Stage 2 (shuffle!)
    .agg(
        count("*").alias("num_employees"),
        avg("salary").alias("avg_salary")
    )
)

# Display explain plan
print("=== EXECUTION PLAN ===")
result.explain()

# MAGIC %md
# MAGIC **Look for:**
# MAGIC - `Exchange` = Shuffle operation = Stage boundary
# MAGIC - `HashAggregate` = Aggregation operation
# MAGIC - `Filter` = Filtering (narrow transformation)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 3: Deployment Modes
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Deployment Modes
# MAGIC
# MAGIC ### CRITICAL: Know the differences!
# MAGIC
# MAGIC | Mode | Driver Location | Use Case | Executors |
# MAGIC |-------------|----------------------------------|------------------------|---------------------------------------|
# MAGIC | **Local**   | Same JVM                         | Development, Testing   | **No remote executors!** Uses threads |
# MAGIC | **Client**  | Submission machine (your laptop) | Interactive, Debugging | Remote worker nodes                   |
# MAGIC | **Cluster** | Inside cluster (worker node)     | Production             | Remote worker nodes                   |
# MAGIC
# MAGIC **"Which Spark deployment mode requires all executors to run on a single worker node?"**
# MAGIC
# MAGIC **Answer: Local mode**
# MAGIC
# MAGIC ### Why Local Mode is Special:
# MAGIC - Runs in **single JVM**
# MAGIC - Uses **threads** instead of executors
# MAGIC - **No network communication**
# MAGIC - Driver **executes tasks** itself
# MAGIC - Perfect for **development/testing**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Client Mode vs Cluster Mode
# MAGIC
# MAGIC ### Client Mode:
# MAGIC ```
# MAGIC Your Laptop                    Spark Cluster
# MAGIC ┌──────────┐                  ┌──────────────────┐
# MAGIC │  Driver  │ ←────────────→   │   Executors      │
# MAGIC │  (Here)  │   sends tasks    │  (Worker Nodes)  │
# MAGIC └──────────┘   gets results   └──────────────────┘
# MAGIC ```
# MAGIC **Use when:**
# MAGIC - Interactive sessions (notebooks, shell)
# MAGIC - Debugging
# MAGIC - You need immediate feedback
# MAGIC
# MAGIC **Problem:**
# MAGIC - If your laptop disconnects, job fails
# MAGIC - Network latency for large results
# MAGIC
# MAGIC ### Cluster Mode:
# MAGIC ```
# MAGIC Your Laptop                    Spark Cluster
# MAGIC ┌──────────┐                  ┌──────────────────┐
# MAGIC │  Submit  │ ──────────────→  │  Driver (inside) │
# MAGIC │   Job    │                  │                  │
# MAGIC └──────────┘                  │   Executors      │
# MAGIC      ↓                        └──────────────────┘
# MAGIC   Done!                           ↑
# MAGIC                                   │
# MAGIC                             Runs independently
# MAGIC ```
# MAGIC **Use when:**
# MAGIC - Production workloads
# MAGIC - Long-running jobs
# MAGIC - Submitting jobs and disconnecting
# MAGIC
# MAGIC **Advantage:**
# MAGIC - Driver failure doesn't affect cluster
# MAGIC - Better for production

# COMMAND ----------

# Check current deployment mode (in Databricks)
print(f"Spark Master: {spark.sparkContext.master}")
print(f"App Name: {spark.sparkContext.appName}")
print(f"Spark Version: {spark.version}")

# In Databricks, you'll typically see "local[*]" or cluster info

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 4: Transformations vs Actions
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Transformations vs Actions 
# MAGIC
# MAGIC ### Transformations (Lazy):
# MAGIC - **Return a DataFrame/RDD**
# MAGIC - **Not executed immediately**
# MAGIC - **Build execution plan (DAG)**
# MAGIC - **Can be chained**
# MAGIC
# MAGIC **Examples:**
# MAGIC ```python
# MAGIC select(), filter(), where(), withColumn(), groupBy(),
# MAGIC join(), distinct(), orderBy(), limit()
# MAGIC ```
# MAGIC
# MAGIC ### Actions (Eager):
# MAGIC - **Trigger execution**
# MAGIC - **Return results to driver**
# MAGIC - **Create Jobs**
# MAGIC - **Actual computation happens**
# MAGIC
# MAGIC **Examples:**
# MAGIC ```python
# MAGIC show(), count(), collect(), first(), take(),
# MAGIC write(), save(), saveAsTable()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Lazy Evaluation - Why Spark Does This
# MAGIC
# MAGIC ### Example: Building a Query

# COMMAND ----------

# These are ALL transformations (lazy - nothing happens yet!)
lazy_df = (df
    .filter(col("salary") > 70000)      # Transformation 1
    .withColumn("bonus", col("salary") * 0.1)  # Transformation 2
    .select("name", "department", "bonus")     # Transformation 3
    .orderBy("bonus", ascending=False)         # Transformation 4
)

print("All transformations defined - NO execution yet!")
print("Type:", type(lazy_df))  # Still just a DataFrame

# COMMAND ----------

# NOW trigger execution with an action
print("\n Calling show() - NOW execution happens!")
lazy_df.show()  # ACTION - triggers entire pipeline!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benefits of Lazy Evaluation:
# MAGIC
# MAGIC 1. **Optimization**: Spark can optimize the entire plan
# MAGIC    - Combine filters
# MAGIC    - Push down predicates
# MAGIC    - Reorder operations
# MAGIC
# MAGIC 2. **Efficiency**: Only compute what's needed
# MAGIC    - `.limit(10)` means only process 10 rows
# MAGIC    - `.count()` doesn't need actual data values
# MAGIC
# MAGIC 3. **Fault Tolerance**: Can replay transformations
# MAGIC    - If executor fails, Spark knows how to rebuild

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Narrow vs Wide Transformations
# MAGIC
# MAGIC ### Narrow Transformations (No Shuffle):
# MAGIC ```
# MAGIC Partition 1  →  Transformation  →  Partition 1
# MAGIC Partition 2  →  Transformation  →  Partition 2
# MAGIC Partition 3  →  Transformation  →  Partition 3
# MAGIC
# MAGIC Each partition processed independently!
# MAGIC ```
# MAGIC
# MAGIC **Examples**: `filter()`, `map()`, `select()`, `withColumn()`
# MAGIC
# MAGIC **Advantages:**
# MAGIC - Fast (no network I/O)
# MAGIC - No shuffle overhead
# MAGIC - Can pipeline operations
# MAGIC
# MAGIC ### Wide Transformations (Shuffle Required):
# MAGIC ```
# MAGIC Partition 1  ─┐
# MAGIC Partition 2  ─┼→  Shuffle  →  Repartition  →  New Partitions
# MAGIC Partition 3  ─┘
# MAGIC
# MAGIC Data must move across network!
# MAGIC ```
# MAGIC
# MAGIC **Examples**: `groupBy()`, `join()`, `repartition()`, `distinct()`
# MAGIC
# MAGIC **Disadvantages:**
# MAGIC - Slow (network I/O)
# MAGIC - Disk spill if memory full
# MAGIC - Creates stage boundaries

# COMMAND ----------

# Narrow transformation example
narrow_df = df.filter(col("salary") > 70000)  # No shuffle
print("Narrow transformation - fast!")

# Wide transformation example
wide_df = df.groupBy("department").count()  # Shuffle required!
print("Wide transformation - shuffle happens!")

# Check execution plan
wide_df.explain()
# Look for "Exchange" in the plan - that's the shuffle!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 5: Spark Connect
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Spark Connect - New Architecture (Spark 3.4+)
# MAGIC
# MAGIC ### Traditional Spark:
# MAGIC ```
# MAGIC Client Application
# MAGIC     │
# MAGIC     │ (embedded driver)
# MAGIC     ▼
# MAGIC ┌─────────────┐
# MAGIC │   Driver    │ ←──→ Executors
# MAGIC └─────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Spark Connect:
# MAGIC ```
# MAGIC Client Application           Spark Cluster
# MAGIC     │                       ┌──────────────┐
# MAGIC     │ gRPC (sc://)          │    Driver    │
# MAGIC     │────────────────────→  │              │
# MAGIC     │  Apache Arrow         │  Executors   │
# MAGIC     │←────────────────────  │              │
# MAGIC                             └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Features:
# MAGIC - **Decoupled client-server** architecture
# MAGIC - **gRPC protocol** for communication
# MAGIC - **Apache Arrow** for data transfer
# MAGIC - **Connection URI**: `sc://remote-host:15002`
# MAGIC
# MAGIC ### BENEFITS:
# MAGIC 1. **Stability**: Client crash doesn't affect server
# MAGIC 2. **Upgradability**: Upgrade client without restarting cluster
# MAGIC 3. **Remote Debugging**: Connect from anywhere
# MAGIC 4. **Multiple Clients**: Share same Spark session

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Connecting with Spark Connect
# MAGIC
# MAGIC ```python
# MAGIC # Traditional Spark Session
# MAGIC from pyspark.sql import SparkSession
# MAGIC spark = SparkSession.builder.appName("app").getOrCreate()
# MAGIC
# MAGIC # Spark Connect Session
# MAGIC from pyspark.sql import SparkSession
# MAGIC spark = SparkSession.builder \
# MAGIC     .remote("sc://spark-cluster:15002") \
# MAGIC     .appName("app") \
# MAGIC     .getOrCreate()
# MAGIC ```
# MAGIC
# MAGIC ### Connection String Format:
# MAGIC - `sc://` - Spark Connect protocol
# MAGIC - `host:port` - Cluster location
# MAGIC - Default port: `15002`

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 6: Spark Modules
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Spark Modules - What They Do
# MAGIC
# MAGIC ### Core Modules You Must Know:
# MAGIC
# MAGIC | Module                   | Purpose              | Key APIs                           |
# MAGIC |--------------------------|----------------------|------------------------------------|
# MAGIC | **Spark Core**           | Foundation, RDD API  | RDD, Broadcast, Accumulators       |
# MAGIC | **Spark SQL**            | Structured data      | DataFrame, Dataset, SQL            |
# MAGIC | **Structured Streaming** | Real-time processing | readStream, writeStream            |
# MAGIC | **Spark ML (MLlib)**     | Machine Learning     | Pipelines, Transformers, Estimators|
# MAGIC | **GraphX**               | Graph processing     | Vertices, Edges                    |
# MAGIC | **Pandas API on Spark**  | Pandas compatibility | pyspark.pandas                     |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Advantages of Spark
# MAGIC
# MAGIC ### "Identify advantages of implementing Spark"
# MAGIC
# MAGIC **Speed**:
# MAGIC - In-memory computation
# MAGIC - 100x faster than MapReduce for certain workloads
# MAGIC
# MAGIC **Ease of Use**:
# MAGIC - High-level APIs (Python, Scala, Java, R, SQL)
# MAGIC - Rich set of built-in functions
# MAGIC
# MAGIC **Unified Engine**:
# MAGIC - Batch + Streaming + ML + SQL in one platform
# MAGIC - No need for multiple tools
# MAGIC
# MAGIC **Fault Tolerance**:
# MAGIC - Automatic recovery from failures
# MAGIC - Lineage tracking via DAG
# MAGIC
# MAGIC **Scalability**:
# MAGIC - Scales from 1 to 1000s of nodes
# MAGIC - Handles petabyte-scale data
# MAGIC
# MAGIC ### Challenges of Spark:
# MAGIC
# MAGIC **Memory Requirements**:
# MAGIC - Needs significant RAM for best performance
# MAGIC - Can spill to disk but slows down
# MAGIC
# MAGIC **Learning Curve**:
# MAGIC - Understanding distributed computing concepts
# MAGIC - Tuning for optimal performance
# MAGIC
# MAGIC **Small Data Overhead**:
# MAGIC - Overkill for small datasets
# MAGIC - Startup overhead for distributed processing

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 7: Partitioning
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Understanding Partitions
# MAGIC
# MAGIC ### What is a Partition?
# MAGIC - A **chunk of data** processed by a single task
# MAGIC - Distributed across executors
# MAGIC - Enables parallel processing
# MAGIC
# MAGIC ```
# MAGIC DataFrame (1 Million Rows)
# MAGIC    │
# MAGIC    ▼ (divided into partitions)
# MAGIC ┌──────────┬──────────┬──────────┬──────────┐
# MAGIC │ Part 1   │ Part 2   │ Part 3   │ Part 4   │
# MAGIC │ 250k rows│ 250k rows│ 250k rows│ 250k rows│
# MAGIC └──────────┴──────────┴──────────┴──────────┘
# MAGIC     │            │            │            │
# MAGIC     ▼            ▼            ▼            ▼
# MAGIC  Task 1      Task 2      Task 3      Task 4
# MAGIC  (Exec 1)    (Exec 2)    (Exec 3)    (Exec 4)
# MAGIC ```

# COMMAND ----------

# Check number of partitions
sample_df = spark.range(1000000)
print(f"Number of partitions: {sample_df.rdd.getNumPartitions()}")

# Manually repartition
repartitioned = sample_df.repartition(10)
print(f"After repartition: {repartitioned.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Partition Configuration
# MAGIC
# MAGIC ### Key Configuration:
# MAGIC - `spark.sql.shuffle.partitions` - **Default: 200**
# MAGIC   - Number of partitions after shuffle operations
# MAGIC   - **EXAM QUESTION 5 PATTERN!**
# MAGIC
# MAGIC "What will be the impact of setting spark.sql.shuffle.partitions to 200?"
# MAGIC
# MAGIC **Answer: ADataFrames will be divided into 200 distinct partitions
# MAGIC during data shuffling operations.**

# COMMAND ----------

# Check current shuffle partitions setting
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# Set shuffle partitions (for this session)
spark.conf.set("spark.sql.shuffle.partitions", "50")

# Now any shuffle operation will create 50 partitions
grouped_df = df.groupBy("department").count()
print(f"Partitions after groupBy: {grouped_df.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1. Cluster Roles:
# MAGIC | Component           | Role                   | Executes Tasks?        |
# MAGIC |---------------------|------------------------|------------------------|
# MAGIC | **Driver**          | Coordinates, schedules | No (except Local mode) |
# MAGIC | **Executor**        | Runs tasks             | Yes                    |
# MAGIC | **Cluster Manager** | Allocates resources    | No                     |
# MAGIC | **Worker Node**     | Hosts executors        | No (executors do)      |
# MAGIC
# MAGIC ### 2. Execution Hierarchy:
# MAGIC ```
# MAGIC Action → Job → Stages (split by shuffles) → Tasks (one per partition)
# MAGIC ```
# MAGIC
# MAGIC ### 3. Deployment Modes:
# MAGIC - **Local**: Single JVM, threads, no remote executors
# MAGIC - **Client**: Driver on your machine, good for interactive
# MAGIC - **Cluster**: Driver in cluster, good for production
# MAGIC
# MAGIC ### 4. Transformations vs Actions:
# MAGIC - **Transformations**: Lazy, return DataFrame, no execution
# MAGIC - **Actions**: Eager, trigger execution, return results
# MAGIC
# MAGIC ### 5. Narrow vs Wide:
# MAGIC - **Narrow**: No shuffle (fast)
# MAGIC - **Wide**: Shuffle required (slow, creates new stage)
# MAGIC
# MAGIC ### 6. Spark Connect:
# MAGIC - Decoupled client-server
# MAGIC - gRPC protocol
# MAGIC - URI: `sc://host:15002`
# MAGIC - Benefits: Stability, upgradability, remote debugging
# MAGIC
# MAGIC ### 7. Shuffle Partitions:
# MAGIC - `spark.sql.shuffle.partitions = 200` (default)
# MAGIC - Controls partitions AFTER shuffle operations
# MAGIC
# MAGIC ## Common Exam Traps:
# MAGIC - Thinking driver executes tasks (it doesn't, except Local mode!)
# MAGIC - Confusing Client vs Cluster mode
# MAGIC - Not knowing what triggers a new stage (wide transformations)
# MAGIC - Forgetting that transformations are lazy
# MAGIC - Not understanding shuffle partitions configuration
