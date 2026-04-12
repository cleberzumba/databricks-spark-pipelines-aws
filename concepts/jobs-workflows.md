# Jobs and Workflows

## What are Databricks Jobs?

A Databricks **Job** is a way to **schedule and automate the execution** of notebooks, scripts, or pipelines on Databricks. Instead of running a notebook manually, a Job runs it automatically — on a schedule, triggered by an event, or called via API.

In simple terms: a Job turns your notebook into a **production pipeline**.

---

## Notebook vs Job

| | Interactive Notebook | Job |
|---|---|---|
| Execution | Manual (you click Run) | Automated |
| Use case | Development & exploration | Production pipelines |
| Cluster | All-purpose (always on) | Job cluster (starts and stops automatically) |
| Cost | Higher | Lower |
| Scheduling | ❌ | ✅ (cron, triggered, continuous) |
| Monitoring | Limited | Full logs, alerts, retries |

---

## What is a Workflow?

A **Workflow** is a Job that contains **multiple tasks** organized in a dependency graph (DAG — Directed Acyclic Graph). Each task can be a notebook, a Python script, a SQL query, or a Delta Live Tables pipeline.

```
Task 1: Ingest Raw Data (Bronze)
        │
        ▼
Task 2: Clean Data (Silver)
        │
        ▼
Task 3: Aggregate Data (Gold)
        │
        ▼
Task 4: Notify on Success
```

---

## Key Concepts

### Task
A single unit of work inside a Workflow. Each task runs independently and can depend on other tasks.

### DAG (Directed Acyclic Graph)
The dependency graph that defines the execution order of tasks. A task only runs after all its dependencies succeed.

### Job Cluster
A cluster that is **automatically created when a Job starts and terminated when it finishes**. This reduces cost compared to keeping an all-purpose cluster running.

### Schedule
Jobs can be triggered in three ways:
- **Cron schedule** — runs at a fixed time (e.g., every day at 6am)
- **File arrival trigger** — runs when new data arrives in a storage location
- **Manual trigger** — run on demand via UI or API

---

## Creating a Simple Job (UI)

1. Go to **Workflows** in the Databricks sidebar
2. Click **Create Job**
3. Add a **Task** and select your notebook
4. Set the **cluster** (use Job Cluster for production)
5. Set the **schedule** (optional)
6. Click **Create**

---

## Creating a Job via API

The example below creates a full **Bronze → Silver → Gold pipeline**, scheduled to run every day at 6am. Each task only starts after the previous one succeeds.

```python
import requests

response = requests.post(
    "https://<databricks-instance>/api/2.1/jobs/create",
    headers={"Authorization": "Bearer <token>"},
    json={
        "name": "Bronze to Gold Pipeline",
        "tasks": [
            {
                "task_key": "bronze_ingestion",
                "notebook_task": {
                    "notebook_path": "/notebooks/01_bronze_ingestion"
                },
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "num_workers": 2
                }
            },
            {
                "task_key": "silver_cleaning",
                "depends_on": [{"task_key": "bronze_ingestion"}],
                "notebook_task": {
                    "notebook_path": "/notebooks/02_silver_cleaning"
                },
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "num_workers": 2
                }
            },
            {
                "task_key": "gold_aggregation",
                "depends_on": [{"task_key": "silver_cleaning"}],
                "notebook_task": {
                    "notebook_path": "/notebooks/03_gold_aggregation"
                },
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "num_workers": 2
                }
            }
        ],
        "schedule": {
            "quartz_cron_expression": "0 0 6 * * ?",
            "timezone_id": "America/Sao_Paulo"
        }
    }
)
```

### What each task does:

| Task | Notebook | Depends On | Description |
|---|---|---|---|
| `bronze_ingestion` | `01_bronze_ingestion` | — | Reads raw data and writes to Bronze layer |
| `silver_cleaning` | `02_silver_cleaning` | `bronze_ingestion` | Cleans Bronze data and writes to Silver layer |
| `gold_aggregation` | `03_gold_aggregation` | `silver_cleaning` | Aggregates Silver data and writes to Gold layer |

---

## Retries and Error Handling

Jobs support automatic retries in case of failure:

- **`max_retries: 3`** — if a task fails, Databricks will retry it up to 3 times
- **`min_retry_interval_millis: 60000`** — waits 60 seconds between each retry attempt

You can also configure **email alerts** for:
- Job success
- Job failure
- Job start

---

## Jobs vs Delta Live Tables (DLT)

| | Databricks Jobs | Delta Live Tables |
|---|---|---|
| Use case | General orchestration | Declarative data pipelines |
| Definition | Imperative (you write the steps) | Declarative (you define expectations) |
| Data quality | Manual | Built-in (expectations) |
| Complexity | Flexible | Simpler for ETL |

---

## How This Project Uses Jobs

In this project, Databricks Jobs will orchestrate the full Medallion pipeline running on the `cluster-spark` cluster (Databricks Runtime 13.3 LTS, Spark 3.4.1, 2 workers):

| Task | Notebook | Description |
|---|---|---|
| Task 1 | `01_bronze_ingestion` | Read raw data and write to Bronze layer |
| Task 2 | `02_silver_cleaning` | Clean Bronze data and write to Silver layer |
| Task 3 | `03_gold_aggregation` | Aggregate Silver data and write to Gold layer |

The pipeline will be scheduled to run daily, using a Job Cluster to minimize costs.
