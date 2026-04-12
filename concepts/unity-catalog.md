# Unity Catalog

## What is Unity Catalog?

Unity Catalog is the **centralized governance and metadata management system** for Databricks. It provides a unified place to manage access, discover data, and audit usage across all your Databricks workspaces.

In simple terms: it is the **organizational structure** that defines where your data lives and who can access it.

---

## The Three-Level Namespace

Unity Catalog organizes all data objects in a **three-level hierarchy**:

```
CATALOG
└── SCHEMA (Database)
    ├── TABLE
    ├── VIEW
    └── VOLUME
```

To reference any object, you always use the full path:

```sql
catalog_name.schema_name.object_name
```

**Example:**
```sql
SELECT * FROM workspace.default.customers;
```

---

## The Four Main Objects

### 1. Catalog
The top-level container. A workspace can have multiple catalogs to separate environments (e.g., `dev`, `staging`, `prod`).

```sql
CREATE CATALOG IF NOT EXISTS my_catalog;
USE CATALOG my_catalog;
```

### 2. Schema
A namespace inside a catalog, equivalent to a traditional database. Groups related tables and volumes together.

```sql
CREATE SCHEMA IF NOT EXISTS my_catalog.bronze;
USE SCHEMA bronze;
```

### 3. Table
A structured data object backed by Delta Lake files.

```sql
CREATE TABLE IF NOT EXISTS my_catalog.bronze.customers (
    customer_id STRING,
    name STRING,
    age INT
);
```

### 4. Volume
A storage object for **non-tabular files** (CSV, JSON, images, models, etc.). Think of it as a managed folder inside Unity Catalog.

```sql
CREATE VOLUME IF NOT EXISTS my_catalog.default.spark_dev;
```

Files inside a volume are accessed via:
```
/Volumes/catalog_name/schema_name/volume_name/file.csv
```

---

## Full Hierarchy Example

```
workspace (catalog)
├── default (schema)
│   ├── customers (table)
│   ├── orders (table)
│   └── spark_dev (volume)
│       ├── bronze/
│       ├── silver/
│       └── gold/
└── analytics (schema)
    └── customer_summary (table)
```

---

## How to Navigate in PySpark

```python
# Select catalog
spark.sql("USE CATALOG workspace")

# Select schema
spark.sql("USE SCHEMA default")

# Create a volume
spark.sql("CREATE VOLUME IF NOT EXISTS spark_dev")

# List all schemas in current catalog
spark.sql("SHOW SCHEMAS").show()

# List all tables in current schema
spark.sql("SHOW TABLES").show()
```

---

## Unity Catalog vs Legacy Hive Metastore

| Feature | Hive Metastore | Unity Catalog |
|---|---|---|
| Scope | Single workspace | Multi-workspace |
| Governance | Limited | Fine-grained (row/column level) |
| Audit logs | No | Yes |
| Data lineage | No | Yes |
| Volume support | No | Yes |
| Recommended | Legacy | Yes Current standard |

---

## Why Unity Catalog Matters

- **Security** — control who can access which tables, schemas, or even specific columns
- **Discoverability** — data catalog with search and tagging
- **Lineage** — track where data came from and where it flows
- **Auditability** — full log of who accessed what and when
- **Multi-workspace** — share data across multiple Databricks workspaces

---

## How This Project Uses It

In this project, Unity Catalog is used to organize all pipeline assets:

| Object | Name | Purpose |
|---|---|---|
| Catalog | `workspace` | Default catalog (Community Edition) |
| Schema | `default` | Default namespace |
| Volume | `spark_dev` | Stores all Bronze, Silver, and Gold files |
