# FinPulse: Technical Deep-Dive & Interview Reference
> **Standard:** Mid-Senior Data Engineer | **Scope:** Everything actually used in this project + what comes next

---

## 1. System Architecture: Medallion Data Lakehouse

### What Is Medallion Architecture?
A layered pattern where data quality increases at each layer. Data only moves forward — never backward.

| Layer | Name | State | Format in FinPulse |
|---|---|---|---|
| Bronze | Raw | Immutable, as-is from source | Hive-partitioned CSV (`date=YYYY-MM-DD`) |
| Silver | Cleaned & Enriched | Schema-enforced, feature-engineered | Apache Iceberg v2 (Snappy Parquet) |
| Gold | Business-Ready | Aggregated, modeled for BI/ML | Databricks Delta Lake (via dbt) |

**Why immutable Bronze?**  
If a Silver bug is found, you reprocess from Bronze — no need to go back to the source system. Bronze is the source of truth. It is **append-only**; never update, never delete.

### FinPulse Ingestion Architecture (Actual)
```
[Yahoo Finance API / PaySim CSV]
          ↓
    Bronze (Local)
  Hive-partitioned CSV
   date=YYYY-MM-DD/
          ↓
   PySpark Silver (Local)
  Apache Iceberg v2 table
  (ACID + time-travel)
          ↓
   Parquet Export (Local)
   data/silver/transactions/
   data/silver/stocks/
          ↓
  Databricks SDK Upload
  /Volumes/workspace/finpulse/staging/
          ↓
  CREATE TABLE AS SELECT (CTAS)
  workspace.finpulse.transactions_silver  (Delta)
  workspace.finpulse.stocks_silver        (Delta)
          ↓
   dbt Gold Models (Databricks SQL Warehouse)
     9 tables: fraud KPIs, volatility, moving averages...
```

---

## 2. PySpark: Every API Used — Explained Properly

### 2.1 Lazy Evaluation vs. Actions (Critical Concept)
PySpark has two kinds of operations:

| Type | What it does | Examples from our code |
|---|---|---|
| **Transformation** (lazy) | Builds a logical plan. Nothing runs. | `filter`, `withColumn`, `dropDuplicates` |
| **Action** (eager) | Triggers an actual Spark Job | `count()`, `collect()`, `show()`, `writeTo()` |

**In our code, every `.count()` call inside `apply_silver_transformations` is an Action.** This is intentional to log progress at each step, but in production you would minimise `.count()` calls because each one is a full dataset scan — 6.3M rows every time.

```python
initial_count = df.count()                    # ACTION — triggers job
df = df.filter(F.col("amount") > 0)          # Transformation — lazy, adds to plan
removed = initial_count - df.count()          # ACTION again — triggers second job
```

**The smart alternative:** Cache the DataFrame before multiple counts to avoid re-scanning from disk.
```python
df.cache()  # Store in memory after first scan — subsequent counts are fast
```

---

### 2.2 `filter()` — Row-Level Predicate
```python
df = df.filter(F.col("amount") > 0)
```
- Removes rows where `amount <= 0` (16 zero-amount transactions removed).
- In SQL: `WHERE amount > 0`.
- `F.col("amount")` creates a **Column object** — this is the correct PySpark way vs. using string column names directly.
- **Also used in validation:** `df.filter(F.col("amount").isNull())` — `isNull()` is a Column method, not a Python `None` check.

---

### 2.3 `withColumn()` — Adding / Replacing Columns
```python
df = df.withColumn("is_balance_discrepancy", F.when(...).otherwise(False))
```
- Adds a new column or replaces an existing one with the same name.
- Returns a **new DataFrame** — PySpark DataFrames are immutable. Every `withColumn` creates a new logical plan node.
- Chaining multiple `withColumn` calls is fine for readability, but each adds a node to the DAG. In extreme cases (50+ withColumns), `select()` with all columns is more efficient.

---

### 2.4 `F.when() / .otherwise()` — Conditional Column Logic
```python
df = df.withColumn(
    "is_balance_discrepancy",
    F.when(
        (F.col("type").isin(["TRANSFER", "CASH_OUT"]))
        & (F.col("oldbalanceOrg") > 0)
        & (F.round(F.col("oldbalanceOrg") - F.col("amount"), 2)
           != F.round(F.col("newbalanceOrig"), 2)),
        True,
    ).otherwise(False),
)
```
- Equivalent to SQL `CASE WHEN ... THEN ... ELSE ... END`.
- Conditions use `&` (AND), `|` (OR), `~` (NOT) — **not Python's `and`/`or`/`not`** (those don't work on Column objects).
- `F.round(..., 2)` used deliberately to handle floating-point precision issues — without it, `0.1 + 0.2 != 0.3` in floating-point math, so `oldbalance - amount != newbalance` would produce false positives.

**The `isin()` optimisation:** `F.col("type").isin(["TRANSFER", "CASH_OUT"])` is more efficient than `(F.col("type") == "TRANSFER") | (F.col("type") == "CASH_OUT")` because Spark can compile `isin` down to a single hash lookup.

---

### 2.5 `F.mean()` + `agg()` + `collect()` — Global Aggregation
```python
mean_amount = df.agg(F.mean("amount")).collect()[0][0]
```
- `agg()` — applies aggregate functions across the whole DataFrame (no grouping).
- `collect()` — brings the result back to the Python driver as a list of `Row` objects.
- `[0][0]` — first row, first column value.

**Why this pattern?**  
We need the mean as a Python float scalar to use in the next `withColumn`. Spark can't use a DataFrame result inside a Column expression directly — you must `collect()` first.

**Caution:** `collect()` on a large dataset brings all data to the driver and can cause OOM. Here, `agg()` returns a single row (the mean), so it is safe. Never `collect()` on 6M rows.

---

### 2.6 `dropDuplicates()` — Deduplication
```python
df = df.dropDuplicates(["nameOrig", "step", "amount", "type"])
```
- Drops rows where all specified columns have identical values.
- This is a **shuffle operation** — Spark has to redistribute data across partitions so that rows with the same key end up on the same executor to be compared. This is the most expensive operation in our pipeline.
- Without arguments, `dropDuplicates()` considers all columns — usually not what you want.

**Why these 4 columns?** This is our **business key** for a transaction: same sender (`nameOrig`), same simulation step, same amount, same transaction type = duplicate event.

---

### 2.7 `F.lit()` — Literal (Constant) Column Values
```python
df = df.withColumn("silver_processed_at", F.lit(INGESTION_TIMESTAMP))
df = df.withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
```
- `F.lit()` wraps a Python scalar into a Column so it can be used in DataFrame operations.
- Without `lit()`, Spark doesn't know how to treat a Python string as a column value.

---

### 2.8 `spark.read.csv()` — Reading Bronze Data
```python
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(file_path)
)
```
- `inferSchema=True` makes Spark scan the CSV once to guess column types. Has a performance cost (one extra pass), but produces correctly typed DataFrames.
- **Production alternative:** Define schema explicitly with `StructType` — no scan overhead, no surprises.

```python
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

schema = StructType([
    StructField("step",   IntegerType(), True),
    StructField("type",   StringType(),  True),
    StructField("amount", DoubleType(),  True),
    ...
])
df = spark.read.schema(schema).csv(file_path)
```

---

### 2.9 `printSchema()` — Debugging Schema
```python
bronze_df.printSchema()
```
- Prints the schema tree to the console — column names, types, nullable flags.
- Purely a debugging/development action. Would be removed or logged in production.

---

### 2.10 `writeTo().tableProperty().createOrReplace()` — Iceberg Write (DataFrameWriterV2)
```python
df.writeTo("local.finpulse.silver_transactions") \
  .tableProperty("format-version", "2") \
  .tableProperty("write.parquet.compression-codec", "snappy") \
  .tableProperty("history.expire.min-snapshots-to-keep", "3") \
  .createOrReplace()
```
- `writeTo()` is the **DataFrameWriterV2 API** — the new API for Iceberg/Delta table writes.
- Different from the old `df.write.format("iceberg").save(path)` — V2 supports Iceberg table properties natively.
- `createOrReplace()` — creates the table if it doesn't exist, replaces (new snapshot) if it does. **Idempotent.**
- `tableProperty()` — sets Iceberg table-level properties at write time.

---

### 2.11 `spark.sql()` + DDL — Creating Namespaces and Querying Iceberg
```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.finpulse")
spark.sql("SELECT snapshot_id, committed_at FROM local.finpulse.silver_transactions.snapshots").show()
spark.sql("DESCRIBE TABLE local.finpulse.silver_transactions").show(truncate=False)
spark.sql("ALTER TABLE local.finpulse.silver_transactions ADD COLUMN risk_score DOUBLE")
```
- `CREATE NAMESPACE` — equivalent to creating a database/schema in Iceberg catalog.
- `.snapshots` — a **metadata table** exposed by Iceberg dynamically inside Spark SQL. You can query the snapshot history of any Iceberg table with this syntax.
- `.history` — another metadata table showing the commit history.
- `DESCRIBE TABLE` — shows column names, types, and comments.
- `ALTER TABLE ... ADD COLUMN` — zero-copy schema evolution (only updates metadata JSON, does not rewrite Parquet files).

---

### 2.12 `spark.conf.set()` — Runtime Config Override
```python
spark.conf.set("spark.sql.catalog.local.warehouse", iceberg_warehouse)
```
- Changes a Spark config at runtime, after the session is already started.
- Used to dynamically point the Iceberg catalog to the correct warehouse path.
- Note: Not all configs are settable at runtime — session-level configs like `spark.driver.memory` cannot be changed once the JVM starts.

---

### 2.13 `Window` Functions — Advanced Aggregations (Moving Averages)
```python
window_7d = Window \
    .partitionBy("ticker") \
    .orderBy("date") \
    .rowsBetween(-6, 0)

df = df.withColumn("moving_avg_7d", F.round(F.avg("close").over(window_7d), 4))
```
- **Windowing** allows us to perform calculations across a set of table rows that are related to the current row.
- `partitionBy("ticker")`: Restricts the calculation within each stock (Apple's average doesn't mix with Microsoft's).
- `orderBy("date")`: Ensures the rows are in chronological order before averaging.
- `rowsBetween(-6, 0)`: Defines the **bounding frame** — include the current row and the 6 preceding rows (total 7 days).
- **Interview Note:** "I used Window functions for moving averages because they are more efficient than self-joins and allow for complex time-series analysis like rolling volatility or RSI."

---

## 3. Shuffle, Partitions, and Performance — The Core of Spark Tuning

### 3.1 What Is a Shuffle?
A shuffle happens when Spark needs to **redistribute data across partitions** — moving rows from one executor to another across the network (or disk in local mode).

```
Partition 1: [row_a, row_d, row_g]         After groupBy("type"):
Partition 2: [row_b, row_e, row_h]   →→→   Partition 1: all "TRANSFER" rows
Partition 3: [row_c, row_f, row_i]         Partition 2: all "CASH_OUT" rows
```

**Operations that trigger a shuffle:**
- `groupBy()` + any aggregation
- `join()` (most join types)
- `orderBy()` / `sort()`
- `dropDuplicates()` ← used in our pipeline
- `repartition()`

**Why shuffles are expensive:**
1. All matching rows must be written to disk as shuffle files.
2. Those files are read back by the destination partitions.
3. In our case this happened inside `blockmgr-*/` — which Google Drive was locking.

### 3.2 `spark.sql.shuffle.partitions` — Our Key Tuning
```python
.config('spark.sql.shuffle.partitions', '8')   # Transactions (6.3M rows)
```
- Default of 200 means after every shuffle, data is split into **200 partitions**.
- On a `local[*]` or `local[8]` machine, 200 tiny files/tasks create massive overhead.
- **Set to 8** for Transactions → optimized for parallel processing across available cores while keeping partition size manageable.
- **Set to 4** for Stocks → Since Stocks has only ~2,500 rows, fewer partitions are better to avoid the overhead of managing many tiny tasks that outweigh the benefits of parallelism.

**In production on a 20-node cluster:** you would use auto-tuning or set this to `10 * number_of_cores`.

---

### 3.3 Data Skew — The Problem We Would Face at Scale
**The Problem:**  
In financial transaction data, `CASH_OUT` and `TRANSFER` are dominant transaction types (over 80% of records). If we ever do a `groupBy("type")`, one partition (containing `CASH_OUT`) would have 5M rows while others have 100K — Spark ends up with one "straggler" task that takes 10× longer than others.

**FinPulse context:** We deliberately **avoid `groupBy` in our Silver transformation** — we use `F.when/otherwise` for conditional column creation instead, which is a **partition-local transformation** (no shuffle needed).

**If we did need to groupBy:**
```python
# BAD: causes skew — all CASH_OUT rows on one partition
df.groupBy("type").agg(F.count("*").alias("count"))

# GOOD: salt the key to distribute skew across multiple partitions
from pyspark.sql.functions import concat, lit, floor, rand
df_salted = df.withColumn("salted_type", concat(F.col("type"), lit("_"), (F.rand() * 5).cast("int")))
df_salted.groupBy("salted_type").agg(F.count("*").alias("count"))
# Then aggregate the salt away in a second pass
```

---

### 3.4 `repartition()` vs `coalesce()` — When to Use Each

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| **What it does** | Full shuffle — evenly redistributes all data into exactly `n` partitions | Merges existing partitions — no shuffle |
| **Direction** | Can increase OR decrease partitions | Can only **decrease** partitions |
| **Data balance** | Perfectly balanced | Can create skewed partitions |
| **When to use** | Before a join if one side is heavily skewed | Before writing output to reduce file count |
| **Cost** | High (full shuffle) | Very low |

**FinPulse use case — where we should add `coalesce`:**
```python
# Before writing Iceberg — reduce output to 2 files (matches local[2])
df.coalesce(2).writeTo("local.finpulse.silver_transactions").createOrReplace()
```
Without this, Spark writes one file per partition (could be many small files — the "small files problem").

---

### 3.5 Joins — The Right Strategy for Each Situation

#### Sort-Merge Join (default for large datasets)
```
1. Both DataFrames are shuffled by join key → large network transfer
2. Both sides are sorted
3. Merge happens partition by partition
```
Used when both sides are large. This is what causes most shuffle-related crashes.

#### Broadcast Hash Join (our recommended pattern for Gold layer)
```python
from pyspark.sql.functions import broadcast

# Small lookup table (e.g., transaction type descriptions)
type_lookup = spark.createDataFrame([
    ("TRANSFER", "Bank Transfer"),
    ("CASH_OUT", "Cash Withdrawal"),
    ...
], ["type", "type_description"])

# Broadcast the small table — it gets sent to every executor, NO shuffle on large side
silver_df.join(broadcast(type_lookup), on="type", how="left")
```
**When to use:** One side of the join is small enough to fit in executor memory (< 10–20MB typically).  
`spark.sql.autoBroadcastJoinThreshold` — Spark auto-broadcasts if a table is under this threshold (default 10MB).

#### Bad Join Pattern — What to Avoid
```python
# BAD: joining two large DataFrames on a high-cardinality key without any hints
silver_df.join(another_large_df, on="nameOrig")  
# nameOrig has 6M unique values → massive shuffle, possible OOM
```

**For our Gold layer future joins (e.g., joining transactions with account metadata):**
- If account metadata is small → broadcast join.
- If both sides are large → ensure they are pre-partitioned on the same key (co-partitioned) to avoid shuffle.

---

### 3.6 Caching — When to Use `cache()` / `persist()`
```python
silver_df.cache()   # Store in memory — subsequent actions are fast
```
**When caching makes sense:** If you scan the same DataFrame more than once (multiple `count()`, multiple validations, multiple writes).

**In our pipeline:** We call `df.count()` 6 times inside `apply_silver_transformations`. Caching after the first scan would save 5 full dataset reads.

**Memory levels:**
```python
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)  # Spills to disk if RAM is full — safer on 8GB machine
```

**Important:** Always `unpersist()` when done — unreleased caches cause OOM.
```python
silver_df.unpersist()
spark.stop()
```

---

## 4. The Complete Bug Chronicle: Every Error and Its Fix

### Bug 1: `PySparkRuntimeError: JAVA_GATEWAY_EXITED`
**Symptom:** SparkSession creation fails with JVM exit immediately.  
**Root Cause:** Space in project path (`C:\FinPulse Project`). The `-Djava.io.tmpdir=` JVM arg passed the path unquoted, causing the JVM to interpret `Project` as a separate argument.  
**Fix:** Quote all paths inside JVM options:
```python
f' -Djava.io.tmpdir="{spark_temp_path}"'
```

### Bug 2: `Py4JNetworkError: ConnectionResetError [WinError 10054]`
**Symptom:** `df.show()` on the ACID aggregation causes the Spark session to crash.  
**Root Cause (3 layers):**
1. JDK 21 ↔ Spark 3.5 incompatibility — `EXCEPTION_ACCESS_VIOLATION` in C1 CompilerThread when JIT-compiling ArrowBuf.
2. Memory oversubscription — trying to run 6GB Spark on 8GB OS results in Windows OOM killing the JVM process.
3. Vectorized reader calling native Arrow C++ memory alloc via JNI — crashed on Windows.  
**Fix stack:**
- Downgraded to JDK 11 (LTS, fully supported by Spark 3.5).
- Reduced driver/executor memory to 2500m.
- Disabled vectorized reader.
- Added JIT exclusion file for ArrowBuf.

### Bug 3: `[FAILED_RENAME_TEMP_FILE] FileSystem.rename returned false`
**Symptom:** Iceberg write fails mid-shuffle with a rename error.  
**Root Cause:** Google Drive for Desktop sync daemon held a read lock on shuffle files inside the project folder (`C:\FinPulse Project\temp\blockmgr-*\`). Windows `MoveFile()` API fails if any process holds a lock.  
**Fix:** Move Spark temp completely outside the project directory:
```python
TEMP_DIR = Path.home() / 'FinPulse_Spark_Temp'  # C:\Users\amana\FinPulse_Spark_Temp
```
This directory is not synced by Google Drive, so no lock contention.

### Bug 4: `SyntaxError: f-string expression part cannot include a backslash`
**Symptom:** Cell fails to compile at `f` string with JVM options.  
**Root Cause:** Python f-strings prior to 3.12 do not allow backslash characters (`\n`, `\\`) inside the `{}` expression.  
**Fix:** Pre-compute path strings outside the f-string:
```python
spark_temp_path = str(TEMP_DIR).replace('\\', '/')  # compute first
JVM_OPTS = f' -Djava.io.tmpdir="{spark_temp_path}"'  # then use
```

### Bug 5: `ConnectionRefusedError: [WinError 10061]`
**Symptom:** After a previous JVM crash, rerunning the SparkSession cell fails immediately.  
**Root Cause:** Python's kernel still has a reference to the dead SparkSession object. `.getOrCreate()` tries to talk to the dead JVM gateway port — connection refused.  
**Fix:** Always:
- Restart the Jupyter kernel entirely to reset all Python state before recreating a Session.
- **Interview point:** *"A dead JVM cannot be recovered from the same Python process. Kernel restart flushes all Python objects including the stale gateway reference."*

### Bug 6: `FileNotFoundError` for Timestamped Bronze Files
**Symptom:** `FileNotFoundError: stocks_raw.csv` — pipeline fails if run after a fresh API pull that appended a timestamp.
**Root Cause:** Yahoo Finance appends a request timestamp to downloaded filenames: `stocks_raw_185807.csv`. Hardcoded path `stocks_raw.csv` breaks the moment the filename changes.
**Fix:**
```python
target_file = os.path.join(partition_dir, "stocks_raw.csv")
if not os.path.exists(target_file):
    csv_files = sorted(f for f in os.listdir(partition_dir) if f.endswith(".csv"))
    if csv_files:
        target_file = os.path.join(partition_dir, csv_files[-1])  # latest by name
```
**Interview point:** *"Hardcoded filenames are a pipeline fragility anti-pattern. The fix is a self-healing resolver — check canonical name first, fall back to latest file in partition. Zero manual intervention needed on API filename drift."*

### Bug 7: dbt `ref()` vs `source()` — Missing Source Registration
**Symptom:** `dbt run` fails: `Model 'model.dbt_finpulse.daily_stock_summary' depends on a node named 'stocks_silver' which was not found.`  
**Root Cause:** All 9 Gold models used `{{ ref('stocks_silver') }}` / `{{ ref('transactions_silver') }}`. `ref()` tells dbt to look for a model it manages internally. Silver tables were pushed externally (via Python SDK) — dbt had no knowledge of them without a `sources.yml` declaration.  
**Fix:**
1. Created `models/sources.yml` declaring both silver tables as external sources:
   ```yaml
   sources:
     - name: finpulse
       database: workspace
       schema: finpulse
       tables:
         - name: transactions_silver
         - name: stocks_silver
   ```
2. Replaced every `ref(...)` call with `source('finpulse', ...)` across all 9 Gold SQL files.  
**Interview point:** *"`ref()` is for dbt-managed models in the project DAG. `source()` is for externally-managed tables. Getting this wrong breaks lineage, causes compilation errors, and prevents impact analysis. Understanding this distinction is a core dbt competency."*

### Bug 8: Databricks SQL Boolean Aggregation Type Error
**Symptom:** `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve sum(is_balance_discrepancy) due to BOOLEAN type.`  
**Root Cause:** `is_balance_discrepancy` was engineered as a BOOLEAN column in the Silver Parquet. Databricks SQL (Delta Lake) follows ANSI SQL strictly — `SUM()` over BOOLEAN is undefined in ANSI SQL and thus rejected. Other SQL engines (PostgreSQL, BigQuery) implicitly cast BOOLEAN to integer for aggregation; Databricks does not.  
**Fix:** Explicit CAST before aggregation in all affected Gold SQL models:
```sql
SUM(CAST(is_balance_discrepancy AS INT)) AS discrepancy_count
```
**Interview point:** *"SQL dialect differences in type strictness are a real production pain point. Databricks follows ANSI SQL more closely than many other engines. Always test Gold SQL on the actual target engine, not assumptions from another dialect."*

---

## 5. SQL & Iceberg DDL — Every Statement Used

### Catalog Namespace (Database equivalent)
```sql
CREATE NAMESPACE IF NOT EXISTS local.finpulse;
-- local = catalog name (our SparkCatalog)
-- finpulse = namespace/database name
```

### Iceberg Table Write (via DataFrameWriterV2)
```python
df.writeTo("local.finpulse.silver_transactions")
  .tableProperty("format-version", "2")              -- Iceberg v2 (row-level deletes, equality deletes)
  .tableProperty("write.parquet.compression-codec", "snappy")  -- Compression
  .tableProperty("history.expire.min-snapshots-to-keep", "3")  -- Snapshot retention
  .createOrReplace()
```

### Querying Iceberg Metadata Tables
```sql
-- All snapshots (time-travel history)
SELECT snapshot_id, committed_at, operation
FROM local.finpulse.silver_transactions.snapshots;

-- Full commit history
SELECT * FROM local.finpulse.silver_transactions.history;

-- Table schema
DESCRIBE TABLE local.finpulse.silver_transactions;

-- Data files with stats (partitioning, size, row counts)
SELECT * FROM local.finpulse.silver_transactions.files;
```

### Zero-Copy Schema Evolution
```sql
-- Add a column — no data files are touched
ALTER TABLE local.finpulse.silver_transactions ADD COLUMN risk_score DOUBLE;

-- Rename a column — Iceberg tracks the original column ID internally  
ALTER TABLE local.finpulse.silver_transactions RENAME COLUMN risk_flag TO is_high_risk;
```

### Time Travel Queries (planned Gold layer)
```sql
-- Query as of a specific snapshot
SELECT COUNT(*) FROM local.finpulse.silver_transactions
VERSION AS OF 8645654373308272336;

-- Query as of a point in time
SELECT COUNT(*) FROM local.finpulse.silver_transactions
TIMESTAMP AS OF '2026-04-06 20:00:00';
```

### ACID Validation Query
```sql
SELECT 
    COUNT(*) as total,
    SUM(CAST(isFraud AS INT)) as fraud_count,
    ROUND(AVG(amount), 2) as avg_amount
FROM local.finpulse.silver_transactions;
```

---

## 6. Python Patterns Used — Production Standards

### 6.1 Self-Healing Project Root Detection
```python
def _find_project_root(marker='pyproject.toml') -> Path:
    curr = Path(os.getcwd()).resolve()
    for cand in [curr] + list(curr.parents):
        if (cand / marker).exists(): return cand
    return curr
```
Walks up directory tree to find project root by marker file. Works from any subdirectory.

### 6.2 Environment-Aware Path Resolution
```python
_GDRIVE_ROOT = Path(os.environ.get('GDRIVE_FINPULSE_ROOT', 'G:/My Drive/FinPulse'))
if _GDRIVE_ROOT.exists():
    _DEF_BRONZE = str(_GDRIVE_ROOT / 'data' / 'bronze' / 'transactions')
else:
    _DEF_BRONZE = str(PROJECT_ROOT / 'data' / 'bronze' / 'transactions')
```
Auto-pivots storage target based on detected environment — no code change needed when switching machines.

### 6.3 Guard-Pattern Environment Variable Loading
```python
BRONZE_PATH = os.environ.get('BRONZE_TRANSACTIONS_PATH', _DEF_BRONZE)
```
Precedence: environment variable (CI/CD override) → auto-detected default. Never hardcoded.

### 6.4 JVM Options as String Concatenation (not list)
```python
JVM_OPTS = (
    '-Dorg.apache.hadoop.io.nativeio.NativeIO.Windows.should_use_native_io=false'
    f' -Djava.io.tmpdir="{spark_temp_path}"'
    f' -XX:CompileCommandFile="{exclude_cfg_path}"'
    ' -XX:+UseG1GC'
)
```
Adjacent string literals are auto-concatenated by Python at compile time — no `+` operator needed, no runtime overhead.

### 6.5 Validation Pattern — Fail Fast
```python
def validate_silver_data(df):
    checks_passed = 0
    checks_failed = 0
    
    zero_amounts = df.filter(F.col("amount") <= 0).count()
    if zero_amounts == 0:
        checks_passed += 1
    else:
        checks_failed += 1
    
    if checks_failed > 0:
        raise Exception(f"{checks_failed} validation checks failed. Pipeline stopped.")
    return True
```
**Fail-fast principle:** Bad data causes an explicit `Exception` that stops the pipeline. Downstream stages never see corrupt data.

---

## 7. SparkSession Configuration Deep-Dive

### Every config in our session and why it's there
```python
.master('local[2]')
# 2 executor threads — matches RAM budget (2 x 2500m = 5GB, leaving room for OS)

.config('spark.driver.memory', '2500m')
.config('spark.executor.memory', '2500m')
# In local mode, driver and executor share the same JVM.
# Lower than default to survive on 8GB RAM with OS overhead.

.config('spark.sql.shuffle.partitions', '2')
# Default 200 is for 20-node clusters. We have 2 threads — 2 partitions.

.config('spark.sql.parquet.enableVectorizedReader', 'false')
# Disables Arrow C++ native memory allocation — JIT crash bypass.

.config('spark.sql.iceberg.vectorization.enabled', 'false')
# Iceberg-specific Arrow bypass — same root cause, Iceberg-side flag.

.config('spark.hadoop.fs.file.impl', 'org.apache.hadoop.fs.LocalFileSystem')
# Forces pure-Java filesystem implementation — no native I/O, no FUSE confusion.

.config('spark.hadoop.fs.verify.checksum', 'false')
# Skips CRC32 checksum verification — removes one more native I/O call.

.config('spark.network.timeout', '1200s')
# 20-minute timeout. Iceberg metadata operations on 6M rows take time.
# Default 120s was too short — executors were declared dead mid-operation.

.config('spark.executor.heartbeatInterval', '150s')
# Heartbeat must be < network.timeout. Prevents premature "executor lost" events.

.config('spark.jars', str(PROJECT_ROOT / 'jars' / 'iceberg-spark-runtime-3.5_2.12-1.4.3.jar'))
# Load the Iceberg runtime JAR. Naming convention: iceberg-spark-runtime-{spark_version}_{scala_version}-{iceberg_version}.jar

.config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
# Registers Iceberg SQL extensions (CREATE/ALTER TABLE syntax, metadata tables).

.config('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog')
.config('spark.sql.catalog.local.type', 'hadoop')
.config('spark.sql.catalog.local.warehouse', ICEBERG_WAREHOUSE)
# Registers a catalog named 'local' using the Hadoop catalog implementation.
# Hadoop catalog stores metadata in the warehouse directory itself (no external service).

.config('spark.sql.defaultCatalog', 'local')
# Makes 'local' the default — so 'finpulse.silver_transactions' resolves to 'local.finpulse.silver_transactions'.
```

---

## 8. System Design Patterns Used

### Idempotency
`createOrReplace()` is idempotent — rerunning the pipeline produces the same table state. Essential for retry logic (Prefect retries won't duplicate data). Applied at both Silver (Iceberg `createOrReplace`) and Gold (dbt `--full-refresh`).

### Stage-then-Sync (Decoupled I/O Pattern) — Local Storage
1. PySpark writes to strongly-consistent local NTFS (atomic Iceberg snapshots).
2. Parquet export copies finalized data to `data/silver/` outside the Iceberg warehouse.
Never mix transactional Spark writes with sync-agent-watched directories (Google Drive).

### Stage-then-Atomic-Load — Cloud Ingestion
1. Local Parquet files already on disk (prior step output).
2. Databricks SDK uploads binary files to a Unity Catalog Volume (direct object storage write).
3. `CREATE TABLE AS SELECT` atomically materializes as a Delta table — no row-by-row overhead.

### Fail-Fast with Validation Gates
Data quality checks run **after** Silver transformations but **before** Iceberg write. A failing check aborts the pipeline before bad data reaches any storage layer. Checks: null amounts, negative amounts, schema column presence, row count bounds.

### Separation of Concerns
Each pipeline step is a separate function with exactly one responsibility:
- `load_bronze_data()` — ingestion only
- `apply_silver_transformations()` — transformations only
- `validate_silver_data()` — quality gates only
- `write_silver_iceberg()` — local persistence only
- `export_silver_parquet()` — cloud-prep export only
- `cloud_ingest_to_databricks()` — cloud sync only

### Self-Healing Path Resolution
All file paths go through `config.py` which:
1. Walks up the directory tree to find `pyproject.toml` (project root marker).
2. Checks for `GDRIVE_FINPULSE_ROOT` env var — uses cloud storage if present.
3. Falls back to local `data/` directory if cloud is unavailable.
Result: same notebook runs identically on Windows laptop, Google Drive mount, and Databricks — no code changes.

---

## 9. Cloud Ingestion Engineering: Staging Volume + CTAS

### The Problem with Standard Spark-to-SQL Pushes
When we initially tried standard Databricks Connect `spark.createDataFrame(pandas_df).write.saveAsTable(table)`, the notebook hung for 22+ minutes without producing any log output. Root cause: the driver serializes all 6.36M rows through Python → JVM → remote SQL endpoint → deserialize → write. Each network round-trip is synchronous, and the SQL endpoint treats each batch as a separate statement.

### The Solution: Staging Volume + CREATE TABLE AS SELECT

**Step 1 — Create Unity Catalog Volume (one-time setup)**
```sql
CREATE VOLUME IF NOT EXISTS workspace.finpulse.staging;
```

**Step 2 — Upload Parquet files via Databricks SDK**
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()  # uses DATABRICKS_HOST + DATABRICKS_TOKEN from env

for f in Path("data/silver/transactions/date=2026-04-05").glob("*.parquet"):
    remote = f"/Volumes/workspace/finpulse/staging/transactions/{f.name}"
    with f.open("rb") as fdata:
        w.files.upload(remote, fdata)  # direct binary transfer to object storage
```
This is equivalent to `aws s3 cp` — a binary file transfer with no SQL involvement.

**Step 3 — Atomic CTAS (schema inferred from files)**
```python
conn = config.get_databricks_connection()  # Databricks SQL connector
cursor = conn.cursor()

# Drop old (if schema mismatch) and recreate atomically
cursor.execute("DROP TABLE IF EXISTS workspace.finpulse.transactions_silver")
cursor.execute("""
    CREATE TABLE workspace.finpulse.transactions_silver
    AS SELECT * FROM parquet.`/Volumes/workspace/finpulse/staging/transactions/*.parquet`
""")
```
Databricks reads directly from the Volume into a new Delta table. Schema is perfectly inferred from the Parquet metadata — no manual column definition needed.

### Why CTAS beats COPY INTO for initial full load
| | COPY INTO | CTAS |
|---|---|---|
| Schema definition | Required (pre-created table) | Automatic (inferred from files) |
| Incremental support | Yes (tracks ingested files) | No (full refresh only) |
| Initial load | Good | **Better** (no pre-schema needed) |
| Speed | High | **Highest** (parallel reads, no overhead) |

For our use case (full Silver refresh from local files), CTAS is the right choice.

### Performance Result
| Method | Rows | Time | Status |
|---|---|---|---|
| `spark.createDataFrame().saveAsTable()` | 6,362,604 | 22+ min | Hangs/timeouts |
| SQL INSERT batches (50K/batch) | 6,362,604 | Never finished | OOM / timeout |
| **Staging Volume + CTAS** | **6,362,604** | **~5 minutes** | ✅ **Success** |

---

## 10. Gold Layer: dbt on Databricks SQL Warehouse

### Project Structure
```
dbt_finpulse/
├── models/
│   ├── sources.yml              ← External Silver table registration
│   └── gold/
│       ├── balance_discrepancy_summary.sql
│       ├── daily_stock_summary.sql
│       ├── daily_transaction_summary.sql
│       ├── fraud_rate_by_type.sql
│       ├── high_risk_accounts.sql
│       ├── hourly_pattern_analysis.sql
│       ├── moving_averages.sql
│       ├── stocks_performance_ranking.sql
│       └── volatility_metrics.sql
└── dbt_project.yml              ← gold: +materialized: table
```

### sources.yml — The Critical Missing Piece
All Gold models reference Silver data via `{{ source() }}` not `{{ ref() }}`:
```yaml
version: 2
sources:
  - name: finpulse
    database: workspace
    schema: finpulse
    tables:
      - name: transactions_silver
      - name: stocks_silver
```
In model SQL: `FROM {{ source('finpulse', 'transactions_silver') }}`

**Why this matters for interviews:** `ref()` creates a dependency on a dbt-managed model in this project's DAG. `source()` declares an external dependency. Using `ref()` for external tables causes compilation failure and breaks the lineage graph.

### Key Gold Models

**1. `volatility_metrics` — Window Function for Rolling Std. Dev.**
```sql
SELECT
    ticker,
    date,
    close,
    STDDEV(close) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS volatility_7d,
    STDDEV(close) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS volatility_30d
FROM {{ source('finpulse', 'stocks_silver') }}
```

**2. `high_risk_accounts` — BOOLEAN cast required for Databricks**
```sql
SELECT
    nameOrig,
    COUNT(*) AS total_transactions,
    SUM(CAST(is_balance_discrepancy AS INT)) AS discrepancy_count,  -- MUST cast
    SUM(amount) AS total_amount_transacted
FROM {{ source('finpulse', 'transactions_silver') }}
WHERE is_balance_discrepancy = true
GROUP BY 1
HAVING COUNT(*) > 1
```

**3. `fraud_rate_by_type` — Business KPI**
```sql
SELECT
    type,
    COUNT(*) AS total_transactions,
    SUM(isFraud) AS fraud_count,
    ROUND(SUM(isFraud) * 100.0 / COUNT(*), 4) AS fraud_rate_pct
FROM {{ source('finpulse', 'transactions_silver') }}
GROUP BY 1
```

### dbt Run Result (Verified)
```
23:08:39  Finished running 9 table models in 0 hours 0 minutes and 53.40 seconds.
23:08:39  Completed successfully
23:08:39  Done. PASS=9 WARN=0 ERROR=0 SKIP=0 TOTAL=9
```

---

## 11. Interview Q&A — Mid-Senior Engineer Level

**Q: Why does `.count()` called multiple times in the pipeline hurt performance?**
> Each `.count()` is an Action that re-triggers the full DAG from source — reading 6.3M rows from disk each time. With 8 count calls, we scan the dataset 8× in dev mode. In production, cache before the first count: `df.persist(StorageLevel.MEMORY_AND_DISK)`, run all counts, then `df.unpersist()`. Caching converts disk reads to memory reads for all subsequent actions.

**Q: Why did you use `dropDuplicates([4 columns])` instead of all columns or just `nameOrig`?**
> All columns: misses duplicates where metadata columns (like `ingestion_timestamp`) differ — same real transaction, different metadata. Just `nameOrig`: one account makes many transactions, so nameOrig alone isn't unique. The 4-column business key (sender + step + amount + type) represents the same real-world event, regardless of when or how many times it was ingested. This is domain logic turned into engineering logic.

**Q: What's the risk of `inferSchema=True` in production?**
> Doubles read cost (file scanned twice — once for schema, once for data). Worse: guesses types from the first N rows. If row N+1 has `"N/A"` in an integer column, inference produced the wrong type at startup and the row silently null-casts or hard-fails. Always define `StructType` explicitly in production. Cost: upfront schema definition. Benefit: deterministic types regardless of data content.

**Q: How does `F.round()` eliminate false positives in balance discrepancy detection?**
> IEEE 754 can't represent all decimal fractions exactly. `1000000.0 - 999000.5` computes to `999.4999999999954` at machine precision — not `999.5`. Without rounding, `oldbalance - amount ≠ newbalance` is True for valid transactions. `F.round(..., 2)` normalizes both sides to cent precision before comparison — same principle banks use by working in integer cents internally.

**Q: Why did you use `source()` instead of `ref()` in your dbt Gold models?**
> `ref()` is for models this dbt project manages — it creates a compile-time dependency in the DAG and tells dbt to build that model. `source()` is for externally-managed tables — dbt knows about them for lineage documentation but doesn't build or own them. Our Silver tables are pushed via Python SDK — they're external. Using `ref()` for them causes a compilation error: dbt looks for a .sql model file that doesn't exist.

**Q: What was the root cause of the 22-minute ingestion hang and how did you fix it?**
> Standard `spark.createDataFrame(pandas_df).write.saveAsTable()` over a remote SQL Warehouse serializes all rows through: Python driver → Py4J → JVM → JDBC → SQL endpoint → Delta write. For 6.36M rows, the combined serialization + synchronous round-trip overhead is hundreds of minutes. The fix: uploading binary Parquet files directly to a Unity Catalog Volume (bypassing SQL entirely), then running a CTAS that Databricks executes server-side in parallel. Reduced from hanging to ~5 minutes.

**Q: What's the difference between COPY INTO and CTAS for bulk loading?**
> `COPY INTO` is incremental — it tracks which files have been ingested in a metadata table and skips already-loaded files. Requires the target table to exist first (schema pre-defined). Best for ongoing incremental loads. `CTAS` (CREATE TABLE AS SELECT) is atomic full-load — drops and recreates. Infers schema automatically from files. Best for initial loads and full-refresh patterns. We used CTAS because our Silver sync is always a full refresh and we needed automatic schema inference from the Parquet files.

**Q: What would you change if this pipeline ran on a 20-node Databricks cluster?**
> Remove all Windows/local workarounds. Set `spark.sql.shuffle.partitions = 400` (20 nodes × 20 cores). Enable `spark.sql.adaptive.enabled=true` for auto-partition optimization. Replace Hadoop catalog with Unity Catalog (external metastore). Use `dbfs://` or `abfss://` paths instead of local C:\. Add broadcast joins for small dimension lookups. Replace CTAS with `COPY INTO` for incremental Silver merges using Delta Lake MERGE statements.

---

## 12. Data Quality & Observability (Soda)

### 12.1 The "Why": Trust at Scale
With 6.36 million records moving from local storage to the cloud, manual row-counting is insufficient. We implemented **Soda Core** (Python CLI) to automate the verification of the Silver layer before analytics consumption.

### 12.2 Isolated Execution via `uvx`
Soda Core 3.x has strict legacy dependencies (e.g., `pandas < 2.0.0`) that conflict with the modern `pandas 2.x` stack used for our feature engineering.
- **Solution:** Instead of compromising the project dependencies, we use `uvx` (ephemeral tool environments).
- **Benefit:** Soda runs in its own isolated container with older `setuptools` to handle the `distutils` removal in Python 3.12, while the main pipeline remains on the cutting edge.

```powershell
uvx --from soda-core --with soda-core-spark --with databricks-sql-connector --with setuptools soda scan ...
```

### 12.3 Key Checks Implemented
We defined 21 distinct checks across Transactions and Stocks:
- **Volumetrics:** `row_count between 6M and 7M` (Catches silent ingestion loss).
- **Statistical Sanity:** `avg(amount)` and `max(daily_return)` bounds (Catches calculation overflows).
- **Schema Integrity:** `missing_count(risk_flag) = 0` (Ensures ML pipelines don't receive nulls).
- **Business Logic:** `invalid_count(type) = 0` with valid enum values (Ensures simulation integrity).

### 12.4 Custom SQL Metrics
For specialized checks like "Ensuring all 5 tickers are present," we use **Custom SQL Metric** blocks within the YAML.
```yaml
- ticker_count = 5:
    name: all_tickers_present
    ticker_count query: |
      SELECT COUNT(DISTINCT ticker) FROM stocks_silver
```
This demonstrates the ability to extend the DQ framework beyond simple threshold checks into complex domain-specific validation.

---
