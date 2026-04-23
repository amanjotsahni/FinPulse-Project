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
[Yahoo Finance API / IBM AML CSV]
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
   data/silver/accounts/
          ↓
  Databricks SDK Upload
  /Volumes/workspace/finpulse/staging/
          ↓
  CREATE TABLE AS SELECT (CTAS)
  workspace.finpulse.transactions_silver  (Delta)
  workspace.finpulse.accounts_silver      (Delta)
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

**In our code, every `.count()` call inside `apply_silver_transformations` is an Action. This is intentional to log progress at each step, but in production you would minimise `.count()` calls because each one is a full dataset scan — 5.2M rows every time.**

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
df = df.withColumn("is_cross_currency", F.when(...).otherwise(False))
```
- Adds a new column or replaces an existing one with the same name.
- Returns a **new DataFrame** — PySpark DataFrames are immutable. Every `withColumn` creates a new logical plan node.
- Chaining multiple `withColumn` calls is fine for readability, but each adds a node to the DAG. In extreme cases (50+ withColumns), `select()` with all columns is more efficient.

---

### 2.4 `F.when() / .otherwise()` — Conditional Column Logic
```python
df = df.withColumn(
    "is_cross_currency",
    F.when(
        (F.col("Payment_Currency") != F.col("Receiving_Currency")),
        True,
    ).otherwise(False),
)
```
- Equivalent to SQL `CASE WHEN ... THEN ... ELSE ... END`.
- Conditions use `&` (AND), `|` (OR), `~` (NOT) — **not Python's `and`/`or`/`not`** (those don't work on Column objects).
- `F.round(..., 2)` used deliberately to handle floating-point precision issues — without it, `0.1 + 0.2 != 0.3` in floating-point math, causing false results in currency comparisons.

**The `isin()` optimisation:** `F.col("payment_format").isin(["Wire Transfer", "ACH"])` is more efficient than chained `|` comparisons because Spark can compile `isin` down to a single hash lookup.

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
df = df.dropDuplicates(["Account", "transaction_timestamp", "Amount_Paid", "Payment_Format"])
```
- Drops rows where all specified columns have identical values.
- This is a **shuffle operation** — Spark has to redistribute data across partitions so that rows with the same key end up on the same executor to be compared. This is the most expensive operation in our pipeline.
- Without arguments, `dropDuplicates()` considers all columns — usually not what you want.

**Why these 4 columns?** This is our **business key** for an IBM AML transaction: same timestamp + same sending bank + same account + same amount paid = same real-world transaction event.

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
Partition 1: [row_a, row_d, row_g]         After groupBy("payment_format"):
Partition 2: [row_b, row_e, row_h]   →→→   Partition 1: all "Wire Transfer" rows
Partition 3: [row_c, row_f, row_i]         Partition 2: all "ACH" rows
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
In financial transaction data, dominant payment formats (e.g., Wire Transfer) may account for a disproportionate share of records. If we do a `groupBy("payment_format")`, one partition gets far more rows and becomes a straggler — every other executor sits idle waiting for it.

**FinPulse context:** We deliberately **avoid `groupBy` in our Silver transformation** — we use `F.when/otherwise` for conditional column creation instead, which is a **partition-local transformation** (no shuffle needed).

**If we did need to groupBy:**
```python
# BAD: causes skew — dominant payment_format rows on one partition
df.groupBy("payment_format").agg(F.count("*").alias("count"))

# GOOD: salt the key to distribute skew across multiple partitions
from pyspark.sql.functions import concat, lit, rand
df_salted = df.withColumn("salted_fmt", concat(F.col("payment_format"), lit("_"), (F.rand() * 5).cast("int")))
df_salted.groupBy("salted_fmt").agg(F.count("*").alias("count"))
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

# Small lookup table (e.g., payment format descriptions)
format_lookup = spark.createDataFrame([
    ("Wire Transfer", "International wire"),
    ("ACH", "Automated clearing house"),
    ...
], ["payment_format", "format_description"])

# Broadcast the small table — it gets sent to every executor, NO shuffle on large side
silver_df.join(broadcast(format_lookup), on="payment_format", how="left")
```
**When to use:** One side of the join is small enough to fit in executor memory (< 10–20MB typically).  
`spark.sql.autoBroadcastJoinThreshold` — Spark auto-broadcasts if a table is under this threshold (default 10MB).

#### Bad Join Pattern — What to Avoid
```python
# BAD: joining two large DataFrames on a high-cardinality key without any hints
silver_df.join(another_large_df, on="account")  
# account has millions of unique values → massive shuffle, possible OOM
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
**Symptom:** `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve sum(is_cross_currency) due to BOOLEAN type.`  
**Root Cause:** `is_cross_currency` is engineered as a BOOLEAN column in the Silver Parquet. Databricks SQL (Delta Lake) follows ANSI SQL strictly — `SUM()` over BOOLEAN is undefined in ANSI SQL and thus rejected. Other SQL engines (PostgreSQL, BigQuery) implicitly cast BOOLEAN to integer for aggregation; Databricks does not.  
**Fix:** Explicit CAST before aggregation in all affected Gold SQL models:
```sql
SUM(CAST(is_cross_currency AS INT)) AS cross_currency_count
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
    SUM(CAST(is_laundering AS INT)) as fraud_count,
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
- `promote_to_databricks()` — cloud sync only

### Self-Healing Path Resolution
All file paths go through `config.py` which:
1. Walks up the directory tree to find `pyproject.toml` (project root marker).
2. Checks for `GDRIVE_FINPULSE_ROOT` env var — uses cloud storage if present.
3. Falls back to local `data/` directory if cloud is unavailable.
Result: same notebook runs identically on Windows laptop, Google Drive mount, and Databricks — no code changes.

---

## 9. Parquet Output Engineering: Spark Partition Sizing & File Count Control

### 9.1 Why the File Count Changed (16 → 8 Files)

After fixing the duplicate partition bug (which created 10,156,632 rows = 2× data), the clean Silver write contains 5,078,316 rows. Spark auto-sizes output partitions based on data volume:

```
Old run: 10,156,632 rows → Spark allocated 16 output tasks → 16 .parquet files
New run:  5,078,316 rows → Spark allocated  8 output tasks →  8 .parquet files
```

This is **expected and correct** — Spark's default task sizing targets ~128MB per partition. At ~1.85GB total and snappy compression (~3× ratio), 8 files of ~19MB each is the right-sized output.

**Why this matters for cloud ingestion:**
Fewer, larger files = fewer SDK upload calls = less SSL handshake overhead. The previous 16-file run exposed a 5-minute SSL timeout on the last file (`part-00007`). The 8-file version cuts total upload time roughly in half.

### 9.2 PySpark UUID-Based Output Filenames

The new output files use UUID-style names:
```
part-00000-411ca40e-36a6-437c-875b-6127dc4559e8-c000.snappy.parquet
```

This is the DataFrameWriterV2 naming convention when writing flat Parquet (not Iceberg). The UUID is the **query execution ID** — unique per Spark job run. It guarantees no filename collision when multiple pipeline runs write to the same directory, even before the `overwrite` mode takes effect.

The `.crc` sidecar files are **Hadoop CRC32 checksums** — written alongside each Parquet file to detect disk corruption. They are completely harmless and are correctly ignored by:
- `cloud_promotion.py`'s `glob("*.parquet")` pattern
- Databricks CTAS `parquet.'*.parquet'` wildcard

### 9.3 Clean-Before-Load Pattern in `cloud_promotion.py`

Every run of `cloud_promotion.py` follows a strict **Clean-Before-Load** sequence:

```python
# Step 1: Delete all existing files in the staging Volume folder
files = w.files.list_directory_contents(base_vol_path)
for f in files:
    w.files.delete(f.path)  # Remove stale files — no accumulation possible

# Step 2: Upload fresh Parquet files from latest Silver partition
for f in parquet_files:
    w.files.upload(remote_path, fdata, overwrite=True)

# Step 3: DROP TABLE + CREATE TABLE AS SELECT (atomic full replace)
cursor.execute("DROP TABLE IF EXISTS ...")
cursor.execute("CREATE TABLE ... AS SELECT * FROM parquet.'...'")
```

**Why not just `overwrite=True` without DELETE first?**  
If the previous run had 16 files and the new run has 8 files, `overwrite=True` would update the 8 files that match but leave 8 stale files in the Volume. The CTAS glob `*.parquet` would then read all 16 files — resulting in double the rows. The explicit Volume cleanup prevents this.

### 9.4 SSL Timeout Handling — 3-Attempt Retry

The Databricks SDK's file upload uses a default ~5-minute HTTP timeout. On large Parquet files (~19MB), intermittent SSL EOF errors occur due to network jitter:

```python
for attempt in range(1, 4):
    try:
        with f.open("rb") as fdata:
            w.files.upload(remote_path, fdata, overwrite=True)
        break  # success — exit retry loop
    except Exception as e:
        if attempt == 3:
            logger.error(f"FAILED after 3 attempts: {f.name} — {e}")
            raise
        logger.warning(f"Attempt {attempt} failed ({e.__class__.__name__}), retrying...")
        import time; time.sleep(5)
```

**Why 3 attempts with 5s sleep:**
- SSL EOF is almost always transient (brief network blip, not a permanent failure).
- 5s sleep allows TCP connection state to fully reset before the next attempt.
- Raising on attempt 3 ensures pipeline fails loudly instead of silently producing an incomplete table.

**Interview point:** *"SSL timeouts on large binary uploads are an operational reality in any cloud SDK. The retry wrapper is a production reliability pattern — identical to how S3 multipart uploads handle transient failures, just at the SDK level."*

---

## 9.5 Evidence-Based Synthetic Wire Laundering Labels

The IBM AML dataset has **0 confirmed Wire laundering labels** in the raw data — not because Wire is safe, but because the simulation was seeded with ACH/Cheque as the primary laundering formats. Wire transfers represent the largest individual transaction sizes ($2B+ in some cases) yet showed 0% laundering rate, which misrepresented the real AML risk profile.

**The problem with random labeling:**
Random synthetic labels are noise — they teach the model nothing and contaminate ground truth.

**Evidence-based approach — 4 AML signals required (2+ must fire simultaneously):**

| Signal | Definition | AML Basis |
|---|---|---|
| Signal A: Dirty bank | `from_bank` in top-10 ACH laundering banks | These banks already confirmed suspicious in other formats |
| Signal B: Round amount | `amount_paid % 1000 < $1` | Structuring signal — round numbers suggest manual entry/coordination |
| Signal C: Large cross-bank | `amount > $1M AND from_bank != to_bank` | Integration-phase capital movement at scale |
| Signal D: Multi-channel account | Account confirmed fraudulent in ACH AND has large Wire activity | Same entity, different payment rails |

```python
signal_count = signal_a.cast("int") + signal_b.cast("int") + signal_c.cast("int") + signal_d.cast("int")

df = df.withColumn("is_laundering",
    F.when(
        (F.col("payment_format") == "Wire") &
        (F.col("from_bank") != F.col("to_bank")) &
        (F.col("amount_paid") > 200_000) &
        (signal_count >= 2),  # at least 2 independent signals must agree
        1
    ).otherwise(F.col("is_laundering"))
)
```

**Result:** 1,057 Wire transactions labeled suspicious out of 171,854 (0.61% rate) — realistic, defensible, evidence-grounded.

**Interview point:** *"The synthetic label strategy mirrors how financial intelligence units actually build training data: you don't randomly assign fraud labels, you identify transactions that satisfy multiple independent AML red flags simultaneously. Two-signal requirement prevents false positives."*

---

## 9.6 Duplicate Partition Detection & Data Integrity

During development, the Silver notebook was run twice on the same date, creating a duplicate partition with 10,156,632 rows (exactly 2× correct). Detection pattern:

```python
# Verify expected row count before uploading
total = sum(len(pd.read_parquet(f)) for f in partition.glob("*.parquet"))
# Expected: ~5,078,316 (Bronze 5,178,345 - 29 duplicates ≈ 5.1M)
# Detected: 10,156,632 → duplicate partition → delete and rerun
```

**Why this matters:** A CTAS from a doubled partition produces a doubled Delta table. All downstream Gold models would show 2× transaction counts, 2× laundering rates, and incorrect fraud statistics. **Always verify Silver row count before cloud promotion.**

---

## 10. Cloud Ingestion Engineering: Staging Volume + CTAS

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

for f in Path("data/silver/transactions/date=2026-04-20").glob("*.parquet"):
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
│       ├── aml_risk_indicators.sql
│       ├── cross_bank_flow.sql
│       ├── daily_stock_summary.sql
│       ├── daily_transaction_summary.sql
│       ├── entity_laundering_profile.sql
│       ├── flagged_account_transactions.sql
│       ├── fraud_rate_by_type.sql
│       ├── high_risk_accounts.sql
│       ├── hourly_pattern_analysis.sql
│       ├── investigation_queue.sql
│       ├── moving_averages.sql
│       ├── stocks_performance_ranking.sql
│       ├── transaction_market_context.sql
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
      - name: accounts_silver
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
    account,
    COUNT(*) AS total_transactions,
    SUM(CAST(is_cross_currency AS INT)) AS cross_currency_count,  -- MUST cast
    SUM(amount_paid) AS total_amount_transacted
FROM {{ source('finpulse', 'transactions_silver') }}
WHERE risk_flag = true
GROUP BY 1
HAVING COUNT(*) > 1
```

**3. `fraud_rate_by_type` — Business KPI**
```sql
SELECT
    type,
    COUNT(*) AS total_transactions,
    SUM(is_laundering) AS fraud_count,
    ROUND(SUM(is_laundering) * 100.0 / COUNT(*), 4) AS fraud_rate_pct
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

**Q: Why did you use `dropDuplicates([4 columns])` instead of all columns or just `account`?**
> All columns: misses duplicates where metadata columns (like `ingestion_timestamp`) differ — same real transaction, different metadata. Just `account`: one account makes many transactions, so account alone isn't unique. The 4-column business key (timestamp + from_bank + account + amount_paid) represents the same IBM AML transaction event, regardless of when or how many times it was ingested. This is domain logic turned into engineering logic.

**Q: What's the risk of `inferSchema=True` in production?**
> Doubles read cost (file scanned twice — once for schema, once for data). Worse: guesses types from the first N rows. If row N+1 has `"N/A"` in an integer column, inference produced the wrong type at startup and the row silently null-casts or hard-fails. Always define `StructType` explicitly in production. Cost: upfront schema definition. Benefit: deterministic types regardless of data content.

**Q: Why use `F.round()` in financial comparisons?**
> IEEE 754 can't represent all decimal fractions exactly. `1000000.0 - 999000.5` computes to `999.4999999999954` at machine precision — not `999.5`. Without rounding, float comparisons against thresholds produce false results. `F.round(..., 2)` normalizes to cent precision before comparison — same principle banks use by working in integer cents internally.

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

## 13. Elementary Data: Regulatory-Grade Observability

### Why Elementary?
While Soda checks the **data**, Elementary monitors the **pipeline**. It provides a regulatory-grade audit trail of every dbt run.

### What it tracks in Databricks:
- **Lineage**: Source CSV → Silver Iceberg → Gold Delta.
- **Freshness**: When was the table last updated?
- **Schema Drift**: Did a column type change unexpectedly?
- **Model Performance**: Which dbt models are slowing down as data grows?

### Interview Talk-Track:
*"I used Elementary to generate a compliance-ready audit trail. It automatically captures model run results and test failures into permanent Delta tables in Databricks. This is the exact pattern used in highly regulated Tier-1 banks for SOX compliance monitoring."*

---

## 14. Prefect Orchestration: Production Patterns

### 14.1 Idempotent Skip Logic
The pipeline checks for a `.last_processed` marker (`C:\FinPulse Project\data\bronze\.last_processed`). If the Bronze data hasn't changed, the Silver step returns a `SKIPPED` state. This saves compute and prevents redundant snapshot creation.

### 14.2 Survival Mode (`return_state=True`)
```python
gold_state = run_dbt_task(return_state=True)
test_state = run_dbt_tests(return_state=True)
run_elementary_report(wait_for=[test_state])
```
Standard pipelines crash on the first error. FinPulse uses **Survival Mode**: even if a dbt test fails, the pipeline continues to generate the **Elementary Report**. 
**Why?** Because a data quality failure is exactly when you need the observability report the most.

### 14.3 RAM-Aware Ingestion (Chunking)
On an 8GB RAM limit, loading a 1.85GB CSV into memory causes an OOM kill. We implemented **chunked ingestion (500,000 rows per batch)** when converting Bronze CSV to partitioned Parquet. This keeps the memory floor low and the pipeline stable on consumer hardware.

---

## 15. Architecture Decision Journal (ADJ)

- **ADJ-01: Local-First Development**: All heavy lifting (PySpark, Iceberg) happens locally to minimize cloud costs (₹0 project target). Databricks is used only for the final Analytics (Gold) layer.
- **ADJ-02: Volume Upload vs. JDBC**: Solved the 22-min timeout by moving to binary cloud promotion. High-volume ingestion is a storage problem, not a SQL problem.
- **ADJ-03: No Docker (Initial Phase)**: Prioritized Windows native performance tuning (winutils, JVM args) over containerization overhead on limited 8GB hardware.

---

## 16. AI Compliance Intelligence System — Full Technical Reference

### 16.0 System Overview
The AI layer is a **4-agent inference system** that sits on top of the completed Medallion pipeline. It reads from pre-computed Gold tables — zero pipeline execution, zero data movement. All agents operate as pure inference layers.

```
Gold Layer (Databricks)               AI Intelligence Layer (Streamlit)
  aml_risk_indicators        ──────►  Agent 1: HUNT     (Isolation Forest)
  investigation_queue        ──────►  Agent 2: INVESTIGATE (Groq Llama 3.3 RAG)
  flagged_account_transactions ────►  Agent 3: REASON   (DeepSeek R1)
  stocks_performance_ranking ──────►  Agent 4: BRIEF    (PDF Reporting)
```

**Design principle:** Agents never trigger pipeline runs. They query Databricks SQL Warehouse directly via `databricks-sql-connector`, cache results with `@st.cache_data(ttl=600)`, and process entirely in-memory on the Streamlit server.

---

## 16.1 Agent 1: HUNT — Complete Technical Dissection

### 16.1.1 The Problem It Solves
From 5M+ IBM AML transaction records, Agent 1 reads the Gold `aml_risk_indicators` table and scores: **"Which transactions are most anomalous, using both ML and domain rules?"**

**Critical design fix — removed `WHERE risk_flag = true` pre-filter:**

The original `aml_risk_indicators.sql` had `WHERE risk_flag = true` which pre-filtered to only already-suspicious rows. This broke Isolation Forest:

```
Without full population → model trains ONLY on suspicious rows
→ No "normal" baseline established
→ All rows look equally suspicious relative to each other
→ Scores are meaningless — contamination threshold fires on noise
```

The fix: expose the **full Silver table** to Isolation Forest, with stratified sampling at inference time:

```sql
-- aml_risk_indicators.sql — NO FILTER
SELECT account as account_id, from_bank, to_bank, payment_format,
       amount_paid as amount, receiving_currency, payment_currency,
       is_cross_currency, is_high_value_wire,
       is_laundering as is_fraud, timestamp as txn_time
FROM {{ source('finpulse', 'transactions_silver') }}
-- Full population — Isolation Forest needs normal baseline rows
```

**Interview point:** *"Isolation Forest is unsupervised — it learns what 'normal' looks like and flags departures. If you pre-filter to only suspicious rows, you remove the normal baseline. The model has nothing to contrast against. This is a fundamental ML architecture mistake that was silently producing garbage scores."*

### 16.1.2 Data Loading — Stratified Sampling Pattern

**Source table:** `finpulse.aml_risk_indicators` (Gold layer)  
**Columns fetched:** `account_id, Payment_Format, amount, is_cross_currency, is_high_value_wire, is_fraud`

**Why stratified, not random:**

| Sampling Method | Result |
|---|---|
| Pure random 50K | Very few confirmed laundering rows — model barely sees positive cases |
| Stratified | ALL confirmed laundering + 50K normal via TABLESAMPLE — full signal preserved |

```python
# Step A: Always include ALL confirmed laundering rows
cursor.execute("... FROM aml_risk_indicators WHERE is_fraud = 1")
df_fraud   # all confirmed laundering rows

# Step B: 10% TABLESAMPLE of normal rows, capped at 50K
cursor.execute("... FROM aml_risk_indicators TABLESAMPLE (10 PERCENT) WHERE is_fraud = 0 LIMIT 50000")
df_normal  # 50,000 normal rows

df = pd.concat([df_fraud, df_normal])
```

**Databricks Decimal → Python float conversion:**
Databricks SQL Connector returns numeric types as Python `decimal.Decimal` objects. Without explicit conversion, pandas operations silently produce incorrect results or raise TypeError.
```python
for col in float_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
```

**Caching strategy:** `@st.cache_data(ttl=600)` — the 600-second TTL means the 50K-row query fires at most once per 10 minutes regardless of user interactions (slider moves, button clicks all cause Streamlit reruns). Without this, every contamination slider adjustment would hit Databricks.

---

### 16.1.2b IBM AML Dataset Format Correctness — `"Wire"` not `"Wire Transfer"`

A critical data discovery during development: the IBM AML HI-Small dataset uses `"Wire"` as the payment format string — **not** `"Wire Transfer"` as assumed from the column description.

```python
# WRONG — matched zero rows in IBM AML dataset
df["is_wire"] = (df["Payment_Format"] == "Wire Transfer").astype(int)

# CORRECT — matches IBM AML actual values
df["is_wire"] = (df["Payment_Format"].str.lower().str.strip() == "wire").astype(int)
```

**Why this mattered:**
- Rule 1 (`is_high_value_wire`) was computing `(payment_format == "Wire Transfer") AND amount > 100K` → matched 0 rows → Rule 1 never fired → all Wire transactions scored as LOW tier regardless of amount
- The Silver notebook's `is_high_value_wire` column also used the wrong string → 0 rows flagged
- `fraud_rate_by_type` showed Wire = 0% laundering — misleading since it was a data string mismatch, not real clean data

**Fix applied in both places:**
1. Silver notebook: `F.col("payment_format") == "Wire"` (Spark, case-sensitive after `.lower()`)  
2. Agent 1: `.str.lower().str.strip() == "wire"` (pandas, defensive against whitespace)

**IBM AML actual Payment Format values:** `Wire`, `ACH`, `Cheque`, `Credit Card`, `Cash`, `Bitcoin`, `Reinvestment`

**Interview point:** *"A single string mismatch caused an entire payment category to show 0% detection. This is why you always validate feature engineering against the actual source data values — not documentation assumptions. We ran a DataFrame `.value_counts()` on the format column and discovered the discrepancy immediately."*

---

### 16.1.2c Bank Name Resolution in `cross_bank_flow` — JOIN via Representative Account

The IBM AML dataset uses numeric bank IDs (1, 10, 11, 802...) internally. The `accounts_silver` table has a `bank_name` column ("First Bank of Dallas") but **no numeric bank ID column** — bank names are only accessible via `account_number → bank_name` join.

**Problem:** `cross_bank_flow` aggregates by `from_bank`/`to_bank` (numeric IDs). The UI was showing corridor charts with raw numbers (802 → 11) — unreadable by stakeholders.

**Initial wrong approach:** `REGEXP_EXTRACT(bank_name, '[0-9]+')` — tried to extract a number from a name like "First Bank of Dallas". Always returned NULL.

**Correct approach — store representative account during aggregation, then join:**

```sql
aggregated AS (
    SELECT
        from_bank, to_bank,
        COUNT(*) AS transaction_count,
        SUM(is_laundering) AS laundering_count,
        ROUND(SUM(is_laundering)*100.0/NULLIF(COUNT(*),0),4) AS laundering_rate_pct,
        FIRST(account)   AS sample_from_account,   -- save one account from this corridor
        FIRST(account_1) AS sample_to_account       -- save one receiving account
    FROM base
    GROUP BY from_bank, to_bank
)
SELECT agg.*,
    COALESCE(a1.bank_name, CONCAT('Institution ', CAST(agg.from_bank AS STRING))) AS from_bank_name,
    COALESCE(a2.bank_name, CONCAT('Institution ', CAST(agg.to_bank   AS STRING))) AS to_bank_name
FROM aggregated agg
LEFT JOIN accounts_silver a1 ON agg.sample_from_account = a1.account_number
LEFT JOIN accounts_silver a2 ON agg.sample_to_account   = a2.account_number
```

**The pattern:** Use `FIRST(account)` to capture a representative account per corridor during aggregation. Then join `accounts_silver` on `account_number` to resolve `bank_name`. `COALESCE` with `'Institution {id}'` fallback ensures no NULLs in the UI if an account isn't found.

**Interview point:** *"When a lookup table doesn't have a direct join key, use a stored representative row from the aggregation. This is the 'bridge key' pattern — FIRST(account) acts as the bridge between the aggregated bank ID and the accounts_silver name table."*

---

### 16.1.3 Feature Engineering — 8 Features from IBM AML Domain Knowledge

All 8 features are derived from IBM AML laundering mechanics, not correlation analysis. Each targets a specific typology.

| Feature | Source | Why It's an AML Signal |
|---|---|---|
| `amount` | `amount_paid` | Raw transaction size. Large amounts = integration-phase risk. |
| `log_amount` | `log1p(amount_paid)` | Log-transforms right-skewed distribution ($0–$92M). Prevents Isolation Forest from over-fixating on extreme outliers; makes mid-range anomalies visible. |
| `is_cross_currency` | derived in Silver | Paying currency ≠ receiving currency = primary layering signal. Obscures money trail via conversion. |
| `is_high_value_wire` | derived in Silver | Wire Transfer >$100K = integration-phase capital movement. Large, fast, cross-border. |
| `is_wire` | `payment_format == "Wire Transfer"` | Binary flag. Wire Transfer has the highest laundering rate in the IBM AML dataset. |
| `is_ach` | `payment_format == "ACH"` | Binary flag. ACH is the primary structuring/smurfing format — multiple small batched payments. |
| `amount_x_cross_ccy` | `amount × is_cross_currency` | Interaction feature: large cross-currency transfers are disproportionately risky. Captures the combined layering + size signal that neither feature alone represents. |
| `bank_pair_launder_rate` | `cross_bank_flow` Gold table | Historical laundering rate for this exact from_bank → to_bank corridor. Injects network-level intelligence: some bank routes are systematically used for laundering regardless of individual transaction features. |

**Why `log_amount` over raw `amount`:**
```python
# Raw amount: $0 – $92,000,000 range
# After StandardScaler: still skewed — a $92M outlier has z-score ~200
# log1p(amount): compresses to 0 – 18.3 range, outliers have z-score ~4
# Result: Isolation Forest now "sees" mid-range anomalies that raw amount hides
df["log_amount"] = np.log1p(df["amount"])
```

**Why the interaction feature `amount_x_cross_ccy`:**
```python
# Neither feature alone captures the combined risk:
#   - Large amount + same currency = normal institutional transfer
#   - Small amount + cross-currency = minor FX transaction
#   - Large amount + cross-currency = layering signal (amount × 1 = amount; otherwise 0)
df["amount_x_cross_ccy"] = df["amount"] * df["is_cross_currency"]
```

---

### 16.1.4 StandardScaler — Why It's Non-Negotiable

```python
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
```

Feature value ranges before scaling:
| Feature | Typical range | Without scaling |
|---|---|---|
| `amount` | $0 – $92,000,000+ | Completely dominates — tree always splits on amount |
| `log_amount` | 0 – 18.3 | Still large without scaling |
| `is_cross_currency` | 0 or 1 | Completely ignored |
| `is_high_value_wire` | 0 or 1 | Completely ignored |
| `is_wire` | 0 or 1 | Completely ignored |
| `is_ach` | 0 or 1 | Completely ignored |
| `amount_x_cross_ccy` | 0 – $92,000,000+ | Dominated by amount |
| `bank_pair_launder_rate` | 0 – ~5% | Swamped by amount |

After `StandardScaler`: all 8 features have mean=0, std=1. Isolation Forest's random split selection allocates equal "attention" to each. The bank corridor rate (0–5%) and binary flags (0 or 1) have equal influence on tree splits as raw transaction amounts in the millions.

---

### 16.1.5 Isolation Forest — Algorithm Deep Dive

**How it works:**
1. Randomly select a feature.
2. Randomly select a split value between the feature's min and max.
3. Record the depth at which the data point gets isolated (separated into its own partition).
4. Repeat with 100 trees (`n_estimators=100`).
5. Average path length across all trees — shorter = more anomalous.

```python
iso_forest = IsolationForest(
    contamination=0.013,   # threshold for anomaly decision boundary
    random_state=42,       # reproducible tree splits
    n_estimators=100,      # 100 trees → stable score estimates
)

# fit_predict: trains AND predicts simultaneously (transductive learning)
# Returns: -1 (anomaly) or +1 (normal) for every row
predictions = iso_forest.fit_predict(X_scaled)

# score_samples: raw path-length score (more negative = more isolated = more anomalous)
# Multiply by -1 so higher = more suspicious (more intuitive for UI display)
scores = iso_forest.score_samples(X_scaled) * -1

# Min-Max normalize to 0–100 for UI progress bar display
scores_normalised = (scores - scores.min()) / (scores.max() - scores.min()) * 100
```

**Contamination parameter decision:**
```
Default contamination = 0.10
→ Flags 10% of 58K rows = 5,800 accounts flagged
→ Compliance team cannot investigate 5,800 accounts
→ Destroys analyst trust → tool abandoned

Domain-calibrated contamination = 0.013
→ Flags ~1.3% of 58K rows = ~755 accounts flagged
→ Matches the actual fraud base rate in the dataset
→ Operationally viable for a compliance team
```

**Why `random_state=42`:** Isolation Forest uses random splits. Without a fixed seed, two runs with the same data produce different scores. `random_state=42` guarantees that clicking "Run Detection" twice produces identical results — critical for audit trail reproducibility.

---

### 16.1.6 Domain Rules Engine — 3-Rule Defense in Depth

```python
avg_amt = df["amount"].mean()

# Rule 1: Integration — high-value wire transfer
rule1 = (df["amount"] > 100_000) & (df["is_high_value_wire"] == 1)

# Rule 2: Layering — cross-currency + above-dataset-average amount
rule2 = (df["is_cross_currency"] == 1) & (df["amount"] > avg_amt)

# Rule 3: Hot corridor — bank pair with >0.5% historical laundering rate
#         Sourced from cross_bank_flow Gold table — network-level intelligence
rule3 = df["bank_pair_launder_rate"] > 0.5

df["rule_flag"]  = (rule1 | rule2 | rule3).astype(int)
df["rule1_flag"] = rule1.astype(int)   # saved individually for UI breakdown
df["rule2_flag"] = rule2.astype(int)
df["rule3_flag"] = rule3.astype(int)
```

**Why three rules:**

| Rule | Typology Targeted | Signal Source |
|---|---|---|
| Rule 1: HVW >$100K | Integration phase | Transaction amount + format |
| Rule 2: Cross-ccy + above-avg | Layering | Currency pair + relative amount |
| Rule 3: Hot bank corridor | Systemic routing | Network-level Gold table |

Rule 3 is the smart addition: it injects knowledge from `cross_bank_flow` (which aggregates 5M rows into bank-pair laundering rates at dbt build time). A transaction can look individually normal but route through a corridor historically used for 2%+ of laundering. Without Rule 3, this pattern is invisible to per-row ML.

**Tier assignment:**
```python
# HIGH:   Isolation Forest flagged AND any rule fired → two independent systems agree
# MEDIUM: Either Isolation Forest OR any rule → investigate further
# LOW:    Neither raised a flag
```

**Interview point:** *"Rule 3 is the key insight. I moved laundering network knowledge from aggregate SQL into individual transaction scoring. cross_bank_flow pre-computes bank-pair rates at Gold layer build time, so Agent 1 joins in historical corridor intelligence without re-scanning 5M rows at inference time."*

---

### 16.1.7 Gold Layer Dashboard — Live from Databricks

**Key metrics (from Gold `fraud_rate_by_type` and `high_risk_accounts`):**
| Metric | Value |
|---|---|
| Total Transactions | 5.07M (IBM AML dataset) |
| Confirmed Laundering | from is_laundering label |
| Contamination Param | 0.013 (domain-calibrated) |

**Laundering by Payment Format (Gold `fraud_rate_by_type`):**
| Format | Key Characteristic |
|---|---|
| Wire Transfer | Highest laundering rate — used in integration phase |
| ACH | Moderate — batch processing, structuring risk |
| Cheque | Lower — slower, paper trail |
| Credit Card | Low — real-time controls |
| Debit Card | Low — real-time controls |

**Detection Engine Results (contamination=0.013, ALL types):**
| Metric | Live Value |
|---|---|
| Transactions Sampled | **58,026** |
| HIGH Risk | Both Isolation Forest AND domain rules agree |
| MEDIUM Risk | One of the two systems flagged |
| LOW (Normal) | Neither system raised a flag |

---

### 16.1.8 Plotly Scatter Plot — Technical Design Decisions

**Layout rationale:**
- X = `amount` (amount_paid): High-value transactions cluster right — key structural signal
- Y = `anomaly_score` (0–100): Higher = more unusual relative to dataset  
- Together: the most suspicious accounts cluster top-right

**Trace design for three risk tiers:**
- LOW (grey): opacity=0.4, size=5 — dense background cluster, visually recedes
- MEDIUM (amber): size=8, opacity=0.75 — intermediate signal
- HIGH (red): size=12, opacity=0.9 — dominant foreground

**Hover tooltip:** Account ID (bold), Payment Format, Amount, Anomaly Score, Tier — exactly the fields needed for a compliance analyst to decide next action.

---

### 16.1.9 Flagged Accounts Table

**Columns rendered:**
1. `account_id` — the IBM AML account identifier
2. `Payment_Format` — Wire Transfer / ACH / Cheque / Credit Card / Debit Card
3. `amount` — formatted with `$` and `,` separators (amount_paid from IBM AML)
4. `anomaly_score` — Isolation Forest score normalised to 0–100
5. `confidence_tier` — HIGH (red) / MEDIUM (amber) — dual-flag agreement

---

### 16.1.10 Session State Architecture

```python
# Agent 1 saves its scored records for Agent 2 to consume
st.session_state.agent1_results = flagged[[
    "account_id", "Payment_Format", "amount",
    "is_cross_currency", "is_high_value_wire", "is_fraud",
    "anomaly_score", "confidence_tier", "rule_flag", "ml_flag",
]].to_dict("records")
```

**Why dict records and not DataFrame:**  
`st.session_state` is serialised across Streamlit reruns. Pandas DataFrames are not JSON-serialisable by default and can fail silently. `dict("records")` converts to a list of plain Python dicts — serialisable, reproducible, and directly usable by Agent 2 without re-importing pandas.

**Downstream consumption:** Agent 2 (INVESTIGATE) checks `if st.session_state.agent1_results is not None` — if populated, it shows the investigation interface with the flagged accounts from Agent 1; if not, it shows the gold-layer-only fallback view.

---

## 16.2 Agent 2: INVESTIGATE (Full-Spectrum RAG)

**Model:** Groq Llama 3.3 70B (fast inference, 128K context)  
**Architecture:** Multi-source RAG — assembles context from 8 Gold tables before every LLM call  
**Input:** `st.session_state.agent1_results` from Agent 1 + direct Databricks queries

### 16.2.1 RAG Context Pipeline — 8 Gold Tables Used

Every LLM call assembles a structured context block from all available Gold tables:

| Context Block | Gold Table | What It Adds |
|---|---|---|
| Agent 1 forensic | (session state) | ML scores, 3-rule breakdown, score band, percentile |
| Transaction history | `flagged_account_transactions` | Per-row bank routing, currency pairs, amounts, timestamps |
| Bank corridor risk | `cross_bank_flow` | Historical laundering rate for this specific bank pair |
| Entity profile | `entity_laundering_profile` | Entity-type laundering rate, bank network size |
| Peer ranking | `investigation_queue` | Where this account ranks vs top 25 by exposure |
| Hourly pattern | `hourly_pattern_analysis` | Which hours see peak laundering — off-hours flagging |
| Payment format rates | `fraud_rate_by_type` | Dataset-wide laundering rate per format |
| Market correlation | `transaction_market_context` + `stocks_performance_ranking` | laundering_intensity_vs_10d_avg, is_market_stress_day |

**Why this multi-table approach vs. one big query:**  
Each table is pre-aggregated at dbt build time at the right granularity. `cross_bank_flow` aggregates 5M rows into bank-pair summaries. `hourly_pattern_analysis` aggregates by hour. Querying each table separately at inference time is fast (pre-computed) and keeps the RAG context structured — the LLM gets labelled sections, not a raw data dump.

### 16.2.2 Off-Hours Detection (New Signal)

```python
ts = pd.to_datetime(df["transaction_timestamp"], errors="coerce")
off_hours_count = int(((ts.dt.hour < 9) | (ts.dt.hour >= 17)).sum())
```

Off-hours transactions (outside 09:00–17:00) are a structuring/smurfing temporal signal. The LLM is given `off_hours_txns={count}/{total}` in the context block and cross-references against `hourly_pattern_analysis` to determine if these hours correspond to known high-laundering windows.

### 16.2.3 Entity-Type Context

```python
entity_type = {0: "Corporation", 1: "Partnership", 2: "Sole Proprietorship"}.get(enc, "Unknown")
```

The `entity_laundering_profile` Gold table (joins `transactions_silver` with `accounts_silver`) gives per-entity-type laundering rates. If a Sole Proprietorship shows Corporation-scale wire transfers, that is a red flag the LLM is explicitly directed to name.

### 16.2.4 Laundering-Market Correlation Context

`transaction_market_context` Gold table pre-computes daily:
- `laundering_intensity_vs_10d_avg`: ratio of today's laundering count vs 10-day rolling average
- `is_market_stress_day`: TRUE when both JPM and GS daily returns are negative

The LLM receives 10 recent days of this data and can reason: *"laundering_intensity = 1.8x on a market stress day suggests coordinated capital flight using the banking sector downturn as cover."*

### 16.2.5 Context Routing Logic

```python
def route_context(question, account_id, agent1_results, all_scores):
    parts = [
        _agent1_forensic_str(account_id, agent1_results, all_scores),  # always
        _account_context_str(account_id),   # bank routing, off-hours, entity
        _peer_context_str(account_id),       # peer ranking with entity+bank names
        _hourly_pattern_str(account_id),     # off-hours vs peak laundering hours
        _fraud_rate_str(),                   # dataset-wide rates by format
        _market_str(),                       # stocks + market-stress correlation
    ]
    return "─"*72.join(parts)
```

The LLM receives all blocks for every question. The system prompt instructs it to answer the specific question asked — not repeat earlier blocks. Prior Q&A is injected as real conversation history (last 6 turns) so the model cannot claim ignorance of facts it already stated.
 
### 16.2.6 Suggestion Chips — Dynamically Driven by Agent 1 Signals

Suggestion chips are generated from `agent1_results` for the selected account:
- If `cross_ccy > 0` → layering typology chip
- If `high_wire > 0` → integration-phase chip  
- If `both_flags > 0` → dual-system agreement chip
- If `rule3_flag > 0` (hot corridor) → bank routing chip (surfaces `bank_pair_launder_rate`)
- Always: off-hours/timing chip, peer comparison chip

Each chip passes a pre-filled question that includes exact account-specific numbers (cross-currency count, wire count, bank corridor rate), so the LLM gets a precise prompt, not a generic one.

### 16.2.7 The Logic Loop: From Question to Forensic Insight

The Agent 2 backend follows a rigorous **Augmented Forensic Loop** that ensures every AI response is grounded in hard evidence from the Databricks Gold layer:

1.  **Request Trigger**: When a user selects a question or types a prompt, the system captures the `selected_account` and current `chat_history` from `st.session_state`.
2.  **Multi-Dimensional Retrieval (The "Querying" Phase)**:
    *   Instead of one massive, slow joined query, the system executes **8 parallel specialized queries** against the Gold layer (fetching transaction forensics, peer ranking, bank corridor risk, etc.).
    *   This is done via `route_context()`, which uses `@st.cache_data` to ensure that subsequent questions for the same account have **sub-10ms context retrieval**.
3.  **Augmentation (Context Inlining)**:
    *   The raw results are transformed into a **High-Density Data String**. This includes formatting transaction rows into monospaced tables and citing exact percentages for laundering rates.
    *   This string is injected into the `=== TRANSACTION EVIDENCE ===` block of the user message.
4.  **Prompt Construction**:
    *   The `_build_messages()` function assembles the `_SYSTEM_PROMPT` (the "Investigator Persona"), the conversation history (to maintain context), and the newly augmented evidence block.
    *   **Crucial Logic**: The system prompt contains **Typology Definitions** (Layering, Integration, etc.). The LLM's job is to "overlay" these definitions onto the retrieved data to find matches.
5.  **Inference (Insights Generation)**:
    *   The prompt is sent to **Groq (Llama 3.3 70B)**. 
    *   Because the data is already pre-aggregated and structured into clear "indicators" (e.g., `off_hours_txns=4/10`), the LLM doesn't have to "calculate" — it only has to **Reason**.
    *   **Result**: A forensic answer that cites specific banks, dates, and amounts, concluding with a domain-specific insight (e.g., *"This matches the Layering typology due to the USD-to-EUR conversion through Bank 802."*)

---

## 16.2.8 Agent 2 Layout & Forensic UI (Production Grade)

*   **Native Containerization**: To avoid the common Streamlit 'empty space' and height recalculation bugs, Agent 2 uses native styled `st.container` and `st.markdown` instead of potentially brittle IFrames.
*   **Icon-Based Forensic Table**: Transaction rows use visual icons (⚠️, ⏫, 🚩) for high-value scale and laundering confirms, reducing visual clutter while maintaining high information density.
*   **Action Tiles**: Suggestion leads are presented as a grid of interactive cards with distinct titles and tactical subtitles, improving the UX for non-technical compliance analysts.

## 16.3 Agent 3: REASON (Regulatory Classification)
- **Model:** DeepSeek R1 (Chain-of-Thought via OpenRouter).
- **Framework:** BSA / AML (Bank Secrecy Act).
- **Actions:** SAR · FREEZE · ESCALATE · MONITOR · CLEAR.
- **Value:** Streaming reasoning provides a human-readable audit trail for compliance officers.
- **Input:** `st.session_state.agent2_context` from Agent 2.
- **Status:** Prompt architecture complete; streaming integration in active development.

## 16.4 Agent 4: BRIEF (Executive Reporting)
- **Consolidation:** Aggregates findings from Agents 1–3 into a board-ready report.
- **Format:** One-click PDF export with integrated Databricks Gold layer statistical grounding.
- **Input:** All three prior agent outputs + live Gold KPIs.
- **Status:** Template design complete; PDF rendering library (`fpdf2`) integrated.

---

## 16.5 Streamlit UI Architecture

### Design System Constants
```python
# Palette defined in custom CSS injected via st.markdown(..., unsafe_allow_html=True)
# Body:    #1A2E4A (deep navy)
# Card:    #172340 (medium navy — distinct from body)
# Raised:  #1E2E50 (hover / elevated surface)
# Sidebar: #090E18 (darkest ink)
# T1:      #C8D8F0  T2: #7E9BB8  T3: #4A6080
# Accent:  #60A5FA (sky blue)
# Danger:  #F87171  Warning: #FBBF24  Clear: #34D399
# Font:    Inter (body) + JetBrains Mono (data values)
```

### CSS Override Approach
All Streamlit native styles are overridden via injected CSS targeting Streamlit's internal `data-testid` attributes. This includes:
- Sidebar collapse buttons → `display:none`
- Radio widget label (nav items) → custom styling
- Toolbar / deploy button → hidden
- Slider + selectbox → design system colours

**Why `!important` everywhere in the CSS:** Streamlit's CSS is compiled with high specificity via Emotion CSS-in-JS. Standard overrides lose the specificity battle. `!important` is the only reliable way to override them from external stylesheets injected at runtime.

### `@st.cache_data` Strategy
| Function | TTL | Rationale |
|---|---|---|
| `load_gold_stats()` | 300s (5 min) | KPI panel — acceptable to show minor staleness |
| `load_discrepancy_data()` | 600s (10 min) | 50K-row query — expensive, rate-limit Databricks |
| Pipeline status | On-demand button | Not cached — always real-time |

---

## 17. Repository Standards for Production Deployment

- **Dependency locking:** `uv.lock` pins exact content hashes for all packages including transitive deps.
- **Environment variables:** All credentials via `.env` (gitignored) loaded with `dotenv.load_dotenv()`.
- **Windows compatibility:** JVM/JDK 11, all paths forward-slash converted, no native I/O calls.
- **Design system:** Documented in CSS comments in `finpulse_app.py`. Palette + typography + spacing all tokenised.
- **Session state documentation:** Each key in `st.session_state` documented with producer and consumer agent.
