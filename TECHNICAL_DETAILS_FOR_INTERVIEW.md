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
| Gold | Business-Ready | Aggregated, modeled for BI/ML | dbt models (planned) |

**Why immutable Bronze?**  
If a Silver bug is found, you reprocess from Bronze — no need to go back to the source system. Bronze is the source of truth. It is **append-only**; never update, never delete.

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
**Symptom:** Ingestion fails because it looks for `stocks_raw.csv` but the source system provided `stocks_raw_185807.csv`.  
**Root Cause:** Yahoo Finance (or other APIs) often append a timestamp or request ID to the filename. Hardcoded paths fail as soon as the timestamp changes.  
**Fix (Robust Path Detection):**
```python
target_file = os.path.join(partition_dir, "stocks_raw.csv")
if not os.path.exists(target_file):
    csv_files = sorted([f for f in os.listdir(partition_dir) if f.endswith(".csv")])
    if csv_files:
        target_file = os.path.join(partition_dir, csv_files[-1]) # Pick latest
```
**Interview Point:** *"I built a self-healing ingestion pattern that auto-detects the latest file in a partition if the standard name is missing — significantly reducing manual pipeline interventions."*

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
`createOrReplace()` is idempotent — rerunning the pipeline produces the same table state. Essential for retry logic (Prefect retries won't duplicate data).

### Stage-then-Sync (Decoupled I/O Pattern)
1. Spark writes to strongly-consistent local storage (atomic NTFS operations).
2. `shutil.copytree()` copies the finalized table to eventual-consistency cloud storage.
Never mix transactional writes with sync-agent-watched directories.

### Fail-Fast with Validation Gates
Data quality checks run **after** Silver transformations but **before** Iceberg write. A failing check aborts the pipeline before bad data reaches storage.

### Separation of Concerns
Each step is a separate function with a single responsibility:
- `load_bronze_transactions()` — ingestion only
- `apply_silver_transformations()` — transformations only
- `validate_silver_data()` — quality gates only
- `write_silver_iceberg()` — persistence only

---

## 9. What Comes Next: Gold Layer Concepts

### dbt Model Example (planned)
```sql
-- models/gold/fraud_daily_summary.sql
WITH daily_stats AS (
    SELECT
        DATE(silver_processed_at) AS processing_date,
        type,
        COUNT(*) AS total_transactions,
        SUM(isFraud) AS fraud_count,
        ROUND(AVG(amount), 2) AS avg_amount,
        SUM(CASE WHEN is_balance_discrepancy THEN 1 ELSE 0 END) AS discrepancy_count
    FROM {{ ref('silver_transactions') }}
    GROUP BY 1, 2
)
SELECT
    *,
    ROUND(fraud_count * 100.0 / total_transactions, 2) AS fraud_rate_pct
FROM daily_stats
```

### Window Functions for Fraud Velocity (planned)
```sql
-- Rolling 7-day fraud count per account
SELECT
    nameOrig,
    silver_processed_at,
    isFraud,
    SUM(isFraud) OVER (
        PARTITION BY nameOrig
        ORDER BY step
        ROWS BETWEEN 167 PRECEDING AND CURRENT ROW  -- 7 days = 7*24 steps
    ) AS rolling_7day_fraud_count
FROM silver_transactions
```

### Soda Core Data Quality Checks (planned)
```yaml
checks for silver_transactions:
  - row_count > 6000000
  - missing_count(amount) = 0
  - duplicate_count(nameOrig, step, amount, type) = 0
  - values in (isFraud) must in [0, 1]
  - avg(amount) between 100 and 500000
```

---

## 10. Interview Q&A — Mid-Senior Engineer Level

**Q: In our pipeline, why do we call `.count()` multiple times and what's the cost?**
> Each `.count()` is an Action that triggers a full DAG execution from source. With 6.3M rows and 8 count calls, we effectively scan the dataset 8 times. In dev this is fine for visibility. In production, we'd materialise the DataFrame with `cache()` or `persist(StorageLevel.MEMORY_AND_DISK)` before the first count, then `unpersist()` after all counts are done.

**Q: Why did we choose `dropDuplicates(["nameOrig", "step", "amount", "type"])` instead of all columns?**
> Using all columns would miss duplicates where only a metadata column differs (like `ingestion_timestamp` which could vary slightly). Our 4-column business key defines a unique transaction event: same sender + same time step + same amount + same type = same transaction. This is domain knowledge encoded as an engineering decision.

**Q: What's the risk of `inferSchema=True` in production?**
> Schema inference reads the full file once before loading data — doubles the read cost. More critically, it guesses types based on sampled values. If the first 100 rows of a column look like integers but row 101 is "N/A", inference makes the column `integer` and the csv reader fails or silently casts. In production, always define `StructType` explicitly.

**Q: How does `F.round()` fix the floating-point balance discrepancy check?**
> IEEE 754 floating-point numbers can't represent all decimal fractions exactly. `1000000.0 - 999000.5` might equal `999.4999999999954` at machine level, not `999.5`. Without rounding, `oldbalance - amount != newbalance` would return `True` for perfectly valid transactions. `F.round(..., 2)` normalises to 2 decimal places (cents), eliminating false positives from floating-point artifacts.

**Q: Why is `local[*]` with specific partition counts (8 for Transactions, 4 for Stocks) better than defaults?**
> `local[*]` uses all available CPU cores. Transactions (6.3M rows) uses 8 partitions to maximize parallelism across cores. However, for Stocks (only 2,505 rows), having too many partitions creates more management overhead than actual benefit. We use 4 partitions for Stocks as a balanced choice for small datasets — ensuring some parallelism while avoiding the "tiny files" overhead. It's about right-sizing the compute to the data volume.

**Q: What would you change if this pipeline ran on a 20-node EMR/Databricks cluster?**
> Remove all the Windows/local fixes. Set `spark.sql.shuffle.partitions = 400` (20 nodes × 20 cores). Use `local[*]` or a proper YARN/Kubernetes master. Replace the Hadoop catalog with a Hive Metastore or AWS Glue catalog. Use `s3a://` paths instead of local C:\ paths. Add broadcast joins for small dimension tables. Enable adaptive query execution (`spark.sql.adaptive.enabled=true`).
