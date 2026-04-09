# FinPulse — Resume, Interview Metrics & Smart Engineering Decisions
> **Purpose:** Resume bullets, quantified achievements, challenges faced, smart decisions made — everything you say in the first 10 minutes of an interview.

---

## 🏆 30-Second Elevator Pitch

> *"FinPulse is an end-to-end financial data lakehouse I built from scratch. It processes 6.3 million real banking transaction records through a Medallion Architecture — Bronze ingestion, Silver cleaning and feature engineering, and an Iceberg-backed Gold layer. The pipeline enforces ACID guarantees, generates 8 ML-ready fraud-detection features, and runs on a hybrid local-plus-Google-Drive storage system. Every decision was deliberate — from diagnosing a multi-layered JVM crash to choosing Iceberg over raw Parquet for audit compliance. It is production-grade infrastructure running on a laptop."*

---

## 📊 Quantified Achievements (Copy-Paste Resume Bullets)

```
• Engineered end-to-end financial data lakehouse processing 6.3M transaction records and 2,500+ stock market records with PySpark + Apache Iceberg,
  achieving ACID compliance, time-travel queries, and zero-downtime schema evolution.

• Feature-engineered 15+ ML-ready signals across financial domains: 8 transaction-risk markers (fraud flags, discrepancies) and 7 stock market indicators (7d/30d moving averages, daily returns, price ranges).

• Resolved production-grade JVM crash (Py4JNetworkError) via 3-layer root cause analysis:
  JDK version incompatibility, memory oversubscription, and OS-level file lock contention
  from a cloud sync daemon — reduced crash rate from 100% to 0%.

• Designed "Stage-then-Sync" cloud persistence pattern decoupling transactional local Spark writes
  from eventual-consistency Google Drive replication — eliminated FAILED_RENAME_TEMP_FILE errors.

• Applied 12-point Spark optimisation strategy on an 8GB RAM machine: reduced shuffle partitions
  from 200 → 8 (for Transactions) and 4 (for Stocks), tuned JVM GC to G1GC, isolated temp I/O,
  and right-sized driver/executor memory — enabling stable processing without hardware scaling.

• Feature-engineered 8 ML-ready signals on 6.3M financial records: temporal features,
  balance accounting invariants, transaction-type encoding, and composite fraud risk flags.

• Implemented 3-layer Medallion Architecture (Bronze/Silver/Gold) with Hive-partitioned ingestion,
  predicate-pushdown-optimised reads, and Prefect-orchestrated pipeline automation.

• Migrated project dependency management from pip to uv (Astral) for hash-locked,
  deterministic builds — eliminating environment drift across Windows/Linux/cloud targets.

• Built self-healing, environment-agnostic path resolver that auto-detects runtime context
  (local, Google Drive, Colab) and pivots storage targets without code changes.
```

---

## 💥 Challenges Faced & How We Solved Them

### Challenge 1: JVM Crashes on Windows — Multi-Layer Root Cause
**The symptom:** `Py4JNetworkError: ConnectionResetError [WinError 10054]` — Spark session dies silently mid-execution.  
**Why it was hard:** The error message pointed to a "network" problem but the real cause was 3 layers deep.

| Layer | Root Cause | Fix Applied |
|---|---|---|
| JDK Incompatibility | Spark 3.5 + JDK 21 → `EXCEPTION_ACCESS_VIOLATION` in JIT CompilerThread when compiling Apache Arrow's `ArrowBuf.<init>` | Downgraded to JDK 11 LTS + added JIT exclusion file via `-XX:CompileCommandFile` |
| Memory Oversubscription | 6GB Spark memory on 8GB OS → Windows emergency-kills the JVM process | Reduced to 2500m driver + 2500m executor, leaving headroom for OS |
| Vectorized Reader Bug | Arrow native C++ memory via JNI → crashes on Windows with certain JDK builds | Disabled both `spark.sql.parquet.enableVectorizedReader` and `spark.sql.iceberg.vectorization.enabled` |

**Interview point:** *"I could have just asked for more RAM. Instead, I did a proper root cause analysis and solved it for any 8GB machine."*

---

### Challenge 2: Google Drive Locking Spark's Shuffle Files
**The symptom:** `[FAILED_RENAME_TEMP_FILE] FileSystem.rename returned false` — Iceberg write aborts mid-shuffle.  
**Why it was hard:** The error came from deep inside the Spark shuffle framework, not from user code.  
**Root cause:** Spark's `BypassMergeSortShuffleWriter` does an atomic rename of temp shuffle files. Google Drive's sync daemon held an exclusive read lock on those files at the exact millisecond of the rename — Windows `MoveFile()` returned `false`.  
**Fix:** Moved `TEMP_DIR` to `Path.home() / 'FinPulse_Spark_Temp'` — completely outside the Google Drive sync perimeter.  
**Interview point:** *"I traced a shuffle failure all the way down to a Windows file locking conflict between Spark's block manager and Google Drive's sync daemon — and fixed it by isolating Spark's temp directory from watched paths."*

---

### Challenge 3: Floating-Point Precision in Financial Validation
**The symptom:** Balance discrepancy check was flagging valid transactions as fraudulent.  
**Root cause:** IEEE 754 floating-point arithmetic — `1000000.0 - 999000.5` produces `999.4999999999954` at machine level, not `999.5`. So `oldbalance - amount != newbalance` returned `True` for legitimate transactions.  
**Fix:** `F.round(F.col("oldbalanceOrg") - F.col("amount"), 2) != F.round(F.col("newbalanceOrig"), 2)` — normalise to 2 decimal places before comparison.  
**Interview point:** *"I knew that financial data requires rounding before equality checks. This is why banks always work in integer cents, not floating-point dollars."*

---

### Challenge 4: Windows Path Spaces Breaking JVM Options
**The symptom:** `PySparkRuntimeError: JAVA_GATEWAY_EXITED` on SparkSession creation.  
**Root cause:** The project lives in `C:\FinPulse Project` — the space in the folder name broke unquoted JVM args. `-Djava.io.tmpdir=C:\FinPulse Project\temp` was parsed as two separate arguments by the JVM launcher.  
**Fix:** Quote all paths inside JVM options: `-Djava.io.tmpdir="C:/FinPulse Project/temp"`.  
**Interview point:** *"Always quote filesystem paths in JVM startup arguments when they may contain spaces — Windows paths are notorious for this."*

---

### Challenge 5: Zombie Spark Session Between Kernel Runs
**The symptom:** `ConnectionRefusedError: [WinError 10061]` — SparkSession creation fails after a previous crash.  
**Root cause:** Python's Jupyter kernel still held a reference to the previous SparkSession. `.getOrCreate()` tried to reconnect to the (now-dead) JVM gateway port — connection refused because the port was no longer listening.  
**Fix:** Restart the Jupyter kernel entirely to reset all Python state before recreating a Session.  
**Interview point:** *"A dead JVM cannot be recovered from the same Python process. Kernel restart flushes all Python objects including the stale gateway reference."*

---

## 🧠 Smart Engineering Decisions

### Decision 1: Iceberg over Raw Parquet for Silver Layer
**Why:** Parquet is a file format. Iceberg is a table format. The difference:
- Parquet: write → files on disk, no transaction semantics, no history.
- Iceberg: write → new snapshot with ACID guarantee, full time-travel history, schema evolution without data rewrite.

For a financial dataset where regulators can ask "show me this table as it existed last month" — Iceberg is not optional. We chose it at Silver (not Gold) because we want ACID from the first persistent layer.

### Decision 2: `createOrReplace()` for Idempotent Writes
Every pipeline run uses `createOrReplace()` instead of append. This means:
- If a run fails and Prefect retries it → no duplicate data.
- If a schema evolution is needed → table is replaced cleanly.
- The previous state is still accessible via time-travel snapshots.

### Decision 3: `F.when/otherwise` Instead of `groupBy` for Type-Based Logic
Financial transaction types are heavily skewed (`CASH_OUT` and `TRANSFER` dominate). Using `groupBy("type")` would cause data skew. We used `F.when/otherwise` for conditional column creation — a **partition-local transformation** that requires zero shuffling and zero network transfer.

### Decision 4: Validate Before Write (Fail-Fast Pattern)
Validation runs **after** all 8 Silver transformations but **before** the Iceberg write. This ensures:
- Iceberg never contains invalid data (no bad snapshots).
- The pipeline stops early with a clear error message.
- The check logic is centralised in `validate_silver_data()` — not scattered across the write function.

### Decision 5: 4-Column Business Key for Deduplication
`dropDuplicates(["nameOrig", "step", "amount", "type"])` — not all columns. Why?
- Using all columns: misses duplicates distinguished only by metadata columns (like ingestion timestamp, which legitimately differs between ingestion runs).
- Using the 4 business columns: identifies the same real-world transaction event regardless of when or how many times it was ingested.

### Decision 6: `uv` Over `pip` for Dependencies
`pip` + `requirements.txt` is **non-deterministic** — two `pip install` runs on different days can install different transitive dependency versions. `uv` produces a `uv.lock` file with exact content hashes. Every install is byte-identical. This is critical when debugging: "the pipeline worked yesterday" becomes diagnosable because yesterday's exact environment is reproducible.

### Decision 7: Separate Spark Temp from Project Directory
`TEMP_DIR = Path.home() / 'FinPulse_Spark_Temp'` — outside the project folder.
- Keeps git status clean (no `.gitignore` needed for temp files).
- Keeps Google Drive sync away from hot Spark I/O files.
- Makes the project folder contain only code + data, not runtime artifacts.

---

## 📋 Stack Summary

| Category | Technology | Version |
|---|---|---|
| **Processing Engine** | PySpark | 3.5.x |
| **Table Format** | Apache Iceberg | 1.4.3 (v2) |
| **Orchestration** | Prefect | 3.x |
| **Java Runtime** | Microsoft JDK | 11 LTS |
| **Dependency Mgmt** | uv (Astral) | Latest |
| **Containerisation** | Docker + Compose | — |
| **Storage (Primary)** | Local NTFS (C:\) | — |
| **Storage (Backup)** | Google Drive FUSE | G:\ mount |
| **Compression** | Snappy Parquet | — |
| **Data Volume** | 6,362,604 records | ~1.85 GB |
| **Languages** | Python 3.11, SQL | — |

---

## 🎯 Numbers to Remember for Interviews
> ✅ Every number below verified line-by-line against the actual notebook output. Safe to state on resume and explain under questioning.

| Metric | Value | Verified From |
|---|---|---|
| **STOCKS SILVER PIPELINE** | | |
| **Bronze stocks loaded** | **2,505** | Cell 5: `Records loaded : 2,505` |
| **Stocks Partition** | **date=2026-03-29** | Cell 3 Output |
| **Tickers processed** | **5 (AAPL, GOOGL, MSFT, JPM, GS)**| Cell 5 Output |
| **Trading days per ticker** | **501 days** | Validation Ticker Summary |
| **New features added** | **8 Indicators** | moving_avgs, price_range, returns etc. |
| **Validation checks** | **4/4 PASSED** | Validation block output |
| **TRANSACTIONS SILVER PIPELINE** | | |
| **Bronze records loaded** | **6,362,620** | Cell 5: `Records loaded : 6,362,620` |
| **Zero-amount transactions removed** | **16** | Step 1: `Removed 16 zero amount transactions` |
| **Silver final record count** | **6,362,604** | `Final record count: 6,362,604` |
| **Balance discrepancies flagged** | **1,199,155 (18.8%)** | Step 2 *(1,199,155 ÷ 6,362,604 = 18.847%)* |
| **Risk-flagged transactions** | **1,326,648 (20.9%)** | Step 5 *(1,326,648 ÷ 6,362,604 = 20.852%)* |
| **Confirmed fraud (isFraud = 1)** | **8,197 (0.13%)** | ACID proof query: `fraud_count = 8197` |
| **Average transaction amount** | **$179,862.36** | ACID proof query: `avg_amount = 179862.36` |
| **Shuffle partitions (tuned)** | **8 (Large) / 4 (Small)** | SparkSession config |
| **JVM stability configs applied** | **12** | SparkSession builder |
| **JVM error types resolved** | **5 distinct types → 0** | Debug session logs |
| **Iceberg snapshots** | **3 (Trans) / 1 (Stocks)** | Iceberg snapshot tables |

---

**Key explanations for each metric — know these cold:**

**Balance discrepancies (18.8%):** `1,199,155 ÷ 6,362,604 = 18.847%` → **18.8%**  
Only checked on TRANSFER and CASH_OUT rows where sender had funds. The check: `ROUND(oldBalance - amount, 2) ≠ ROUND(newBalance, 2)`. Fraud rate *within* discrepancy records = **0.00%** — so discrepancies are accounting anomalies in the simulation dataset, not actual fraud signals. This is an important nuance to explain.

**Risk-flagged (20.9%):** `1,326,648 ÷ 6,362,604 = 20.852%` → **20.9%**  
TRANSFER or CASH_OUT where `amount > mean_amount`. A behavioural threshold signal, not a fraud label. Computed independently from discrepancy check.

**Confirmed fraud (0.13%):** `8,197 ÷ 6,362,604 = 0.1288%` → **0.13%**  
The dataset's ground-truth `isFraud = 1` label. Highly imbalanced — any downstream ML model needs SMOTE, class weights, or adjusted decision thresholds.

**Average transaction = $179,862.36:** This is the *mean* of ALL 6.3M transactions. Used as the threshold in `risk_flag` logic — transactions above this mean are flagged as high-risk.

**Current table ~300 MB vs total warehouse ~960 MB:** The 3 separate pipeline runs created 3 Iceberg snapshots. Old snapshots are still on disk (retained per `history.expire.min-snapshots-to-keep = 3` setting). The *live* data is only the latest 300 MB snapshot.

## 📈 Key Stock Metric Summary (The "Domain Awareness" Section)
> Use these to show you understand the financial data, not just the code.

| Ticker | Trading Days | Avg Close Price | Avg Daily Return | Why this matters |
|---|---|---|---|---|
| **GS** (Goldman) | 501 | **$625.85** | **0.101%** | Highest absolute price; show knowledge of high-value stock volatility. |
| **AAPL** (Apple) | 501 | **$228.08** | **0.091%** | Strong daily performance; consistent benchmark. |
| **JPM** (JP Morgan) | 501 | **$252.96** | **0.070%** | Key banking sector representative in the dashboard. |
| **MSFT** (Microsoft)| 501 | **$440.79** | **-0.029%** | Negative return example; essential for testing "Bear Market" logic in Gold layer. |

*Note: All data based on a 501-day historical simulation window.*
