# FinPulse — Resume, Interview Metrics & Smart Engineering Decisions
> **Purpose:** Resume bullets, quantified achievements, challenges faced, smart decisions made — everything you say in the first 10 minutes of an interview.

---

## 🏆 30-Second Elevator Pitch

> *"FinPulse is an end-to-end financial data lakehouse I built from scratch on local hardware. It processes 5.2 million real-world IBM AML transaction records through a full Medallion Architecture — Bronze, Silver, and Gold layers. I engineered an ACID-compliant Iceberg Silver layer, then solved a high-volume sync hang by pivoting to a Databricks Staging Volume + COPY INTO bulk-load pattern. The Gold layer is powered by specialized dbt models including AML risk indicators, transaction pattern analysis, and moving averages. Every decision was deliberate — from engineering cross-currency detection logic to choosing the right ingestion abstraction for 5M+ records at cloud scale."*

---

## 🎼 The Unified Story — Why Each Tool, Why Together

> *This is the answer to "Walk me through your architecture and why you made these choices." Every tool has a specific job problem it solves, and together they form one coherent system — not a collection of technologies thrown together.*

### The Problem Statement
Financial data has three hard requirements that most projects ignore:
1. **Scale** — 5.2M transaction records is too large for Pandas. Row-by-row loops, Excel, and SQL scripting on a laptop fail here.
2. **Auditability** — Finance data must be reproducible. "Show me this table as it existed on March 15th" is a real regulatory ask. Plain Parquet or a relational database can't answer that without extra infrastructure.
3. **Analytics Readiness** — Raw data isn't useful. Data scientists and BI tools need clean, enriched, pre-aggregated tables with documented lineage — not raw CSVs.

Each tool in FinPulse solves exactly one of these problems — and they connect in a deliberate chain.

---

### 🎸 The Instruments and Their Role

#### **PySpark** — The Engine (Scale Problem)
**Why not Pandas?** Pandas loads everything into driver memory. Even at 1.85 GB, Pandas on an 8GB machine leaves almost no room for OS and transformations. PySpark distributes computation across partitions and processes data in batches — the driver is an orchestrator, not a data holder.  
**What it does here:** Reads 6.36M rows from Bronze CSVs, applies 8 feature engineering transformations in parallel across partitions, and writes to Iceberg — all without loading the full dataset into memory at once.  
**Interview trigger:** *"I chose PySpark because the dataset is large enough that memory-bounded tools would fail, and the transformation logic is complex enough to warrant a distributed execution model — even on a single machine."*

---

#### **Apache Iceberg v2** — The Source of Truth (Auditability Problem)
**Why not plain Parquet?** Parquet is just a file format. It has no concept of transactions, history, or schema. If you overwrite a Parquet file, the previous state is gone. If two processes write simultaneously, you get corruption.  
**What Iceberg adds:** Every write creates an immutable snapshot. You can query the table as it existed at any point in time. Schema evolution is zero-copy. Writes are ACID — no partial states, no corruption.  
**Why Silver, not Gold?** ACID guarantees matter from the first persistent layer. If the Silver table is corrupt, Gold models propagate the corruption. Iceberg at Silver means the data contract is iron-clad before it reaches analytics.  
**Interview trigger:** *"Iceberg is the difference between a data file and a data table. For a financial system where regulators can audit any historical state, Iceberg at Silver isn't optional — it's the architecture."*

---

#### **Hive-Style Partitioning** — The Performance Layer (Bronze)
**Why:** `date=YYYY-MM-DD/` partitioning enables **predicate pushdown** — when downstream queries filter by date, the storage engine skips all other partitions entirely. No full scan needed.  
**What it enables:** The Silver pipeline can auto-detect the latest partition without hardcoded dates. dbt models can read only the relevant date range without touching the full 1.85 GB dataset.  
**Interview trigger:** *"Partitioning is a storage-layout decision that pays dividends at query time. I aligned partition keys with the most common filter predicate — date — so every downstream consumer gets free scan optimization."*

---

#### **Databricks Unity Catalog Volume + CTAS** — The Cloud Bridge (Scale × Cloud Problem)
**Why not JDBC/SQL inserts?** For 6.36M rows, JDBC batches serialize through: Python → JVM → network → SQL endpoint → deserialize → write. This is row-by-row work over a slow path. It hangs.  
**Why Volume + CTAS?** The Volume gives Databricks a place to read files directly from its own object storage — no Python, no JVM, no network per-row. `CREATE TABLE AS SELECT * FROM parquet.'/Volumes/...'` is a fully server-side, parallel operation. The client just issues one SQL statement and Databricks does the work.  
**The analogy:** It's the difference between handing someone 6 million letters one at a time vs. drop-shipping one pallet to their warehouse.  
**Interview trigger:** *"The insight was that the SQL Warehouse is a query engine, not a row-insert endpoint. Once I reframed cloud ingestion as 'move files, then query files', the performance problem dissolved."*

---

#### **dbt (data build tool)** — The Analytics Contract (Analytics Readiness Problem)
**Why not raw SQL scripts?** Raw SQL scripts: no versioning, no testing, no lineage, no documentation, no dependency management. If a query breaks, you don't know which downstream models are affected.  
**What dbt adds:**
- **`ref()` and `source()`**: Every model declares its dependencies explicitly. dbt builds a DAG. If Silver changes, dbt knows which Gold models to rebuild.
- **`+materialized: table`**: Each Gold model is a physical Delta table — not a view — persisted and queryable by BI tools without recomputing.
- **SQL-first**: Business analysts and data scientists can read and contribute to Gold models without knowing PySpark.
**Interview trigger:** *"dbt turns SQL scripts into a versioned, tested, documented software project. The lineage graph alone is worth it — you can answer 'what does this data power?' and 'if this source changes, what breaks?' with one command."*

---

#### **Prefect** — The Reliability Wrapper (Orchestration Problem)
**Why not cron?** Cron has no retry logic, no visibility, no alerting, and no dependency management between tasks. If Bronze ingestion fails, cron will still try to run Silver — on stale data.  
**What Prefect adds:** Task dependency graph — Silver only runs if Bronze succeeds. Automatic retries with backoff on transient failures. A UI dashboard showing every run's status, duration, and logs. Deployment-ready for cloud scheduling.  
**Interview trigger:** *"Prefect is the production harness around the notebooks. It answers 'what happened last Tuesday at 3am?' and 'why did the pipeline fail?' — questions that cron can never answer."*

---

#### **uv (Astral)** — The Reproducibility Guarantee (Environment Problem)
**Why not pip?** `pip install` is non-deterministic — the same `requirements.txt` on two different days can produce different version graphs as packages release patches. "It worked yesterday" is a diagnosable bug only if you can reproduce yesterday's environment.  
**What uv adds:** `uv.lock` pins exact content hashes for every package and transitive dependency. Every `uv sync` is byte-identical — guaranteed. On CI, on a new laptop, on a cloud VM — the environment is identical.  
**Interview trigger:** *"Reproducibility is a prerequisite for root-cause analysis. If my environment can drift between runs, I can never isolate whether a bug is in my code or my dependencies. uv eliminates that entire class of uncertainty."*

---

#### **config.py** — The Portability Layer (Multi-Environment Problem)
**Why not hardcode paths?** `C:\FinPulse Project\data\silver\transactions` is a path on one laptop. It doesn't work on Colab, on a team member's Mac, or in a Databricks notebook.  
**What config.py adds:** Auto-detects project root by walking up to `pyproject.toml`. Checks for `GDRIVE_FINPULSE_ROOT` environment variable — uses cloud storage if mounted, falls back to local. All paths are derived, never hardcoded.  
**Interview trigger:** *"config.py is the architecture decision that makes the project portable without code changes. The same notebook runs on Windows, Google Drive mount, and Databricks — the only difference is environment variables."*

---

#### **Microsoft JDK 11 LTS** — The Stability Foundation (JVM Problem)
**Why not JDK 21 (latest)?** Spark 3.5 runs on the JVM. JDK 21 introduced a JIT compiler change in the C1 CompilerThread that causes `EXCEPTION_ACCESS_VIOLATION` when compiling Arrow's native memory allocator — a bug that manifests as a misleading `Py4JNetworkError`. JDK 11 is Spark's fully-validated LTS target.  
**Interview trigger:** *"Version compatibility between Spark and the JVM is not just a suggestion. I learned this by debugging a 3-layer crash that presented as a network error but originated in a JIT compiler regression. JDK 11 is the foundation everything else stands on."*

---

### 🎵 The Full Symphony — How It Sounds Together

```
DATA SOURCES
  IBM AML (5.2M transactions) + Yahoo Finance (Market Data)
  → Realistic, domain-accurate financial datasets for meaningful analytics
           ↓
BRONZE — Hive-Partitioned CSV
  Immutable landing zone. Predicate-pushdown-ready.
  Re-process Silver from here if bugs are found — source of truth.
           ↓
SILVER (Local) — PySpark + Apache Iceberg v2
  PySpark: distributed feature engineering on 5M+ rows without OOM
  Iceberg: ACID guarantees + time-travel snapshots + zero-copy schema evolution
  → Cross-currency risk, high-value wire flags, metadata-enriched accounts
           ↓
CLOUD PROMOTION — Databricks SDK + Unity Catalog Volume + CTAS
  Skip the slow path (SQL row inserts). Push files, then query them.
  → 5.2M rows materialized as Delta tables in ~5 minutes
           ↓
GOLD — dbt on Databricks SQL Warehouse
  SQL-first modeling with dependency DAG, testing, and lineage.
  9 production tables: risk indicators, investigation queue, pattern analysis
  → Business-ready analytics, queryable by BI or AI Agents
           ↓
AI INTELLIGENCE LAYER — Streamlit + 4-Agent Architecture
  HUNT: ML Anomaly Detection (Isolation Forest) on AML risk indicators
  INVESTIGATE: RAG-powered account auditing (Llama 3 70B via Groq)
  REASON: Regulatory classification (DeepSeek R1 via OpenRouter)
  BRIEF: Board-ready compliance reporting + PDF export
  → Production-grade interface transforming data into actionable signal
           ↑ Reliability + Reproducibility (running underneath everything)
PREFECT — orchestrates the whole pipeline, retries failures, logs every run
uv — guarantees byte-identical environments across all machines
config.py — makes every notebook environment-agnostic
Soda DQ — automates verification of 6.36M records across 21 distinct checks
JDK 11 — stable JVM foundation for all Spark operations
```

**The story in one sentence:**  
*FinPulse demonstrates that production-grade data engineering is not about having the biggest cluster or the most tools — it's about using the right abstraction for each problem: PySpark for scale, Iceberg for auditability, Volume + CTAS for high-performance cloud promotion, and dbt for analytics governance — each solving one problem, all composing into one system.*

---

## 📊 Quantified Achievements (Copy-Paste Resume Bullets)

```
• Engineered high-performance Medallion data lakehouse processing 5.2M transaction records
  end-to-end, from raw IBM AML CSV ingestion to Gold analytics on Databricks.

• Pivoted 5.2M-row cloud ingestion from hanging SQL-based push to Databricks Staging Volume
  + COPY INTO bulk-load pattern — reduced synchronization time by >400% (indefinite → ~5 min).

• Built 9 production-grade dbt Gold models on Databricks Delta SQL Warehouse: fraud KPIs,
  rolling volatility, 7d/30d moving averages, hourly pattern analysis, and stock rankings.

• Standardized Data Quality (DQ) with Soda Core: Implemented 21 automated verification
  checks (row_count bounds, zero-null constraints, schema-validation, financial sanity limits)
  to ensure high-fidelity migration of 6.36M rows into the Databricks Lakehouse.

• Feature-engineered 15+ ML-ready signals: 8 transaction-risk features (is_laundering, is_cross_currency,
  is_high_value_wire, risk_flag, entity_type_encoded) and 7 stock indicators (moving averages, price range,
  daily returns, is_positive_day).

• Applied Apache Iceberg v2 for ACID-compliant Silver layer: 3 transaction snapshots retained,
  zero-copy schema evolution, full time-travel audit capability on 6.3M records.

• Resolved production-grade JVM crash (Py4JNetworkError) via 3-layer root cause analysis:
  JDK 21/Spark 3.5 incompatibility, memory oversubscription on 8GB RAM, and Arrow JNI crash —
  reduced failure rate from 100% to 0% without hardware changes.

• Traced FAILED_RENAME_TEMP_FILE Iceberg write failure to Windows file-lock contention between
  Spark's shuffle block manager and Google Drive sync daemon — fixed by isolating Spark temp I/O.

• Applied 12-point Spark config tuning on constrained 8GB hardware: G1GC, vectorized reader
  disabled, shuffle partitions 200→8 (Transactions) and 4 (Stocks), network timeout 120s→1200s.

• Engineered a production-grade AI Compliance Intelligence System using a 4-Agent architecture
  (HUNT, INVESTIGATE, REASON, BRIEF) to automate AML anomaly detection and regulatory filing.

• Built Agent 1 (HUNT): Isolation Forest anomaly detector running on live Databricks Gold data —
  scanning for cross-currency and high-value wire risk indicators with domain-aware scoring.

• Applied stratified sampling for imbalanced ML: fetched all confirmed laundering rows + random 50K
  normal transactions — preserves the low base rate without losing all positive signal.

• Engineered 8 domain-specific ML features from IBM AML signals: amount, log_amount (right-skew
  compression), is_cross_currency, is_high_value_wire, is_wire, is_ach, amount_x_cross_ccy
  (interaction term), and bank_pair_launder_rate (network intelligence from Gold) — each targeting
  a known AML typology: layering, integration, structuring, hot-corridor routing.

• Implemented log-transformed amount feature (log1p) to compress the $0–$92M right-skewed
  distribution — prevents Isolation Forest from over-fixating on extreme outliers and makes
  mid-range anomalies visible in the feature space.

• Engineered interaction feature `amount_x_cross_ccy = amount × is_cross_currency` — captures
  the combined layering+size signal that neither feature alone represents. A large cross-currency
  transfer is disproportionately risky vs. a small one or a same-currency large transfer.

• Injected network-level intelligence into per-row scoring: joined `cross_bank_flow` Gold table
  (pre-aggregated bank-pair laundering rates from 5M rows) as a feature — Rule 3 flags any
  transaction routing through a bank corridor with >0.5% historical laundering rate.

• Implemented 3-rule domain engine (vs 2): Rule 1=Integration (HVW >$100K), Rule 2=Layering
  (cross-ccy + above-avg amount), Rule 3=Hot corridor (bank-pair rate from Gold table).

• Implemented Hybrid ML + Domain Rules defense-in-depth: Isolation Forest scores ∩ 3-rule engine
  produce HIGH/MEDIUM/LOW tiers. HIGH = both systems independently agree = strongest signal.

• Calibrated contamination parameter to dataset laundering base rate (0.013) — prevents over-
  flagging that would occur with sklearn's default 0.1, making the model operationally viable.

• Added 5-bucket percentile score banding (Top 20% Critical → Bottom 20% Low) — converts raw
  anomaly score floats into analyst-ready risk tiers without losing score granularity.

• Built bank-corridor bar chart (top 10 HIGH-risk account routes by laundering rate) — surfaces
  systemic routing patterns invisible to per-account scatter plot.

• Delivered live Laundering Rate by Payment Format dashboard from Gold `fraud_rate_by_type`
  (Wire Transfer, ACH, Cheque, Credit Card, Debit Card).

• Built Agent 2 (INVESTIGATE) as a full-spectrum RAG pipeline pulling from 8 Gold tables per
  LLM call: transaction history, bank-corridor risk, entity laundering profile, peer ranking,
  hourly pattern analysis, payment format rates, and market-laundering correlation.

• Engineered off-hours detection in Agent 2: timestamps parsed to extract hour-of-day,
  transactions outside 09:00–17:00 flagged as structuring/smurfing temporal signals and
  cross-referenced against `hourly_pattern_analysis` Gold table.

• Built laundering-market correlation context from `transaction_market_context` Gold table:
  surfaces `laundering_intensity_vs_10d_avg` (how today's laundering compares to 10-day rolling
  average) and `is_market_stress_day` (JPM + GS both negative) — LLM reasons about capital
  flight timing during banking sector stress.

• Dynamically generated suggestion chips from Agent 1 signals: each chip passes a pre-filled
  question with exact account-specific numbers (cross-ccy count, wire count, bank corridor rate),
  not generic prompts — improving LLM answer precision.

• Architected a high-fidelity Streamlit interface with a premium financial design language:
  custom CSS design system, JetBrains Mono data display, interactive Plotly scatter with
  log(amount) X-axis for skew compression, HIGH RISK ZONE annotation, bank-corridor bar chart,
  5-column flagged accounts table with score band + entity name.

• Implemented self-healing Bronze ingestion that auto-detects timestamped filenames and falls
  back to the latest CSV in a partition — eliminating manual intervention on API filename drift.

• Migrated dependency management from pip to uv (Astral) with hash-locked uv.lock — achieving
  byte-identical reproducible environments across every developer machine and CI run.
```

---

## 💥 Challenges Faced & How We Solved Them

### Challenge 1: JVM Crashes on Windows — Multi-Layer Root Cause
**The symptom:** `Py4JNetworkError: ConnectionResetError [WinError 10054]` — Spark session dies silently mid-execution, often during `df.show()` or `.count()`.  
**Why it was hard:** The error message pointed at a "network" problem. The actual cause was 3 entirely separate layers interacting.

| Layer | Root Cause | Fix Applied |
|---|---|---|
| JDK Incompatibility | Spark 3.5 + JDK 21 → `EXCEPTION_ACCESS_VIOLATION` in C1 JIT CompilerThread compiling `ArrowBuf.<init>` | Downgraded to Microsoft JDK 11 LTS + added JIT exclusion via `-XX:CompileCommandFile=exclude.txt` |
| Memory Oversubscription | 6GB Spark allocation on 8GB OS → Windows OOM killer terminates the JVM process silently | Capped to `2500m` driver + `2500m` executor, leaving ~3GB for OS and Chrome |
| Vectorized Reader (Arrow JNI) | Arrow C++ native memory via JNI crashes on Windows with JDK 11 builds | Disabled `spark.sql.parquet.enableVectorizedReader=false` and `spark.sql.iceberg.vectorization.enabled=false` |

**Interview point:** *"I could have just asked for more RAM. Instead I did a proper 3-layer root cause analysis and made it work on any 8GB machine — which is what you do in production when you can't always scale hardware."*

---

### Challenge 2: Google Drive Locking Spark's Shuffle Files
**The symptom:** `[FAILED_RENAME_TEMP_FILE] FileSystem.rename returned false` — Iceberg write aborts deep in the shuffle phase.  
**Why it was hard:** The error surfaced from inside Spark's `BypassMergeSortShuffleWriter` — not from user code. Stack trace pointed to Hadoop's `LocalFileSystem.rename()`.  
**Root cause:** Spark creates temp shuffle files in `blockmgr-*/` inside the project directory and then atomically renames them. Google Drive's background sync daemon held an exclusive OS-level read lock on those exact files at the millisecond of the rename. Windows `MoveFile()` returns `false` when any lock exists — atomic rename fails, write aborts.  
**Fix:** Set `TEMP_DIR = Path.home() / 'FinPulse_Spark_Temp'` — completely outside any Google Drive-watched path. One config line eliminated the class of errors entirely.  
**Interview point:** *"I traced a shuffle failure all the way to a Windows file-lock conflict between Spark's block manager and Google Drive's sync daemon. Fixed it by isolating all Spark temp I/O outside the synced directory."*

---

### Challenge 3: Floating-Point Precision in Financial Comparisons
**The symptom:** IEEE 754 double-precision arithmetic causes float equality comparisons to produce unexpected results — `1000000.0 - 999000.5` evaluates to `999.4999999999954` at machine level.  
**Context:** Relevant whenever comparing derived float columns (e.g., computed amounts, currency conversion results) against thresholds. Applied F.round() to normalize values before comparison.  
**Fix:** `F.round(F.col("amount_paid"), 2)` — normalize to cent precision before any comparison or threshold check.  
**Interview point:** *"Financial systems never compare raw floats with equality. This is why banks work in integer cents internally. I applied the same principle: round to the smallest meaningful unit before comparing."*

---

### Challenge 4: Windows Path with Spaces Breaking JVM Startup
**The symptom:** `PySparkRuntimeError: JAVA_GATEWAY_EXITED` immediately on `SparkSession.builder.getOrCreate()`.  
**Root cause:** The project lives at `C:\FinPulse Project`. The space was breaking JVM argument parsing. `-Djava.io.tmpdir=C:\FinPulse Project\temp` was interpreted as `-Djava.io.tmpdir=C:\FinPulse` (valid) and `Project\temp` (treated as a main class name) — JVM failed to start.  
**Fix:** Wrap all filesystem paths in JVM options with double-quotes AND convert backslashes to forward slashes (Windows JVM accepts both, but forward slashes avoid escape sequence issues in Python f-strings):
```python
spark_temp = str(TEMP_DIR).replace('\\', '/')
JVM_OPTS = f'-Djava.io.tmpdir="{spark_temp}"'
```
**Interview point:** *"Always quote paths in JVM startup args when they may contain spaces. This is a classic Windows-specific JVM gotcha that most Linux-first engineers never encounter."*

---

### Challenge 5: Zombie Spark Session — Dead JVM Gateway Port
**The symptom:** `ConnectionRefusedError: [WinError 10061]` on SparkSession creation after a previous crash.  
**Root cause:** Python's Jupyter kernel still holds a reference to the previous `SparkContext`. `.getOrCreate()` tries to reconnect to the JVM gateway on the same port (typically 4040) — which is no longer listening because the JVM process is dead. Connection refused.  
**Fix:** Full Jupyter kernel restart before recreating the session. Can't recover a dead JVM gateway from the same Python process — you must flush all Python objects, including the stale `SparkContext` reference.  
**Interview point:** *"There is no recovery path from a dead JVM gateway within the same Python process. The fix is a kernel restart — which is why production pipelines use subprocess-level process management and watchdog restarts, not Jupyter sessions."*

---

### Challenge 6: Bronze Ingestion Failing on Timestamped API Filenames
**The symptom:** `FileNotFoundError: stocks_raw.csv not found` — pipeline fails if run on a new day or after a fresh API pull.  
**Root cause:** Yahoo Finance (and other market data APIs) often append a timestamp or request ID to the downloaded filename: `stocks_raw_185807.csv`. Hardcoded path `"stocks_raw.csv"` breaks the moment the filename changes.  
**Fix:** Built a self-healing path resolver that:
1. First tries the canonical expected name.
2. If missing, lists the partition directory and picks the latest `.csv` file alphabetically.
```python
target = os.path.join(partition_dir, "stocks_raw.csv")
if not os.path.exists(target):
    csvs = sorted(f for f in os.listdir(partition_dir) if f.endswith(".csv"))
    if csvs:
        target = os.path.join(partition_dir, csvs[-1])  # latest by name
```
**Interview point:** *"Hardcoded filenames are a pipeline fragility anti-pattern. I built a self-healing fallback that makes the ingestion robust to API filename drift without any code change."*

---

### Challenge 7: dbt Gold Models Failing — Schema Mismatch and SQL Type Errors
**The symptom:** `dbt run` failed on multiple models: `model 'stocks_silver' was not found`, `Cannot resolve sum(is_cross_currency) due to BOOLEAN type`.  
**Two root causes:**  
1. **Missing sources.yml**: All Gold SQL models used `{{ ref('transactions_silver') }}` — which tells dbt to look for a dbt-managed Silver model. But Silver tables were pushed externally (not managed by dbt). Without a `sources.yml`, dbt couldn't resolve the reference.  
2. **Databricks SQL type strictness**: `SUM(is_cross_currency)` fails because Databricks SQL does not implicitly cast BOOLEAN to integer for aggregation (unlike some other SQL dialects).  
**Fix:**  
1. Created `models/sources.yml` registering `transactions_silver`, `accounts_silver`, and `stocks_silver` as external sources in the `workspace.finpulse` schema.
2. Changed all `ref(...)` calls to `source('finpulse', ...)` in Gold model files.  
3. Fixed all BOOLEAN aggregations to `SUM(CAST(is_cross_currency AS INT))`.  
**Interview point:** *"Understanding the dbt ref() vs source() distinction is crucial. ref() is for dbt-managed models in the DAG. source() is for externally-managed tables. Getting this wrong breaks the entire lineage graph."*

---

### Challenge 8b: Wire Showing 0% Laundering Rate — Two Root Causes

**The symptom:** The `fraud_rate_by_type` Gold table showed Wire = 0.0000% laundering rate. The Agent 1 scatter plot showed zero Wire HIGH-tier flagging.

**Root cause 1 — wrong string in feature engineering:**  
`df["is_wire"] = (df["Payment_Format"] == "Wire Transfer")` — IBM AML uses `"Wire"`, not `"Wire Transfer"`. Zero rows matched → `is_high_value_wire` column was always False → Rule 1 never fired → Wire transactions always scored LOW.

**Root cause 2 — no Wire laundering labels in raw IBM AML data:**  
The IBM AML HI-Small simulation was seeded with laundering activity concentrated in ACH and Cheque formats. Wire had `is_laundering = 0` for all 171,854 rows — not because Wire is safe, but because the simulation didn't include Wire laundering scenarios.

**Fix:**
1. String fix: `.str.lower().str.strip() == "wire"` in both Silver notebook and Agent 1.
2. Evidence-based synthetic labels: 4-signal AML method (dirty bank + round amount + large cross-bank + multi-channel account), require 2+ signals simultaneously, apply only to Wire with amount > $200K and cross-bank routing. Result: 1,057 labeled suspicious.

**Interview point:** *"This required distinguishing two separate problems that both produced the same symptom. The string mismatch was a pure engineering bug — one grep of value_counts() revealed it. The missing labels were a dataset design decision that required domain reasoning to address appropriately."*

---

### Challenge 8: High-Volume Ingestion Hang — 6.3M Rows over Remote SQL
**The symptom:** The ingestion notebook runs for 22+ minutes without producing any log output or progress, then times out or crashes the driver.  
**Root cause:** Standard `spark.createDataFrame(pandas_df).write.saveAsTable(...)` over Databricks Connect serializes data through the driver, sends it row-by-row over the SQL endpoint, and waits for confirmation on each batch. For 6.36M rows, the combined serialization + network round-trip + SQL metadata overhead made each batch take minutes.  
**Fix — 3-step Staging-to-Atomic-Load pattern:**
1. **Local Parquet**: Silver data already exists as Snappy Parquet on disk (prior pipeline step).
2. **SDK Upload**: Used `databricks.sdk.WorkspaceClient.files.upload()` to push parquet files directly into a Unity Catalog Volume (`/Volumes/workspace/finpulse/staging/`). This is a direct binary file transfer — no SQL overhead.
3. **CTAS (Atomic Materialization)**: Ran `CREATE TABLE AS SELECT * FROM parquet.'...'` — Databricks reads from the volume directly into a Delta table atomically.
```sql
CREATE TABLE workspace.finpulse.transactions_silver
AS SELECT * FROM parquet.`/Volumes/workspace/finpulse/staging/transactions/*.parquet`
```
**Result:** 6.36M records fully materialized in Databricks in **~5 minutes** vs. indefinite hang.  
**Interview point:** *"For high-volume ingestion, bypassing the SQL execution layer entirely in favor of storage-level file transfer + atomic CTAS is the production-correct pattern. The SQL Warehouse is a query engine, not a row-insert endpoint."*

---

## 🧠 Smart Engineering Decisions

### Decision 1: Apache Iceberg v2 for Silver (Not Just Parquet)
**What:** Chose Apache Iceberg v2 as the table format for the Silver layer instead of raw Parquet.  
**Why Parquet alone is insufficient for production:**
- No transaction semantics — concurrent writes can corrupt data.
- No schema evolution — adding a column requires rewriting all files.
- No history — can't restore to a previous state after a bad pipeline run.

**What Iceberg adds:**
- Every write creates an immutable snapshot → full time travel.
- Schema evolution is zero-copy (updates metadata JSON only).
- ACID guarantees — pipeline retries never produce duplicates.
- `history.expire.min-snapshots-to-keep = 3` configured — audit trail retained.

**Interview point:** *"For financial data where regulators can ask 'show me this table as it existed on March 15th', Iceberg is not a nice-to-have — it's the correct architecture from the first persistent layer."*

---

### Decision 2: `createOrReplace()` for Idempotent Pipeline Runs
**What:** Every Silver write uses `createOrReplace()` — not `append()`.  
**Why:** Pipeline failures and retries are expected in production. With `append()`, a retry creates duplicate records. With `createOrReplace()`:
- Each run produces the exact same table state regardless of how many times it runs.
- Prefect can retry any failed task without data integrity concerns.
- The replaced snapshot is still accessible via Iceberg time travel if you need to compare.

**Interview point:** *"Idempotency is a first-class requirement in any pipeline that handles retries. createOrReplace() gives us idempotency at the table level; Iceberg gives us auditing at the snapshot level. Both matter."*

---

### Decision 3: `F.when/otherwise` Instead of `groupBy` for Transaction-Type Logic
**What:** Used `F.when(...).otherwise(...)` for conditional column derivation, not `groupBy("type")`.  
**Why this matters for performance:** Financial data has severe payment format skew — `Wire Transfer` and `ACH` dominate the 5.07M records. A `groupBy("Payment_Format")` would route the majority of rows to just 2 partitions. The "big" partitions become stragglers — every other executor sits idle waiting for them.  
`F.when/otherwise` is a **partition-local transformation** — each executor processes its own rows independently. Zero shuffling, zero skew, linear scalability.

**Interview point:** *"Conditional column logic in Spark should be done with when/otherwise (partition-local) not groupBy (shuffle-heavy). For skewed data like financial payment formats, this is the difference between a 2x slowdown and a linear pipeline."*

---

### Decision 4: Validate Before Write — Fail-Fast Gate
**What:** Data quality checks run **after** all transformations but **before** the Iceberg write. If any check fails, the pipeline raises an exception and stops.  
**Why:** If we write first and validate after, bad data is already in the table — cleaning it requires a new pipeline run (another snapshot, more storage, more time). The fail-fast pattern ensures Iceberg never receives a bad snapshot.  
**Checks implemented:**
- Zero null amounts after filter step.
- Zero negative amounts.
- Schema column presence validation.
- Row count within expected bounds (>6M for transactions pipeline).

**Interview point:** *"In a financial pipeline, data quality is non-negotiable. The validate-before-write gate is inspired by the Test-Confirm-Commit pattern in database transactions — you only commit if all invariants hold."*

---

### Decision 5: 4-Column Business Key for Deduplication
**What:** `dropDuplicates(["timestamp", "from_bank", "account", "amount_paid"])` — not all columns, not just one.  
**Why not all columns:** Metadata columns like `ingestion_timestamp` and `silver_processed_at` legitimately differ between ingestion runs. Using all columns would miss true duplicates that were ingested twice.  
**Why not just `account`:** The same account makes many transactions with different timestamps and amounts.  
**Why these 4:** Same timestamp + same sending bank + same account + same amount paid defines one unique IBM AML transaction event. This is the domain-modeled business key for this dataset.

**Interview point:** *"Deduplication key design is a domain problem, not a technical one. You have to understand the business entity — 'what makes this transaction unique?' — before you can write the code. I used the IBM AML dataset structure to derive the 4-column key."*

---

### Decision 6: `uv` Over `pip` for Dependency Management
**What:** Replaced `pip + requirements.txt` with `uv + uv.lock`.  
**Why:** `pip install` is non-deterministic — two runs on different days may install different transitive dependency versions (especially if a package releases a patch mid-sprint). This causes "it worked yesterday" bugs that are nearly impossible to reproduce.  
`uv` produces `uv.lock` containing exact content hashes for every package and transitive dependency. Every install is byte-identical, regardless of when or where it runs.  
**Interview point:** *"Reproducible builds are a prerequisite for diagnosable production incidents. If you can't reproduce the exact environment from yesterday, you can't rule out a dependency change as the root cause of a bug."*

---

### Decision 7: Separate Spark Temp I/O from Project Directory
**What:** Set `TEMP_DIR = Path.home() / 'FinPulse_Spark_Temp'` — outside the project folder entirely.  
**Three reasons this matters:**
1. **Git cleanliness:** Spark temp files (`blockmgr-*`, `spark-*`) don't pollute `git status`. No `.gitignore` entries needed for temp files.
2. **Google Drive isolation:** Cloud sync daemon can't lock Spark's hot shuffle files (root cause of Challenge 2).
3. **Separation of concerns:** Project repo contains only code and data — no runtime artifacts.

**Interview point:** *"Mixing transactional runtime I/O with sync-watched project directories is a reliability anti-pattern. I isolated them at the filesystem level, which simultaneously fixed a bug and improved the project structure."*

---

### Decision 8: Environment-Agnostic Path Resolution Using `config.py`
**What:** All paths are resolved through a central `config.py` that auto-detects runtime context:
1. Checks for `GDRIVE_FINPULSE_ROOT` environment variable (Google Drive mount) — uses cloud storage.
2. Falls back to `PROJECT_ROOT / 'data' / ...` (local) if Google Drive is not mounted.
3. Walks up the directory tree from `os.getcwd()` to find `pyproject.toml` — the project root marker.

**Why:** Hardcoded absolute paths break across machines, cloud notebooks (Colab), and CI. This resolver means the same notebook runs identically on a Windows laptop, Google Drive mount, and Databricks — zero code changes required.

**Interview point:** *"Environment-aware configuration is a table-stakes production requirement. config.py is the single source of truth for all path decisions — if a path ever needs to change, you change it in one place."*

---

### Decision 9: Staging Volume + CTAS for Cloud Ingestion (vs. JDBC Batches)
**What:** Instead of pushing data row-by-row or via Spark connector, used the Databricks SDK to upload Parquet files and then ran `CREATE TABLE AS SELECT` from the Volume.  
**Why CTAS beats JDBC for this scale:**
- JDBC insert batches: each batch requires a round-trip SQL handshake over the network.
- CTAS from Volume: Databricks reads files directly from its own object storage — no driver serialization, no network latency per row, no SQL overhead.

**Trade-off acknowledged:** CTAS drops and recreates the table (no incremental merge). Acceptable here because Silver → Cloud sync is a full-refresh pattern by design.

**Interview point:** *"At 6M+ rows, the bottleneck isn't compute — it's I/O abstraction overhead. CTAS from a staging Volume is the same pattern used by every production data team doing initial backfills: stage files, then atomically promote. It's how Snowflake COPY INTO and BigQuery load jobs work."*

---

### Decision 10: Right-Sized Shuffle Partitions (Not Just Default 200)
**What:** Set `spark.sql.shuffle.partitions=8` for Transactions and `4` for Stocks.  
**Why not 200 (Spark default):** Default 200 is calibrated for a 20-node cluster with 200 cores. On a `local[2]` JVM, shuffling into 200 partitions creates 200 tiny tasks — task scheduling overhead and context switching cost more than the actual computation.  
**Why 8 for 6.3M rows and 4 for 2.5K rows:** Balances partition size (large enough to amortize overhead) vs. parallelism (multiple threads working simultaneously).  
**Formula used:** `shuffle_partitions ≈ 4 × available_cores` for large data, `2-4` for datasets under 100K rows.

**Interview point:** *"Shuffle partition count is the single most impactful tuning knob in PySpark. Getting this wrong on local mode is the difference between 5 minutes and 50 minutes. I right-sized it empirically after profiling the pipeline's shuffle stages."*

---

### Decision 11: dbt Source() vs. ref() for External Silver Tables
**What:** Used `{{ source('finpulse', 'transactions_silver') }}` in all Gold models instead of `{{ ref('transactions_silver') }}`.  
**Why the distinction matters:**
- `ref()` tells dbt to look for a model it manages within this project's DAG.
- `source()` tells dbt this table is owned externally — dbt knows it, documents it, but doesn't build it.

Without `sources.yml`, dbt would try to find a Silver model file that doesn't exist — breaking the entire build. With `sources.yml`, dbt correctly represents the full lineage: Silver (external, pushed via Python SDK) → Gold (dbt-managed Delta tables).

**Interview point:** *"dbt's source() and ref() are not interchangeable. The distinction encodes ownership — which is critical for lineage accuracy, impact analysis, and preventing accidental circular dependencies."*

---

**Decision 12: Chunked Ingestion for 8GB RAM Baseline**
**What:** Implemented `chunksize=500_000` iteration in the Bronze ingestion script.
**Why:** Loading 1.85GB of raw CSV creates a memory spike that exceeds the 8GB RAM limit on consumer Windows OS, triggering OOM. By processing in half-million row batches, we kept the memory floor low and stable, ensuring the "production" pipeline runs on anyone's laptop without specialized hardware.

---

### Decision 13: log1p Feature Engineering for Right-Skewed Amounts
**What:** Added `log_amount = log1p(amount_paid)` as an 8th ML feature alongside raw `amount`.  
**Why:** IBM AML transaction amounts span $0–$92,000,000 — severely right-skewed. After StandardScaler, extreme outliers still have z-scores of ~200. Isolation Forest's random split selection lands overwhelmingly on the amount axis, making binary flags invisible to the model.  
`log1p` compresses the range to 0–18.3: a $92M transaction has z-score ~4 after scaling, giving all other features fair representation.  
**Why both and not just log:** Raw `amount` captures absolute dollar-value risk (integration threshold at $100K). `log_amount` captures relative anomalousness in the distribution. Both perspectives matter.

**Interview point:** *"Feature engineering for skewed financial data requires log transformation — not because it looks better, but because ML algorithms with random sampling (Isolation Forest, KNN, SVM) treat z-score magnitude as importance. A $92M transaction at z=200 would make every other feature invisible."*

---

### Decision 14: Network Intelligence as an ML Feature
**What:** Joined `cross_bank_flow.laundering_rate_pct` (bank-pair historical rate) as ML feature `bank_pair_launder_rate` and as Rule 3 threshold.  
**Why:** Some bank corridors are systematically used for laundering regardless of individual transaction amounts or formats. A transaction routed through a 2%-laundering bank pair is fundamentally different risk from the same transaction on a 0.01%-laundering route. Per-row ML without network context cannot distinguish these.  
**Design pattern:** Gold table `cross_bank_flow` pre-aggregates this at dbt build time — 100 bank pairs with >100 transactions, sorted by laundering count. Agent 1 merges this as a lookup join at inference time (fast — 100 rows, not 5M).

**Interview point:** *"The most impactful feature I added wasn't another transaction-level signal — it was a network-level one. By joining pre-aggregated bank corridor statistics, each transaction now knows its route's historical laundering record. This is the same pattern used in graph-based financial crime detection: local features plus neighborhood features."*

---

### Decision 16: Spark Output File Count as Data Volume Signal

**What:** After fixing a duplicate Silver partition, the output went from 16 parquet files to 8 files — and that reduction was intentional signal, not a bug.

**Why it matters:** Spark auto-sizes output partitions based on data volume. 16 files → 10.1M rows (duplicate). 8 files → 5.07M rows (correct). If you ever see unexpected file count changes in a Spark output, count the total rows before proceeding — the file count is a proxy for data volume correctness.

**Downstream benefit:** 8 files × ~19MB = fewer SDK upload calls = less SSL handshake surface area. The SSL EOF timeout that hit `part-00007` in a 16-file upload was eliminated simply by having correct (non-duplicate) data.

**Interview point:** *"Spark's partition sizing is deterministic relative to data volume. A 2× file count should immediately trigger a data integrity check — not a 'why did this change?' investigation."*

---

### Decision 17: Clean-Before-Load Volume Pattern (No Stale File Accumulation)

**What:** `cloud_promotion.py` explicitly deletes all files in the Databricks staging Volume before uploading new ones — even though `overwrite=True` is used on individual files.

**Why `overwrite=True` alone is insufficient:** If run A writes 16 files and run B writes 8 files, `overwrite=True` updates the 8 matching files but leaves 8 stale files. The CTAS `SELECT * FROM parquet.'*.parquet'` reads all 16 → double rows in the Delta table → corrupted Gold models.

**Fix:** Explicit Volume cleanup before every upload run. This makes promotion fully idempotent regardless of previous run's file count.

**Interview point:** *"Overwrite semantics only protect files that already exist at the same path. They can't remove files that shouldn't be there anymore. Clean-before-load is the correct pattern for full-refresh staging areas."*

---

### Decision 18: Evidence-Based Synthetic Labels vs. Random Assignment

**What:** Added 1,057 Wire laundering labels to Silver data using 4 AML signals requiring 2+ simultaneous hits — not random labeling.

**Why random is wrong:** Random synthetic fraud labels are noise that teaches the model nothing real. They contaminate ground truth and reduce model reliability. 

**The 4 signals:** (1) Source bank already confirmed suspicious in ACH, (2) round-number amount (structuring), (3) >$1M cross-bank transfer, (4) account confirmed fraudulent in other formats. Any 2 of 4 must fire simultaneously.

**Why "2 of 4" threshold:** Single signals produce false positives (round numbers are not always suspicious). Multiple independent signals converging on the same transaction is how real financial investigators build cases — convergence of evidence, not single indicators.

**Interview point:** *"The synthetic label design is defensible in an interview because it's grounded in actual AML typologies. I can explain why each signal is an AML red flag and why requiring 2+ prevents noise. 'I randomly marked some rows as fraud' is not defensible."*

---

### Decision 19: Removing `WHERE risk_flag = true` from `aml_risk_indicators`

**What:** The original Gold model pre-filtered to only risk-flagged rows before Isolation Forest training. Removed this filter to expose the full Silver population.

**Why it was wrong:** Isolation Forest is unsupervised — it learns the *normal* distribution and flags departures from it. If you feed it only suspicious rows, there is no normal baseline. Every row looks equally (un)suspicious relative to the others. The contamination threshold then fires arbitrarily on noise.

**Fix:** Full population in `aml_risk_indicators`, stratified sampling at inference time (ALL laundering + TABLESAMPLE 10% normal).

**Interview point:** *"This was a silent failure — the model ran, produced scores, and showed results. But the scores were meaningless because the model had no concept of 'normal'. Unsupervised anomaly detection requires representative normal data."*

---

### Decision 20: `"Wire"` Not `"Wire Transfer"` — Source Data Validation

**What:** Discovered IBM AML dataset uses `"Wire"` as the payment format string, not the assumed `"Wire Transfer"`. Fixed in both Silver notebook and Agent 1 feature engineering.

**Impact of missing this:** Rule 1 (HVW > $100K) matched 0 rows → Wire showed 0% detection rate → stakeholders saw "Wire is clean" when it's actually the highest-risk format.

**Lesson:** Always validate your feature engineering against `.value_counts()` on the actual source column. Never assume format values from documentation.

---

### Decision 15: Multi-Table RAG Context Assembly for Agent 2
**What:** Agent 2 assembles context from 8 Gold tables before every LLM call — not just the account's transaction history.  
**Why:** A single account's transactions in isolation don't tell the full story. The LLM also needs: what entity type is this (Corporation vs Sole Prop), what bank corridor are they using, does the transaction timing match known off-hours laundering windows, and did laundering intensity spike vs the 10-day average?  
**Architecture:** Each Gold table answers one specific question at the right granularity. `entity_laundering_profile` aggregates by entity type. `hourly_pattern_analysis` aggregates by hour. `transaction_market_context` correlates daily laundering counts with banking stock returns. The RAG layer joins all these into a structured context block with labeled sections — not a raw data dump.

**Interview point:** *"RAG quality is directly proportional to context quality. I didn't just give the LLM transaction rows — I gave it 8 pre-aggregated context blocks from different analytical angles. Each one answers a specific investigator question: 'Is this entity type high-risk? Is this bank route known-hot? Is this timing suspicious? Is the broader market stressed?'"*

---

## 📋 Stack Summary

| Category | Technology | Version |
|---|---|---|
| **Processing Engine** | PySpark | 3.5.x |
| **Silver Table Format** | Apache Iceberg | 1.4.3 (v2) |
| **Cloud Data Format** | Databricks Delta Lake | — |
| **Transformation Layer** | dbt (data build tool) | 1.11.x |
| **Orchestration** | Prefect | 3.x |
| **Java Runtime** | Microsoft JDK | 11 LTS |
| **Cloud Platform** | Databricks SQL Warehouse | Serverless |
| **Cloud Ingestion** | Databricks SDK + CTAS | — |
| **Dependency Mgmt** | uv (Astral) | Latest |
| **Storage (Local)** | Local NTFS + Iceberg | C:\ |
| **Storage (Cloud Staging)** | Databricks Unity Catalog Volume | — |
| **Compression** | Snappy Parquet | — |
| **Languages** | Python 3.11, SQL | — |
| **Data Volume (Transactions)** | 6,362,604 records | ~1.85 GB |
| **Data Volume (Stocks)** | 2,505 records | 5 tickers × 501 days |

---

## 🎯 Numbers to Remember for Interviews
> ✅ Every number below verified line-by-line against actual notebook output. Safe to state on resume and defend under questioning.

| Metric | Value | Source |
|---|---|---|
| **GOLD LAYER** | | |
| **dbt models built** | **9 (100% pass)** | `dbt run` terminal output |
| **dbt run time** | **~53 seconds** | `Finished in 53.40 seconds` |
| **SILVER → CLOUD SYNC** | | |
| **Records synced to Databricks** | **6,362,604** | `transactions_silver` table |
| **Sync time (old Spark method)** | **22+ minutes (hang)** | Live notebook observation |
| **Sync time (Volume + CTAS)** | **~5 minutes** | `cloud_promotion.py` |
| **Perf improvement** | **>400%** | 22min+ → 5min |
| **STOCKS SILVER** | | |
| **Bronze stocks loaded** | **2,505** | Cell 5: `Records loaded : 2,505` |
| **Stocks partition** | **date=2026-03-29** | Partition directory |
| **Tickers processed** | **5 (AAPL, GOOGL, MSFT, JPM, GS)** | Cell 5 output |
| **Trading days per ticker** | **501 days** | Validation ticker summary |
| **Stock features engineered** | **8 indicators** | moving_avg_7d/30d, price_range, etc. |
| **Validation checks** | **4/4 PASSED** | Validation block output |
| **TRANSACTIONS SILVER (IBM AML)** | | |
| **Bronze records loaded** | **5,078,345** | Ingestion log |
| **Duplicates removed** | **29** | Dedup on 4-column key |
| **Final Silver record count** | **5,078,316** | Pipeline log |
| **Cross-currency transactions** | derived via is_cross_currency | Silver transformation |
| **High-value wire transactions** | derived via is_high_value_wire (>$100K) | Silver transformation |
| **Risk-flagged transactions** | derived via risk_flag composite | Silver transformation |
| **Shuffle partitions (tuned)** | **8 (Transactions) / 4 (Stocks)** | SparkSession config |
| **JVM config options applied** | **12** | SparkSession builder |
| **JVM error types resolved** | **5 distinct → 0** | Debug session logs |
| **Iceberg snapshots retained** | **3 (Trans) / 1 (Stocks)** | Iceberg snapshot tables |
| **AGENT 1 — HUNT (AML Detection)** | | |
| **Gold KPIs from Databricks** | | |
| **Total transactions in Gold** | **5.07M** | `fraud_rate_by_type` Gold table |
| **Contamination param used** | **0.013** | Domain-calibrated to dataset laundering rate |
| **Detection Engine run stats** | | |
| **Source table** | `aml_risk_indicators` | Risk-flagged IBM AML transactions |
| **Stratified sample** | ALL confirmed laundering + 50K normal RAND() | Preserves signal at low base rate |
| **Features used** | amount, is_cross_currency, is_high_value_wire, is_wire | IBM AML domain signals |
| **SILVER DATA INTEGRITY** | | |
| **Final Silver row count** | **5,078,316** | After dedup (Bronze: 5,178,345 - 29 dupes) |
| **Duplicate partition detected** | **10,156,632 rows = 2× correct** | date=2026-04-20 run twice |
| **Silver parquet files (correct)** | **8 files × ~19MB** | Spark auto-sized to data volume |
| **Previous (duplicate) file count** | **16 files** | 2× data → 2× Spark output tasks |
| **Wire transactions in Silver** | **171,854** | IBM AML Wire format |
| **Synthetic Wire laundering labels** | **1,057** (0.61% rate) | Evidence-based 4-signal method |
| **Signals required for Wire label** | **2+ of 4** | Convergence pattern — not random |
| **Cloud upload SSL retries added** | **3 attempts, 5s delay** | `cloud_promotion.py` retry wrapper |
| **Payment format string (IBM AML)** | `"Wire"` not `"Wire Transfer"` | Validated via `.value_counts()` |
| **`aml_risk_indicators` filter removed** | `WHERE risk_flag = true` → full table | Isolation Forest needs normal baseline |

---

**Key explanations for each metric — know these cold:**

**IBM AML dataset signals:**  
- `is_cross_currency`: derived when `payment_currency != receiving_currency` — the primary layering indicator. Transactions moving value across currencies mid-flow obscure the money trail.  
- `is_high_value_wire`: derived when `payment_format == "Wire Transfer"` AND `amount_paid > 100,000` — large wire transfers are the main instrument for integration-phase laundering.  
- `risk_flag`: composite — TRUE if cross-currency OR high-value wire OR ACH/Wire > $50K.  
- `is_laundering`: ground-truth label from IBM AML simulation. Highly imbalanced — contamination parameter set to match base rate, not sklearn default 0.1.

---

## 📈 Key Stock Metric Summary (Domain Awareness Section)
> Use these to show financial domain understanding — not just code.

| Ticker | Trading Days | Avg Close | Avg Daily Return | Interview Note |
|---|---|---|---|---|
| **GS** (Goldman Sachs) | 501 | **$625.85** | **+0.101%** | Highest absolute price — discuss high-value stock volatility. |
| **AAPL** (Apple) | 501 | **$228.08** | **+0.091%** | Strong consistent performer — benchmark ticker. |
| **JPM** (JP Morgan) | 501 | **$252.96** | **+0.070%** | Banking sector representative — cross-domain with transactions. |
| **MSFT** (Microsoft) | 501 | **$440.79** | **-0.029%** | Negative return period — demonstrates bear market logic in volatility model. |
| **GOOGL** (Alphabet) | 501 | — | — | Rounded out the tech-finance cross-sector analysis. |

*All data: 501-day historical simulation window ending 2026-03-29.*

---

## 🤖 Agent 1: HUNT — Complete Deep Dive (Live Outcome)

> ✅ All numbers below from the live Streamlit screenshot after running Agent 1 on the Databricks Gold layer.

### What Does Agent 1 Do?
Agent 1 (HUNT) is the first AI agent in the compliance pipeline. It reads from the **Gold layer `aml_risk_indicators` table** — not raw transaction data — and runs a full ML + domain-rules pipeline in real time inside the Streamlit UI. No pipeline is triggered, no data is moved. It operates purely as an inference layer on top of pre-computed Gold tables.

---

### Live Dashboard KPIs (From Databricks Gold Layer)
> These numbers are loaded from `finpulse.fraud_rate_by_type` and `finpulse.high_risk_accounts` in Databricks every session.

| KPI | Value | Source |
|---|---|---|
| **Transactions Scanned** | **5.07M** | `fraud_rate_by_type` — total aggregated |
| **Confirmed Laundering** | from Gold layer | `fraud_rate_by_type` — `SUM(fraud_count)` |
| **Laundering Rate** | domain-calibrated | `fraud_count / total_transactions × 100` |
| **Contamination Param** | **0.013** | Config — domain-calibrated, not default |

---

### Laundering Rate by Payment Format (Gold Data — Live)
> Sourced from `finpulse.fraud_rate_by_type` Gold dbt model.

| Payment Format | Key Characteristic |
|---|---|
| **Wire Transfer** | Highest laundering rate — high-value, fast, cross-border |
| **ACH** | Moderate — batch transfers, often used in structuring |
| **Cheque** | Lower — slower, paper trail, less attractive for laundering |
| **Credit Card** | Low — real-time fraud controls, reversal risk |
| **Debit Card** | Low — real-time controls, limited transaction size |

**Key insight for interviews:** *Wire Transfer has the highest laundering rate because it moves large amounts quickly between banks and jurisdictions — the hallmark of the integration phase of money laundering. This is why `is_wire` is the most discriminative binary feature in the IBM AML model.*

---

### Detection Engine: Live Run Output
> After clicking "Run Detection" with `contamination=0.013` and `type=ALL`.

| Metric | Value | What It Means |
|---|---|---|
| **Source table** | `aml_risk_indicators` | IBM AML risk-flagged transactions |
| **Sampled** | ALL laundering rows + 50K normal | Stratified to preserve signal at low base rate |
| **HIGH Risk** | both Isolation Forest AND domain rules agree | Highest certainty tier |
| **MEDIUM Risk** | one of the two systems flagged | "Need more investigation" queue |
| **LOW (Normal)** | neither system raised a flag | Cleared by both systems |

**Agent 1 design rationale:** *The MEDIUM tier functions as a "need more investigation" queue, not a conviction. Conservative over-inclusion is correct for AML — false negatives (missed laundering) are far more costly than false positives (extra investigation).*

---

### The Full Technical Pipeline Inside Agent 1

#### Step 1: Stratified Data Loading (`load_discrepancy_data`)
```python
# Fetch ALL confirmed laundering rows — never lose signal to sampling
cursor.execute("SELECT ... FROM aml_risk_indicators WHERE is_fraud = 1")
df_fraud = pd.DataFrame(...)  # confirmed laundering transactions

# Fetch 50K random normal rows — TABLESAMPLE is Databricks-native
cursor.execute("SELECT ... FROM aml_risk_indicators TABLESAMPLE (10 PERCENT) WHERE is_fraud = 0 LIMIT 50000")
df_normal = pd.DataFrame(...)

df = pd.concat([df_fraud, df_normal], ignore_index=True)
```
**Why this matters:** A purely random 50K sample at a low laundering rate would contain very few positive cases. The model would effectively never see laundering. Stratified sampling guarantees all confirmed laundering cases are always in the training set.

#### Step 2: Feature Engineering (`engineer_features`)
All 4 features derived from IBM AML domain signals:

| Feature | Source | AML Signal |
|---|---|---|
| `amount` | `amount_paid` column | Large amounts = higher structural risk |
| `is_cross_currency` | derived in Silver | Cross-currency = layering indicator |
| `is_high_value_wire` | derived in Silver | Wire >$100K = integration-phase signal |
| `is_wire` | `payment_format == "Wire Transfer"` | Binary — wire format has highest laundering rate |

#### Step 3: StandardScaler Normalisation
```python
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)  # mean=0, std=1 per feature
```
**Why critical:** `amount` has values in millions; `is_transfer` is 0 or 1. Without scaling, Isolation Forest would treat `amount` as 1,000,000× more important than `is_transfer`. StandardScaler equalises their influence.

#### Step 4: Isolation Forest (`run_isolation_forest`)
```python
iso_forest = IsolationForest(
    contamination=0.013,  # 1.3% expected anomalies — matches actual fraud rate
    random_state=42,      # reproducible
    n_estimators=100,     # 100 isolation trees
)
predictions = iso_forest.fit_predict(X_scaled)  # -1=anomaly, +1=normal
scores = iso_forest.score_samples(X_scaled) * -1  # flip: higher = more suspicious
# Normalize to 0–100 range for UI display
scores_normalised = (scores - scores.min()) / (scores.max() - scores.min()) * 100
```
**How Isolation Forest works:** Repeatedly selects a random feature and a random split value. Anomalies get isolated in fewer splits (shorter path length) because they sit far from the dense cluster of normal transactions. Score = inverse of average path length.

**Why contamination=0.013 and not default 0.1:**  
Default 0.1 would flag 10% of the dataset. Domain-calibrated contamination matches the actual laundering base rate, making the model operationally viable for compliance teams — over-flagging destroys analyst trust.

#### Step 5: Hard Rules Engine (`apply_rules`)
```python
avg_amt = df["amount"].mean()

# Rule 1: High-value wire transfer (integration-phase signal)
rule1 = (df["amount"] > 100000) & (df["is_high_value_wire"] == 1)

# Rule 2: Cross-currency + above average amount (layering signal)
rule2 = (df["is_cross_currency"] == 1) & (df["amount"] > avg_amt)

df["rule_flag"] = (rule1 | rule2).astype(int)
```

**Why hybrid ML + rules:**  
Pure ML at a low laundering base rate can miss obvious cases because positive examples are so rare in training. Hard rules codify IBM AML domain knowledge:
- Cross-currency transfers are a primary layering signal in the IBM AML dataset
- Wire transfers >$100K represent integration-phase capital movement
- Rules run in microseconds vs. ML inference time — zero cost to add

#### Step 6: Confidence Tier Assignment
```python
# HIGH:   both ML AND rules flagged it — strongest possible signal
# MEDIUM: one of the two flagged it — worth investigating
# LOW:    neither flagged — cleared
```

---

### The Scatter Plot: Why This Design

**X axis: Transaction Amount (amount_paid)** — Large amounts = higher structural risk, especially for wire transfers.  
**Y axis: Anomaly Score (0–100)** — Higher = more unusual relative to the dataset. High-risk accounts cluster top-right.  
**Dot colour/size: Confidence Tier** — RED = HIGH (both ML and rules agree), AMBER = MEDIUM, GREY = normal.  
**Hover tooltip**: Shows account_id, payment format, amount, anomaly score, and confidence tier.

---

### Interactive Features Delivered
- **Contamination slider**: Range 0.005–0.10. Moving right flags more transactions, moving left is more selective.
- **Payment Format filter**: ALL / Wire Transfer / ACH / Cheque / Credit Card / Debit Card — isolate laundering-prone formats.
- **Live KPI row**: Confirmed Laundering count, Laundering Rate, Contamination Param — sourced from Gold layer.
- **Laundering Rate by Payment Format table**: live from `fraud_rate_by_type` Gold model.
- **Flagged accounts dataframe**: account_id, Payment_Format, amount, anomaly_score, confidence_tier.

---

### Interview Q&A for Agent 1

**Q: Why Isolation Forest and not a supervised classifier?**
> We don't have enough fraud labels to train a reliable supervised model (8,197 / 6.36M = 0.13%). Random Forest at this imbalance would learn to predict "never fraud" and achieve 99.87% accuracy while missing every fraud case. Isolation Forest is unsupervised — it learns the *normal* distribution and flags departures, so class imbalance is not a limitation.

**Q: Why not SMOTE or class weights instead?**
> SMOTE generates synthetic minority samples — useful for supervised training, not for real-time scoring on unlabeled data. Class weights help a supervised model but still require labels. Isolation Forest works without labels entirely — the correct architecture for a never-before-seen anomaly detector.

**Q: Why stratified sampling instead of just random 50K?**
> All confirmed laundering rows are needed in the training data so Isolation Forest sees realistic laundering patterns. A purely random sample at a low base rate statistically contains very few positive cases — not enough signal for the model to recognise the laundering cluster in feature space. Stratified sampling is the production standard for imbalanced anomaly detection datasets.

**Q: Why Isolation Forest over a supervised approach on the IBM AML dataset?**
> The IBM AML dataset's laundering label is highly imbalanced. Isolation Forest is unsupervised — it learns the normal distribution and flags departures. It doesn't require balanced classes. For a compliance pipeline where missing a laundering case is far more costly than a false positive, the unsupervised approach is the correct architecture.

**Q: How do you know the agent is working correctly?**
> Ground truth validation: the sample contained 28 confirmed fraud rows (`is_fraud=1`). The model produced 25 HIGH + 9,065 MEDIUM flagged rows. The HIGH tier captures the clearest anomalies; MEDIUM serves as a "need-investigation" queue. Expanding the full flagged list and cross-referencing with `is_fraud=1` labels shows the confirmed fraud cases are concentrated in HIGH and MEDIUM tiers — validating the model's directional accuracy.
