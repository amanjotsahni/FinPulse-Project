# FinPulse — Complete Master Project Prompt v8.0

**Project Name:** FinPulse — Intelligent Financial Compliance & Risk Intelligence System
**Developer:** Amanjot Kaur
**Goal:** Portfolio project targeting JP Morgan, Deloitte, Walmart Global Tech, American Express level companies
**IDE:** Google Antigravity with Gemini Pro
**Python Version:** 3.11.1
**OS:** Windows 10, 8GB RAM
**Project Location:** `C:\FinPulse Project`
**GitHub:** github.com/amanjotsahni/FinPulse (public)
**Java:** Microsoft JDK 11.0.30.7-hotspot at `C:\Program Files\Microsoft\jdk-11.0.30.7-hotspot`
**Hadoop:** Project-local at `C:\FinPulse Project\hadoop\` (winutils.exe + hadoop.dll installed)
**Iceberg JAR:** `C:\FinPulse Project\jars\iceberg-spark-runtime-3.5_2.12-1.4.3.jar` (29MB)
**dbt version:** 1.11.6 (dbt-databricks 1.11.6, dbt-spark 1.10.1)
**Elementary version:** 0.14.1 (installed, dbt deps complete, monitoring tables created)

---

## Problem Statement

Financial institutions process millions of transactions daily. Detecting fraudulent or anomalous transactions manually is slow, reactive, and error prone. FinPulse is an end-to-end data engineering pipeline that ingests real financial transaction data and live stock market data, processes it through a Medallion architecture with enterprise-grade data quality and lineage tracking, detects anomalies using machine learning, and routes findings through a multi-agent AI compliance intelligence system — delivering a daily risk briefing with SAR filing recommendations that a compliance officer can act on, without touching a single dashboard.

---

## Dataset Details

**Transaction Data:**
- Source: Kaggle PaySim Synthetic Financial Dataset
- Location: `C:\Users\amana\Downloads\Finance data\Finance_dataset.csv`
- Shape: 6,362,620 rows × 11 columns
- Columns: step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig, nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud
- Zero null values, 0.13% fraud rate, zero duplicates
- Fraud ONLY in TRANSFER and CASH_OUT types
- Balance discrepancies: 1,199,155 records (18.8%)
- Risk flagged: 1,326,648 records (20.8%)
- Zero amount transactions removed: 16

**Stock Market Data:**
- Source: Yahoo Finance via yfinance
- Tickers: AAPL, GOOGL, MSFT, JPM, GS
- 2,505 records (~501 trading days × 5 tickers)
- GS total return: +100.5% (ranked 1st — doubled in 2 years)
- GOOGL total return: +83.3% (ranked 2nd)
- JPM total return: +47.4% (ranked 3rd)
- AAPL total return: +46.4% (ranked 4th)
- MSFT total return: -13.9% (ranked 5th — only negative performer)

---

## Complete Tech Stack v8.0

| Layer | Tool | Where | Cost |
|-------|------|-------|------|
| IDE | Google Antigravity + Gemini Pro | Local | Free |
| Language | Python 3.11.1 | Local | Free |
| Ingestion | Python + yfinance | Local | Free |
| Orchestration | Prefect (Ephemeral Mode) | Local | Free |
| Bronze Storage | CSV partitioned by date | Local C: drive | Free |
| Silver Processing | PySpark 3.5.8 + Apache Iceberg v2 | Local Antigravity notebook | Free |
| Silver Storage | Iceberg Hadoop catalog | Local C: (primary) + Google Drive G: (backup) | Free |
| Silver → Databricks | Volume Upload + COPY INTO | Antigravity notebook | Free |
| Data Quality | Soda Core (YAML checks) | Local → Databricks | Free |
| Gold Transformation | dbt 1.11.6 | Local dbt → Databricks SQL | Free |
| Gold Lineage | Elementary 0.14.1 | dbt package | Free |
| AI Interface | Streamlit | Local Python | Free |
| AI Agent 1 — Hunt | Scikit-learn Isolation Forest + Rules | Local Python | Free |
| AI Agent 2 — Investigate | LlamaIndex + Gemini 2.5 Flash | Local Python | Free |
| AI Agent 3 — Reason | DeepSeek R1 via OpenRouter | Local Python | Free |
| AI Agent 4 — Brief | Groq Llama3-70b | Local Python | Free |
| LLM Gateway | LiteLLM | Local Python | Free |
| AI Findings Storage | Databricks Gold table (ai_risk_findings) | Databricks | Free |
| Visualization | Power BI Desktop → Databricks SQL | Local | Free |
| Version Control | GitHub Desktop + GitHub | Local | Free |

**Total Cost: ₹0**

---

## API Keys — All Configured ✅

```
GROQ_API_KEY           = ✅
GEMINI_API_KEY         = ✅ Gemini 2.5 Flash, 5 RPM, 250K TPM
OPENROUTER_API_KEY     = ✅ DeepSeek R1
DATABRICKS_HOST        = dbc-6ee18cda-1d4e.cloud.databricks.com
DATABRICKS_HTTP_PATH   = /sql/1.0/warehouses/5e0d68e9ac57b378
DATABRICKS_TOKEN       = ✅ 90 day, SQL + clusters + workspace scope
DATABRICKS_CATALOG     = hive_metastore
DATABRICKS_SCHEMA      = finpulse
ICEBERG_WAREHOUSE      = C:\FinPulse Project\data\silver\iceberg_warehouse
BRONZE_TRANSACTIONS_PATH = C:\FinPulse Project\data\bronze\transactions
```

**Databricks:**
- Catalog: workspace (hive_metastore)
- Schema: finpulse
- Warehouse: Serverless Starter Warehouse
- Unity Catalog: NOT used — free tier uses hive_metastore
- JDBC: `jdbc:databricks://dbc-6ee18cda-1d4e.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5e0d68e9ac57b378;`

---

## Storage Architecture v8.0

```
Layer         Primary                                               Backup
──────────────────────────────────────────────────────────────────────────
Bronze     →  C:\FinPulse Project\data\bronze\                     None
Silver     →  C:\FinPulse Project\data\silver\iceberg_warehouse\   G:\My Drive\FinPulse\data\silver\iceberg_warehouse\
Databricks →  workspace.finpulse (Databricks SQL Warehouse)        None
Gold       →  workspace.finpulse (Databricks SQL Warehouse)        None
AI Reports →  C:\FinPulse Project\data\ai_reports\                 None
AI Findings→  hive_metastore.finpulse.ai_risk_findings (Databricks)None
```

**Key decisions:**
- Iceberg writes to C: — Google Drive atomic rename conflict with ACID
- Drive sync happens AFTER spark.stop() — completed tables only
- Silver pushed to Databricks via Volume Upload + COPY INTO (5 min vs 22min+ JDBC timeout)
- Gold lives natively in Databricks — dbt creates and manages all tables
- AI findings written back to Databricks — closes the pipeline loop, feeds Power BI
- dbt profiles: `C:\Users\amana\.dbt\profiles.yml`
- dbt project: `C:\FinPulse Project\dbt_finpulse\`

---

## Apache Iceberg — Implementation Details

**Catalog:** Hadoop local filesystem
**Catalog name in Spark:** `local`
**Namespace:** `local.finpulse`
**Warehouse:** `C:\FinPulse Project\data\silver\iceberg_warehouse`

**Tables confirmed:**
- `local.finpulse.silver_transactions` — 6,362,604 records, 21 cols, 3 snapshots
- `local.finpulse.silver_stocks` — 2,505 records, 19 cols, 1 snapshot

**Demonstrated features (all working):**
- Time travel — snapshot `8040436300429848040` queried ✅
- Schema evolution — `risk_score DOUBLE` added without rewrite ✅
- ACID proof — atomic write confirmed ✅
- Snapshot history — 3 runs tracked ✅

**Spark config for Windows (copy exactly every time):**
```python
spark = SparkSession.builder \
    .appName("FinPulse") \
    .master("local[*]") \
    .config("spark.jars", r"C:\FinPulse Project\jars\iceberg-spark-runtime-3.5_2.12-1.4.3.jar") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", r"C:\FinPulse Project\data\silver\iceberg_warehouse") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.extraJavaOptions", "-Dorg.apache.hadoop.io.nativeio.NativeIO.Windows.should_use_native_io=false") \
    .config("spark.executor.extraJavaOptions", "-Dorg.apache.hadoop.io.nativeio.NativeIO.Windows.should_use_native_io=false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
    .config("spark.sql.defaultCatalog", "local") \
    .getOrCreate()
```

---

## Silver Layer — Confirmed Results

**silver_transactions (8 transformations):**
1. Remove zero amounts → 16 removed
2. is_balance_discrepancy flag → 1,199,155 flagged
3. hour_of_simulation + day_of_simulation
4. type_encoded (TRANSFER=0, CASH_OUT=1, PAYMENT=2, DEBIT=3, CASH_IN=4)
5. risk_flag → 1,326,648 flagged
6. balance_diff
7. Dedup → 0 duplicates
8. Pipeline metadata

**silver_stocks (9 transformations):**
1. Lowercase column names
2. Date cast to DateType
3. daily_return = (close-open)/open*100
4. price_change via lag() window
5. is_positive_day
6. price_range = high-low
7. moving_avg_7d (rowsBetween -6,0)
8. moving_avg_30d (rowsBetween -29,0)
9. Pipeline metadata

---

## Databricks Silver Tables (Confirmed)

| Table | Rows | Method |
|-------|------|--------|
| `finpulse.transactions_silver` | 6,362,604 | Volume Upload + COPY INTO |
| `finpulse.stocks_silver` | 2,505 | Volume Upload + COPY INTO |

**IMPORTANT:** Tables in Databricks are named `transactions_silver` and `stocks_silver`
NOT `silver_transactions` / `silver_stocks` — Antigravity named them differently.
dbt sources.yml references these exact names.

---

## Gold Layer — dbt (ALL 9 MODELS COMPLETE ✅)

**dbt project:** `C:\FinPulse Project\dbt_finpulse\`
**profiles.yml:** `C:\Users\amana\.dbt\profiles.yml`
**Schema:** `finpulse` in `hive_metastore`

**All 9 models verified in Databricks:**

| Model | Rows | Key Finding |
|-------|------|-------------|
| daily_transaction_summary | 5 | 1 per type — all share date=2026-03-29 |
| fraud_rate_by_type | 5 | TRANSFER 0.77%, CASH_OUT 0.18%, rest 0% |
| hourly_pattern_analysis | 1,000 | Hour × type transaction patterns |
| high_risk_accounts | Many | Accounts with 2+ balance discrepancies |
| balance_discrepancy_summary | 1,000 | Individual flagged transactions |
| daily_stock_summary | 2,505 | OHLCV per ticker per day |
| volatility_metrics | Many | 7d and 30d rolling std deviation |
| moving_averages | Many | 7d and 30d MA per ticker |
| stocks_performance_ranking | 5 | One per ticker, GS #1, MSFT #5 |

**Gold model #10 — TO BUILD (ai_risk_findings):**
- Created by dbt after AI layer writes findings back to Databricks
- Bridges AI layer output into Power BI
- See AI Layer section for full schema

**dbt commands (always use uv run):**
```bash
cd "C:\FinPulse Project\dbt_finpulse"
uv run dbt debug                                    # verify connection
uv run dbt run                                      # run all models
uv run dbt run --select stocks_performance_ranking  # run single model
uv run dbt run --select elementary                  # run Elementary models
uv run dbt test                                     # run all tests
uv run dbt deps                                     # install packages
```

---

## Soda Core Data Quality — COMPLETE ✅

**transactions_silver — 10/10 checks PASSED**
**stocks_silver — 11/11 checks PASSED**

**Files:**
- `data_quality/checks_transactions.yml`
- `data_quality/checks_stocks.yml`
- `data_quality/soda_config.yml`

**Run command:**
```bash
uvx --from soda-core --with soda-core-spark --with databricks-sql-connector --with setuptools soda scan -d transactions_silver -c data_quality/soda_config.yml data_quality/checks_transactions.yml
```

---

## Elementary Data Observability — COMPLETE ✅

**Status:** Fully operational. 39/39 models passing. PASS=41.

**What's running in Databricks:**
- Schema: `hive_metastore.finpulse_elementary`
- 15 incremental audit tables: `dbt_models`, `dbt_run_results`, `dbt_columns`, `dbt_invocations`, `dbt_sources`, `schema_columns_snapshot`, `data_monitoring_metrics`, `elementary_test_results`, etc.
- 14 views: `alerts_schema_changes`, `alerts_anomaly_detection`, `model_run_results`, `metrics_anomaly_score`, etc.
- 2 hooks wrap every dbt run automatically (on-run-start, on-run-end)

**Commands:**
```bash
cd "C:\FinPulse Project\dbt_finpulse"
uv run dbt run --select elementary          # run Elementary models
uv run dbt run                              # run all — Elementary tracks automatically
pip install elementary-data                 # install CLI
edr report --profiles-dir "C:\Users\amana\.dbt" --project-dir "C:\FinPulse Project\dbt_finpulse"
```

**Key facts:**
- Every `uv run dbt run` automatically appends to Elementary audit tables
- 6 tests total: 5 passed, 1 error fixed (total_discrepancy → amount in schema.yml)
- Elementary findings available for AI layer: query `finpulse_elementary.model_run_results` and `finpulse_elementary.alerts_anomaly_detection`
- Differentiation from Soda: Soda = data correctness gate, Elementary = pipeline health observability

**Interview line:** "Elementary auto-generates a regulatory-grade audit trail — every Gold model's lineage, run history, and data health captured automatically. Same pattern banks use for SOX compliance."

---

## Prefect Pipeline Orchestration — COMPLETE ✅

**File:** `C:\FinPulse Project\prefect_flows\finpulse_pipeline.py`
**Command:** `uv run python prefect_flows/finpulse_pipeline.py`

**Full pipeline sequence:**
```
Bronze Ingestion (chunked 500k rows, idempotent skip if today's data exists)
    ↓
Silver Transformation (skip if Bronze partition unchanged — checks .last_processed marker)
    ↓
Soda Data Quality Gate (fail-fast on Silver tables)
    ↓
Promote to Cloud (Volume Upload + COPY INTO to Databricks)
    ↓
Gold dbt run (uv run dbt run)
    ↓
dbt test (uv run dbt test)
    ↓ (survival mode — continues even if test fails)
edr report (Elementary dashboard — full flags command with --profiles-dir and --project-dir)
    ↓
Pipeline ends — PASS if all green, FAIL logged if any test failed
```

**Key implementation details:**
- Ephemeral mode — no persistent Prefect server needed
- `.last_processed` marker at `C:\FinPulse Project\data\bronze\.last_processed`
- Bronze ingestion uses chunking to prevent MemoryError on 8GB RAM
- `return_state=True` for survival mode on dbt test step
- edr report uses full command: `edr report --profiles-dir "C:\Users\amana\.dbt" --project-dir "C:\FinPulse Project\dbt_finpulse"`

---

## AI Layer — Compliance Intelligence System (TO BUILD 🔲)

### Target Audience
**Internal user: Compliance & Fraud Risk Officer at a bank**
They need to act on reports every morning. They don't dig through dashboards.
Regulated by AML / BSA law — explainability is mandatory, not optional.

### The One Sentence
*"FinPulse monitors 6.3 million transactions, autonomously identifies suspicious activity, explains why it's suspicious in regulatory language, and delivers a morning briefing a compliance officer can act on — without touching a single dashboard."*

### Interface
**Streamlit app — single command:**
```bash
streamlit run ai_layer/finpulse_app.py
```
Four screens in sidebar: HUNT → INVESTIGATE → REASON → BRIEF
Plus pipeline status bar at top showing Bronze/Silver/Gold/Soda/Elementary health.

### Differentiation from Power BI
- **Streamlit = Intelligence Layer** — "What should I do RIGHT NOW?" — forward looking, actionable, for compliance officer
- **Power BI = Analytics Layer** — "What happened historically?" — backward looking, descriptive, for analysts/management
- Add to Streamlit sidebar: "For historical analytics → Power BI Dashboard"

---

### Agent 1 — HUNT (Screen 1)
**Modality: Live Visual Anomaly Map**
**Tech: Scikit-learn Isolation Forest + Plotly scatter**

- Isolation Forest contamination=0.013 (matches actual 0.13% fraud rate)
- Hard rule layered on top: TRANSFER or CASH_OUT + balance_discrepancy + amount >75th percentile = auto-flag (fraud only exists in these types — domain knowledge)
- Two signals: ML anomaly score + rule-based flag. HIGH needs both, MEDIUM needs one.
- Live scatter plot populates in real time as model scores transactions
- Axes: amount vs balance_diff. Grey = normal, Red = anomalous.
- User controls: lasso select clusters, toggle transaction type, drag confidence threshold slider
- Output passed to Agent 2: flagged transaction list with anomaly scores

**Interview line:** "Hybrid ML + rules because pure ML fails on 0.13% class imbalance. Domain-aware contamination=0.013, not default 0.1. Same hybrid approach real banks use."

---

### Agent 2 — INVESTIGATE (Screen 2)
**Modality: Conversational Chat Interface**
**Tech: LlamaIndex + Gemini 2.5 Flash (250K context)**

- Chat window where compliance officer interrogates flagged accounts in plain English
- Indexes 3 data sources: Gold transaction tables + Gold stock volatility tables + Elementary model_run_results
- Cross-references transaction anomalies with stock market stress on same date
- Example questions it handles:
  - "Why is account C1234567 suspicious?"
  - "Was JPM volatile the same day?"
  - "Show me similar patterns from last 30 days"
  - "How confident is the pipeline data right now?"
- Output: contextual paragraph per flagged account passed to Agent 3

**Interview line:** "RAG over 3 data sources — transactions, market data, and pipeline health. A compliance officer interrogates the data in plain English. The cross-referencing of fraud signals with market stress is standard in quant risk teams."

---

### Agent 3 — REASON (Screen 3)
**Modality: Live Streaming Chain of Thought + Risk Meter**
**Tech: DeepSeek R1 via OpenRouter (streaming)**

- DeepSeek R1 reasoning streamed token by token — watch the AI think live
- Risk meter on the side fills in real time as reasoning builds: LOW → MEDIUM → HIGH
- Forces structured 3-point reasoning framework:
  1. Transaction Evidence — what the numbers say
  2. Historical Context — what Agent 2 found
  3. Regulatory Classification — does this match a known fraud typology
- Regulatory actions output: `FILE_SAR / FREEZE_ACCOUNT / ESCALATE / MONITOR_24H / CLEAR`
- SAR = Suspicious Activity Report (Bank Secrecy Act — real regulatory terminology)
- Structured JSON output:
```json
{
  "risk_level": "HIGH",
  "confidence": 0.91,
  "regulatory_flag": "POTENTIAL_LAYERING",
  "recommended_action": "FILE_SAR",
  "reasoning": "Three consecutive CASH_OUTs structuring below $10,000..."
}
```

**Interview line:** "DeepSeek R1 chain-of-thought streamed live. Risk meter fills in real time. Output uses real AML regulatory terminology — SAR filing under Bank Secrecy Act. Explainability isn't a feature here, it's a legal requirement."

---

### Agent 4 — BRIEF (Screen 4)
**Modality: Executive Dashboard + PDF Export**
**Tech: Groq Llama3-70b + ReportLab PDF**

- Final screen looks like an actual bank morning briefing
- Risk status banner: RED / AMBER / GREEN
- 3 KPI cards: Transactions Reviewed / Flagged / $ At Risk
- Priority action list: account ID, amount, type, recommended action
- Pipeline health bar (from Elementary data — all 9 models healthy)
- One button: "Export Compliance Report" → timestamped PDF

**Two output files saved to `data/ai_reports/`:**
- `executive_brief_YYYY-MM-DD.txt` — 5 bullet points, C-suite readable
- `compliance_report_YYYY-MM-DD.json` — full reasoning chain, data sources, Elementary pipeline health, timestamp

**Interview line:** "Groq for fastest inference. Output is a board-ready compliance briefing exportable as PDF. The compliance report references Elementary pipeline health at time of analysis — that's genuine enterprise-grade auditability."

---

### AI Findings Writeback — CRITICAL ARCHITECTURAL DECISION
**After Agent 4 runs, write findings back to Databricks:**

```sql
hive_metastore.finpulse.ai_risk_findings
```

| Column | Content |
|--------|---------|
| run_date | when AI ran |
| account_id | flagged account |
| transaction_type | TRANSFER / CASH_OUT |
| amount | transaction value |
| anomaly_score | Agent 1 Isolation Forest score |
| risk_level | HIGH / MEDIUM / LOW |
| regulatory_flag | POTENTIAL_LAYERING / STRUCTURING / CLEAR |
| recommended_action | FILE_SAR / FREEZE / MONITOR / CLEAR |
| market_stress_day | boolean — stock volatility elevated same day |
| ai_confidence | 0.0 to 1.0 |
| reasoning_summary | one line from Agent 3 |

**Also add dbt model #10:**
- `dbt_finpulse/models/gold/ai_risk_findings.sql`
- Makes Elementary track AI findings table — lineage goes from source CSV all the way to AI output

**This creates a closed loop:**
```
Raw Data → Pipeline → Gold Tables → AI Analysis → ai_risk_findings (Databricks) → Power BI
```

---

## Power BI Dashboards (TO BUILD 🔲)

**Connection:** Power BI Desktop → Databricks SQL Warehouse connector
**Differentiation from Streamlit:** Historical analytics (what happened) vs Streamlit intelligence (what to do now)

**Dashboard 1 — Transaction Overview (historical):**
- Total transactions by type — bar chart
- Fraud vs legitimate — donut chart
- Volume trend over time — line chart
- Top 10 high value transactions — table

**Dashboard 2 — Anomaly & Risk Analytics (historical):**
- Balance discrepancy heatmap by transaction type
- High risk accounts table
- Fraud rate by type (TRANSFER 0.77%, CASH_OUT 0.18%, rest 0%)
- Hourly transaction pattern heatmap

**Dashboard 3 — AI Risk Intelligence (the unique one — fed by ai_risk_findings table):**
- AI Risk Score Trend — line chart of average anomaly score over time
- Regulatory Flag Distribution — LAYERING vs STRUCTURING vs CLEAR donut
- SAR Filing Tracker — FILE_SAR recommendations per day
- Market Stress Correlation — on `market_stress_day=true` days, do anomaly scores spike?
- AI Confidence Distribution — histogram
- Human vs AI — compare `isFraud` ground truth vs `risk_level` AI prediction (precision/recall visual)

---

## Complete Folder Structure v8.0

```
C:\FinPulse Project\
│
├── ingestion\
│   ├── explore_data.py                          ✅ DONE
│   ├── ingest_bronze.py                         ✅ DONE (chunked 500k rows)
│   └── fetch_stock_data.py                      ✅ DONE (idempotent)
│
├── prefect_flows\
│   └── finpulse_pipeline.py                     ✅ DONE (full Medallion orchestrator)
│
├── notebooks\
│   ├── silver_transactions.ipynb                ✅ DONE
│   ├── silver_stocks.ipynb                      ✅ DONE
│   └── gold_summary.ipynb                       ✅ DONE (bridge notebook)
│
├── data_quality\
│   ├── checks_transactions.yml                  ✅ DONE (10 checks, all pass)
│   ├── checks_stocks.yml                        ✅ DONE (11 checks, all pass)
│   └── soda_config.yml                          ✅ DONE
│
├── ingestion\
│   └── cloud_promotion.py                       ✅ DONE (Volume Upload + COPY INTO)
│
├── data_quality\
│   └── soda_utils.py                            ✅ DONE (Prefect-wired Soda runner)
│
├── dbt_finpulse\
│   ├── models\gold\
│   │   ├── daily_transaction_summary.sql        ✅ DONE
│   │   ├── fraud_rate_by_type.sql               ✅ DONE
│   │   ├── hourly_pattern_analysis.sql          ✅ DONE
│   │   ├── high_risk_accounts.sql               ✅ DONE
│   │   ├── balance_discrepancy_summary.sql      ✅ DONE
│   │   ├── daily_stock_summary.sql              ✅ DONE
│   │   ├── volatility_metrics.sql               ✅ DONE
│   │   ├── moving_averages.sql                  ✅ DONE
│   │   ├── stocks_performance_ranking.sql       ✅ DONE + FIXED
│   │   └── ai_risk_findings.sql                 🔲 TO BUILD (Gold model #10)
│   ├── models\gold\schema.yml                   ✅ DONE (fixed total_discrepancy → amount)
│   ├── sources.yml                              ✅ DONE
│   ├── packages.yml                             ✅ DONE (Elementary 0.14.1)
│   ├── dbt_project.yml                          ✅ DONE
│   └── profiles.yml → C:\Users\amana\.dbt\      ✅ DONE
│
├── ai_layer\
│   ├── finpulse_app.py                          🔲 TO BUILD (Streamlit main app)
│   ├── agent1_hunt.py                           🔲 TO BUILD (Isolation Forest + Plotly)
│   ├── agent2_investigate.py                    🔲 TO BUILD (LlamaIndex RAG chat)
│   ├── agent3_reason.py                         🔲 TO BUILD (DeepSeek R1 streaming)
│   ├── agent4_brief.py                          🔲 TO BUILD (Groq + PDF export)
│   └── writeback.py                             🔲 TO BUILD (writes ai_risk_findings to Databricks)
│
├── powerbi\
│   └── finpulse_dashboard.pbix                  🔲 TO BUILD (3 dashboards)
│
├── data\
│   ├── bronze\transactions\date=2026-03-29\     ✅
│   ├── bronze\stocks\date=2026-03-29\           ✅
│   ├── bronze\.last_processed                   ✅ (Silver skip marker)
│   ├── silver\iceberg_warehouse\finpulse\
│   │   ├── silver_transactions\ (6,362,604 records, 3 snapshots) ✅
│   │   └── silver_stocks\ (2,505 records, 1 snapshot)            ✅
│   └── ai_reports\                              🔲 TO BUILD
│       ├── executive_brief_YYYY-MM-DD.txt
│       └── compliance_report_YYYY-MM-DD.json
│
├── jars\iceberg-spark-runtime-3.5_2.12-1.4.3.jar  ✅
├── hadoop\bin\winutils.exe + hadoop.dll            ✅
├── config.py, .env, .gitignore, pyproject.toml    ✅
└── README.md                                       🔲 TO BUILD
```

---

## Complete Architecture — Closed Loop v8.0

```
Raw CSV + yfinance
        ↓
Bronze (date-partitioned CSV)
        ↓ [skip if unchanged]
Silver (PySpark + Iceberg ACID)
        ↓ [Soda DQ Gate — 21 checks]
Databricks Silver Tables
        ↓
Gold dbt Models (9 tables + ai_risk_findings)
        ↓ [Elementary tracks lineage + health]
Streamlit AI Compliance System
   ├── Screen 1: HUNT — Live Anomaly Scatter (Isolation Forest + Plotly)
   ├── Screen 2: INVESTIGATE — Chat with Data (LlamaIndex + Gemini)
   ├── Screen 3: REASON — Streaming Risk Assessment (DeepSeek R1)
   └── Screen 4: BRIEF — Executive Dashboard + PDF (Groq + ReportLab)
        ↓ [writes back]
ai_risk_findings (Gold table #10 in Databricks)
        ↓
Power BI — 3 Dashboards
   ├── Dashboard 1: Transaction Overview (historical)
   ├── Dashboard 2: Anomaly & Risk Analytics (historical)
   └── Dashboard 3: AI Risk Intelligence (fed by ai_risk_findings)
```

---

## GitHub Commits So Far

```
feat: Bronze layer ingestion script for financial transactions
feat: Add stock market data ingestion for Bronze layer
feat: Add Prefect orchestration flow for Bronze pipeline
feat: Silver transactions — Apache Iceberg with ACID, time travel, schema evolution and Google Drive sync
feat: Silver stocks — Apache Iceberg with financial metrics, moving averages and validation
feat: Gold layer — 9 dbt models on Databricks SQL Warehouse
feat: Soda Core data quality — 21 checks across transactions and stocks silver, all passing
feat: Elementary data observability — 30 monitoring models, lineage tracking, edr report
feat: Prefect full Medallion orchestrator — Bronze to Elementary, survival mode, idempotent Silver
```

---

## What To Build Next — In Order

```
1. ai_layer/finpulse_app.py          Streamlit skeleton + sidebar + 4 screen navigation
2. ai_layer/agent1_hunt.py           Isolation Forest + live Plotly scatter
3. ai_layer/agent2_investigate.py    LlamaIndex RAG chat (index Gold tables + Elementary)
4. ai_layer/agent3_reason.py         DeepSeek R1 streaming + risk meter
5. ai_layer/agent4_brief.py          Groq executive brief + PDF export
6. ai_layer/writeback.py             Write ai_risk_findings to Databricks
7. dbt_finpulse/models/gold/ai_risk_findings.sql   Gold model #10
8. powerbi/finpulse_dashboard.pbix   3 dashboards (Dashboard 3 reads ai_risk_findings)
9. README.md                         Architecture diagram + setup guide
```

---

## Key Interview Talking Points v8.0

1. *"6.3M records through Medallion architecture — PySpark 3.5 + Apache Iceberg v2. Iceberg gives ACID, time travel, schema evolution — demonstrated all four features in the notebook"*

2. *"1.2 million balance discrepancies detected — transactions where sender balance doesn't mathematically reconcile. Real banking fraud indicator, not just ML output"*

3. *"100% of fraud concentrated in TRANSFER (0.77%) and CASH_OUT (0.18%) only. Three other types show zero fraud — directly informs fraud detection prioritization"*

4. *"GS delivered 100% total return over 2 years. MSFT only negative performer at -13.9% despite highest start price. Gold layer surfaces this automatically"*

5. *"22-minute JDBC timeout solved by switching to Volume Upload + COPY INTO — Databricks native bulk load pattern. 4x performance improvement"*

6. *"dbt manages 9 Gold models + 1 AI findings model on Databricks. Elementary auto-generates lineage graphs — audit trail from source CSV to AI output. SOX compliance pattern."*

7. *"Soda Core data quality gate — 21 checks, all passing. Pipeline stops if any check fails — fail-fast before promotion to cloud."*

8. *"Prefect orchestrates the entire Medallion pipeline in one command. Ephemeral mode — no server dependency. Idempotent Silver layer skips if Bronze unchanged. Survival mode continues to Elementary report even if dbt tests fail."*

9. *"AI layer is a compliance intelligence system, not just anomaly detection. Agent 1 hunts with hybrid ML + rules on a live scatter plot. Agent 2 investigates via plain English chat powered by RAG over transaction and market data. Agent 3 streams its reasoning live with a risk meter — output uses real AML terminology: SAR filing, layering, structuring. Agent 4 produces a board-ready compliance briefing exportable as PDF."*

10. *"AI findings written back to Databricks as a 10th Gold table. Power BI Dashboard 3 visualizes AI confidence trends, SAR recommendations, and market stress correlation over time. The pipeline is a closed loop — raw transactions go in, actionable intelligence comes out, everything is queryable and auditable."*

11. *"Isolation Forest contamination=0.013 matching actual 0.13% fraud rate — not default 0.1. Domain-aware parameter tuning. Hybrid approach because pure ML fails on extreme class imbalance."*

12. *"₹0 total cost. Local PySpark + Google Drive + Databricks free tier + open source LLMs. In production: ADLS Gen2 + Azure Databricks + Azure ML + Azure Compliance Center."*

---

## Timeline v8.0

| Week | Phase | Status |
|------|-------|--------|
| Week 1 | Bronze + Prefect | ✅ COMPLETE |
| Week 2 | Silver PySpark + Iceberg | ✅ COMPLETE |
| Week 3 | Databricks bridge + Gold dbt | ✅ COMPLETE |
| Week 4 | Soda Core + Elementary + Prefect full pipeline | ✅ COMPLETE |
| Week 5 | AI Layer — Streamlit + 4 Agents + Writeback | 🔲 NEXT |
| Week 6 | Power BI 3 Dashboards | 🔲 |
| Week 7 | README + GitHub polish + Interview prep | 🔲 |

**Target completion: Mid May 2026**
**Interview ready: Late May 2026**

---

## Critical Notes For Every Session

- Windows — always `r""` for paths
- Python venv: `C:\FinPulse Project\.venv`
- Java: `C:\Program Files\Microsoft\jdk-11.0.30.7-hotspot`
- Hadoop: `C:\FinPulse Project\hadoop\bin\`
- Iceberg JAR: `C:\FinPulse Project\jars\iceberg-spark-runtime-3.5_2.12-1.4.3.jar`
- Iceberg warehouse: `C:\FinPulse Project\data\silver\iceberg_warehouse`
- ALWAYS set NativeIO=false + RawLocalFileSystem in Spark config on Windows
- Spark temp dir: `C:\Users\amana\FinPulse_Spark_Temp`
- Silver notebooks run locally in Antigravity — NOT browser Colab
- Databricks tables: `transactions_silver` and `stocks_silver` (NOT silver_transactions/silver_stocks)
- dbt project: `C:\FinPulse Project\dbt_finpulse\`
- dbt profiles: `C:\Users\amana\.dbt\profiles.yml`
- Unity Catalog NOT used — hive_metastore only
- All keys in .env — never hardcode
- Commit after every component
- dbt commands always use `uv run dbt` not plain `dbt`
- edr report always use full flags: `--profiles-dir "C:\Users\amana\.dbt" --project-dir "C:\FinPulse Project\dbt_finpulse"`
- Streamlit app entry point: `streamlit run ai_layer/finpulse_app.py`
- AI findings table: `hive_metastore.finpulse.ai_risk_findings`
- Elementary findings for AI agents: query `finpulse_elementary.model_run_results` and `finpulse_elementary.alerts_anomaly_detection`
- Explain every function line by line — no blind copy paste

---

*Save this. Paste at the start of every new Claude or Antigravity session for full context.*
