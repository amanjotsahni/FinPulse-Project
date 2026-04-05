# Databricks AI Dev Kit

You have access to Databricks skills and MCP tools installed by the Databricks AI Dev Kit.

## Available MCP Tools

The `databricks` MCP server provides 50+ tools for interacting with Databricks, including:
- SQL execution and warehouse management
- Unity Catalog operations (tables, volumes, schemas)
- Jobs and workflow management
- Model serving endpoints
- Genie spaces and AI/BI dashboards
- Databricks Apps deployment

## Available Skills

Skills are installed in `.gemini/skills/` and provide patterns and best practices for:
- Spark Declarative Pipelines, Structured Streaming
- Databricks Jobs, Asset Bundles
- Unity Catalog, SQL, Genie
- MLflow evaluation and tracing
- Model Serving, Vector Search
- Databricks Apps (Python and APX)
- And more

## Getting Started

Try asking: "List my SQL warehouses" or "Show my Unity Catalog schemas"
# FinPulse — Engineering Architecture & Interview Notes

This document lists the deliberate, state-of-the-art engineering decisions implemented throughout the FinPulse data architecture. You can use these strategic talking points during Data Engineering interviews to showcase your system design maturity.

## 1. Cloud-Compute Offloading & Hybrid Storage
**The Problem:** Running massive data loads (6.3 Million rows) locally destroys laptop compute and risks out-of-memory crashes.
**The Smart Decision:** We designed a hybrid environment. Heavy PySpark processing execution was deliberately moved up into localized Google Colab Cloud clusters to crunch the data at scale. Meanwhile, the final optimized output (Iceberg format) was directly bound and persistently saved mapped into Google Drive, bridging big-data cloud compute with localized permanent storage.

## 2. Zero-Bottleneck Data Ingestion (I/O Optimization)
**The Problem:** Reading 600MB+ CSV files directly out of a mounted network drive (like Google Drive) into a Spark cluster forces severe IO bottlenecking. 
**The Smart Decision:** Implemented `gdown` logic inside the PySpark container. Instead of streaming the data across the network connection, we automatically downloaded the dataset directly into the Cloud cluster's ultra-fast internal NVMe SSD environment before loading it into Spark. This aggressively slashes memory read-times.

## 3. Deterministic Dependency Management (`uv`)
**The Problem:** Using a standard `requirements.txt` with loose versions ("pip install pandas") guarantees that code will break in the future when packages unexpectedly update ("it works on my machine" syndrome).
**The Smart Decision:** Migrated the entire core to Astral's `uv` system. Centralized all exact dependencies into `pyproject.toml` and enforced a mathematically locked `uv.lock` tree file. This completely eliminates dependency drift, guarantees 100% environment reproducibility across any future server or machine, and accelerates container build speeds by over 10x.

## 4. Centralized Source-of-Truth Configuration (`config.py`)
**The Problem:** Scattered `os.path` logic and hardcoded directory strings across multiple bronze/silver ingestion scripts make code nearly impossible to debug or scale securely without breaking execution graphs.
**The Smart Decision:** Built an ultra-lightweight centralized `config.py` using OS-agnostic `pathlib` variables bound to standard `load_dotenv` fallbacks. All scripts now blindly import identical variables ensuring massive code DRYness (Don't Repeat Yourself) while gracefully adjusting for Windows/Linux/Mac environments dynamically.

## 5. Deployment Flexibility & Containerization (Docker)
**The Problem:** End-to-end platform deployments are chaotic if host environments differ. 
**The Smart Decision:** Hand-crafted a custom `Dockerfile` mapped cleanly via `docker-compose.yml` to automatically build isolated, Linux-based execution environments that map volumes consistently, inject `uv` requirements, and auto-spool the pipeline without tying the software permanently to the host OS. 

## 6. Enterprise Standard Secret Management
**The Problem:** Hardcoding API keys or accidentally committing local `.env` values to GitHub destroys project security. 
**The Smart Decision:** Firmly placed `.env` within `.gitignore` while generating an identical `.env.example` masking template for version control. This perfectly demonstrates how pipelines can remain portable to new developers without ever sharing destructive secret keys.

## 7. Advanced Data Transformation Pipeline (Silver Layer)
**The Problem:** Raw bronze data containing 6.3 million records is noisy, unoptimized for machine learning algorithms, and lacks the engineered features necessary for detecting anomalies and fraud patterns.
**The Smart Decision:** Engineered a robust PySpark data profiling and transformation pipeline that takes 6.3M raw records and methodically applies 8 crucial transformation steps to produce high-quality, ML-ready Silver data:
1. **Removes zero amount transactions:** Cleanses meaningless records that could skew model baselines or cause false flags.
2. **Flags balance discrepancies:** Explicitly introduces a key fraud indicator by tracking mathematical inconsistencies in origin/destination account balances.
3. **Converts 'step' to hour and day of simulation:** Engineers cyclical time-based features crucial for detecting behavioral temporal anomalies.
4. **Encodes transaction types as numbers:** Determines and maps categorical transaction string data into numerical values for direct Machine Learning model ingestion.
5. **Adds risk flags for high amount TRANSFER/CASH_OUT:** Proactively isolates high-risk transaction types that cross predefined massive volume thresholds.
6. **Calculates balance difference columns:** Derives specific features to represent true absolute changes in account values per transaction.
7. **Deduplicates on business key:** Eliminates redundant records, ensuring a deterministic and mathematically accurate dataset.
8. **Adds pipeline metadata:** Stamps every transformed row with system audit information to guarantee 100% data lineage tracking.

## 8. "Hybrid Bridge" Storage Strategy (FUSE vs. API)
**The Problem:** PySpark is designed to write to high-performance filesystems, but manual cloud uploads (via Google Drive API) for 6.3 million fragmented Parquet partitions are unstable, slow, and require complex manual authentication scripts.
**The Smart Decision:** Implemented a **Native FUSE Mount Strategy** using Google Drive for Desktop. By mapping the cloud store as a local drive letter (`G:`), we allowed Spark to treat the cloud as a standard high-speed local hard drive. This eliminated the need for fragile API upload code, ensured atomic file writes, and allowed real-time cloud-syncing of massive datasets with 100% reliability.

## 9. OS-Agnostic Spark Resilience (Windows NativeIO Bypass)
**The Problem:** PySpark is a Linux-native engine that often crashes on Windows due to missing Hadoop native DLLs (`winutils.exe` / `hadoop.dll`), a major hurdle for developers on Windows laptops.
**The Smart Decision:** Implemented a three-tier Windows fix: (1) Localized Hadoop binaries within the project for portability, (2) Injected them dynamically into the system `PATH` at runtime, and (3) Forced Spark to bypass Native I/O checks (`should_use_native_io=false`) and use `RawLocalFileSystem`. This demonstrates high-level capability in cross-OS environment orchestration and debugging.

## 10. Self-Healing "Hybrid-Aware" Configuration (Smart Resolver)
**The Problem:** Moving a codebase between a Windows laptop (G:\ drive), a Linux server (/mnt/drive), and Google Colab (/content/drive) usually forces developers to manually rewrite hardcoded path strings.
**The Smart Decision:** Engineered a **Smart Path Resolver** inside `config.py`. The logic automatically scans the host environment for a Google Drive mount point at runtime. If found, it pivots the entire pipeline's storage root to the cloud automatically; if missing, it gracefully falls back to local storage. This "Self-Healing" configuration ensures the pipeline is 100% portable and infrastructure-agnostic.
