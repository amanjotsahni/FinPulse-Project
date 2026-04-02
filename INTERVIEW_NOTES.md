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
