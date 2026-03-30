# ============================================================
# FinPulse — Central Configuration
# Purpose: Single source of truth for all paths and settings
# Change values here to run on any system
# ============================================================

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Base Project Path ────────────────────────────────────────
# Path() makes this work on Windows, Mac, Linux automatically
BASE_DIR = Path(__file__).parent

# ── Data Paths ───────────────────────────────────────────────
DATA_DIR = BASE_DIR / "data"

BRONZE_TRANSACTIONS = DATA_DIR / "bronze" / "transactions"
BRONZE_STOCKS = DATA_DIR / "bronze" / "stocks"

SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
AI_REPORTS_DIR = DATA_DIR / "ai_reports"

ICEBERG_WAREHOUSE = DATA_DIR / "silver" / "iceberg_warehouse"

# ── Source Data ──────────────────────────────────────────────
# Load from environment (.env) first, otherwise default to "data/raw/" inside the project folder
RAW_TRANSACTIONS_PATH = Path(
    os.getenv("RAW_TRANSACTIONS_PATH", str(DATA_DIR / "raw" / "Finance_dataset.csv"))
)

# ── Stock Config ─────────────────────────────────────────────
TICKERS = ["AAPL", "GOOGL", "MSFT", "JPM", "GS"]
STOCK_PERIOD = "2y"

# ── API Keys ─────────────────────────────────────────────────
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# ── Spark Config ─────────────────────────────────────────────
SPARK_APP_NAME = "FinPulse"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# ── Model Config ─────────────────────────────────────────────
GROQ_MODEL = "llama3-70b-8192"
GEMINI_MODEL = "gemini/gemini-2.5-flash"
DEEPSEEK_MODEL = "openrouter/deepseek/deepseek-r1"

# ── Anomaly Detection ────────────────────────────────────────
FRAUD_CONTAMINATION_RATE = 0.013
HIGH_CONFIDENCE_THRESHOLD = 0.85
MEDIUM_CONFIDENCE_THRESHOLD = 0.60

# ── Pipeline Config ──────────────────────────────────────────
PIPELINE_VERSION = "v1.0"
DATA_SOURCE_TRANSACTIONS = "kaggle_paysim"
DATA_SOURCE_STOCKS = "yahoo_finance"

print("✅ Config loaded successfully")
