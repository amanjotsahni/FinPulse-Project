# ============================================================
# FinPulse — Central Configuration
# Purpose: Single source of truth for all paths and settings
# Change values here to run on any system
# ============================================================

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def get_logger(name: str) -> logging.Logger:
    """Returns a consistently formatted logger. Use across all modules."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    return logger

# ── Base Project Path ────────────────────────────────────────
# Path() makes this work on Windows, Mac, Linux automatically
BASE_DIR = Path(__file__).parent

# ── Smart Data Resolver ──────────────────────────────────────
# Prioritize Google Drive (G: drive) if connected, otherwise fallback to local project folder
_GDRIVE_ENV = os.getenv("GDRIVE_FINPULSE_ROOT")
_GDRIVE_DEFAULT = "G:/My Drive/FinPulse"

if _GDRIVE_ENV and Path(_GDRIVE_ENV).exists():
    DATA_DIR = Path(_GDRIVE_ENV) / "data"
elif Path(_GDRIVE_DEFAULT).exists():
    DATA_DIR = Path(_GDRIVE_DEFAULT) / "data"
else:
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
# Override TICKERS via env var as comma-separated string: "AAPL,TSLA,NVDA"
_tickers_env = os.getenv("TICKERS")
TICKERS = _tickers_env.split(",") if _tickers_env else ["AAPL", "GOOGL", "MSFT", "JPM", "GS"]
STOCK_PERIOD = os.getenv("STOCK_PERIOD", "2y")

# ── API Keys ─────────────────────────────────────────────────
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# ── Spark Config ─────────────────────────────────────────────
SPARK_APP_NAME = "FinPulse"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# ── Model Config ─────────────────────────────────────────────
GROQ_MODEL = "llama3-70b-8192"
GEMINI_MODEL = "gemini/gemini-2.0-flash"
DEEPSEEK_MODEL = "openrouter/deepseek/deepseek-r1"

# ── Anomaly Detection ────────────────────────────────────────
FRAUD_CONTAMINATION_RATE = 0.013
HIGH_CONFIDENCE_THRESHOLD = 0.85
MEDIUM_CONFIDENCE_THRESHOLD = 0.60

# ── Databricks SQL Config ────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "hive_metastore")
DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA", "finpulse")


def get_databricks_connection():
    """Returns a connection to the Databricks SQL Warehouse."""
    from databricks import sql
    if not all([DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN]):
        raise ValueError("Databricks credentials missing from .env")
    
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )


# ── Pipeline Config ──────────────────────────────────────────
PIPELINE_VERSION = "v1.0"
DATA_SOURCE_TRANSACTIONS = "kaggle_paysim"
DATA_SOURCE_STOCKS = "yahoo_finance"

get_logger(__name__).info("Config loaded successfully")
