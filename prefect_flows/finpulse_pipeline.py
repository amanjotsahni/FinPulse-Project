import sys
import os
import time
import webbrowser
import subprocess
import atexit
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
PREFECT_UI_PORT = os.getenv("PREFECT_UI_PORT", "8200")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config import get_logger
logger = get_logger(__name__)

# Resolve prefect CLI relative to current Python executable (works with or without activated venv)
PREFECT_BIN = str(Path(sys.executable).parent / "prefect")

logger.info("Starting temporary Prefect UI server...")
server_process = subprocess.Popen(
    [PREFECT_BIN, "server", "start", "--port", PREFECT_UI_PORT],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)

def cleanup_server():
    logger.info("Shutting down Prefect Server...")
    server_process.terminate()
    try:
        server_process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        server_process.kill()

atexit.register(cleanup_server)

# Point script to this new server
os.environ["PREFECT_API_URL"] = os.getenv("PREFECT_API_URL", f"http://127.0.0.1:{PREFECT_UI_PORT}/api")
time.sleep(5)  # Let server boot up

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from ingestion.ingest_bronze import run_bronze_ingestion
from ingestion.fetch_stock_data import run_stock_ingestion

# ============================================================
# FinPulse — Prefect Orchestration Flow
# Purpose: Orchestrate and schedule all pipeline tasks
# Author: Amanjot Kaur
# ============================================================

@task(name="Transaction Bronze Ingestion",
      retries=3,
      retry_delay_seconds=30,
      cache_key_fn=task_input_hash,
      cache_expiration=timedelta(hours=24))
def ingest_transaction():
    """
    Prefect task wrapping transaction Bronze ingestion.
    Retries 3 times if fails - handles network/file issues.
    """
    logger.info("Starting transaction ingestion task...")
    run_bronze_ingestion()
    logger.info("Transaction ingestion task complete.")

@task(name="Stock Market Bronze Ingestion",
      retries=3,
      retry_delay_seconds=60)
def ingest_stocks():
    """
    Prefect task wrapping stock market Bronze ingestion.
    Longer retry delay - Yahoo Finance rate limits sometimes hit.
    """
    logger.info("Starting stock ingestion task...")
    run_stock_ingestion()
    logger.info("Stock ingestion task complete.")

@flow(name="FinPulse Bronze Pipeline",
      description="Daily Bronze layer ingestion for transactions and market data")
def finpulse_bronze_flow():
    """
    Main Prefect flow - orchestrates full Bronze ingestion.
    Tasks run sequentially — transactions first, then stocks.
    """
    # Open browser to the UI (skip if running in headless/docker env)
    if not os.environ.get("HEADLESS_MODE"):
        try:
            webbrowser.open(f"http://127.0.0.1:{PREFECT_UI_PORT}/runs")
        except Exception:
            pass

    logger.info("FINPULSE PIPELINE STARTING")
    ingest_transaction()
    ingest_stocks()
    logger.info("FINPULSE PIPELINE COMPLETE")

if __name__ == "__main__":
    finpulse_bronze_flow()
    logger.info("Pipeline complete! Syncing final logs and shutting down the UI server...")
    time.sleep(3)  # Let Prefect flush final state events before server shuts down