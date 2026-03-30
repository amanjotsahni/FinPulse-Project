import sys
import os
import time
import webbrowser
import subprocess
import atexit
from dotenv import load_dotenv

load_dotenv()
PREFECT_UI_PORT = os.getenv("PREFECT_UI_PORT", "8200")

print("Starting temporary Prefect UI server...")
server_process = subprocess.Popen(
    f"prefect server start --port {PREFECT_UI_PORT}", 
    shell=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL
)

def cleanup_server():
    print("Shutting down Prefect Server...")
    subprocess.run(f"taskkill /F /T /PID {server_process.pid}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

atexit.register(cleanup_server)

# Point script to this new server
os.environ["PREFECT_API_URL"] = os.getenv("PREFECT_API_URL", f"http://127.0.0.1:{PREFECT_UI_PORT}/api")
time.sleep(5)  # Let server boot up

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

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
    print("Starting transaction ingestion task...")
    run_bronze_ingestion()
    print("Transaction ingestion task complete.")
    
@task(name="Stock Market Bronze Ingestion",
      retries=3,
      retry_delay_seconds=60)
def ingest_stocks():
      """
      Prefect task wrapping stock market Bronze ingestion.
      Longer retry delay - Yahoo Finance rate limits sometimes hit.
      """
      print("Starting stock ingestion task...")
      run_stock_ingestion()
      print("Stock ingestion task complete.")

@flow(name="FinPulse Bronze Pipeline",
      description="Daily Bronze layer ingestion for transactions and market data")
def finpulse_bronze_flow():
      """
      Main Prefect flow - orchestrates full Bronze ingestion.
      Tasks run sequentially-transactions first, then stocks.
      """
      # Open browser to the UI as soon as the flow starts (skip if running in headless/docker env)
      if not os.environ.get("HEADLESS_MODE"):
            try:
                  webbrowser.open(f"http://127.0.0.1:{PREFECT_UI_PORT}/runs")
            except Exception:
                  pass

      print("="*60)
      print("FINPULSE PIPELINE STARTING")
      print("="*60)
      ingest_transaction()
      ingest_stocks()

      print("="*60)
      print("FINPULSE PIPELINE COMPLETE")
      print("="*60)

if __name__== "__main__":
      finpulse_bronze_flow()
      
      print("\n" + "=" * 60)
      print("Pipeline complete! Syncing final logs and shutting down the UI server...")
      print("=" * 60)
      
      # Brief pause to ensure final flow state events are sent to the local server
      time.sleep(3)