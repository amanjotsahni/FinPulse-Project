import sys
import os
import time
import webbrowser
import subprocess
from pathlib import Path
from datetime import timedelta
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

# Load environment variables
load_dotenv()

# Project Path Setup
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config import get_logger
from ingestion.ingest_bronze import run_bronze_ingestion
from ingestion.fetch_stock_data import run_stock_ingestion
from ingestion.cloud_promotion import promote_to_databricks
from data_quality.soda_utils import run_all_soda_checks

# Global Logger (Standard)
logger = get_logger(__name__)

# ============================================================
# Task Definitions
# ============================================================

@task(name="Transaction Bronze Ingestion", retries=3, retry_delay_seconds=30)
def ingest_transaction():
    run_logger = get_run_logger()
    run_logger.info("Starting transaction ingestion task...")
    run_bronze_ingestion()
    return True

@task(name="Stock Market Bronze Ingestion", retries=3, retry_delay_seconds=60)
def ingest_stocks():
    run_logger = get_run_logger()
    run_logger.info("Starting stock ingestion task...")
    run_stock_ingestion()
    return True

@task(name="Check for New Data (Skip Logic)")
def check_for_new_data():
    """
    Checks if there are new Bronze partitions that haven't been processed into Silver.
    Returns True if new data is found, otherwise False.
    """
    run_logger = get_run_logger()
    marker_file = Path(PROJECT_ROOT) / "data" / "bronze" / ".last_processed"
    bronze_dir = Path(PROJECT_ROOT) / "data" / "bronze" / "transactions"
    
    # Get sorted list of date= partitions
    partitions = sorted([d.name for d in bronze_dir.glob("date=*") if d.is_dir()])
    if not partitions:
        run_logger.warning("No Bronze partitions found at all!")
        return True # Run anyway to initialize
        
    latest_partition = partitions[-1]
    
    if marker_file.exists():
        last_processed = marker_file.read_text().strip()
        if latest_partition == last_processed:
            run_logger.info(f"No new data. Latest partition {latest_partition} matches marker. Skipping Silver.")
            return False
            
    run_logger.info(f"New data detected: {latest_partition}. Triggering Silver layer.")
    return True

@task(name="Silver Layer Transformation")
def run_silver_transformation():
    run_logger = get_run_logger()
    run_logger.info("Executing PySpark Notebooks for Silver Layer...")
    
    notebooks = [
         "notebooks/silver_transactions.ipynb",
         "notebooks/silver_stocks.ipynb"
    ]
    
    for nb in notebooks:
        nb_path = Path(PROJECT_ROOT) / nb
        run_logger.info(f"Running notebook: {nb}")
        # Use jupyter nbconvert for non-interactive execution
        result = subprocess.run(
            ["uv", "run", "jupyter", "nbconvert", "--to", "notebook", "--execute", str(nb_path), "--inplace"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            run_logger.error(f"Error executing {nb}: {result.stderr}")
            raise Exception(f"Notebook {nb} failed!")
            
    # Update marker file AFTER successful execution
    marker_file = Path(PROJECT_ROOT) / "data" / "bronze" / ".last_processed"
    bronze_dir = Path(PROJECT_ROOT) / "data" / "bronze" / "transactions"
    partitions = sorted([d.name for d in bronze_dir.glob("date=*") if d.is_dir()])
    if partitions:
        marker_file.write_text(partitions[-1])
        
    return True

@task(name="Promote to Cloud (Databricks)")
def run_cloud_promotion():
    run_logger = get_run_logger()
    run_logger.info("Starting Cloud Promotion to Databricks...")
    success = promote_to_databricks()
    if not success:
        raise Exception("Cloud Promotion failed!")
    return True

@task(name="Data Quality (Soda checks)")
def run_soda_checks():
    run_logger = get_run_logger()
    run_logger.info("Executing Soda DQ checks on Databricks...")
    success = run_all_soda_checks()
    if not success:
        run_logger.warning("Soda checks found issues. Check Soda Cloud / logs.")
    return success

@task(name="Gold Layer (dbt run)")
def run_gold_layer():
    run_logger = get_run_logger()
    run_logger.info("Executing dbt run for Gold layer...")
    dbt_dir = Path(PROJECT_ROOT) / "dbt_finpulse"
    result = subprocess.run(
        ["uv", "run", "dbt", "run"],
        cwd=str(dbt_dir), capture_output=True, text=True
    )
    if result.returncode != 0:
        run_logger.error(f"dbt run failed: {result.stderr}")
        raise Exception("dbt run failed!")
    return True

@task(name="Data Quality (dbt test)")
def run_data_tests():
    run_logger = get_run_logger()
    run_logger.info("Executing dbt tests...")
    dbt_dir = Path(PROJECT_ROOT) / "dbt_finpulse"
    result = subprocess.run(
        ["uv", "run", "dbt", "test"],
        cwd=str(dbt_dir), capture_output=True, text=True
    )
    if result.returncode != 0:
        run_logger.error(f"Some dbt tests failed: {result.stdout}")
        return False
    return True

@task(name="Observability Report (Elementary)")
def run_observability_report():
    run_logger = get_run_logger()
    run_logger.info("Generating Elementary Observability Dashboard...")
    
    dbt_dir = Path(PROJECT_ROOT) / "dbt_finpulse"
    profiles_dir = str(Path.home() / ".dbt")
    
    cmd = [
        "uv", "run", "edr", "report",
        "--profiles-dir", profiles_dir,
        "--project-dir", str(dbt_dir)
    ]
    
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        run_logger.error(f"Elementary report failed: {result.stderr}")
    else:
        run_logger.info("Elementary report generated successfully.")
    return True

# ============================================================
# Main Orchestration Flow
# ============================================================

@flow(name="FinPulse Full Medallion Pipeline",
      description="End-to-end Medallion pipeline running in Ephemeral mode.")
def finpulse_full_pipeline():
    logger.info("--- MEDALLION PIPELINE STARTING ---")
    
    # 1. Bronze Layer
    ingest_transaction()
    ingest_stocks()
    
    # 2. Silver Layer (Conditional)
    new_data = check_for_new_data()
    if new_data:
        run_silver_transformation()
    else:
        logger.info("SKIP: Silver layer transformation (no new Bronze data)")
        
    # 3. Cloud Promotion (New Step)
    run_cloud_promotion()
    
    # 4. Soda Data Quality (New Step)
    soda_passed = run_soda_checks()
    
    # 5. Gold Layer
    run_gold_layer()
    
    # 4. Data Quality (Proceed on Fail)
    tests_passed = run_data_tests()
    
    # 5. Observability (Always run)
    run_observability_report()
    
    if not tests_passed or not soda_passed:
        logger.error("PIPELINE ENDED WITH ERRORS: One or more data tests failed. See Elementary/Soda dashboards.")
    else:
        logger.info("--- MEDALLION PIPELINE COMPLETE: SUCCESS ---")

if __name__ == "__main__":
    finpulse_full_pipeline()