import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from databricks import sql

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import config

logger = config.get_logger(__name__)

def promote_to_databricks():
    """
    Optimized Cloud Promotion using Databricks Volumes + CTAS.
    Implements 'Clean-Before-Load' pattern for staging and final tables.
    """
    load_dotenv()
    
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    catalog = os.getenv("DATABRICKS_CATALOG", "workspace")
    schema = os.getenv("DATABRICKS_SCHEMA", "finpulse")
    volume_name = "staging"
    
    if not all([host, token]):
        logger.error("Databricks credentials missing from .env")
        return False

    w = WorkspaceClient(host=host, token=token)
    
    # 1. Ensure Volume exists and Cleanup
    logger.info(f"Ensuring volume '{catalog}.{schema}.{volume_name}' exists...")
    try:
        w.volumes.create(catalog_name=catalog, schema_name=schema, name=volume_name, volume_type=VolumeType.MANAGED)
    except Exception as e:
        if "already exists" in str(e).lower():
            logger.info("Volume already exists.")
        else:
            logger.error(f"Failed to create volume: {e}")
            return False

    # -- NEW: Cleanup Volume --
    logger.info("Cleaning staging volume...")
    try:
        # Delete recursively from root of the staging folder
        base_vol_path = f"/Volumes/{catalog}/{schema}/{volume_name}/"
        files = w.files.list_directory_contents(base_vol_path)
        for f in files:
            logger.info(f"  Removing stale path: {f.path}")
            w.files.delete(f.path)
    except Exception as e:
        logger.warning(f"Volume cleanup encountered issues (might be empty): {e}")

    # 2. Upload Parquet files from Flat Silver Folders
    # We pull from the partitioned folders (e.g., data/silver/transactions/date=...)
    silver_base = config.DATA_DIR / "silver"
    
    targets = [
        {"name": "transactions", "path": silver_base / "transactions"},
        {"name": "stocks",       "path": silver_base / "stocks"},
        {"name": "accounts",     "path": silver_base / "accounts"}
    ]

    for target in targets:
        name = target["name"]
        local_base = target["path"]
        
        if not local_base.exists():
            logger.warning(f"Local silver path {local_base} does not exist. Skipping {name}.")
            continue
            
        # Find latest date= partition
        partitions = sorted([d.name for d in local_base.iterdir() if d.is_dir() and d.name.startswith("date=")])
        if not partitions:
            logger.warning(f"No partitions found in {local_base}. Skipping {name}.")
            continue
        
        latest_partition = partitions[-1]
        partition_path = local_base / latest_partition
        
        logger.info(f"Uploading {name} ({latest_partition}) to Databricks Volume...")
        
        parquet_files = list(partition_path.glob("*.parquet"))
        if not parquet_files:
            logger.warning(f"No parquet files found in {partition_path}. Skipping.")
            continue

        for f in parquet_files:
            remote_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{name}/{f.name}"
            # Retry up to 3 times — SSL/network timeouts happen on large files
            for attempt in range(1, 4):
                try:
                    logger.info(f"  Uploading {f.name} (attempt {attempt}/3)")
                    with f.open("rb") as fdata:
                        w.files.upload(remote_path, fdata, overwrite=True)
                    break  # success
                except Exception as e:
                    if attempt == 3:
                        logger.error(f"  FAILED after 3 attempts: {f.name} — {e}")
                        raise
                    logger.warning(f"  Attempt {attempt} failed ({e.__class__.__name__}), retrying...")
                    import time; time.sleep(5)

    # 3. Finalize with CTAS via Databricks SQL
    logger.info("Materializing Delta tables via CTAS (with schema cleanup)...")
    with config.get_databricks_connection() as conn:
        with conn.cursor() as cursor:
            # Transactions
            logger.info("Refreshing transactions_silver...")
            cursor.execute(f"DROP TABLE IF EXISTS {catalog}.{schema}.transactions_silver")
            cursor.execute(f"""
                CREATE TABLE {catalog}.{schema}.transactions_silver
                AS SELECT * FROM parquet.`/Volumes/{catalog}/{schema}/{volume_name}/transactions/*.parquet`
            """)
            
            # Stocks (Standardizing to User requested name: silver_stocks)
            logger.info("Refreshing silver_stocks...")
            cursor.execute(f"DROP TABLE IF EXISTS {catalog}.{schema}.stocks_silver") # Drop old variant if exists
            cursor.execute(f"DROP TABLE IF EXISTS {catalog}.{schema}.silver_stocks")
            cursor.execute(f"""
                CREATE TABLE {catalog}.{schema}.silver_stocks
                AS SELECT * FROM parquet.`/Volumes/{catalog}/{schema}/{volume_name}/stocks/*.parquet`
            """)
            
            # Accounts
            logger.info("Refreshing accounts_silver...")
            cursor.execute(f"DROP TABLE IF EXISTS {catalog}.{schema}.accounts_silver")
            cursor.execute(f"""
                CREATE TABLE {catalog}.{schema}.accounts_silver
                AS SELECT * FROM parquet.`/Volumes/{catalog}/{schema}/{volume_name}/accounts/*.parquet`
            """)

    logger.info("### CLOUD PROMOTION & CLEANUP COMPLETE ###")
    return True

if __name__ == "__main__":
    promote_to_databricks()
