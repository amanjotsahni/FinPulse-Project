import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
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
    Implements the 'Staging Volume + Atomic Load' pattern for 6.3M+ records.
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
    
    # 1. Ensure Volume exists
    logger.info(f"Ensuring volume '{catalog}.{schema}.{volume_name}' exists...")
    try:
        w.volumes.create(catalog_name=catalog, schema_name=schema, name=volume_name, volume_type="MANAGED")
    except Exception as e:
        if "ALREADY_EXISTS" in str(e):
            logger.info("Volume already exists.")
        else:
            logger.error(f"Failed to create volume: {e}")
            return False

    # 2. Upload Parquet files for Transactions and Stocks
    targets = [
        {"name": "transactions", "path": config.DATA_DIR / "silver" / "transactions"},
        {"name": "stocks", "path": config.DATA_DIR / "silver" / "stocks"}
    ]

    for target in targets:
        name = target["name"]
        local_base = target["path"]
        
        # Find latest partition
        if not local_base.exists():
            logger.warning(f"Local silver path {local_base} does not exist. Skipping {name}.")
            continue
            
        partitions = sorted([d.name for d in local_base.iterdir() if d.is_dir() and d.name.startswith("date=")])
        if not partitions:
            logger.warning(f"No partitions found in {local_base}. Skipping {name}.")
            continue
        
        latest_partition = partitions[-1]
        partition_path = local_base / latest_partition
        
        logger.info(f"Uploading {name} ({latest_partition}) to Databricks Volume...")
        
        parquet_files = list(partition_path.glob("*.parquet"))
        for f in parquet_files:
            remote_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{name}/{f.name}"
            logger.info(f"  Uploading {f.name} -> {remote_path}")
            with f.open("rb") as fdata:
                w.files.upload(remote_path, fdata, overwrite=True)

    # 3. Finalize with CTAS via Databricks SQL
    logger.info("Materializing Delta tables via CTAS...")
    with config.get_databricks_connection() as conn:
        with conn.cursor() as cursor:
            # Transactions
            cursor.execute(f"DROP TABLE IF EXISTS {catalog}.{schema}.transactions_silver")
            cursor.execute(f"""
                CREATE TABLE {catalog}.{schema}.transactions_silver
                AS SELECT * FROM parquet.`/Volumes/{catalog}/{schema}/{volume_name}/transactions/*.parquet`
            """)
            logger.info(f"Created table {catalog}.{schema}.transactions_silver")
            
            # Stocks
            cursor.execute(f"DROP TABLE IF EXISTS {catalog}.{schema}.stocks_silver")
            cursor.execute(f"""
                CREATE TABLE {catalog}.{schema}.stocks_silver
                AS SELECT * FROM parquet.`/Volumes/{catalog}/{schema}/{volume_name}/stocks/*.parquet`
            """)
            logger.info(f"Created table {catalog}.{schema}.stocks_silver")

    logger.info("CLOUD PROMOTION COMPLETE")
    return True

if __name__ == "__main__":
    promote_to_databricks()
