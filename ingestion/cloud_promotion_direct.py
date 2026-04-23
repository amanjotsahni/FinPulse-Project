import os
import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from databricks.connect import DatabricksSession
import logging

# -- Setup Logging --
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("CloudPromotionDirect")

# -- Load Config --
load_dotenv(override=True)
PROJECT_ROOT = Path(__file__).parent.parent
SILVER_BASE = PROJECT_ROOT / "data" / "silver"

def get_spark_connect():
    """Initializes Databricks Connect Session."""
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    
    if not host or not token:
        raise ValueError("DATABRICKS_HOST or DATABRICKS_TOKEN missing from .env")
        
    logger.info(f"Connecting to Databricks Connect: {host}")
    return DatabricksSession.builder \
        .host(host) \
        .token(token) \
        .serverless(True) \
        .getOrCreate()

def push_table(spark, table_name, silver_folder_name):
    """
    Pushes data directly to Databricks using the 'Chunking Partition Technique'.
    Iterates through local parquet files and pushes each as a batch.
    """
    logger.info(f"\n=== MIGRATING {silver_folder_name.upper()} ===")
    
    local_dir = SILVER_BASE / silver_folder_name
    if not local_dir.exists():
        logger.error(f"Local silver folder not found: {local_dir}")
        return

    # Find latest partition
    partitions = sorted([d.name for d in local_dir.iterdir() if d.is_dir() and d.name.startswith("date=")])
    if not partitions:
        logger.error(f"No partitions found in {local_dir}")
        return
        
    latest_partition = partitions[-1]
    partition_path = local_dir / latest_partition
    parquet_files = list(partition_path.glob("*.parquet"))
    
    logger.info(f"Source Partition: {latest_partition}")
    logger.info(f"Found {len(parquet_files)} parquet files to migration.")

    target_table = f"workspace.finpulse.{table_name}"
    
    # Clean-Before-Load: Drop existing table to ensure 'Latest Only'
    print(f"  [CLEANUP] Dropping existing table {target_table}...")
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")

    total_rows = 0
    for i, f in enumerate(parquet_files):
        logger.info(f"  [Batch {i+1}/{len(parquet_files)}] Reading {f.name}...")
        
        # Read file into Pandas
        df_pd = pd.read_parquet(f)
        
        # Stability fix: ensure all object columns are strings (avoids Arrow mixed-type issues)
        for col in df_pd.columns:
            if df_pd[col].dtype == 'object':
                df_pd[col] = df_pd[col].astype(str)
        
        # Convert to Spark and push
        df_spark = spark.createDataFrame(df_pd)
        
        # First batch creates the table (implicitly), subsequent batches append
        mode = "append" # Since we dropped it, first insert creates it
        
        logger.info(f"  [Batch {i+1}/{len(parquet_files)}] Pushing {len(df_pd):,} rows to {target_table}...")
        df_spark.write \
            .format("delta") \
            .mode(mode) \
            .saveAsTable(target_table)
            
        total_rows += len(df_pd)

    logger.info(f"SUCCESS: {target_table} migration complete. Total rows: {total_rows:,}")

if __name__ == "__main__":
    try:
        spark = get_spark_connect()
        
        # Migration targets: (iceberg_name, folder_name)
        targets = [
            ("transactions_silver", "transactions"),
            ("silver_stocks",       "stocks"),
            ("accounts_silver",     "accounts")
        ]
        
        for table_name, folder_name in targets:
            push_table(spark, table_name, folder_name)
            
        logger.info("\n=== ALL MIGRATIONS COMPLETE ===")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()
