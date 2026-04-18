import pandas as pd 
import os
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path manually
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config import RAW_TRANSACTIONS_PATH, BRONZE_TRANSACTIONS, PIPELINE_VERSION, DATA_SOURCE_TRANSACTIONS

# ============================================================
# FinPulse — Bronze Layer Ingestion (Memory Optimized)
# Purpose: Load raw financial transaction data into Bronze
# using chunking to support 8GB RAM systems.
# ============================================================

INGESTION_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def run_bronze_ingestion():
    """
    Orchestrates memory-efficient Bronze ingestion using chunking.
    Skips if partition for today already exists to save resources.
    """
    print("="*60)
    print("FinPulse - BRONZE LAYER INGESTION (Optimized)")
    print("="*60)

    partition_date = datetime.now().strftime("%Y-%m-%d")
    partition_path = Path(BRONZE_TRANSACTIONS) / f"date={partition_date}"
    output_file = partition_path / "transactions_raw.csv"

    # Optimization: Skip if already ingested today
    if output_file.exists():
        print(f"[{INGESTION_TIMESTAMP}] Skip: Bronze partition for {partition_date} already exists.")
        print(f"Path: {output_file}")
        return

    print(f"[{INGESTION_TIMESTAMP}] Starting Bronze ingestion...")
    print(f"Source: {RAW_TRANSACTIONS_PATH}")

    # Step 1: Create partition folder
    os.makedirs(partition_path, exist_ok=True)

    # Step 2: Use Chunking to handle 8GB RAM limit
    chunk_size = 500000 # 500k rows at a time
    total_rows = 0
    
    first_chunk = True
    for chunk in pd.read_csv(RAW_TRANSACTIONS_PATH, chunksize=chunk_size):
        # Add metadata
        chunk['ingestion_timestamp'] = INGESTION_TIMESTAMP
        chunk['data_source'] = DATA_SOURCE_TRANSACTIONS
        chunk['pipeline_version'] = PIPELINE_VERSION
        
        # Save chunk
        mode = 'w' if first_chunk else 'a'
        header = True if first_chunk else False
        chunk.to_csv(output_file, mode=mode, header=header, index=False)
        
        total_rows += len(chunk)
        print(f"Processed {total_rows:,} records...")
        first_chunk = False

    print("\n" + "=" * 60)
    print("BRONZE INGESTION COMPLETE")
    print(f"Total Records Saved: {total_rows:,}")
    print(f"Output: {output_file}")
    print("=" * 60)

if __name__=="__main__":
    run_bronze_ingestion()
