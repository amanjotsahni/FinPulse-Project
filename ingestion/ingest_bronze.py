import pandas as pd
import os
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path manually
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config import (
    RAW_TRANSACTIONS_PATH, RAW_ACCOUNTS_PATH,
    BRONZE_TRANSACTIONS_TARGETS, BRONZE_ACCOUNTS_TARGETS,
    PIPELINE_VERSION, DATA_SOURCE_TRANSACTIONS, DATA_SOURCE_ACCOUNTS
)

# ============================================================
# FinPulse — Bronze Layer Ingestion (Multi-Source)
# Purpose: Load IBM AML transaction and account data into Bronze
# using chunking to support 8GB RAM systems.
# ============================================================

INGESTION_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def ingest_file(source_path, target_dirs, label, data_source_name):
    """Generic chunked ingestion for large CSV files. Supports multiple targets."""
    print("-" * 60)
    print(f"INGESTING {label}")
    print("-" * 60)
    
    if not os.path.exists(source_path):
        print(f"ERROR: Source file not found: {source_path}")
        return False

    partition_date = datetime.now().strftime("%Y-%m-%d")
    
    # ── Check all targets first ───────────────────────────────────
    all_targets_exist = True
    for t_dir in target_dirs:
        partition_path = Path(t_dir) / f"date={partition_date}"
        output_file = partition_path / f"{label.lower()}_raw.csv"
        if not output_file.exists():
            all_targets_exist = False
            break

    if all_targets_exist:
        print(f"[{INGESTION_TIMESTAMP}] Skip: Bronze partitions for {partition_date} already exist on all sites.")
        return True

    print(f"[{INGESTION_TIMESTAMP}] Starting multi-site ingestion...")
    print(f"Source: {source_path}")
    
    # Ensure directories exist
    for t_dir in target_dirs:
        os.makedirs(Path(t_dir) / f"date={partition_date}", exist_ok=True)

    chunk_size = 500000 
    total_rows = 0
    first_chunk = True
    
    try:
        for chunk in pd.read_csv(source_path, chunksize=chunk_size):
            # Add metadata
            chunk['ingestion_timestamp'] = INGESTION_TIMESTAMP
            chunk['data_source'] = data_source_name
            chunk['pipeline_version'] = PIPELINE_VERSION
            
            # Save chunk to all targets
            for t_dir in target_dirs:
                partition_path = Path(t_dir) / f"date={partition_date}"
                output_file = partition_path / f"{label.lower()}_raw.csv"
                mode = 'w' if first_chunk else 'a'
                header = True if first_chunk else False
                chunk.to_csv(output_file, mode=mode, header=header, index=False)
            
            total_rows += len(chunk)
            print(f"Processed {total_rows:,} records (Writing to {len(target_dirs)} sites)...")
            first_chunk = False
            
        print(f"SUCCESS: {total_rows:,} records saved to {len(target_dirs)} locations.")
        return True
    except Exception as e:
        print(f"FAILED during ingestion of {label}: {e}")
        return False

def run_bronze_ingestion():
    """Orchestrates ingestion for all Bronze sources."""
    print("="*60)
    print("FINPULSE - BRONZE LAYER INGESTION (v2.0)")
    print("="*60)
    
    t_success = ingest_file(RAW_TRANSACTIONS_PATH, BRONZE_TRANSACTIONS_TARGETS, "TRANSACTIONS", DATA_SOURCE_TRANSACTIONS)
    a_success = ingest_file(RAW_ACCOUNTS_PATH, BRONZE_ACCOUNTS_TARGETS, "ACCOUNTS", DATA_SOURCE_ACCOUNTS)
    
    if t_success and a_success:
        print("\n" + "=" * 60)
        print("ALL BRONZE INGESTION TASKS COMPLETE")
        print("=" * 60)
    else:
        print("\n" + "!" * 60)
        print("SOME INGESTION TASKS FAILED. CHECK LOGS.")
        print("!" * 60)

if __name__=="__main__":
    run_bronze_ingestion()
