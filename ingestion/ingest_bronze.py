import pandas as pd 
import os
import sys
from datetime import datetime

# Add project root to path manually to ensure config can be imported securely when run as script
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config import RAW_TRANSACTIONS_PATH, BRONZE_TRANSACTIONS, PIPELINE_VERSION, DATA_SOURCE_TRANSACTIONS

# ============================================================
# FinPulse — Bronze Layer Ingestion
# Purpose: Load raw financial transaction data into Bronze
# layer as immutable raw landing zone
# Author: Amanjot Kaur
# ============================================================

INGESTION_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def load_raw_transactions(file_path):
    """
    Loads raw financial transaction data from CSV source.
    Bronze layer principle: ingest as-is, no transformations.
    """
    print(f"[{INGESTION_TIMESTAMP}] Starting Bronze ingestion...")
    print(f"Source: {file_path}")

    df = pd.read_csv(file_path)
    print(f"Records loaded: {df.shape[0]:,}")
    print(f"Columns: {df.columns.tolist()}")
    return df

    
def add_metadata(df):
    """
    Adds pipeline metadata columns to raw dataframe.
    Tracks ingestion timestamp and source for data lineage.
    """
    df['ingestion_timestamp']= INGESTION_TIMESTAMP
    df['data_source'] = DATA_SOURCE_TRANSACTIONS
    df['pipeline_version'] = PIPELINE_VERSION

    return df 

def save_bronze_layer(df, output_path):
    """
    Saves Bronze layer data as partitioned CSV files.
    Partitioned by date for efficient downstream querying.
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)

    # Create partition folder by today's date
    partition_date = datetime.now().strftime("%Y-%m-%d")
    partition_path = os.path.join(output_path, f"date={partition_date}")
    os.makedirs(partition_path, exist_ok=True)

    # Save as CSV
    output_file = os.path.join(partition_path, "transactions_raw.csv")
    df.to_csv(output_file, index=False)
    print(f"Bronze data saved to:{output_file}")
    print(f"Total records saved : {df.shape[0]:,}")

    return output_file

def run_bronze_ingestion():
    """
    Main function - orchestrates full Bronze ingestion pipeline.
    """
    print("="*60)
    print("FinPulse - BRONZE LAYER INGESTION")
    print("="*60)

    # Step 1 - Load raw data
    df = load_raw_transactions(RAW_TRANSACTIONS_PATH)
    # Step 2 - Add metadata
    df = add_metadata(df)
    # Step 3 - Save to Bronze layer
    output_file = save_bronze_layer(df, BRONZE_TRANSACTIONS)

    print("\n" + "=" * 60)
    print("BRONZE INGESTION COMPLETE")
    print(f"Records: {df.shape[0]:,}")
    print(f"Columns: {df.shape[1]}")
    print(f"Output: {output_file}")
    print("=" * 60)

if __name__=="__main__":
    run_bronze_ingestion()
    
