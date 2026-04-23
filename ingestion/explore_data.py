import pandas as pd
import sys
import os
from pathlib import Path

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config import RAW_TRANSACTIONS_PATH, RAW_ACCOUNTS_PATH

def explore_csv(file_path, label):
    print("\n" + "="*60)
    print(f"EXPLORING {label}: {file_path}")
    print("="*60)
    
    if not os.path.exists(file_path):
        print(f"ERROR: File not found: {file_path}")
        return

    # Read a sample to avoid memory issues with large files
    df = pd.read_csv(file_path, nrows=1000)
    
    print(f"Sample Columns: {df.columns.tolist()}")
    print(f"\nFirst 3 rows:\n{df.head(3)}")
    print(f"\nData Types:\n{df.dtypes}")
    
    # Get total row count (efficiently)
    count = 0
    for chunk in pd.read_csv(file_path, chunksize=100000, usecols=[df.columns[0]]):
        count += len(chunk)
    print(f"\nTotal Rows: {count:,}")

if __name__ == "__main__":
    explore_csv(RAW_TRANSACTIONS_PATH, "TRANSACTIONS")
    explore_csv(RAW_ACCOUNTS_PATH, "ACCOUNTS")