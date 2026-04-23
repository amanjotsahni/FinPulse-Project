import os
import time
import requests
import yfinance as yf 
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add project root to path manually
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config import BRONZE_STOCKS_TARGETS, TICKERS, DATA_SOURCE_STOCKS
# ============================================================
# FinPulse — Stock Market Data Ingestion (Bronze Layer)
# Purpose: Fetch real market data aligned to IBM AML period
# Date range: 2022-08-15 to 2022-09-30 (fixed, not rolling)
# ============================================================

# Fixed date range — aligned to IBM AML transaction period
STOCK_START_DATE = "2022-08-15"
STOCK_END_DATE   = "2022-09-30"

INGESTION_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def run_stock_ingestion():
    """
    Main orchestrator for stock data ingestion.
    Optimization: skips if today's data already exists to avoid Yahoo IP bans.
    """
    print("="*60)
    print("FINPULSE - STOCK DATA BRONZE INGESTION")
    print("="*60)

    partition_date = datetime.now().strftime("%Y-%m-%d")
    
    # ── Check all targets first ───────────────────────────────────
    all_targets_exist = True
    for t_dir in BRONZE_STOCKS_TARGETS:
        partition_path = Path(t_dir) / f"date={partition_date}"
        if not (partition_path.exists() and list(partition_path.glob("stocks_raw_*.csv"))):
            all_targets_exist = False
            break

    if all_targets_exist:
        print(f"[{INGESTION_TIMESTAMP}] Skip: Stock data for {partition_date} already exists on all sites.")
        return

    # Proceed to fetch
    print(f"[{INGESTION_TIMESTAMP}] Fetching market data for: {TICKERS}")
    print(f"Date range: {STOCK_START_DATE} to {STOCK_END_DATE}")
    
    all_stocks = []
    for ticker in TICKERS:
        print(f" Downloading {ticker}...")
        
        df = pd.DataFrame()
        for attempt in range(3):
            try:
                # Use standard download 
                df = yf.download(
                    ticker, 
                    start=STOCK_START_DATE, 
                    end=STOCK_END_DATE, 
                    progress=False
                )
                if not df.empty:
                    break
            except Exception as e:
                print(f"  Attempt {attempt+1} failed for {ticker}: {e}")
            
            time.sleep(2 * (attempt + 1))
            
        if df.empty:
            print(f"  WARNING: Failed to fetch {ticker}. Skipping.")
            continue

        df = df.reset_index()
        df['ticker'] = ticker
        df['ingestion_timestamp'] = INGESTION_TIMESTAMP
        df['data_source'] = DATA_SOURCE_STOCKS

        all_stocks.append(df)
        print(f" {ticker}: {len(df)} records fetched")
        
        # Polite delay
        time.sleep(random.uniform(2, 4))
        
    if not all_stocks:
        raise ValueError("Failed to fetch data for all tickers (IP Block). Run again later.")
        
    combined_df = pd.concat(all_stocks, ignore_index=True)

    # Save to all Bronze sites
    timestamp = datetime.now().strftime("%H%M%S")
    for t_dir in BRONZE_STOCKS_TARGETS:
        partition_path = Path(t_dir) / f"date={partition_date}"
        os.makedirs(partition_path, exist_ok=True)
        output_file = partition_path / f"stocks_raw_{timestamp}.csv"
        combined_df.to_csv(output_file, index=False)
        print(f" Saved to: {output_file}")

    print("\n" + "="*60)
    print("STOCK INGESTION COMPLETE")
    print(f"Records   : {combined_df.shape[0]:,}")
    print(f"Tickers   : {combined_df['ticker'].nunique()}")
    print(f"Date range: {combined_df['Date'].min()} → {combined_df['Date'].max()}")
    print(f"Output    : {output_file}")
    print("="*60)

if __name__ == "__main__":
    run_stock_ingestion()