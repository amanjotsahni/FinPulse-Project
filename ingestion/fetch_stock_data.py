import os
import time
import random
from dotenv import load_dotenv
load_dotenv()
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Robust class-level monkey-patch for curl_cffi to force verify=False
try:
    import curl_cffi.requests
    orig_Session = curl_cffi.requests.Session
    class PatchedSession(orig_Session):
        def request(self, method, url, **kwargs):
            kwargs['verify'] = False
            return super().request(method, url, **kwargs)
    curl_cffi.requests.Session = PatchedSession
except Exception:
    pass

import requests
orig_request = requests.Session.request
def _patched_request(self, method, url, **kwargs):
    kwargs['verify'] = False
    return orig_request(self, method, url, **kwargs)
requests.Session.request = _patched_request

import yfinance as yf 
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path

# Add project root to path manually
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config import BRONZE_STOCKS, TICKERS, STOCK_PERIOD, DATA_SOURCE_STOCKS

# ============================================================
# FinPulse — Stock Market Data Ingestion (Bronze Layer)
# Purpose: Fetch real market data for financial analysis
# (Optimized with Idempotency to prevent IP blocks)
# ============================================================

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
    partition_path = Path(BRONZE_STOCKS) / f"date={partition_date}"

    # Optimization: Skip if we already have files for today
    if partition_path.exists() and list(partition_path.glob("stocks_raw_*.csv")):
        print(f"[{INGESTION_TIMESTAMP}] Skip: Stock data for {partition_date} already exists.")
        print(f"Directory: {partition_path}")
        return

    # Proceed to fetch
    print(f"[{INGESTION_TIMESTAMP}] Fetching fresh market data for: {TICKERS}")
    
    all_stocks = []
    for ticker in TICKERS:
        print(f" Downloading {ticker}...")
        
        df = pd.DataFrame()
        for attempt in range(3):
            try:
                stock = yf.Ticker(ticker)
                df = stock.history(period=STOCK_PERIOD)
                if not df.empty:
                    break
            except Exception as e:
                print(f"  Attempt {attempt+1} failed for {ticker}: {e}")
            
            time.sleep(random.uniform(2, 5) * (attempt + 1))
            
        if df.empty:
            print(f"  WARNING: Failed to fetch {ticker}. Skipping.")
            continue

        df = df.reset_index()
        df['ticker'] = ticker
        df['ingestion_timestamp'] = INGESTION_TIMESTAMP
        df['data_source'] = DATA_SOURCE_STOCKS

        all_stocks.append(df)
        print(f" {ticker}:{len(df)} records fetched")
        
        # Polite delay
        time.sleep(random.uniform(2, 4))
        
    if not all_stocks:
        raise ValueError("Failed to fetch data for all tickers (IP Block). Run again later.")
        
    combined_df = pd.concat(all_stocks, ignore_index=True)

    # Save to Bronze Layer
    os.makedirs(partition_path, exist_ok=True)
    timestamp = datetime.now().strftime("%H%M%S")
    output_file = partition_path / f"stocks_raw_{timestamp}.csv"
    combined_df.to_csv(output_file, index=False)

    print("\n" + "=" *60)
    print("STOCK INGESTION COMPLETE")
    print(f"Records: {combined_df.shape[0]:,}")
    print(f"Output: {output_file}")
    print("="*60)

if __name__ == "__main__":
    run_stock_ingestion()