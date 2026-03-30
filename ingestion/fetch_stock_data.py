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

# Add project root to path manually to ensure config can be imported securely when run as script
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config import BRONZE_STOCKS, TICKERS, STOCK_PERIOD, DATA_SOURCE_STOCKS

# ============================================================
# FinPulse — Stock Market Data Ingestion (Bronze Layer)
# Purpose: Fetch real market data for financial analysis
# Author: Amanjot Kaur
# ============================================================

INGESTION_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def fetch_stock_data(tickers, period):
    """
    Fetches OHLCV stock data for given tickers from Yahoo Finance.
    Returns a combined dataframe with all tickers.
    """
    all_stocks = []
    for ticker in tickers:
        print(f" Downloading {ticker}...")
        
        # Robust Retry Logic wrapper
        df = pd.DataFrame()
        for attempt in range(3):
            try:
                stock = yf.Ticker(ticker)
                df = stock.history(period=period)
                if not df.empty:
                    break
            except Exception as e:
                print(f"  Attempt {attempt+1} failed for {ticker}: {e}")
            
            # Exponential backoff before retrying
            time.sleep(random.uniform(2, 5) * (attempt + 1))
            
        if df.empty:
            print(f"  WARNING: Failed to fetch {ticker}. Skipping.")
            continue

        df = df.reset_index()
        df['ticker'] = ticker
        df['ingestion_timestamp']=INGESTION_TIMESTAMP
        df['data_source']=DATA_SOURCE_STOCKS

        all_stocks.append(df)
        print(f" {ticker}:{len(df)} records fetched")
        
        # Polite delay to prevent Yahoo IP Ban
        sleep_time = random.uniform(2, 4)
        print(f"  ...waiting {sleep_time:.1f}s to respect rate limits...")
        time.sleep(sleep_time)
        
    if not all_stocks:
        raise ValueError("Failed to fetch data for all tickers due to IP block or network issues.")
        
    combined_df = pd.concat(all_stocks, ignore_index=True)

    print(f"\nTotal records fetched: {combined_df.shape[0]:,}")
    print(f"Columns: {combined_df.columns.tolist()}")

    return combined_df

def save_stock_bronze(df, output_path):
    """
    Saves raw stock data to Bronze layer partitioned by date.
    """
    os.makedirs(output_path, exist_ok=True)

    partition_date = datetime.now().strftime("%Y-%m-%d")
    partition_path = os.path.join(output_path, f"date={partition_date}")
    os.makedirs(partition_path, exist_ok=True)

    timestamp = datetime.now().strftime("%H%M%S")
    output_file = os.path.join(partition_path, f"stocks_raw_{timestamp}.csv")
    df.to_csv(output_file, index=False)
    
    print(f"\nStock data saved to: {output_file}")
    print(f"Total records saved: {df.shape[0]:,}")
    
    return output_file

def run_stock_ingestion():
    """
      Main orchestrator for stock data Bronze ingestion.
    """
    print("="*60)
    print("FINPULSE - STOCK DATA BRONZE INGESTION")
    print("="*60)

    # Step 1: Fetch from Yahoo Finance
    df = fetch_stock_data(TICKERS, STOCK_PERIOD)

    # Step 2: Save to Bronze Layer
    output_file = save_stock_bronze(df, BRONZE_STOCKS)

    print("\n" + "=" *60)
    print("STOCK INGESTION COMPLETE")
    print(f"Tickers: {TICKERS}")
    print(f"Records: {df.shape[0]:,}")
    print(f"Output: {output_file}")
    print("="*60)

if __name__ == "__main__":
    run_stock_ingestion()
    