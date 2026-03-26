import yfinance as yf 
import pandas as pd
import os 
from datetime import datetime

# ============================================================
# FinPulse — Stock Market Data Ingestion (Bronze Layer)
# Purpose: Fetch real market data for financial analysis
# Author: Amanjot Kaur
# ============================================================

TICKERS = ["AAPL","GOOGL","MSFT","JPM","GS"]
PERIOD = "2y"
BRONZE_STOCK_PATH = r"C:\FinPulse Project\data\bronze\stocks"
INGESTION_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def fetch_stock_data(tickers, period):
    """
    Fetches OHLCV stock data for given tickers from Yahoo Finance.
    Returns a combined dataframe with all tickers.
    """
    all_stocks = []
    for ticker in tickers:
        print(f" Downloading {ticker}...")
        stock = yf.Ticker(ticker)
        df = stock.history(period=period)

        df = df.reset_index()
        df['ticker'] = ticker
        df['ingestion_timestamp']=INGESTION_TIMESTAMP
        df['data_source']='yahoo_finance'

        all_stocks.append(df)
        print(f" {ticker}:{len(df)} records fetched")
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

    output_file = os.path.join(partition_path, "stocks_raw.csv")
    df.to_csv(output_file, index=False)
    
    print(f"\nStock data saved to: {output_file}")
    print(f"Total records saved: {df.shape[0]:,}")
    
    return output_file
