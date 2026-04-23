import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from pathlib import Path

def generate_mock_stocks():
    print("Generating high-fidelity mock stock data for Sep 2022 (IBM AML Alignment)...")
    
    tickers = ['AAPL', 'GOOGL', 'MSFT', 'JPM', 'GS']
    start_date = datetime(2022, 8, 15)
    end_date = datetime(2022, 9, 30)
    
    # Create the date range
    dates = pd.date_range(start_date, end_date, freq='B')
    
    # Base prices for Aug 2022
    base_prices = {
        'AAPL': 150.0,
        'GOOGL': 110.0,
        'MSFT': 260.0,
        'JPM': 115.0,
        'GS': 330.0
    }
    
    all_data = []
    
    for ticker in tickers:
        price = base_prices[ticker]
        for date in dates:
            # Simulate a slight downward trend in Sep 2022
            daily_change = np.random.normal(-0.001, 0.015) 
            open_p = price
            close_p = price * (1 + daily_change)
            high_p = max(open_p, close_p) * (1 + abs(np.random.normal(0, 0.005)))
            low_p = min(open_p, close_p) * (1 - abs(np.random.normal(0, 0.005)))
            vol = np.random.randint(5000000, 50000000)
            
            all_data.append({
                'Date': date.strftime('%Y-%m-%d'),
                'Open': round(open_p, 2),
                'High': round(high_p, 2),
                'Low': round(low_p, 2),
                'Close': round(close_p, 2),
                'Volume': vol,
                'Dividends': 0.0,
                'Stock Splits': 0.0,
                'ticker': ticker,
                'ingestion_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data_source': 'Mock Yahoo Finance (SSL Workaround)'
            })
            price = close_p # Next day open
            
    df = pd.DataFrame(all_data)
    
    # Save to Bronze folder
    partition_date = datetime.now().strftime("%Y-%m-%d")
    target_dir = Path("data/bronze/stocks") / f"date={partition_date}"
    os.makedirs(target_dir, exist_ok=True)
    
    target_file = target_dir / "stocks_raw.csv"
    df.to_csv(target_file, index=False)
    print(f"Successfully landed mock data at: {target_file}")

if __name__ == "__main__":
    generate_mock_stocks()
