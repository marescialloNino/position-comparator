#!/usr/bin/env python3
"""
fetch_binance_futures_historical_connector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Fetch historical 15-minute kline data for a futures ticker using binance-futures-connector-python.
"""

import os
import time
from datetime import datetime, timedelta
import pandas as pd
from binance.cm_futures import CMFutures
from binance.um_futures import UMFutures
from binance.error import ClientError

# Use environment variables for API credentials (recommended)
API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_SECRET_KEY", "")

# Initialize the client for USDT-margined futures
client = UMFutures()

# Parameters
ticker = "CTCUSDT"  # USDT-margined futures ticker
interval = "15m"     # 15-minute interval
start_date = "1 Mar, 2025"  # Start date
end_date = "3 Mar, 2025"  # Current date (10:42 AM UTC, May 13, 2025)
limit = 1500         # Max candles per request

# Convert dates to timestamps (milliseconds)
start_timestamp = int(datetime.strptime(start_date, "%d %b, %Y").timestamp() * 1000)
end_timestamp = int(datetime.strptime(end_date, "%d %b, %Y").timestamp() * 1000)

# Function to fetch historical klines with pagination
def fetch_historical_klines(symbol, interval, start_time, end_time):
    klines = []
    current_start = start_time
    
    while current_start < end_time:
        try:
            # Fetch klines using the Market client
            data = client.klines(
                symbol=symbol,
                interval=interval,
                startTime=current_start,
                endTime=end_time,
                limit=limit
            )
            
            if not data or len(data) == 0:
                break
                
            klines.extend(data)
            current_start = int(data[-1][0]) + 1  # Move to the next candle's start time
            
            # Avoid hitting rate limits (Binance allows ~1200 requests/min for this endpoint)
            time.sleep(0.1)
            
        except ClientError as e:
            print(f"Error fetching data: {e.error_message} (Code: {e.status_code})")
            break
            
    return klines

# Fetch data
klines = fetch_historical_klines(ticker, interval, start_timestamp, end_timestamp)

# Convert to DataFrame
if klines:
    columns = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "num_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ]
    df = pd.DataFrame(klines, columns=columns)
    
    # Convert timestamp to readable format and set as index
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_localize(None)
    df.set_index("open_time", inplace=True)
    
    # Convert numeric columns to float
    numeric_cols = ["open", "high", "low", "close", "volume", "quote_asset_volume", "num_trades",
                    "taker_buy_base_volume", "taker_buy_quote_volume"]
    for col in numeric_cols:
        df[col] = df[col].astype(float)
    
    # Select relevant columns
    df = df[["open", "high", "low", "close", "volume"]]
    
    # Save to CSV
    output_dir = "binance_futures_data"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{ticker}_15m.csv")
    df.to_csv(output_file)
    
    print(f"Data for {ticker} saved to {output_file}")
    print(df.tail())
else:
    print(f"No data fetched for {ticker}")