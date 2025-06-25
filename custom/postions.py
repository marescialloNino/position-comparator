import pandas as pd
from datetime import datetime
import ccxt
import os
import time

# Step 1: Create price_data folder
output_folder = 'price_data'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
    print(f"Created folder: {output_folder}")

# Step 2: Read current_state.log to extract tickers and timestamps
log_data = []
tickers_set = set()
try:
    with open('nickel/basket3b/current_state.log', 'r') as f:
        header = f.readline().strip()  # Skip header (ts:positions)
        for line in f:
            ts_str, positions_str = line.strip().split(';')
            positions = eval('{' + positions_str + '}')
            log_data.append({'ts': ts_str, 'positions': positions})
            tickers_set.update(positions.keys())
except FileNotFoundError:
    print("Error: nickel/basket3b/current_state.log not found.")
    exit(1)

# Get unique timestamps as strings
timestamps = sorted(list(set([entry['ts'] for entry in log_data])))
first_timestamp = min(timestamps, key=lambda x: datetime.strptime(x, '%Y/%m/%d %H:%M:%S.%f'))
last_timestamp = max(timestamps, key=lambda x: datetime.strptime(x, '%Y/%m/%d %H:%M:%S.%f'))

# Convert to milliseconds for CCXT
first_ts_millis = int(datetime.strptime(first_timestamp, '%Y/%m/%d %H:%M:%S.%f').timestamp() * 1000)
last_ts_millis = int(datetime.strptime(last_timestamp, '%Y/%m/%d %H:%M:%S.%f').timestamp() * 1000)

print(f"Unique Tickers: {tickers_set}")
print(f"First Timestamp: {first_timestamp} ({first_ts_millis} ms)")
print(f"Last Timestamp: {last_timestamp} ({last_ts_millis} ms)")

# Step 3: Initialize Binance for USDT-M futures (testnet)
exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

# Step 4: Fetch all OHLCV data to cover the full time window
price_data = {}
timeframe = '3m'
candle_duration_ms = 3 * 60 * 1000  # 3 minutes in milliseconds
max_candles_per_fetch = 1000

for ticker in sorted(tickers_set):  # Sort for consistency
    print(f"Fetching 3m OHLCV data for {ticker}...")
    try:
        all_ohlcv = []
        current_ts = first_ts_millis
        while current_ts < last_ts_millis:
            print(f"Fetching batch for {ticker} from {pd.to_datetime(current_ts, unit='ms')}")
            ohlcv = exchange.fetch_ohlcv(ticker, timeframe, since=current_ts, limit=max_candles_per_fetch)
            if not ohlcv:
                break
            all_ohlcv.extend(ohlcv)
            # Update current_ts to the timestamp of the last candle + 1
            last_candle_ts = ohlcv[-1][0]
            current_ts = last_candle_ts + candle_duration_ms
            time.sleep(0.5)  # Avoid rate limits
        if all_ohlcv:
            df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            # Remove duplicates and sort by timestamp
            df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            price_data[ticker] = df
            # Save to CSV
            ticker_csv = os.path.join(output_folder, f"{ticker}.csv")
            df.to_csv(ticker_csv, index=False)
            print(f"Successfully fetched and saved data for {ticker}: {len(df)} candles to {ticker_csv}")
        else:
            print(f"No data returned for {ticker}")
            price_data[ticker] = None
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        price_data[ticker] = None

# Step 5: Aggregate close prices into price_data.csv
print("Aggregating close prices into price_data.csv...")
price_df = pd.DataFrame({'timestamp': timestamps})
for ticker in sorted(tickers_set):
    ticker_csv = os.path.join(output_folder, f"{ticker}.csv")
    if os.path.exists(ticker_csv):
        try:
            df = pd.read_csv(ticker_csv)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            prices = []
            for ts in timestamps:
                ts_dt = datetime.strptime(ts, '%Y/%m/%d %H:%M:%S.%f')
                if not df.empty:
                    df['time_diff'] = abs(df['timestamp'] - ts_dt)
                    closest_row = df.loc[df['time_diff'].idxmin()]
                    # Check if the closest timestamp is within a reasonable range (e.g., 3 minutes)
                    if closest_row['time_diff'].total_seconds() <= 180:
                        price = closest_row['close']
                    else:
                        price = None
                else:
                    price = None
                prices.append(price)
            price_df[ticker] = prices
        except Exception as e:
            print(f"Error reading {ticker_csv}: {e}")
            price_df[ticker] = [None] * len(timestamps)
    else:
        print(f"CSV not found for {ticker}")
        price_df[ticker] = [None] * len(timestamps)

# Step 6: Save aggregated CSV
output_csv = 'price_data.csv'
price_df.to_csv(output_csv, index=False)
print(f"Aggregated CSV saved to {output_csv}")