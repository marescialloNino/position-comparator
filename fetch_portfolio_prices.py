#!/usr/bin/env python3
"""
fetch_portfolio_prices.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Scans closed and open positions for required tickers and time windows,
downloads 15-minute price data from Binance using a custom BinanceFutureMarket wrapper,
and stores it in individual CSV files for portfolio unbalance and PNL calculations.
"""

import json
import os
from pathlib import Path
from typing import List, Tuple, Set, Dict
from datetime import datetime

import pandas as pd
from tqdm import tqdm

# Adjust the path to import your modules
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datafeed.utils_online import build_symbol, extract_coin
import datafeed.binancefeed as bf

# --- PATHS & CONFIG ---
MODULE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = MODULE_DIR / "binance_test"; OUTPUT_DIR.mkdir(exist_ok=True)
TIMEFRAME = "15m"  # Match the original 15-minute interval

# Define file paths for progress and error logging
PROGRESS_LOG = OUTPUT_DIR / "download_progress.log"
ERROR_LOG = OUTPUT_DIR / "download_errors.log"

# --- TICKER & TIME WINDOW SCANNING ---
def scan_tickers_and_windows(config_file: str = "config_test.json") -> List[Tuple[str, Tuple[datetime, datetime]]]:
    """
    Scan closed and open positions to determine required tickers and time windows.
    Returns a list of (ticker, (start_dt, end_dt)) tuples with naive UTC datetimes.
    """
    # Load configuration
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Error: Config file {config_file} not found.")
        return []

    strategies = list(config.get("strategy", {}).keys())
    output_dir = config.get("output_directory", "output_bin/")

    # Collect tickers and time windows
    ticker_windows: Dict[str, List[Tuple[datetime, datetime]]] = {}
    now = datetime.utcnow()  # Naive UTC

    for strat_name in strategies:
        # 1. Process Closed Positions and Open Positions from closed_positions_<strat>.json
        closed_file = f"{output_dir}closed_positions_{strat_name}.json"
        if os.path.isfile(closed_file):
            with open(closed_file, "r") as f:
                closed_positions = json.load(f)
            for timestamp, positions in closed_positions.items():
                for pos in positions:
                    ticker = pos["asset"]
                    entry_time = pos["entry_time"]
                    entry_dt = datetime.strptime(entry_time, "%Y/%m/%d %H:%M:%S.%f")
                    status = pos.get("status", "CLOSED")  # Default to "CLOSED" for backward compatibility
                    if status == "CLOSED":
                        exit_time = pos["exit_time"]
                        exit_dt = datetime.strptime(exit_time, "%Y/%m/%d %H:%M:%S.%f")
                    else:  # status == "OPEN"
                        exit_dt = now  # Use current time for open positions
                    if ticker not in ticker_windows:
                        ticker_windows[ticker] = []
                    ticker_windows[ticker].append((entry_dt, exit_dt))

        # 2. Open Positions from portfolio_<strat>.json (for consistency, but likely redundant)
        portfolio_file = f"{output_dir}portfolio_{strat_name}.json"
        if os.path.isfile(portfolio_file):
            with open(portfolio_file, "r") as f:
                portfolio = json.load(f)
            for timestamp, data in portfolio.items():
                for pos in data.get("portfolio", []):
                    ticker = pos["asset"]
                    entry_time = pos["entry_time"]
                    entry_dt = datetime.strptime(entry_time, "%Y/%m/%d %H:%M:%S.%f")
                    if ticker not in ticker_windows:
                        ticker_windows[ticker] = []
                    ticker_windows[ticker].append((entry_dt, now))

    # Merge overlapping time windows for each ticker
    result: List[Tuple[str, Tuple[datetime, datetime]]] = []
    for ticker, windows in ticker_windows.items():
        windows.sort(key=lambda x: x[0])  # Sort by start time
        merged = []
        current_start, current_end = windows[0]
        for start, end in windows[1:]:
            if start <= current_end + pd.Timedelta(minutes=15):  # Match TIMEFRAME
                current_end = max(current_end, end)
            else:
                merged.append((current_start, current_end))
                current_start, current_end = start, end
        merged.append((current_start, current_end))
        for start, end in merged:
            result.append((ticker, (start, end)))

    return result

# --- PRICE DOWNLOADING ---
def fetch_binance_prices(requests: List[Tuple[str, Tuple[datetime, datetime]]]) -> None:
    # Initialize Binance futures market
    market = bf.BinanceFutureMarket()

    # Load completed pairs from progress log
    completed_pairs = set()
    if PROGRESS_LOG.is_file():
        with open(PROGRESS_LOG, 'r') as f:
            completed_pairs = {line.strip() for line in f if line.strip()}

    # Process each ticker and its time window
    for ticker, (start_dt, end_dt) in tqdm(requests, desc="Tickers"):
        if ticker in completed_pairs:
            print(f"Skipping {ticker}: already processed.")
            continue

        try:
            print(f"Fetching data for {ticker}")
            # Convert datetime to timestamp (seconds since epoch)
            start_ts = int(start_dt.timestamp())
            end_ts = int(end_dt.timestamp())

            # Fetch data using the market wrapper
            data, done = market.read_bars(
                symbol=ticker.replace("-USDT-SWAP", "USDT"),  # Normalize to <TICKER>USDT
                timeframe=TIMEFRAME,
                start_time=start_ts,
                end_time=end_ts
            )

            if not done or data.empty:
                print(f"[WARN] {ticker}: no data fetched for {start_dt}–{end_dt}")
                with open(ERROR_LOG, 'a') as err_file:
                    err_file.write(f"{ticker}: no data fetched for {start_dt}–{end_dt}\n")
                continue

            # Sort the DataFrame by index (timestamps)
            data.sort_index(inplace=True)

            # Save the data to a CSV file
            filename = OUTPUT_DIR / f"{ticker}_price_data.csv"
            data.to_csv(filename)
            print(f"Saved data for {ticker} to {filename}")

            # Log successful processing
            with open(PROGRESS_LOG, 'a') as log_file:
                log_file.write(ticker + "\n")

        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            with open(ERROR_LOG, 'a') as err_file:
                err_file.write(f"{ticker}: {e}\n")
            continue

# --- MAIN FLOW ---
if __name__ == "__main__":
    # 1. Scan tickers and time windows
    windows = scan_tickers_and_windows()
    if not windows:
        print("No tickers/time windows found.")
        exit(1)

    print("Symbols and intervals to download:")
    for ticker, (s, e) in windows:
        print(f"  {ticker}: {s} – {e}")

    # 2. Fetch prices
    fetch_binance_prices(windows)