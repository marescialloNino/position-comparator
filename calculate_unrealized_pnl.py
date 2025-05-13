#!/usr/bin/env python3
"""
calculate_unrealized_pnl_for_strategies.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Calculates the unrealized PnL for all positions across all strategies using the
aggregated price matrix from fetch_portfolio_prices.py.
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
from typing import List, Tuple, Optional, Dict

# Add project root to path for imports (if needed for utils)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datafeed.utils_online import extract_coin

# --- CONFIG ---
MODULE_DIR    = Path(__file__).resolve().parent
DATA_DIR      = MODULE_DIR / "binance_data"
PRICE_MATRIX  = DATA_DIR / "binance_open_15m.csv"
OUTPUT_DIR    = MODULE_DIR / "output_bin"  # Match your main.py output directory
OUTPUT_CSV    = DATA_DIR / "unrealized_pnl_all_strategies.csv"

# --- SYMBOL BUILDER ---
def map_to_display_ticker(ticker: str) -> str:
    """
    Maps the ticker to the display format used in the price matrix.
    For tiny tokens, prepends '1000' to the ticker name.
    """
    coin = extract_coin(ticker)
    tiny = {"SHIB", "PEPE", "SATS", "LUNC", "XEC", "BONK", "FLOKI", "CAT", "RATS", "WHY", "X"}
    if coin in tiny:
        return f"1000{ticker}"
    return ticker

# --- LOAD PRICE MATRIX ---
def load_price_matrix(price_file: str = PRICE_MATRIX) -> pd.DataFrame:
    """Load the aggregated price matrix from CSV."""
    if not Path(price_file).exists():
        print(f"Price matrix {price_file} not found.")
        return pd.DataFrame()
    
    df = pd.read_csv(price_file, parse_dates=['timestamp']).set_index('timestamp')
    df.index = pd.to_datetime(df.index, utc=True).tz_localize(None)
    return df

# --- LOAD PORTFOLIO DATA ---
def load_strategy_portfolios(output_dir: str = OUTPUT_DIR) -> Dict[str, Dict]:
    """
    Load portfolio data for all strategies from JSON files in the output directory.
    Assumes files are named like 'closed_positions_<strategy>.json'.
    """
    portfolio_data = {}
    for file_path in Path(output_dir).glob("closed_positions_*.json"):
        strategy_name = file_path.stem.replace("closed_positions_", "")
        try:
            with open(file_path, 'r') as f:
                portfolio_data[strategy_name] = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in {file_path}: {e}")
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
    return portfolio_data

# --- CALCULATE UNREALIZED PNL ---
def calculate_unrealized_pnl(portfolio_data: Dict, price_matrix: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate unrealized PnL for each position across all strategies.
    Returns a DataFrame with columns: strategy, timestamp, asset, qty, entry_price,
    current_price, unrealized_pnl, total_pnl.
    """
    results = []
    
    # Iterate over each strategy
    for strategy, strategy_portfolio in portfolio_data.items():
        # Iterate over each portfolio entry timestamp
        for timestamp_str, entry in strategy_portfolio.items():
            portfolio_timestamp = datetime.strptime(timestamp_str, "%Y/%m/%d %H:%M:%S.%f")
            portfolio_timestamp = portfolio_timestamp.replace(tzinfo=None)
            
            total_pnl = 0.0
            
            # Process each position in the portfolio
            portfolio = entry.get("portfolio", [])
            for position in portfolio:
                asset = position["asset"]
                qty = position["qty"]
                entry_price = position["entry_price"]
                entry_time = datetime.strptime(position["entry_time"], "%Y/%m/%d %H:%M:%S.%f")
                entry_time = entry_time.replace(tzinfo=None)
                
                # Skip if the position was opened after this portfolio timestamp
                if entry_time > portfolio_timestamp:
                    continue
                
                # Map the asset to the display ticker (for tiny tokens)
                display_ticker = map_to_display_ticker(asset)
                
                # Check if the asset exists in the price matrix
                if display_ticker not in price_matrix.columns:
                    print(f"[WARN] Asset {display_ticker} not found in price matrix at {timestamp_str} for strategy {strategy}. Skipping.")
                    continue
                
                # Find the closest timestamp in the price matrix
                price_timestamps = price_matrix.index
                time_diffs = abs(price_timestamps - portfolio_timestamp)
                closest_idx = time_diffs.argmin()
                closest_timestamp = price_timestamps[closest_idx]
                
                # Ensure the closest timestamp is not in the future
                if closest_timestamp > portfolio_timestamp:
                    past_timestamps = price_timestamps[price_timestamps <= portfolio_timestamp]
                    if len(past_timestamps) == 0:
                        print(f"[WARN] No price data available before {timestamp_str} for {display_ticker} in strategy {strategy}. Skipping.")
                        continue
                    closest_timestamp = past_timestamps[-1]
                
                # Get the current price at the closest timestamp
                current_price = price_matrix.at[closest_timestamp, display_ticker]
                
                if pd.isna(current_price):
                    print(f"[WARN] No valid price data at {closest_timestamp} for {display_ticker} in strategy {strategy}. Skipping.")
                    continue
                
                # Calculate unrealized PnL
                unrealized_pnl = (current_price - entry_price) * qty
                total_pnl += unrealized_pnl
                
                # Record the result
                results.append({
                    "strategy": strategy,
                    "timestamp": portfolio_timestamp,
                    "asset": asset,
                    "qty": qty,
                    "entry_price": entry_price,
                    "current_price": current_price,
                    "unrealized_pnl": unrealized_pnl,
                    "total_pnl": total_pnl
                })
    
    # Convert results to DataFrame
    if not results:
        return pd.DataFrame(columns=["strategy", "timestamp", "asset", "qty", "entry_price", "current_price", "unrealized_pnl", "total_pnl"])
    
    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values(by=["strategy", "timestamp"])
    return result_df

# --- MAIN ---
if __name__ == "__main__":
    # Load the price matrix
    price_matrix = load_price_matrix()
    if price_matrix.empty:
        print("No price matrix data to process. Exiting.")
        sys.exit(1)
    
    # Load portfolio data for all strategies
    portfolio_data = load_strategy_portfolios()
    if not portfolio_data:
        print("No portfolio data found for any strategy. Exiting.")
        sys.exit(1)
    
    # Calculate unrealized PnL
    print("Calculating unrealized PnL for all strategies...")
    unrealized_pnl_df = calculate_unrealized_pnl(portfolio_data, price_matrix)
    
    if unrealized_pnl_df.empty:
        print("No unrealized PnL data calculated. Check warnings for details.")
    else:
        # Save to CSV
        unrealized_pnl_df.to_csv(OUTPUT_CSV, index=False)
        print(f"Unrealized PnL results saved to {OUTPUT_CSV}")
        
        # Print a summary
        print("\nSample of unrealized PnL results:")
        print(unrealized_pnl_df.head())