#!/usr/bin/env python3
"""
fetch_portfolio_prices.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Incrementally fetch 15-minute open prices for Binance futures tickers, limited to
expanded time-windows when positions existed (closed or open), writing per-ticker CSV files
immediately, and then aggregating into a consolidated matrix.
"""


# HOW TO HANDLE TINY TOKENS? ARE THEM ALREADY WITH THE PROPER NAME ON THE LOGS FILE?


import json
import os
import sys
from pathlib import Path
from typing import List, Tuple, Dict
from datetime import datetime, timedelta

import pandas as pd
from tqdm import tqdm
from pandas.errors import InvalidIndexError

# Add project root to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datafeed.utils_online import extract_coin
import datafeed.binancefeed as bf

# --- CONFIG ---
MODULE_DIR                = Path(__file__).resolve().parent
OUTPUT_DIR                = MODULE_DIR / "binance_data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
COVERAGE_FILE             = OUTPUT_DIR / "binance_price_coverage.json"
UNAVAILABLE_TICKERS_FILE  = OUTPUT_DIR / "binance_unavailable_tickers.json"
TIMEFRAME                 = "15m"
BAR_INTERVAL              = pd.Timedelta(minutes=15)
PROGRESS_LOG              = OUTPUT_DIR / "download_progress.log"
ERROR_LOG                 = OUTPUT_DIR / "download_errors.log"
AGG_CSV                   = OUTPUT_DIR / "binance_open_15m.csv"

# --- SYMBOL BUILDER ---
def map_to_binance_symbol_and_factor(ticker: str) -> Tuple[str, float]:
    coin = extract_coin(ticker)
    base = f"{coin}USDT"
    tiny = {"SHIB", "PEPE", "SATS", "LUNC", "XEC", "BONK", "FLOKI", "CAT", "RATS", "WHY", "X"}
    if coin in tiny:
        return f"1000{coin}USDT", 1e-3
    return base, 1.0

# --- SCAN POSITION WINDOWS ---
def scan_tickers_and_windows(config_file: str = "config_test.json") -> Dict[str, List[Tuple[datetime, datetime]]]:
    """
    Return a dict mapping each ticker to a list of merged windows when it had closed positions across all strategies,
    expanded by 30 minutes before and after each window.
    """
    try:
        cfg = json.loads(Path(config_file).read_text())
    except FileNotFoundError:
        print(f"Config {config_file} not found.")
        return {}

    output_dir = cfg.get("output_directory", "output_bin/")
    now = datetime.utcnow()
    strategy_windows: Dict[str, Dict[str, List[Tuple[datetime, datetime, str]]]] = {}

    # Step 1: Collect windows per ticker per strategy from closed positions
    for strat in cfg.get("strategy", {}):
        strategy_windows[strat] = {}
        closed_file = Path(f"{output_dir}closed_positions_{strat}.json")
        if closed_file.exists():
            closed_data = json.loads(closed_file.read_text())
            for timestamp, positions in closed_data.items():
                for pos in positions:
                    ticker = pos["asset"]
                    start = datetime.strptime(pos["entry_time"], "%Y/%m/%d %H:%M:%S.%f")
                    if "exit_time" in pos:
                        end = datetime.strptime(pos["exit_time"], "%Y/%m/%d %H:%M:%S.%f")
                        source = f"closed_positions_{strat} at {timestamp}"
                    else:
                        print(f"[WARN] Missing exit_time for {ticker} in closed_positions_{strat}.json at {timestamp}")
                        end = now
                        source = f"closed_positions_{strat} at {timestamp} (missing exit_time)"
                    strategy_windows[strat].setdefault(ticker, []).append((start, end, source))

    # Step 2: Merge windows across strategies for each ticker
    merged_windows: Dict[str, List[Tuple[datetime, datetime]]] = {}
    all_tickers = {t for strat_data in strategy_windows.values() for t in strat_data}

    for ticker in all_tickers:
        # Gather all windows
        ticker_windows = []
        for strat_data in strategy_windows.values():
            ticker_windows.extend(strat_data.get(ticker, []))

        if not ticker_windows:
            continue

        # Sort and merge
        ticker_windows.sort(key=lambda x: x[0])
        merged = []
        current_start, current_end = ticker_windows[0][:2]
        for start, end, _ in ticker_windows[1:]:
            if start <= current_end:
                current_end = max(current_end, end)
            else:
                merged.append((current_start, current_end))
                current_start, current_end = start, end
        merged.append((current_start, current_end))

        # Expand each window by ±30 minutes
        expanded = []
        for start, end in merged:
            exp_start = start - timedelta(minutes=30)
            exp_end   = end + timedelta(minutes=30)
            expanded.append((exp_start, exp_end))
        merged_windows[ticker] = expanded

    return merged_windows

# --- COVERAGE JSON ---
def load_coverage() -> Dict[str, List[List[str]]]:
    return json.loads(Path(COVERAGE_FILE).read_text()) if Path(COVERAGE_FILE).exists() else {}

def save_coverage(cov: Dict[str, List[List[str]]]) -> None:
    Path(COVERAGE_FILE).write_text(json.dumps(cov, indent=2, sort_keys=True))

# --- UNAVAILABLE TICKERS LOGGING ---
def load_unavailable_tickers() -> Dict[str, Dict[str, str]]:
    return json.loads(Path(UNAVAILABLE_TICKERS_FILE).read_text()) if Path(UNAVAILABLE_TICKERS_FILE).exists() else {}

def save_unavailable_ticker(ticker: str, symbol: str, reason: str) -> None:
    unavailable = load_unavailable_tickers()
    unavailable[ticker] = {"symbol": symbol, "reason": reason}
    Path(UNAVAILABLE_TICKERS_FILE).write_text(json.dumps(unavailable, indent=2, sort_keys=True))

# --- FETCH PER TICKER ---
def fetch_single_ticker_open(
    ticker: str,
    windows: List[Tuple[datetime, datetime]]
) -> None:
    """
    Fetch and append per-ticker CSV for each window separately, ensuring only data within the specified windows is saved.
    Handle cases where the symbol is not available on Binance (e.g., delisted tokens) and skip to the next ticker on any read_bars exception.
    Fetch only the uncovered portion of each window to avoid duplicate timestamps.
    For tiny tokens (with factor 1e-3), prepend '1000' to the ticker name in the CSV column.
    """
    market = bf.BinanceFutureMarket()
    cov = load_coverage()
    existing_coverage = []
    if ticker in cov:
        existing_coverage = [(datetime.fromisoformat(s), datetime.fromisoformat(e)) for s, e in cov[ticker]]

    updated = existing_coverage.copy()
    outpath = OUTPUT_DIR / f"{ticker}_open.csv"

    # Determine the full range of requested windows
    if not windows:
        print(f"No windows to process for {ticker}")
        return
    window_starts = [start for start, end in windows]
    window_ends = [end for start, end in windows]
    earliest_start = min(window_starts)
    latest_end = max(window_ends)

    # Load existing data and filter to requested windows
    if outpath.exists():
        df_existing = pd.read_csv(outpath, parse_dates=['timestamp']).set_index('timestamp')
        df_existing.index = pd.to_datetime(df_existing.index, utc=True).tz_localize(None)
        # Drop duplicates in the existing data to prevent issues later
        df_existing = df_existing[~df_existing.index.duplicated(keep='first')]
        # Filter existing data to only include timestamps within the requested windows
        mask = pd.Series(False, index=df_existing.index)
        for start, end in windows:
            window_mask = (df_existing.index >= start) & (df_existing.index <= end)
            mask |= window_mask
        df_existing = df_existing[mask]
        if df_existing.empty:
            df_existing = pd.DataFrame()
    else:
        df_existing = pd.DataFrame()

    parts = []
    sym, factor = map_to_binance_symbol_and_factor(ticker)
    try:
        for start, end in windows:
            # Check if the window is already fully covered
            covered = False
            for cov_start, cov_end in existing_coverage:
                if start >= cov_start and end <= cov_end:
                    covered = True
                    break
            if covered:
                print(f"[SKIP] {ticker} window {start}–{end}: already covered")
                continue

            # Determine the uncovered portion of the window
            fetch_start = start
            fetch_end = end
            for cov_start, cov_end in existing_coverage:
                # If the window starts within a covered range, adjust the start time
                if start >= cov_start and start <= cov_end:
                    fetch_start = cov_end
                # If the window ends within a covered range, adjust the end time
                if end >= cov_start and end <= cov_end:
                    fetch_end = cov_start
                # If the window is partially covered, adjust both start and end
                if start < cov_start and end > cov_end:
                    # Fetch the portion after the covered range
                    fetch_start = cov_end

            # If the adjusted window is invalid (start >= end), skip it
            if fetch_start >= fetch_end:
                print(f"[SKIP] {ticker} window {start}–{end}: adjusted window {fetch_start}–{fetch_end} is invalid")
                continue

            print(f"Fetching {ticker} for adjusted window {fetch_start}–{fetch_end}")
            bars, _ = market.read_bars(
                symbol=sym,
                timeframe=TIMEFRAME,
                start_time=int(fetch_start.timestamp()),
                end_time=int(fetch_end.timestamp())
            )
            if bars is None or bars.empty:
                print(f"[WARN] {ticker} no data {fetch_start}–{fetch_end}")
                with open(ERROR_LOG, 'a') as ef:
                    ef.write(f"{ticker}: no data for {fetch_start}–{fetch_end}\n")
                continue
            idx = pd.to_datetime(bars.index, unit='s', utc=True).tz_localize(None)
            series = pd.Series(bars['open'].values, index=idx, name=ticker)
            # Drop duplicates in the fetched data
            series = series[~series.index.duplicated(keep='first')]
            parts.append(series)
            # Update coverage for the fetched portion
            updated.append((fetch_start, fetch_end))
    except Exception as e:
        error_msg = str(e).lower()
        if "does not have market symbol" in error_msg or "invalid symbol" in error_msg:
            print(f"[UNAVAILABLE] {ticker} (symbol: {sym}) is not available on Binance, likely delisted. Skipping.")
            save_unavailable_ticker(ticker, sym, "Symbol not available on Binance (likely delisted)")
            with open(ERROR_LOG, 'a') as ef:
                ef.write(f"{ticker}: Symbol {sym} not available on Binance\n")
        else:
            print(f"[ERROR] {ticker} failed to fetch data: {e}. Skipping to next ticker.")
            with open(ERROR_LOG, 'a') as ef:
                ef.write(f"{ticker}: Failed to fetch data: {e}\n")
        return  # Skip to the next ticker

    if not parts:
        print(f"No new data for {ticker}")
        return

    # Determine the display ticker name for the CSV column
    display_ticker = f"1000{ticker}" if factor == 1e-3 else ticker

    # Combine new data with existing
    new_data = pd.DataFrame(pd.concat(parts, axis=0)).sort_index()
    # Set the column name to the display ticker
    new_data.columns = [display_ticker]
    # Ensure no duplicates in new_data
    new_data = new_data[~new_data.index.duplicated(keep='first')]
    # Debug: Log the structure of new_data
    print(f"[DEBUG] {ticker}: new_data shape: {new_data.shape}, columns: {new_data.columns.tolist()}")
    if new_data.empty or display_ticker not in new_data.columns:
        print(f"[WARN] {ticker}: new_data is empty or missing expected column after fetch")
        return

    if not df_existing.empty:
        try:
            # If df_existing exists, ensure its column name matches display_ticker
            if ticker in df_existing.columns:
                df_existing = df_existing.rename(columns={ticker: display_ticker})
            # Prevent duplicate columns by using join='outer' and selecting the first occurrence
            combined = pd.concat([df_existing, new_data], axis=1, join='outer')
            # Drop duplicate columns, keeping the first
            combined = combined.loc[:, ~combined.columns.duplicated()]
            # Debug: Log the structure of combined
            print(f"[DEBUG] {ticker}: combined shape: {combined.shape}, columns: {combined.columns.tolist()}")
            # Ensure combined[display_ticker] is a Series
            if display_ticker in combined.columns:
                if pd.api.types.is_scalar(combined[display_ticker]):
                    combined[display_ticker] = pd.Series([combined[display_ticker]], index=combined.index[:1])
                combined[display_ticker] = combined[display_ticker].combine_first(new_data[display_ticker])
            else:
                combined[display_ticker] = new_data[display_ticker]
        except InvalidIndexError:
            print(f"[WARNING] Duplicate indices detected for {ticker}. Removing duplicates and retrying.")
            df_existing = df_existing[~df_existing.index.duplicated(keep='first')]
            new_data = new_data[~new_data.index.duplicated(keep='first')]
            combined = pd.concat([df_existing, new_data], axis=1, join='outer')
            combined = combined.loc[:, ~combined.columns.duplicated()]
            print(f"[DEBUG] {ticker}: combined shape after duplicate removal: {combined.shape}, columns: {combined.columns.tolist()}")
            if display_ticker in combined.columns:
                if pd.api.types.is_scalar(combined[display_ticker]):
                    combined[display_ticker] = pd.Series([combined[display_ticker]], index=combined.index[:1])
                combined[display_ticker] = combined[display_ticker].combine_first(new_data[display_ticker])
            else:
                combined[display_ticker] = new_data[display_ticker]
    else:
        combined = new_data.copy()

    # Filter combined data to only include timestamps within the requested windows
    mask = pd.Series(False, index=combined.index)
    for start, end in windows:
        window_mask = (combined.index >= start) & (combined.index <= end)
        mask |= window_mask
    combined = combined[mask]

    if combined.empty:
        print(f"No data within the requested windows for {ticker}")
        return

    # Write updated CSV
    combined.to_csv(outpath, index_label='timestamp')
    print(f"Saved/Updated {ticker} → {outpath}")

    # Update coverage
    # Merge overlapping coverage windows
    updated.sort(key=lambda x: x[0])
    merged = []
    if updated:
        current_start, current_end = updated[0]
        for start, end in updated[1:]:
            if start <= current_end:
                current_end = max(current_end, end)
            else:
                merged.append((current_start, current_end))
                current_start, current_end = start, end
        merged.append((current_start, current_end))

    cov[ticker] = [[s.isoformat(), e.isoformat()] for s, e in merged]
    save_coverage(cov)

# --- AGGREGATION ---
def aggregate_all() -> pd.DataFrame:
    files = list(OUTPUT_DIR.glob("*_open.csv"))
    dfs = []
    for f in files:
        df = pd.read_csv(f, parse_dates=['timestamp']).set_index('timestamp')
        dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    agg = pd.concat(dfs, axis=1, join='outer')
    agg.to_csv(AGG_CSV, index_label='timestamp')
    print(f"Aggregated matrix → {AGG_CSV}")
    return agg

# --- MAIN ---
if __name__ == "__main__":
    windows_map = scan_tickers_and_windows()
    print("Tickers to fetch with their time windows:")
    for ticker, windows in windows_map.items():
        print(f"{ticker}:")
        for start, end in windows:
            print(f"  {start} → {end}")
    for ticker, windows in windows_map.items():
        print(f"Fetching windows for {ticker}:")
        for s, e in windows:
            print(f"  {s} → {e}")
        fetch_single_ticker_open(ticker, windows)

    # Finally aggregate
    df_all = aggregate_all()
    print(df_all.head())