import json
import os
import sys
import re
from pathlib import Path
from typing import List, Tuple, Dict
from datetime import datetime, timedelta
import pandas as pd
from pandas.errors import InvalidIndexError

# Add project root to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import datafeed.binancefeed as bf
import datafeed.bitgetfeed as bitget_df
import datafeed.okexfeed as okex_df

# --- CONFIG ---
MODULE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = MODULE_DIR / "price_data"
BOT_DATA_DIR = MODULE_DIR / "bot_data"
TIMEFRAME = "15m"
BAR_INTERVAL = pd.Timedelta(minutes=15)
CONFIG_FILE = MODULE_DIR / "config_pair_session_bitget.json"

# --- UTILITIES ---
def load_config() -> Dict[str, any]:
    """Load configuration file."""
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Config file not found at {CONFIG_FILE}")
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)

def get_exchange_paths(exchange: str) -> Dict[str, Path]:
    """Return exchange-specific file paths."""
    exchange_dir = OUTPUT_DIR / exchange.lower()
    exchange_dir.mkdir(parents=True, exist_ok=True)
    return {
        "exchange_dir": exchange_dir,
        "coverage": exchange_dir / "theoretical_price_coverage.json",
        "unavailable_tickers": exchange_dir / "theoretical_unavailable_tickers.json",
        "price_windows": exchange_dir / "theoretical_price_windows.json",
        "progress_log": exchange_dir / "theoretical_download_progress.log",
        "error_log": exchange_dir / "theoretical_download_errors.log",
        "agg_csv": exchange_dir / "theoretical_open_15m.csv"
    }

# --- PARSE CURRENT_STATE.LOG ---
def parse_current_state_log(strategy: str) -> Dict[str, List[Tuple[datetime, datetime]]]:
    """
    Parse current_state.log to extract time windows for each ticker.
    File format: timestamp:'asset1':1, 'asset2':-1, ...
    Returns: {ticker: [(start_time, end_time), ...], ...}
    """
    file_path = BOT_DATA_DIR / strategy / "current_state.log"
    if not file_path.exists():
        print(f"Warning: current_state.log not found for strategy '{strategy}' at {file_path}")
        return {}
    
    ticker_windows = {}
    position_states = {}  # {ticker: {'start_time': datetime, 'direction': int}}
    timestamp_formats = [
        "%Y/%m/%d %H:%M:%S.%f",
        "%Y/%m/%d %H:%M:%S"  # Fallback without microseconds
    ]
    
    # Regex to match timestamp at the start of the line
    timestamp_pattern = re.compile(r'^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)')
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for i, line in enumerate(lines):
                line = line.strip()
                if not line:
                    continue
                try:
                    # Extract timestamp using regex
                    timestamp_match = timestamp_pattern.match(line)
                    if not timestamp_match:
                        raise ValueError(f"Invalid timestamp format in line {i+1}")
                    timestamp_str = timestamp_match.group(1)
                    pos_str = line[len(timestamp_match.group(0))+1:].strip()
                    
                    # Parse timestamp
                    timestamp = None
                    for fmt in timestamp_formats:
                        try:
                            timestamp = datetime.strptime(timestamp_str, fmt)
                            break
                        except ValueError:
                            continue
                    if timestamp is None:
                        raise ValueError(f"Cannot parse timestamp: {timestamp_str}")
                    
                    # Parse position string
                    current_positions = {}
                    if not pos_str:
                        continue  # Empty position list
                    items = pos_str.split(',')
                    for item in items:
                        item = item.strip()
                        if not item:
                            continue
                        # Split on last colon to handle potential colons in asset names
                        asset_qty = item.rsplit(':', 1)
                        if len(asset_qty) != 2:
                            print(f"Warning: Invalid asset-quantity pair in line {i+1}: {item}")
                            continue
                        asset, qty = asset_qty
                        asset = asset.strip("'").strip('"')
                        try:
                            direction = int(float(qty.strip()))  # Expect 1 or -1
                            if direction not in [1, -1]:
                                print(f"Warning: Invalid direction {direction} for {asset} in line {i+1}")
                                continue
                            current_positions[asset] = direction
                        except ValueError:
                            print(f"Warning: Invalid quantity in line {i+1}: {qty}")
                            continue
                    
                    # Update position states
                    for ticker, direction in current_positions.items():
                        if ticker not in position_states or position_states[ticker]['direction'] != direction:
                            position_states[ticker] = {'start_time': timestamp, 'direction': direction}
                    
                    # Check for closed positions
                    for ticker in list(position_states.keys()):
                        if ticker not in current_positions:
                            start_time = position_states[ticker]['start_time']
                            end_time = timestamp
                            ticker_windows.setdefault(ticker, []).append((start_time, end_time))
                            del position_states[ticker]
                
                except Exception as e:
                    print(f"Warning: Failed to parse line in {file_path}, line {i+1}: {line}. Error: {e}")
                    continue
            
            # Handle open positions at file end
            if lines and position_states:
                last_line = lines[-1].strip()
                if last_line:
                    try:
                        timestamp_match = timestamp_pattern.match(last_line)
                        if timestamp_match:
                            last_timestamp_str = timestamp_match.group(1)
                            last_timestamp = None
                            for fmt in timestamp_formats:
                                try:
                                    last_timestamp = datetime.strptime(last_timestamp_str, fmt)
                                    break
                                except ValueError:
                                    continue
                            if last_timestamp:
                                for ticker, state in position_states.items():
                                    ticker_windows.setdefault(ticker, []).append((state['start_time'], last_timestamp))
                    except Exception as e:
                        print(f"Warning: Failed to parse last line for open positions: {last_line}. Error: {e}")
    
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return {}
    
    return ticker_windows

# --- SCAN TICKER WINDOWS ---
def scan_theoretical_tickers_and_windows() -> Dict[str, List[Tuple[datetime, datetime]]]:
    """
    Scan current_state.log files for all active strategies and return merged ticker windows,
    expanded by ±30 minutes, for the exchange specified in the config.
    """
    cfg = load_config()
    exchange = cfg.get("exchange", "bitget").lower()
    strategy_windows = {}
    
    # Collect windows per strategy
    for strat_name, strat_config in cfg.get("strategy", {}).items():
        if not strat_config.get("active", False):
            continue
        windows = parse_current_state_log(strat_name)
        if windows:
            strategy_windows[strat_name] = windows
    
    # Merge windows per ticker per strategy
    for strat_name in strategy_windows:
        for ticker in strategy_windows[strat_name]:
            windows = strategy_windows[strat_name][ticker]
            windows.sort(key=lambda x: x[0])
            merged = []
            if windows:
                current_start, current_end = windows[0]
                for start, end in windows[1:]:
                    if start <= current_end + timedelta(minutes=1):
                        current_end = max(current_end, end)
                    else:
                        merged.append((current_start, current_end))
                        current_start, current_end = start, end
                merged.append((current_start, current_end))
                strategy_windows[strat_name][ticker] = merged
    
    # Merge windows across strategies
    merged_windows: Dict[str, List[Tuple[datetime, datetime]]] = {}
    all_tickers = {t for strat_data in strategy_windows.values() for t in strat_data}
    
    for ticker in all_tickers:
        ticker_windows = []
        for strat_data in strategy_windows.values():
            ticker_windows.extend(strat_data.get(ticker, []))
        
        if not ticker_windows:
            continue
        
        ticker_windows.sort(key=lambda x: x[0])
        merged = []
        current_start, current_end = ticker_windows[0]
        for start, end in ticker_windows[1:]:
            if start <= current_end + timedelta(minutes=1):
                current_end = max(current_end, end)
            else:
                merged.append((current_start, current_end))
                current_start, current_end = start, end
        merged.append((current_start, current_end))
        
        # Expand by ±30 minutes
        expanded = [(start - timedelta(minutes=30), end + timedelta(minutes=30)) for start, end in merged]
        merged_windows[ticker] = expanded
    
    # Save price windows
    paths = get_exchange_paths(exchange)
    price_windows_data = {
        ticker: [[start.isoformat(), end.isoformat()] for start, end in windows]
        for ticker, windows in merged_windows.items()
    }
    with open(paths["price_windows"], 'w') as f:
        json.dump(price_windows_data, f, indent=2)
    print(f"Saved price windows to {paths['price_windows']} for {exchange}")
    
    return merged_windows

# --- COVERAGE JSON ---
def load_coverage(exchange: str) -> Dict[str, List[List[str]]]:
    coverage_file = get_exchange_paths(exchange)["coverage"]
    return json.loads(coverage_file.read_text()) if coverage_file.exists() else {}

def save_coverage(cov: Dict[str, List[List[str]]], exchange: str) -> None:
    coverage_file = get_exchange_paths(exchange)["coverage"]
    coverage_file.write_text(json.dumps(cov, indent=2, sort_keys=True))

# --- UNAVAILABLE TICKERS LOGGING ---
def load_unavailable_tickers(exchange: str) -> Dict[str, Dict[str, str]]:
    unavailable_file = get_exchange_paths(exchange)["unavailable_tickers"]
    return json.loads(unavailable_file.read_text()) if unavailable_file.exists() else {}

def save_unavailable_ticker(ticker: str, reason: str, exchange: str) -> None:
    unavailable = load_unavailable_tickers(exchange)
    unavailable[ticker] = {"symbol": ticker, "reason": reason}
    unavailable_file = get_exchange_paths(exchange)["unavailable_tickers"]
    unavailable_file.write_text(json.dumps(unavailable, indent=2, sort_keys=True))

# --- FETCH PER TICKER ---
def fetch_single_ticker(ticker: str, windows: List[Tuple[datetime, datetime]], exchange: str = "bitget") -> None:
    """
    Fetch 15-minute open prices for a ticker within the specified windows from the given exchange.
    """
    if exchange.lower() == "binance":
        market = bf.BinanceFutureMarket()
    elif exchange.lower() == "bitget":
        market = bitget_df.BitgetMarket()
    elif exchange.lower() == "okex":
        market = okex_df.OkexMarket()
    else:
        raise ValueError(f"Unsupported exchange: {exchange}")
    
    paths = get_exchange_paths(exchange)
    cov = load_coverage(exchange)
    existing_coverage = [(datetime.fromisoformat(s), datetime.fromisoformat(e)) for s, e in cov.get(ticker, [])]
    updated = existing_coverage.copy()
    outpath = paths["exchange_dir"] / f"{ticker}_open.csv"
    
    if not windows:
        print(f"No windows to process for {ticker} on {exchange}")
        return
    
    # Load existing data
    if outpath.exists():
        df_existing = pd.read_csv(outpath, parse_dates=['timestamp']).set_index('timestamp')
        df_existing.index = pd.to_datetime(df_existing.index, utc=True).tz_localize(None)
        df_existing = df_existing[~df_existing.index.duplicated(keep='first')]
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
    try:
        for start, end in windows:
            if any(cov_start <= start and cov_end >= end for cov_start, cov_end in existing_coverage):
                print(f"[SKIP] {ticker} window {start}–{end} on {exchange}: already covered")
                continue
            
            fetch_start = start
            fetch_end = end
            for cov_start, cov_end in existing_coverage:
                if start >= cov_start and start <= cov_end:
                    fetch_start = cov_end
                if end >= cov_start and end <= cov_end:
                    fetch_end = cov_start
                if start < cov_start and end > cov_end:
                    fetch_start = cov_end
            
            if fetch_start >= fetch_end:
                print(f"[SKIP] {ticker} window {start}–{end} on {exchange}: adjusted window {fetch_start}–{fetch_end} is invalid")
                continue
            
            print(f"Fetching {ticker} for adjusted window {fetch_start}–{fetch_end} on {exchange}")
            bars, _ = market.read_bars(
                symbol=ticker,
                timeframe=TIMEFRAME,
                start_time=int(fetch_start.timestamp()),
                end_time=int(fetch_end.timestamp())
            )
            if bars is None or bars.empty:
                print(f"[WARN] {ticker} no data {fetch_start}–{fetch_end} on {exchange}")
                with open(paths["error_log"], 'a') as ef:
                    ef.write(f"{ticker}: no data for {fetch_start}–{fetch_end} on {exchange}\n")
                continue
            idx = pd.to_datetime(bars.index, unit='s', utc=True).tz_localize(None)
            series = pd.Series(bars['open'].values, index=idx, name=ticker)
            series = series[~series.index.duplicated(keep='first')]
            parts.append(series)
            updated.append((fetch_start, fetch_end))
    except Exception as e:
        error_msg = str(e).lower()
        if "does not have market symbol" in error_msg or "invalid symbol" in error_msg:
            print(f"[UNAVAILABLE] {ticker} is not available on {exchange}, likely delisted. Skipping.")
            save_unavailable_ticker(ticker, f"Symbol not available on {exchange} (likely delisted)", exchange)
            with open(paths["error_log"], 'a') as ef:
                ef.write(f"{ticker}: Symbol {ticker} not available on {exchange}\n")
        else:
            print(f"[ERROR] {ticker} failed to fetch data on {exchange}: {e}. Skipping to next ticker.")
            with open(paths["error_log"], 'a') as ef:
                ef.write(f"{ticker}: Failed to fetch data on {exchange}: {e}\n")
        return
    
    if not parts:
        print(f"No new data for {ticker} on {exchange}")
        return
    
    new_data = pd.concat(parts, axis=0).to_frame().sort_index()
    new_data.columns = [ticker]
    new_data = new_data[~new_data.index.duplicated(keep='first')]
    
    if not df_existing.empty:
        try:
            combined = pd.concat([df_existing, new_data], axis=1, join='outer')
            combined = combined.loc[:, ~combined.columns.duplicated()]
            if ticker in combined.columns:
                combined[ticker] = combined[ticker].combine_first(new_data[ticker])
            else:
                combined[ticker] = new_data[ticker]
        except InvalidIndexError:
            print(f"[WARNING] Duplicate indices detected for {ticker} on {exchange}. Removing duplicates.")
            df_existing = df_existing[~df_existing.index.duplicated(keep='first')]
            new_data = new_data[~new_data.index.duplicated(keep='first')]
            combined = pd.concat([df_existing, new_data], axis=1, join='outer')
            combined = combined.loc[:, ~combined.columns.duplicated()]
            combined[ticker] = combined[ticker].combine_first(new_data[ticker])
    else:
        combined = new_data.copy()
    
    mask = pd.Series(False, index=combined.index)
    for start, end in windows:
        window_mask = (combined.index >= start) & (combined.index <= end)
        mask |= window_mask
    combined = combined[mask]
    
    if combined.empty:
        print(f"No data within the requested windows for {ticker} on {exchange}")
        return
    
    combined.to_csv(outpath, index_label='timestamp')
    print(f"Saved/Updated {ticker} → {outpath} for {exchange}")
    
    # Update coverage
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
    save_coverage(cov, exchange)

# --- AGGREGATION ---
def aggregate_all(exchange: str) -> pd.DataFrame:
    """Aggregate per-ticker CSVs for the specified exchange into a single price matrix."""
    exchange_dir = OUTPUT_DIR / exchange.lower()
    files = list(exchange_dir.glob("*_open.csv"))
    dfs = []
    for f in files:
        df = pd.read_csv(f, parse_dates=['timestamp']).set_index('timestamp')
        df.index = pd.to_datetime(df.index, utc=True).tz_localize(None)
        dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    agg = pd.concat(dfs, axis=1, join='outer')
    agg = agg.loc[:, ~agg.columns.duplicated()]
    agg_csv = get_exchange_paths(exchange)["agg_csv"]
    agg.to_csv(agg_csv, index_label='timestamp')
    print(f"Aggregated matrix for {exchange} → {agg_csv}")
    return agg

# --- MAIN ---
if __name__ == "__main__":
    cfg = load_config()
    exchange = cfg.get("exchange", "bitget").lower()
    windows_map = scan_theoretical_tickers_and_windows()
    print("Tickers to fetch with their time windows:")
    for ticker, windows in windows_map.items():
        print(f"{ticker}:")
        for start, end in windows:
            print(f"  {start} → {end}")
    print(f"\nProcessing exchange: {exchange}")
    for ticker, windows in windows_map.items():
        print(f"Fetching windows for {ticker} on {exchange}:")
        for s, e in windows:
            print(f"  {s} → {e}")
        fetch_single_ticker(ticker, windows, exchange)
    df_all = aggregate_all(exchange)
    print(f"Head of aggregated data for {exchange}:\n{df_all.head()}")