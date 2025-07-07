import json
import os
import sys
import re
from pathlib import Path
from typing import List, Tuple, Dict
from datetime import datetime, timedelta
import pandas as pd
from pandas.errors import InvalidIndexError
import logging
import yaml

from paths import ROOT_DIR, PNL_DATA_DIR, PRICES_DIR, SESSION_CONFIG_FILE, LOG_DIR
from reporting.bot_reporting import TGMessenger
import datafeed.binancefeed as bf
import datafeed.bitgetfeed as bitget_df
import datafeed.okexfeed as okex_df

# --- CONFIG ---
TIMEFRAME = "15m"
BAR_INTERVAL = pd.Timedelta(minutes=15)
FETCH_EXTENSION = timedelta(hours=48)  # Extend fetch windows
DATA_DIR = ROOT_DIR / "data" / "ms"
DATA_RETENTION_DAYS = 180
CUTOFF_DATE = datetime.now() - timedelta(days=DATA_RETENTION_DAYS)

# Configure logging
LOG_DIR = LOG_DIR / "pnl_data_fetcher"
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'pnl_data_fetcher.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# --- CONFIG LOADER ---
def load_core_ms_config() -> Dict:
    """Load configuration from web_processor.yml and session-specific JSON files."""
    if not SESSION_CONFIG_FILE.exists():
        logger.error(f"Session config file not found at {SESSION_CONFIG_FILE}")
        try:
            TGMessenger.send_message(f"PNL Data Fetcher: Session config file not found at {SESSION_CONFIG_FILE}", "AwsMonitor")
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
        raise FileNotFoundError(f"Session config file not found at {SESSION_CONFIG_FILE}")
    
    logger.info(f"Loading session config from {SESSION_CONFIG_FILE}")
    with open(SESSION_CONFIG_FILE, 'r') as f:
        yaml_config = yaml.safe_load(f)
    
    session_config = yaml_config.get('session', {})
    session_configs = {}
    session_matching_configs = {}
    
    # Base directory for YAML paths is ROOT_DIR/microservice/config
    config_dir = ROOT_DIR / "microservice" / "config"
    
    for session, config in session_config.items():
        # Resolve config_file relative to config_dir
        config_file = config_dir / Path(config.get('config_file', '')).name
        if config_file.exists():
            with open(config_file, 'r') as f:
                session_configs[session] = json.load(f)
            logger.info(f"Loaded config for session {session} from {config_file}")
        else:
            logger.warning(f"No config file for session {session} at {config_file}")
            try:
                TGMessenger.send_message(f"PNL Data Fetcher: No config file for {session} at {config_file}", "AwsMonitor")
            except Exception as e:
                logger.error(f"Failed to send Telegram message: {e}")
        
        # Resolve config_position_matching_file relative to config_dir
        matching_file = config_dir / Path(config.get('config_position_matching_file', '')).name
        if matching_file.exists():
            with open(matching_file, 'r') as f:
                session_matching_configs[session] = json.load(f)
            logger.info(f"Loaded matching config for session {session} from {matching_file}")
        else:
            logger.warning(f"No matching config file for session {session} at {matching_file}")
            try:
                TGMessenger.send_message(f"PNL Data Fetcher: No matching config file for {session} at {matching_file}", "AwsMonitor")
            except Exception as e:
                logger.error(f"Failed to send Telegram message: {e}")
    
    return {
        'session': session_config,
        'session_configs': session_configs,
        'session_matching_configs': session_matching_configs
    }

def get_exchange_paths(exchange: str) -> Dict[str, Path]:
    """Return exchange-specific file paths under PRICES_DIR."""
    exchange_dir = PRICES_DIR / exchange.lower()
    single_token_dir = exchange_dir / "single_token_price"
    single_token_dir.mkdir(parents=True, exist_ok=True)
    exchange_dir.mkdir(parents=True, exist_ok=True)
    return {
        "exchange_dir": exchange_dir,
        "single_token_dir": single_token_dir,
        "coverage": exchange_dir / "theoretical_price_coverage.json",
        "unavailable_tickers": exchange_dir / "theoretical_unavailable_tickers.json",
        "price_windows": exchange_dir / "theoretical_price_windows.json",
        "progress_log": exchange_dir / "theoretical_download_progress.log",
        "error_log": exchange_dir / "theoretical_download_errors.log",
        "agg_csv": exchange_dir / "theoretical_open_15m.csv"
    }

def clean_old_data(exchange: str) -> None:
    """Remove price data older than 180 days and update coverage."""
    paths = get_exchange_paths(exchange)
    single_token_dir = paths["single_token_dir"]
    cutoff = CUTOFF_DATE
    
    # Load coverage
    cov = load_coverage(exchange)
    updated_cov = {}
    
    # Process each single-token CSV
    for file in single_token_dir.glob("*_open.csv"):
        ticker = file.stem.replace("_open", "")
        df = pd.read_csv(file, parse_dates=['timestamp']).set_index('timestamp')
        df.index = pd.to_datetime(df.index, utc=True).tz_localize(None)
        
        # Filter out data older than cutoff
        df_new = df[df.index >= cutoff]
        if df_new.empty:
            logger.info(f"Removing {file} as all data is older than {cutoff}")
            file.unlink()
            continue
        
        # Save filtered data
        df_new.to_csv(file, index_label='timestamp')
        logger.info(f"Updated {file} to keep data from {df_new.index.min()} to {df_new.index.max()}")
        
        # Update coverage for this ticker
        if ticker in cov:
            updated_windows = [
                [start, end] for start, end in cov[ticker]
                if pd.Timestamp(end) >= cutoff
            ]
            if updated_windows:
                updated_cov[ticker] = updated_windows
    
    # Save updated coverage
    if updated_cov != cov:
        save_coverage(updated_cov, exchange)

# --- PARSE CURRENT_STATE.LOG ---
def parse_current_state_log(session: str, strategy: str) -> Dict[str, List[Tuple[datetime, datetime]]]:
    """
    Parse current_state.log to extract time windows for each ticker.
    File format: timestamp:'asset1':1, 'asset2':-1, ...
    Returns: {ticker: [(start_time, end_time), ...], ...}
    """
    file_path = DATA_DIR / session / strategy / "current_state.log"
    if not file_path.exists():
        logger.warning(f"current_state.log not found for session '{session}', strategy '{strategy}' at {file_path}")
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
                    
                    # Skip entries older than 180 days
                    if timestamp < CUTOFF_DATE:
                        logger.debug(f"Skipping line {i+1} in {file_path}: timestamp {timestamp} is older than {CUTOFF_DATE}")
                        continue
                    
                    # Parse position string
                    current_positions = {}
                    if not pos_str:
                        continue
                    items = pos_str.split(',')
                    for item in items:
                        item = item.strip()
                        if not item:
                            continue
                        # Split on last colon
                        asset_qty = item.rsplit(':', 1)
                        if len(asset_qty) != 2:
                            logger.warning(f"Invalid asset-quantity pair in line {i+1}: {item}")
                            continue
                        asset = asset_qty[0].strip("'").strip()
                        qty = asset_qty[1]
                        try:
                            direction = int(float(qty.strip()))
                            if direction not in [1, -1]:
                                logger.warning(f"Invalid direction {direction} for {asset} in line {i+1}")
                                continue
                            current_positions[asset] = direction
                        except ValueError:
                            logger.warning(f"Invalid quantity in line {i+1}: {qty}")
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
                            # Only include if end_time is within 180 days
                            if end_time >= CUTOFF_DATE:
                                ticker_windows.setdefault(ticker, []).append((start_time, end_time))
                            del position_states[ticker]
                
                except Exception as e:
                    logger.warning(f"Failed to parse line in {file_path}, line {i+1}: {line}. Error: {e}")
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
                            if last_timestamp and last_timestamp >= CUTOFF_DATE:
                                for ticker, state in position_states.items():
                                    ticker_windows.setdefault(ticker, []).append((state['start_time'], last_timestamp))
                    except Exception as e:
                        logger.warning(f"Failed to parse last line for open positions: {last_line}. Error: {e}")
    
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        try:
            TGMessenger.send_message(f"PNL Data Fetcher: Error reading {file_path}: {e}", "AwsMonitor")
        except Exception as te:
            logger.error(f"Failed to send Telegram message: {te}")
        return {}
    
    return ticker_windows

# --- SCAN TICKER WINDOWS ---
def scan_theoretical_tickers_and_windows(exchange: str) -> Dict[str, List[Tuple[datetime, datetime]]]:
    """
    Scan current_state.log files for all active strategies for the given exchange and return merged ticker windows,
    expanded by ±30 minutes.
    """
    cfg = load_core_ms_config()
    session_configs = cfg.get('session_configs', {})
    strategy_windows = {}
    
    # Collect windows per strategy for the specified exchange
    for session, config in session_configs.items():
        if config.get('exchange', '').lower() != exchange.lower():
            continue
        for strat_name, strat_config in config.get('strategy', {}).items():
            if not strat_config.get('active', False):
                continue
            windows = parse_current_state_log(session, strat_name)
            if windows:
                strategy_windows[f"{session}/{strat_name}"] = windows
    
    # Merge windows per ticker per strategy
    for strat_key in strategy_windows:
        for ticker in strategy_windows[strat_key]:
            windows = strategy_windows[strat_key][ticker]
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
                strategy_windows[strat_key][ticker] = merged
    
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
    with open(paths["price_windows"], 'w', encoding='utf-8') as f:
        json.dump(price_windows_data, f, indent=2)
    logger.info(f"Saved price windows to {paths['price_windows']} for {exchange}")
    
    return merged_windows

# --- COVERAGE JSON ---
def load_coverage(exchange: str) -> Dict[str, List[List[str]]]:
    coverage_file = get_exchange_paths(exchange)["coverage"]
    if coverage_file.exists():
        return json.loads(coverage_file.read_text(encoding='utf-8'))
    return {}

def save_coverage(cov: Dict[str, List[List[str]]], exchange: str) -> None:
    coverage_file = get_exchange_paths(exchange)["coverage"]
    coverage_file.write_text(json.dumps(cov, indent=2, sort_keys=True), encoding='utf-8')
    logger.info(f"Updated coverage file {coverage_file}")

# --- UNAVAILABLE TICKERS LOGGING ---
def load_unavailable_tickers(exchange: str) -> Dict[str, Dict[str, str]]:
    unavailable_file = get_exchange_paths(exchange)["unavailable_tickers"]
    if unavailable_file.exists():
        return json.loads(unavailable_file.read_text(encoding='utf-8'))
    return {}

def save_unavailable_ticker(ticker: str, reason: str, exchange: str) -> None:
    unavailable = load_unavailable_tickers(exchange)
    unavailable[ticker] = {"symbol": ticker, "reason": reason}
    unavailable_file = get_exchange_paths(exchange)["unavailable_tickers"]
    unavailable_file.write_text(json.dumps(unavailable, indent=2, sort_keys=True), encoding='utf-8')
    logger.info(f"Logged unavailable ticker {ticker} to {unavailable_file}")

# --- FETCH PER TICKER ---
def fetch_single_ticker(ticker: str, windows: List[Tuple[datetime, datetime]], exchange: str) -> None:
    """
    Fetch 15-minute open prices for a ticker within the specified windows from the given exchange,
    extended by ±12 hours to ensure coverage, and save the full extended data.
    """
    if exchange.lower() == "binance":
        market = bf.BinanceFutureMarket()
    elif exchange.lower() == "bitget":
        market = bitget_df.BitgetMarket()
    elif exchange.lower() == "okx":
        market = okex_df.OkexMarket()
    else:
        logger.error(f"Unsupported exchange: {exchange}")
        try:
            TGMessenger.send_message(f"PNL Data Fetcher: Unsupported exchange: {exchange}", "AwsMonitor")
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
        raise ValueError(f"Unsupported exchange: {exchange}")
    
    paths = get_exchange_paths(exchange)
    cov = load_coverage(exchange)
    existing_coverage = [(pd.Timestamp(s), pd.Timestamp(e)) for s, e in cov.get(ticker, [])]
    updated = existing_coverage.copy()
    outpath = paths["single_token_dir"] / f"{ticker}_open.csv"
    
    # Filter windows to exclude those ending before cutoff
    filtered_windows = [(start, end) for start, end in windows if end >= CUTOFF_DATE]
    if not filtered_windows:
        logger.info(f"No recent windows to process for {ticker} on {exchange} (all before {CUTOFF_DATE})")
        return
    
    # Load existing data
    if outpath.exists():
        df_existing = pd.read_csv(outpath, parse_dates=['timestamp']).set_index('timestamp')
        df_existing.index = pd.to_datetime(df_existing.index, utc=True).tz_localize(None)
        df_existing = df_existing[~df_existing.index.duplicated()]
    else:
        df_existing = pd.DataFrame()
    
    parts = []
    try:
        for start, end in filtered_windows:
            # Extend fetch window by ±12 hours
            fetch_start = start - FETCH_EXTENSION
            fetch_end = end + FETCH_EXTENSION
            # Ensure fetch_start is not before cutoff
            fetch_start = max(fetch_start, CUTOFF_DATE)
            logger.info(f"Processing {ticker} window {start} -> {end}, fetching {fetch_start} -> {fetch_end} (+-12 hours) on {exchange}")
            
            # Check coverage for the extended window
            if any(cov_start <= fetch_start and cov_end >= fetch_end for cov_start, cov_end in existing_coverage):
                logger.info(f"[SKIP] {ticker} extended window {fetch_start} -> {fetch_end} on {exchange}: already covered")
                continue
            
            # Adjust for partial coverage
            for cov_start, cov_end in existing_coverage:
                if fetch_start >= cov_start and fetch_start <= cov_end:
                    fetch_start = cov_end
                if fetch_end >= cov_start and fetch_end <= cov_end:
                    fetch_end = cov_start
            
            if fetch_start >= fetch_end:
                logger.info(f"[SKIP] {ticker} window {start} -> {end} on {exchange}: adjusted window {fetch_start} -> {fetch_end} is invalid")
                continue
            
            logger.info(f"Fetching {ticker} for adjusted window {fetch_start} -> {fetch_end} on {exchange}")
            bars, _ = market.read_bars(
                symbol=ticker,
                timeframe=TIMEFRAME,
                start_time=int(fetch_start.timestamp()),
                end_time=int(fetch_end.timestamp())
            )
            if bars is None or bars.empty:
                logger.warning(f"[WARN] {ticker} no data {fetch_start} -> {fetch_end} on {exchange}")
                with open(paths["error_log"], 'a', encoding='utf-8') as ef:
                    ef.write(f"{ticker}: no data for {fetch_start} -> {fetch_end} on {exchange}\n")
                continue
            idx = pd.to_datetime(bars.index, unit='s', utc=True).tz_localize(None)
            series = pd.Series(bars['open'].values, index=idx, name=ticker)
            series = series[~series.index.duplicated(keep='first')]
            logger.info(f"Fetched {len(series)} bars for {ticker} from {series.index.min()} to {series.index.max()}")
            parts.append(series)
            updated.append((fetch_start, fetch_end))
    except Exception as e:
        error_msg = str(e).lower()
        if "does not have market symbol" in error_msg or "invalid symbol" in error_msg:
            logger.warning(f"[UNAVAILABLE] {ticker} is not available on {exchange}, likely delisted. Skipping.")
            save_unavailable_ticker(ticker, f"Symbol not available on {exchange} (likely delisted)", exchange)
            with open(paths["error_log"], 'a', encoding='utf-8') as ef:
                ef.write(f"{ticker}: Symbol {ticker} not available on {exchange}\n")
        else:
            logger.error(f"[ERROR] {ticker} failed to fetch data on {exchange}: {e}. Skipping to next ticker.")
            try:
                TGMessenger.send_message(f"PNL Data Fetcher: Failed to fetch {ticker} on {exchange}: {e}", "AwsMonitor")
            except Exception as te:
                logger.error(f"Failed to send Telegram message: {te}")
            with open(paths["error_log"], 'a', encoding='utf-8') as ef:
                ef.write(f"{ticker}: Failed to fetch data on {exchange}: {e}\n")
        return
    
    if not parts:
        logger.info(f"No new data for {ticker} on {exchange}")
        return
    
    new_data = pd.concat(parts, axis=0).to_frame().sort_index()
    new_data.columns = [ticker]
    new_data = new_data[~new_data.index.duplicated(keep='first')]
    logger.info(f"Combined new data for {ticker}: {len(new_data)} bars from {new_data.index.min()} to {new_data.index.max()}")
    
    if not df_existing.empty:
        try:
            combined = pd.concat([df_existing, new_data], axis=1, join='outer')
            combined = combined.loc[:, ~combined.columns.duplicated()]
            if ticker in combined.columns:
                combined[ticker] = combined[ticker].combine_first(new_data[ticker])
            else:
                combined[ticker] = new_data[ticker]
        except InvalidIndexError:
            logger.warning(f"Duplicate indices detected for {ticker} on {exchange}. Removing duplicates.")
            df_existing = df_existing[~df_existing.index.duplicated(keep='first')]
            new_data = new_data[~new_data.index.duplicated(keep='first')]
            combined = pd.concat([df_existing, new_data], axis=1, join='outer')
            combined = combined.loc[:, ~combined.columns.duplicated()]
            combined[ticker] = combined[ticker].combine_first(new_data[ticker])
    else:
        combined = new_data.copy()
    
    # Save all data, ensuring no data before cutoff
    if combined.empty:
        logger.info(f"No data fetched for {ticker} on {exchange}")
        return
    
    combined = combined[combined.index >= CUTOFF_DATE]
    combined.to_csv(outpath, index_label='timestamp')
    logger.info(f"Saved/Updated {ticker} -> {outpath} with {len(combined)} bars from {combined.index.min()} to {combined.index.max()}")
    
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
    
    merged = [(max(start, CUTOFF_DATE), end) for start, end in merged if end >= CUTOFF_DATE]
    cov[ticker] = [[s.isoformat(), e.isoformat()] for s, e in merged]
    save_coverage(cov, exchange)

# --- AGGREGATION ---
def aggregate_all(exchange: str) -> pd.DataFrame:
    """Aggregate per-ticker CSVs for the specified exchange into a single price matrix."""
    paths = get_exchange_paths(exchange)
    single_token_dir = paths["single_token_dir"]
    files = list(single_token_dir.glob("*_open.csv"))
    dfs = []
    for f in files:
        df = pd.read_csv(f, parse_dates=['timestamp']).set_index('timestamp')
        df.index = pd.to_datetime(df.index, utc=True).tz_localize(None)
        dfs.append(df)
    if not dfs:
        logger.warning(f"No price data files found for {exchange} in {single_token_dir}")
        return pd.DataFrame()
    agg = pd.concat(dfs, axis=1, join='outer')
    agg = agg.loc[:, ~agg.columns.duplicated()]
    agg = agg[agg.index >= CUTOFF_DATE]
    agg_csv = paths["agg_csv"]
    agg.to_csv(agg_csv, index_label='timestamp')
    logger.info(f"Aggregated matrix for {exchange} -> {agg_csv}")
    return agg

# --- MAIN ---
if __name__ == "__main__":
    cfg = load_core_ms_config()
    exchanges = [session for session, config in cfg.get('session_configs', {}).items()]
    for exchange in exchanges:
        logger.info(f"Processing exchange: {exchange}")
        # Clean old data before fetching
        clean_old_data(exchange)
        windows_map = scan_theoretical_tickers_and_windows(exchange)
        logger.info(f"Tickers to fetch for {exchange}:")
        for ticker, windows in windows_map.items():
            logger.info(f"{ticker}:")
            for start, end in windows:
                logger.info(f"  {start} -> {end}")
        for ticker, windows in windows_map.items():
            logger.info(f"Fetching windows for {ticker} on {exchange}:")
            for s, e in windows:
                logger.info(f"  {s} -> {e}")
            fetch_single_ticker(ticker, windows, exchange)
        df_all = aggregate_all(exchange)
        logger.info(f"Head of aggregated data for {exchange}:\n{df_all.head()}")