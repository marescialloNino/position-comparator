#!/usr/bin/env python3
"""
fetch_portfolio_prices.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Incrementally fetch 15-minute open prices for Binance futures tickers, limited to
time-windows when positions existed (closed or open), writing per-ticker CSV files
immediately, and then aggregating into a consolidated matrix.
"""

import json
import os
import sys
from pathlib import Path
from typing import List, Tuple, Optional, Dict
from datetime import datetime

import pandas as pd
from tqdm import tqdm

# Add project root to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datafeed.utils_online import extract_coin
import datafeed.binancefeed as bf

# --- CONFIG ---
MODULE_DIR    = Path(__file__).resolve().parent
OUTPUT_DIR    = MODULE_DIR / "binance_data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
COVERAGE_FILE = OUTPUT_DIR / "binance_price_coverage.json"
TIMEFRAME     = "15m"
BAR_INTERVAL  = pd.Timedelta(minutes=15)
PROGRESS_LOG  = OUTPUT_DIR / "download_progress.log"
ERROR_LOG     = OUTPUT_DIR / "download_errors.log"
AGG_CSV       = OUTPUT_DIR / "binance_open_15m.csv"

# --- SYMBOL BUILDER ---
def map_to_binance_symbol_and_factor(ticker: str) -> Tuple[str, float]:
    coin = extract_coin(ticker)
    base = f"{coin}USDT"
    tiny = {"SHIB","PEPE","SATS","LUNC","XEC","BONK","FLOKI","CAT","RATS","WHY","X"}
    if coin in tiny:
        return f"1000{coin}USDT", 1e-3
    return base, 1.0

# --- SCAN POSITION WINDOWS ---
def scan_tickers_and_windows(config_file: str = "config_test.json") -> Dict[str, List[Tuple[datetime, datetime]]]:
    """Return a dict mapping each ticker to a list of merged windows when it had positions."""
    try:
        cfg = json.loads(Path(config_file).read_text())
    except FileNotFoundError:
        print(f"Config {config_file} not found.")
        return {}

    output_dir = cfg.get("output_directory", "output_bin/")
    now = datetime.utcnow()
    windows: Dict[str, List[Tuple[datetime, datetime]]] = {}

    for strat in cfg.get("strategy", {}):
        closed = Path(f"{output_dir}closed_positions_{strat}.json")
        if closed.exists():
            for positions in json.loads(closed.read_text()).values():
                for pos in positions:
                    t = pos["asset"]
                    start = datetime.strptime(pos["entry_time"], "%Y/%m/%d %H:%M:%S.%f")
                    end   = datetime.strptime(pos.get("exit_time", now.strftime("%Y/%m/%d %H:%M:%S.%f")), "%Y/%m/%d %H:%M:%S.%f")
                    windows.setdefault(t, []).append((start, end))
        openf = Path(f"{output_dir}portfolio_{strat}.json")
        if openf.exists():
            for items in json.loads(openf.read_text()).values():
                for pos in items.get("portfolio", []):
                    t = pos["asset"]
                    start = datetime.strptime(pos["entry_time"], "%Y/%m/%d %H:%M:%S.%f")
                    windows.setdefault(t, []).append((start, now))

    # merge overlaps per ticker
    merged: Dict[str, List[Tuple[datetime, datetime]]] = {}
    for t, ws in windows.items():
        ws.sort(key=lambda x: x[0])
        out = []
        cur_s, cur_e = ws[0]
        for s, e in ws[1:]:
            if s <= cur_e + BAR_INTERVAL:
                cur_e = max(cur_e, e)
            else:
                out.append((cur_s, cur_e))
                cur_s, cur_e = s, e
        out.append((cur_s, cur_e))
        merged[t] = out
    return merged

# --- COVERAGE JSON ---
def load_coverage() -> Dict[str, List[List[str]]]:
    return json.loads(Path(COVERAGE_FILE).read_text()) if Path(COVERAGE_FILE).exists() else {}

def save_coverage(cov: Dict[str, List[List[str]]]) -> None:
    Path(COVERAGE_FILE).write_text(json.dumps(cov, indent=2, sort_keys=True))

# --- FETCH PER TICKER ---
def fetch_single_ticker_open(
    ticker: str,
    windows: List[Tuple[datetime, datetime]]
) -> None:
    """
    Fetch and immediately write per-ticker CSV for given windows.
    """
    market = bf.BinanceFutureMarket()
    cov = load_coverage()
    existing = []
    if ticker in cov:
        existing = [(datetime.fromisoformat(s), datetime.fromisoformat(e)) for s, e in cov[ticker]]

    updated = existing.copy()
    parts = []
    for start, end in windows:
        # skip covered subranges
        gaps = [ (start, end) ]
        # here you may refine by subtracting existing intervals if needed
        for g_s, g_e in gaps:
            sym, factor = map_to_binance_symbol_and_factor(ticker)
            bars, _ = market.read_bars(
                symbol=sym,
                timeframe=TIMEFRAME,
                start_time=int(start.timestamp()),
                end_time=int(end.timestamp())
            )
            if bars is None or bars.empty:
                print(f"[WARN] {ticker} no data {start}–{end}")
                with open(ERROR_LOG, 'a') as ef:
                    ef.write(f"{ticker}: no data for {start}–{end}\n")
                continue
            idx = pd.to_datetime(bars.index, unit='s', utc=True).tz_localize(None)
            series = bars['open'].rename(ticker)
            series.index = idx
            parts.append(series)
            updated.append((start, end))

    if not parts:
        print(f"No new data for {ticker}")
        return

    df = pd.concat(parts).sort_index()
    # write per-ticker CSV
    outpath = OUTPUT_DIR / f"{ticker}_open.csv"
    df.to_csv(outpath, index_label='timestamp')
    print(f"Saved {ticker} → {outpath}")

    # update coverage
    cov[ticker] = [[s.isoformat(), e.isoformat()] for s, e in sorted(updated)]
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
    for ticker, win in windows_map.items():
        print(f"Fetching windows for {ticker}:")
        for s, e in win:
            print(f"  {s} → {e}")
        fetch_single_ticker_open(ticker, win)

    # finally aggregate
    df_all = aggregate_all()
    print(df_all.head())
