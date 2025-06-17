#!/usr/bin/env python3
"""
calculate_theoretical_pnl.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Computes portfolio realized PnL, unrealized PnL, total PnL, and return percentage for each strategy
and account based on portfolio_*.json and closed_positions_*.json from strategies.py.
Writes pnl_timeline_{strategy}.json and pnl_timeline_account_{account}.json in output_bitget/.
Missing prices are null in JSON.
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple

import pandas as pd
import numpy as np

from common.paths import OUTPUT_DIR, CONFIG_FILE, PRICES_DIR

PORTFOLIO_PATTERN = "portfolio_{strategy}.json"
CLOSED_PATTERN = "closed_positions_{strategy}.json"

# --- UTILITIES ---
def load_config() -> Dict[str, Any]:
    """Load configuration file."""
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Config file not found at {CONFIG_FILE}")
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)

def get_output_paths() -> Dict[str, Path]:
    """Return output file paths."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return {"output_dir": OUTPUT_DIR}

def load_price_matrix() -> pd.DataFrame:
    """Load bitget price matrix."""
    price_matrix_path = PRICES_DIR/ "bitget" / "theoretical_open_15m.csv"
    if not price_matrix_path.exists():
        raise FileNotFoundError(f"No price matrix found at {price_matrix_path}")
    df = pd.read_csv(price_matrix_path, parse_dates=["timestamp"]).set_index("timestamp")
    df.index = pd.to_datetime(df.index, utc=True).tz_localize(None)
    return df

def sanitize(obj: Any) -> Any:
    """Convert NaN values to None for JSON serialization."""
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return None
    return obj

def get_price_at_time(price_matrix: pd.DataFrame, asset: str, time: datetime) -> Optional[float]:
    """Fetch the price of an asset at a given time from the price matrix."""
    if not isinstance(time, datetime):
        print(f"Warning: Time {time} is not a datetime object")
        return None
    if asset not in price_matrix.columns:
        print(f"Warning: Asset {asset} not found in price matrix")
        return None
    deltas = abs(price_matrix.index - time)
    idx = deltas.argmin()
    if deltas[idx] <= timedelta(minutes=120):
        price = float(price_matrix.iloc[idx][asset])
        if pd.isna(price):
            print(f"Warning: NaN price for {asset} at {time}")
            return None
        print(f"Price for {asset} at {time}: {price}")
        return price
    print(f"Warning: No price within 120 minutes of {time} for {asset}")
    return None

def get_strategy_aum(strat_config: Dict, global_config: Dict, account: str, exchange: str, strategy_type: str) -> float:
    """Calculate strategy AUM, splitting among active basketspread strategies."""
    allocations = global_config.get('allocations', {}).get(exchange, {}).get(account, {})
    allocation = allocations.get('amount', 0.0)
    leverage = allocations.get('leverage', 1.0)
    allocation_ratio = allocations.get('allocation_ratios', {}).get(strategy_type, 0.0)
    base_aum = allocation * leverage * allocation_ratio

    if strategy_type == 'basketspread':
        active_basket_strats = sum(
            1 for cfg in global_config.get('strategy', {}).values()
            if cfg.get('active', False) and
            cfg.get('type') == 'basketspread' and
            cfg.get('account_trade') == account and
            cfg.get('exchange_trade', 'bitget') == exchange
        )
        if active_basket_strats > 0:
            return base_aum / active_basket_strats
    return base_aum

def get_account_aum(global_config: Dict, account: str, exchange: str) -> float:
    """Calculate total account AUM."""
    allocations = global_config.get('allocations', {}).get(exchange, {}).get(account, {})
    allocation = allocations.get('amount', 0.0)
    leverage = allocations.get('leverage', 1.0)
    return allocation * leverage

# --- LOAD PORTFOLIO DATA ---
def load_portfolios() -> Dict[str, Dict[str, Any]]:
    """Load portfolio snapshots from portfolio_*.json files."""
    cfg = load_config()
    portfolios: Dict[str, Dict[str, Any]] = {}
    paths = get_output_paths()

    for strat in cfg["strategy"]:
        if not cfg["strategy"][strat].get("active", False):
            print(f"Strategy {strat} is not active, skipping")
            continue
        file_path = paths["output_dir"] / PORTFOLIO_PATTERN.format(strategy=strat)
        if not file_path.exists():
            print(f"Warning: Portfolio file not found for strategy '{strat}' at {file_path}")
            continue
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                portfolios[strat] = data
                print(f"Loaded portfolio for strategy {strat} with {len(data)} snapshots")
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            continue
    
    if not portfolios:
        print("Error: No portfolios loaded for any strategy")
    return portfolios

def load_closed_positions() -> Dict[str, List[Tuple[datetime, float]]]:
    """Load closed positions for each strategy."""
    cfg = load_config()
    closed_map: Dict[str, List[Tuple[datetime, float]]] = {}
    paths = get_output_paths()
    
    for strat in cfg["strategy"]:
        fname = CLOSED_PATTERN.format(strategy=strat)
        path = paths["output_dir"] / fname
        entries: List[Tuple[datetime, float]] = []
        if path.exists():
            try:
                data = json.loads(path.read_text())
                for ts_str, positions in data.items():
                    for pos in positions:
                        if pos.get("status") == "CLOSED":
                            pnl = pos.get("realized_pnl", 0.0)
                            exit_ts = pos.get("exit_time") or ts_str
                            try:
                                dt = datetime.strptime(exit_ts, "%Y/%m/%d %H:%M:%S.%f")
                                entries.append((dt, pnl))
                            except Exception:
                                print(f"Warning: Invalid timestamp {exit_ts} in {path}")
                                continue
            except Exception as e:
                print(f"Warning: Failed to parse closed positions from {path}: {e}")
        closed_map[strat] = sorted(entries, key=lambda x: x[0])
        print(f"Loaded {len(entries)} closed positions for strategy {strat}")
    return closed_map

# --- COMPUTE PNL ---
def compute_pnl_timeline(
    portfolios: Dict[str, Dict[str, Any]],
    account: str,
    exchange: str = "bitget"
) -> Dict[str, Any]:
    """Compute PnL timeline for each strategy and account."""
    cfg = load_config()
    closed_map = load_closed_positions()
    price_matrix = load_price_matrix()
    timeline: Dict[str, Any] = {}
    
    # Collect all timestamps across strategies
    all_timestamps = set()
    for strategy, snaps in portfolios.items():
        if (cfg["strategy"].get(strategy, {}).get("account_trade") == account and
            cfg["strategy"].get(strategy, {}).get("exchange_trade", "bitget") == exchange and
            cfg["strategy"].get(strategy, {}).get("active", False)):
            all_timestamps.update(snaps.keys())
    print(f"Found {len(all_timestamps)} unique timestamps for account {account}")

    if not all_timestamps:
        print(f"Error: No timestamps found for account {account} on {exchange}")
        return timeline

    # Strategy-level timelines
    for strategy, snaps in portfolios.items():
        strat_cfg = cfg["strategy"].get(strategy, {})
        if not strat_cfg.get("active", False):
            print(f"Strategy {strategy} is not active, skipping")
            continue
        strategy_type = strat_cfg.get('type', 'basketspread')
        aum = get_strategy_aum(strat_cfg, cfg, account, exchange, strategy_type)
        if aum <= 0:
            print(f"Warning: Invalid AUM ({aum}) for strategy {strategy}")
            continue
        
        realized_list = closed_map.get(strategy, [])
        run_realized = 0.0
        idx_real = 0
        n_real = len(realized_list)
        
        # Sort strategy timestamps
        strat_timestamps = sorted(
            datetime.strptime(ts, "%Y/%m/%d %H:%M:%S.%f") for ts in snaps.keys()
        )
        strategy_timeline = {}
        last_positions = []
        
        for ts_str in sorted(all_timestamps):
            entry_time = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S.%f")
            
            # Update realized PnL
            while idx_real < n_real and realized_list[idx_real][0] <= entry_time:
                run_realized += realized_list[idx_real][1]
                idx_real += 1
            
            # Find the most recent snapshot at or before entry_time
            recent_ts = max(
                (t for t in strat_timestamps if t <= entry_time),
                default=None
            )
            if recent_ts is None:
                positions = []
                print(f"Strategy {strategy}: No prior snapshot for {ts_str}, using empty positions")
            else:
                recent_ts_str = recent_ts.strftime("%Y/%m/%d %H:%M:%S.%f")
                positions = snaps.get(recent_ts_str, {}).get("portfolio", [])
                if positions != last_positions:
                    print(f"Strategy {strategy}: Using {len(positions)} positions from {recent_ts_str} for {ts_str}")
                last_positions = positions
            
            # Recalculate unrealized PnL for positions
            updated_positions = []
            total_unreal = 0.0
            has_missing_price = False
            for pos in positions:
                asset = pos["asset"]
                qty = pos["qty"]
                entry_price = pos["entry_price"]
                entry_time_str = pos["entry_time"]
                usd_value_at_entry = pos["usd_value_at_entry"]
                
                # Parse entry_time to datetime
                try:
                    pos_time = datetime.strptime(entry_time_str, "%Y/%m/%d %H:%M:%S.%f")
                except ValueError as e:
                    print(f"Warning: Invalid entry_time {entry_time_str} for {asset} in {strategy}: {e}")
                    has_missing_price = True
                    current_price = None
                    current_value = None
                    unreal_pnl = None
                else:
                    current_price = get_price_at_time(price_matrix, asset, entry_time)
                    if current_price is None:
                        has_missing_price = True
                    current_value = qty * current_price if current_price is not None else None
                    unreal_pnl = (current_value - usd_value_at_entry) if current_value is not None else None
                    if unreal_pnl is not None:
                        total_unreal += unreal_pnl
                
                updated_positions.append({
                    "asset": asset,
                    "qty": qty,
                    "entry_price": entry_price,
                    "entry_time": entry_time_str,
                    "usd_value_at_entry": usd_value_at_entry,
                    "current_price": current_price,
                    "current_value": current_value,
                    "unrealized_pnl": unreal_pnl
                })
            
            portfolio_realized = run_realized
            portfolio_unrealized = total_unreal if not has_missing_price else None
            portfolio_total = (portfolio_realized + total_unreal) if not has_missing_price else None
            portfolio_return_pct = portfolio_total / aum if aum > 0 and portfolio_total is not None else None
            
            strategy_timeline[ts_str] = {
                "positions": updated_positions,
                "portfolio_realized_pnl": portfolio_realized,
                "portfolio_unrealized_pnl": portfolio_unrealized,
                "portfolio_total_pnl": portfolio_total,
                "portfolio_return_pct": portfolio_return_pct
            }
        
        timeline[strategy] = strategy_timeline
        print(f"Computed timeline for strategy {strategy} with {len(strategy_timeline)} entries")
    
    # Account-level timeline
    account_aum = get_account_aum(cfg, account, exchange)
    print(f"Account {account} AUM: {account_aum}")
    if account_aum <= 0:
        print(f"Warning: Invalid account AUM ({account_aum}) for account {account}")
    
    account_timeline = {}
    for ts_str in sorted(all_timestamps):
        entry_time = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S.%f")
        total_realized = 0.0
        total_unrealized = 0.0
        has_missing_price = False
        
        for strategy, snaps in portfolios.items():
            strat_cfg = cfg["strategy"].get(strategy, {})
            if (strat_cfg.get("account_trade") != account or
                strat_cfg.get("exchange_trade", "bitget") != exchange or
                not strat_cfg.get("active", False)):
                continue
            
            # Use strategy timeline entry
            strategy_entry = timeline.get(strategy, {}).get(ts_str, {})
            realized = strategy_entry.get("portfolio_realized_pnl", 0.0)
            unrealized = strategy_entry.get("portfolio_unrealized_pnl")
            if realized is not None:
                total_realized += realized
            if unrealized is not None:
                total_unrealized += unrealized
            else:
                has_missing_price = True
        
        portfolio_realized = total_realized
        portfolio_unrealized = total_unrealized if not has_missing_price else None
        portfolio_total = (portfolio_realized + total_unrealized) if not has_missing_price else None
        portfolio_return_pct = portfolio_total / account_aum if account_aum > 0 and portfolio_total is not None else None
        
        account_timeline[ts_str] = {
            "portfolio_realized_pnl": portfolio_realized,
            "portfolio_unrealized_pnl": portfolio_unrealized,
            "portfolio_total_pnl": portfolio_total,
            "portfolio_return_pct": portfolio_return_pct
        }
    
    timeline[f"account_{account}"] = account_timeline
    print(f"Computed account timeline for account {account} with {len(account_timeline)} entries")
    return timeline

# --- MAIN ---
if __name__ == "__main__":
    cfg = load_config()
    exchange = "bitget"
    account = "2"
    
    print(f"Processing exchange: {exchange}, account: {account}")
    try:
        portfolios = load_portfolios()
        if not portfolios:
            print(f"No portfolios found for {exchange}, account {account}")
            sys.exit(1)
        
        print(f"Calculating PnL for strategies and account {account} on {exchange}...")
        result = compute_pnl_timeline(portfolios, account, exchange)
        safe = sanitize(result)
        
        paths = get_output_paths()
        for key, data in safe.items():
            out_file = paths["output_dir"] / f"pnl_timeline_{key}.json"
            with open(out_file, "w") as f:
                json.dump(data, f, indent=2)
            print(f"Wrote PnL timeline for {key} to {out_file}")
        
    except Exception as e:
        print(f"Error processing {exchange}, account {account}: {e}")
        sys.exit(1)