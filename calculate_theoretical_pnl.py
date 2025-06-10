#!/usr/bin/env python3
"""
calculate_theoretical_pnl.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Computes portfolio realized PnL, total PnL, and return percentage for each strategy
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

# Ensure project root on path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- CONFIG ---
MODULE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = MODULE_DIR / "output_bitget"
CONFIG_FILE = MODULE_DIR / "config_pair_session_bitget.json"
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
    price_matrix_path = MODULE_DIR / "price_data" / "bitget" / "theoretical_open_15m.csv"
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

def get_aum(strat_config: Dict, global_config: Dict, account: str, exchange: str, strategy_type: str) -> float:
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

# --- LOAD PORTFOLIO DATA ---
def load_portfolios() -> Dict[str, Dict[str, Any]]:
    """Load portfolio snapshots from portfolio_*.json files."""
    cfg = load_config()
    portfolios: Dict[str, Dict[str, Any]] = {}
    paths = get_output_paths()

    for strat in cfg["strategy"]:
        if not cfg["strategy"][strat].get("active", False):
            continue
        file_path = paths["output_dir"] / PORTFOLIO_PATTERN.format(strategy=strat)
        if not file_path.exists():
            print(f"Warning: Portfolio file not found for strategy '{strat}' at {file_path}")
            continue
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                portfolios[strat] = data
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            continue
    
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
                                continue
            except Exception as e:
                print(f"Warning: Failed to parse closed positions from {path}: {e}")
        closed_map[strat] = sorted(entries, key=lambda x: x[0])
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
    timeline: Dict[str, Any] = {}
    
    # Strategy-level timelines
    for strategy, snaps in portfolios.items():
        strat_cfg = cfg["strategy"].get(strategy, {})
        strategy_type = strat_cfg.get('type', 'basketspread')
        aum = get_aum(strat_cfg, cfg, account, exchange, strategy_type)
        if aum <= 0:
            print(f"Warning: Invalid AUM ({aum}) for strategy {strategy}")
            continue
        
        realized_list = closed_map.get(strategy, [])
        run_realized = 0.0
        idx_real = 0
        n_real = len(realized_list)
        
        strategy_timeline = {}
        for ts_str, snap in sorted(snaps.items()):
            entry_time = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S.%f")
            while idx_real < n_real and realized_list[idx_real][0] <= entry_time:
                run_realized += realized_list[idx_real][1]
                idx_real += 1
            
            positions = snap.get("portfolio", [])
            total_unreal = sum(pos.get("unrealized_pnl", 0.0) for pos in positions if pos.get("unrealized_pnl") is not None)
            has_missing_price = any(pos.get("current_price") is None for pos in positions)
            
            portfolio_realized = run_realized
            portfolio_unrealized = total_unreal if not has_missing_price else None
            portfolio_total = (portfolio_realized + total_unreal) if not has_missing_price else None
            portfolio_return_pct = portfolio_total / aum if aum > 0 and portfolio_total is not None else None
            
            strategy_timeline[ts_str] = {
                "positions": positions,  # Reuse portfolio_*.json positions
                "portfolio_realized_pnl": portfolio_realized,
                "portfolio_unrealized_pnl": portfolio_unrealized,
                "portfolio_total_pnl": portfolio_total,
                "portfolio_return_pct": portfolio_return_pct
            }
        
        timeline[strategy] = strategy_timeline
    
    # Account-level timeline
    account_timeline = {}
    all_timestamps = set()
    for strategy, snaps in portfolios.items():
        if (cfg["strategy"].get(strategy, {}).get("account_trade") == account and
            cfg["strategy"].get(strategy, {}).get("exchange_trade", "bitget") == exchange):
            all_timestamps.update(snaps.keys())
    
    price_matrix = load_price_matrix()
    account_aum = sum(
        get_aum(cfg["strategy"][strat], cfg, account, exchange, cfg["strategy"][strat].get('type', 'basketspread'))
        for strat in cfg["strategy"]
        if cfg["strategy"][strat].get('active', False) and
        cfg["strategy"][strat].get('account_trade') == account and
        cfg["strategy"][strat].get('exchange_trade', 'bitget') == exchange
    )
    
    for ts_str in sorted(all_timestamps):
        entry_time = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S.%f")
        positions = {}
        run_realized = 0.0
        
        for strategy, snaps in portfolios.items():
            strat_cfg = cfg["strategy"].get(strategy, {})
            if (strat_cfg.get("account_trade") != account or
                strat_cfg.get("exchange_trade", "bitget") != exchange or
                not strat_cfg.get("active", False)):
                continue
            snap = snaps.get(ts_str, {}).get("portfolio", [])
            for pos in snap:
                asset = pos["asset"]
                qty = pos["qty"]
                positions[asset] = positions.get(asset, 0.0) + qty
            
            # Accumulate realized PnL
            realized_list = closed_map.get(strategy, [])
            for dt, pnl in realized_list:
                if dt <= entry_time:
                    run_realized += pnl
        
        # Compute account-level metrics
        account_positions = []
        total_unreal = 0.0
        has_missing_price = False
        for asset, qty in positions.items():
            if abs(qty) < 1e-8:
                continue
            current_price = None
            if asset in price_matrix.columns:
                deltas = abs(price_matrix.index - entry_time)
                idx = deltas.argmin()
                if deltas[idx] <= timedelta(minutes=30):
                    current_price = float(price_matrix.iloc[idx][asset])
                    if pd.isna(current_price):
                        current_price = None
            
            current_value = current_price * qty if current_price is not None else None
            unreal_pnl = None
            for strategy, snaps in portfolios.items():
                snap = snaps.get(ts_str, {}).get("portfolio", [])
                for pos in snap:
                    if pos["asset"] == asset:
                        entry_price = pos["entry_price"]
                        if current_price is not None and entry_price is not None:
                            unreal_pnl = (current_value - qty * entry_price) if unreal_pnl is None else unreal_pnl + (current_value - qty * entry_price)
            
            if current_price is None:
                has_missing_price = True
            
            account_positions.append({
                "asset": asset,
                "qty": qty,
                "current_price": current_price,
                "current_value": current_value,
                "unrealized_pnl": unreal_pnl
            })
            if unreal_pnl is not None:
                total_unreal += unreal_pnl
        
        portfolio_realized = run_realized
        portfolio_unrealized = total_unreal if not has_missing_price else None
        portfolio_total = (portfolio_realized + total_unreal) if not has_missing_price else None
        portfolio_return_pct = portfolio_total / account_aum if account_aum > 0 and portfolio_total is not None else None
        
        account_timeline[ts_str] = {
            "positions": account_positions,
            "portfolio_realized_pnl": portfolio_realized,
            "portfolio_unrealized_pnl": portfolio_unrealized,
            "portfolio_total_pnl": portfolio_total,
            "portfolio_return_pct": portfolio_return_pct
        }
    
    timeline[f"account_{account}"] = account_timeline
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