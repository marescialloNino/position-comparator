#!/usr/bin/env python3
"""
compare_portfolio_positions.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Fetches theoretical positions from bot_data/{strategy}/current_state.json and actual positions
from bot_data/{exchange}_{account}/current_state.log. Aggregates theoretical positions by account,
compares with actual positions, and generates a discrepancy report.
Outputs JSON files to output_bitget/.
"""

import json
import os
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

from config_manager import ConfigManager

# --- CONFIG ---
MODULE_DIR = Path(__file__).resolve().parent
BOT_DATA_DIR = MODULE_DIR / "bot_data"

# --- UTILITIES ---
def parse_actual_positions(file_path: Path) -> tuple[Dict[str, Any], str]:
    """Read the most recent line from current_state.log."""
    if not file_path.exists():
        raise FileNotFoundError(f"Actual positions file not found at {file_path}")
    
    timestamp_pattern = re.compile(r'^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{6})')
    last_line = None
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                last_line = line
    
    if not last_line:
        raise ValueError(f"No valid entries in {file_path}")
    
    match = timestamp_pattern.match(last_line)
    if not match:
        raise ValueError(f"Invalid timestamp in {last_line}")
    
    timestamp = match.group(1)
    pos_str = last_line[len(match.group(0))+1:].strip()
    positions = {}
    items = pos_str.split(',')
    for item in items:
        item = item.strip()
        if not item:
            continue
        key_val = item.split(':')
        if len(key_val) != 2:
            print(f"Warning: Invalid pair in {item}")
            continue
        key, val = key_val
        key = key.strip("'").strip('"')
        try:
            val = float(val.strip())
            positions[key] = val
        except ValueError:
            print(f"Warning: Invalid value in {item}")
            continue
    
    equity = positions.pop('equity', None)
    return {"positions": {k: {"quantity": v} for k, v in positions.items()}, "equity": equity}, timestamp

# --- MAIN LOGIC ---
def build_theoretical_portfolio(config_manager: ConfigManager) -> Dict[str, Dict]:
    """Build theoretical portfolio for each account."""
    account_portfolios = {}
    for strat in config_manager.get_active_strategies():
        strat_name = strat['name']
        account = strat['account_trade']
        exchange = strat['exchange_trade']
        
        # Initialize account portfolio
        if account not in account_portfolios:
            account_portfolios[account] = {"positions": {}, "strategies": []}
        
        # Calculate AUM and max positions
        aum, max_positions = config_manager.get_aum_and_max_positions(strat_name, account, exchange)
        if aum <= 0 or max_positions <= 0:
            print(f"Warning: Invalid AUM ({aum}) or max positions ({max_positions}) for {strat_name}")
            continue
        notional_per_position = aum / max_positions
        
        # Read current_state.json
        file_path = BOT_DATA_DIR / strat_name / "current_state.json"
        if not file_path.exists():
            print(f"Warning: {file_path} not found for {strat_name}")
            continue
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            continue
        
        # Process positions
        for asset, info in data.get('current_coin_info', {}).items():
            position = info.get('position', 0)
            if position not in [1, -1]:
                continue  # Skip invalid positions
            entry_exec = info.get('entry_exec')
            in_execution = info.get('in_execution', False)
            if entry_exec is None or entry_exec <= 0:
                print(f"Warning: Invalid entry_exec for {asset} in {strat_name}")
                continue
            
            quantity = (notional_per_position / entry_exec) * position
            account_portfolios[account]["positions"].setdefault(asset, {"quantity": 0.0, "in_execution": False})
            account_portfolios[account]["positions"][asset]["quantity"] += quantity
            account_portfolios[account]["positions"][asset]["in_execution"] |= in_execution
        
        account_portfolios[account]["strategies"].append(strat_name)
    
    return account_portfolios

def compare_portfolios(
    theoretical: Dict[str, Dict], actual: Dict[str, Any], account: str, timestamp: str
) -> Dict[str, Any]:
    """Compare theoretical and actual portfolios."""
    discrepancies = []
    theoretical_pos = theoretical.get("positions", {})
    actual_pos = actual.get("positions", {})
    
    all_assets = set(theoretical_pos.keys()) | set(actual_pos.keys())
    for asset in sorted(all_assets):
        theo_qty = theoretical_pos.get(asset, {}).get("quantity", 0.0)
        theo_exec = theoretical_pos.get(asset, {}).get("in_execution", False)
        actual_qty = actual_pos.get(asset, {}).get("quantity", 0.0)
        diff = theo_qty - actual_qty
        
        if abs(theo_qty) < 1e-8 and abs(actual_qty) < 1e-8:
            continue  # Skip near-zero positions
        
        if abs(diff) < 1e-8:
            status = "matched"
        elif asset not in theoretical_pos:
            status = "missing_in_theoretical"
        elif asset not in actual_pos:
            status = "missing_in_actual"
        else:
            status = "quantity_mismatch"
        
        discrepancies.append({
            "asset": asset,
            "theoretical_quantity": theo_qty,
            "actual_quantity": actual_qty,
            "difference": diff,
            "in_execution": theo_exec,
            "status": status
        })
    
    return {
        "account": account,
        "timestamp": timestamp,
        "discrepancies": discrepancies
    }

def main():
    """Main function to process portfolios and generate outputs."""
    config_manager = ConfigManager()
    output_dir = config_manager.get_output_directory()
    os.makedirs(output_dir, exist_ok=True)
    
    # Build theoretical portfolios
    theoretical_portfolios = build_theoretical_portfolio(config_manager)
    
    # Process each account
    for account, theo_portfolio in theoretical_portfolios.items():
        exchange = "bitget"  # Assume bitget based on context
        actual_file = BOT_DATA_DIR / f"{exchange}_{account}" / "current_state.log"
        
        try:
            actual_portfolio, timestamp = parse_actual_positions(actual_file)
        except Exception as e:
            print(f"Error processing actual positions for account {account}: {e}")
            continue
        
        # Save theoretical portfolio
        theo_output = {
            "account": account,
            "timestamp": timestamp,
            "positions": theo_portfolio["positions"]
        }
        theo_file = output_dir / f"account_{account}_theoretical_portfolio.json"
        with open(theo_file, 'w') as f:
            json.dump(theo_output, f, indent=4)
        print(f"Wrote theoretical portfolio to {theo_file}")
        
        # Save actual portfolio
        actual_output = {
            "account": account,
            "timestamp": timestamp,
            "positions": actual_portfolio["positions"],
            "equity": actual_portfolio["equity"]
        }
        actual_file = output_dir / f"account_{account}_actual_portfolio.json"
        with open(actual_file, 'w') as f:
            json.dump(actual_output, f, indent=4)
        print(f"Wrote actual portfolio to {actual_file}")
        
        # Compare portfolios
        comparison = compare_portfolios(theo_portfolio, actual_portfolio, account, timestamp)
        comp_file = output_dir / f"account_{account}_position_comparison.json"
        with open(comp_file, 'w') as f:
            json.dump(comparison, f, indent=4)
        print(f"Wrote comparison report to {comp_file}")

if __name__ == "__main__":
    main()