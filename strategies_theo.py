from typing import Dict, List, Optional
from dataclasses import dataclass
import json
import os
import re
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

MODULE_DIR = Path(__file__).resolve().parent
BOT_DATA_DIR = MODULE_DIR / "bot_data"

@dataclass
class PortfolioEntry:
    asset: str
    qty: float  # Number of tokens
    entry_price: float
    entry_time: str  # Entry timestamp
    usd_value_at_entry: float  # qty * entry_price
    current_price: float  # Price at current timestamp
    current_value: float  # qty * current_price
    unrealized_pnl: float  # current_value - usd_value_at_entry

class Strategy:
    def __init__(self, name: str, family: str, strategy_config: Dict, global_config: Dict):
        self.name = name
        self.family = family
        self.config = strategy_config
        self.global_config = global_config
        print(f"Initializing strategy {name} with config: {self.config}")
        self.portfolio: Dict[str, Dict] = {}  # asset -> {qty, entry_price, entry_time, usd_value_at_entry}
        self.first_timestamp: str = None
        self.closed_positions: Dict[str, List[Dict]] = {}  # exit_time -> list of closed positions
        self.portfolio_snapshots: Dict[str, Dict] = {}  # Store snapshots for aggregation

    def get_aum_and_max_positions(self) -> tuple[float, int]:
        """Calculate strategy AUM and maximum number of positions, splitting AUM among active basketspread strategies."""
        account = self.config.get('account_trade')
        exchange = self.config.get('exchange_trade', 'bitget')
        allocations = self.global_config.get('allocations', {}).get(exchange, {}).get(account, {})
        allocation = allocations.get('amount', 0.0)
        leverage = allocations.get('leverage', 1.0)
        allocation_ratio = allocations.get('allocation_ratios', {}).get(self.config.get('type'), 0.0)
        base_aum = allocation * leverage * allocation_ratio

        # Count active basketspread strategies for the same account and exchange
        active_basket_strats = sum(
            1 for strat_config in self.global_config.get('strategy', {}).values()
            if strat_config.get('active', False) and
            strat_config.get('type') == 'basketspread' and
            strat_config.get('account_trade') == account and
            strat_config.get('exchange_trade', 'bitget') == exchange
        )
        if active_basket_strats > 0 and self.family == 'basketspread':
            aum = base_aum / active_basket_strats
        else:
            aum = base_aum

        if self.family == 'basketspread':
            nb_long = int(self.config.get('nb_long', 8))
            nb_short = int(self.config.get('nb_short', 8))
            max_positions = nb_long + nb_short
        else:
            max_positions = int(self.config.get('max_total_expo', 16)) * 2  # For pair strategies

        return aum, max_positions

    def get_price_at_time(self, price_matrix: pd.DataFrame, asset: str, time: datetime) -> Optional[float]:
        """Fetch the price of an asset at a given time from the price matrix."""
        if asset not in price_matrix.columns:
            print(f"Warning: Asset {asset} not found in price matrix for strategy {self.name}")
            return None
        deltas = abs(price_matrix.index - time)
        idx = deltas.argmin()
        if deltas[idx] <= timedelta(minutes=30):  # Allow 30-minute tolerance
            price = float(price_matrix.iloc[idx][asset])
            return price if not pd.isna(price) else None
        print(f"Warning: No price within 30 minutes of {time} for {asset} in strategy {self.name}")
        return None

    def process_current_state_log(self, price_matrix: pd.DataFrame, output_file: str, closed_file: str):
        """Process current_state.log to generate portfolio and closed positions JSON files."""
        file_path = BOT_DATA_DIR / self.name / "current_state.log"
        if not file_path.exists():
            print(f"Warning: current_state.log not found for strategy '{self.name}' at {file_path}")
            return

        # Calculate AUM and max positions
        aum, max_positions = self.get_aum_and_max_positions()
        if aum <= 0 or max_positions <= 0:
            print(f"Warning: Invalid AUM ({aum}) or max positions ({max_positions}) for strategy {self.name}")
            return
        notional_per_position = aum / max_positions  # Absolute value per position

        # Parse the log file
        timestamps = []
        portfolio_states = {}
        timestamp_pattern = re.compile(r'^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)')
        timestamp_formats = [
            "%Y/%m/%d %H:%M:%S.%f",
            "%Y/%m/%d %H:%M:%S"  # Fallback without microseconds
        ]

        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    # Extract timestamp using regex
                    timestamp_match = timestamp_pattern.match(line)
                    if not timestamp_match:
                        raise ValueError(f"Invalid timestamp format in line {i+1}")
                    timestamp_str = timestamp_match.group(1)
                    pos_str = line[len(timestamp_match.group(0)):].lstrip(':').strip()
                    
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
                    positions = {}
                    if not pos_str:
                        continue
                    items = pos_str.split(',')
                    for item in items:
                        item = item.strip()
                        if not item:
                            continue
                        asset_qty = item.rsplit(':', 1)
                        if len(asset_qty) != 2:
                            print(f"Warning: Invalid asset-quantity pair in line {i+1}: {item}")
                            continue
                        asset, direction = asset_qty
                        asset = asset.strip("'").strip('"')
                        try:
                            direction = int(float(direction.strip()))  # Convert to int, expecting 1 or -1
                            if direction not in [1, -1]:
                                print(f"Warning: Invalid direction {direction} for {asset} in {file_path}")
                                continue
                            positions[asset] = direction
                        except ValueError:
                            print(f"Warning: Invalid quantity in line {i+1}: {direction}")
                            continue
                    timestamps.append(timestamp)
                    portfolio_states[timestamp] = positions
                except Exception as e:
                    print(f"Warning: Failed to parse line in {file_path}: {line}. Error: {e}")
                    continue

        if not timestamps:
            print(f"Warning: No valid entries found in {file_path} for strategy {self.name}")
            return

        timestamps.sort()
        current_positions = {}  # {asset: {'qty': float, 'entry_time': datetime, 'entry_price': float, 'usd_value_at_entry': float}}
        self.portfolio_snapshots = {}

        for i, ts in enumerate(timestamps):
            prev_state = portfolio_states[timestamps[i-1]] if i > 0 else {}
            current_state = portfolio_states[ts]

            # Detect closed positions
            for asset in list(current_positions.keys()):
                if asset not in current_state:
                    pos = current_positions[asset]
                    exit_time = ts
                    exit_price = self.get_price_at_time(price_matrix, asset, exit_time)
                    if exit_price is not None:
                        entry_price = pos['entry_price']
                        qty = pos['qty']
                        realized_pnl = qty * (exit_price - entry_price)
                        closed_entry = {
                            'asset': asset,
                            'entry_time': pos['entry_time'].strftime('%Y/%m/%d %H:%M:%S.%f'),
                            'entry_price': entry_price,
                            'entry_value': qty * entry_price,
                            'exit_time': exit_time.strftime('%Y/%m/%d %H:%M:%S.%f'),
                            'exit_value': qty * exit_price,
                            'realized_pnl': realized_pnl,
                            'lifetime': (exit_time - pos['entry_time']).total_seconds(),
                            'status': 'CLOSED'
                        }
                        self.closed_positions.setdefault(exit_time.strftime('%Y/%m/%d %H:%M:%S.%f'), []).append(closed_entry)
                    del current_positions[asset]

            # Update current positions with new positions
            for asset, direction in current_state.items():
                if asset not in current_positions:
                    entry_time = ts
                    entry_price = self.get_price_at_time(price_matrix, asset, entry_time)
                    if entry_price is not None and entry_price > 0:
                        qty = (notional_per_position / entry_price) * direction  # direction is 1 or -1
                        usd_value_at_entry = qty * entry_price
                        current_positions[asset] = {
                            'qty': qty,
                            'entry_time': entry_time,
                            'entry_price': entry_price,
                            'usd_value_at_entry': usd_value_at_entry
                        }

            # Record portfolio snapshot
            snapshot = []
            total_unrealized_pnl = 0.0
            for asset, pos in current_positions.items():
                current_price = self.get_price_at_time(price_matrix, asset, ts)
                if current_price is None:
                    continue  # Skip if no price available
                current_value = pos['qty'] * current_price
                unrealized_pnl = current_value - pos['usd_value_at_entry']
                total_unrealized_pnl += unrealized_pnl
                snapshot.append({
                    'asset': asset,
                    'qty': pos['qty'],
                    'entry_price': pos['entry_price'],
                    'entry_time': pos['entry_time'].strftime('%Y/%m/%d %H:%M:%S.%f'),
                    'usd_value_at_entry': pos['usd_value_at_entry'],
                    'current_price': current_price,
                    'current_value': current_value,
                    'unrealized_pnl': unrealized_pnl
                })
            
            self.portfolio_snapshots[ts.strftime('%Y/%m/%d %H:%M:%S.%f')] = {
                'strategy': self.name,
                'portfolio': snapshot,
                'portfolio_unbalance_usd': total_unrealized_pnl
            }

        # Record remaining open positions at the last timestamp
        if current_positions and timestamps:
            last_ts = timestamps[-1].strftime('%Y/%m/%d %H:%M:%S.%f')
            if last_ts not in self.closed_positions:
                self.closed_positions[last_ts] = []
            for asset, pos in current_positions.items():
                entry = {
                    'asset': asset,
                    'entry_time': pos['entry_time'].strftime('%Y/%m/%d %H:%M:%S.%f'),
                    'entry_price': pos['entry_price'],
                    'entry_value': pos['usd_value_at_entry'],
                    'status': 'OPEN'
                }
                self.closed_positions[last_ts].append(entry)

        # Write output files
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(self.portfolio_snapshots, f, indent=4)
        with open(closed_file, 'w') as f:
            json.dump(self.closed_positions, f, indent=4)
        print(f"Generated {output_file} and {closed_file} for strategy {self.name}")

class StrategyManager:
    def __init__(self, config_file: str = 'config_test.json'):
        self.strategies: Dict[str, Strategy] = {}
        try:
            with open(config_file, 'r') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            print(f"Error: Config file {config_file} not found. Using empty config.")
            self.config = {'strategy': {}, 'allocations': {}}

    def add_strategy(self, name: str, family: str, strategy_config: Dict):
        self.strategies[name] = Strategy(name, family, strategy_config, self.config)

    def load_price_matrix(self, exchange: str) -> pd.DataFrame:
        """Load the price matrix for a given exchange."""
        price_matrix_path = MODULE_DIR / "price_data" / exchange.lower() / "theoretical_open_15m.csv"
        if not price_matrix_path.exists():
            raise FileNotFoundError(f"No price matrix found at {price_matrix_path}")
        df = pd.read_csv(price_matrix_path, parse_dates=["timestamp"]).set_index("timestamp")
        df.index = pd.to_datetime(df.index, utc=True).tz_localize(None)
        return df

    def aggregate_account_portfolio(self, account: str, exchange: str, price_matrix: pd.DataFrame, output_dir: str):
        """Aggregate positions across all strategies in the same account."""
        # Collect all timestamps and positions
        all_timestamps = set()
        for strat in self.strategies.values():
            if (strat.config.get('active', False) and
                strat.config.get('account_trade') == account and
                strat.config.get('exchange_trade', 'bitget') == exchange):
                all_timestamps.update(
                    datetime.strptime(ts, '%Y/%m/%d %H:%M:%S.%f')
                    for ts in strat.portfolio_snapshots.keys()
                )
        
        if not all_timestamps:
            print(f"No portfolio snapshots found for account {account} on exchange {exchange}")
            return

        # Aggregate positions at each timestamp
        account_portfolio = {}
        for ts in sorted(all_timestamps):
            positions = {}
            for strat in self.strategies.values():
                if not (strat.config.get('active', False) and
                        strat.config.get('account_trade') == account and
                        strat.config.get('exchange_trade', 'bitget') == exchange):
                    continue
                ts_str = ts.strftime('%Y/%m/%d %H:%M:%S.%f')
                snapshot = strat.portfolio_snapshots.get(ts_str, {}).get('portfolio', [])
                for pos in snapshot:
                    asset = pos['asset']
                    qty = pos['qty']
                    positions[asset] = positions.get(asset, 0.0) + qty
            
            # Build portfolio entry
            portfolio_entry = []
            for asset, quantity in positions.items():
                if abs(quantity) < 1e-8:  # Skip zero quantities
                    continue
                current_price = strat.get_price_at_time(price_matrix, asset, ts)
                if current_price is None:
                    print(f"Warning: No price for {asset} at {ts} for account {account}")
                    continue
                current_value = quantity * current_price
                portfolio_entry.append({
                    'assetname': asset,
                    'quantity': quantity,
                    'currentprice': current_price,
                    'currentvalue': current_value
                })
            
            if portfolio_entry:
                account_portfolio[ts.strftime('%Y/%m/%d %H:%M:%S.%f')] = portfolio_entry

        # Write output file
        output_file = os.path.join(output_dir, f"account_{account}_portfolio.json")
        os.makedirs(output_dir, exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(account_portfolio, f, indent=4)
        print(f"Generated {output_file} for account {account} on exchange {exchange}")

    def process_all_current_state_logs(self):
        """Process current_state.log for all active strategies and aggregate account portfolios."""
        print("Processing current_state.log for all strategies:")
        exchanges = set(
            strat_config.get('exchange_trade', 'bitget')
            for strat_config in self.config.get('strategy', {}).values()
            if strat_config.get('active', False)
        )
        price_matrices = {}
        for exchange in exchanges:
            try:
                price_matrices[exchange] = self.load_price_matrix(exchange)
            except FileNotFoundError as e:
                print(e)
                continue

        accounts = set()
        output_dir = self.config.get('output_directory', 'output_bitget')
        for strat_name, strat in self.strategies.items():
            if not strat.config.get('active', False):
                continue
            exchange = strat.config.get('exchange_trade', 'bitget')
            account = strat.config.get('account_trade')
            accounts.add((account, exchange))
            price_matrix = price_matrices.get(exchange)
            if price_matrix is None:
                print(f"Warning: Price matrix not found for exchange '{exchange}', skipping strategy '{strat_name}'")
                continue
            output_file = f"{output_dir}/portfolio_{strat_name}.json"
            closed_file = f"{output_dir}/closed_positions_{strat_name}.json"
            print(f"Processing strategy: {strat_name}")
            strat.process_current_state_log(price_matrix, output_file, closed_file)

        # Aggregate portfolios for each account
        for account, exchange in accounts:
            price_matrix = price_matrices.get(exchange)
            if price_matrix is None:
                print(f"Warning: Price matrix not found for exchange '{exchange}', skipping account '{account}'")
                continue
            self.aggregate_account_portfolio(account, exchange, price_matrix, output_dir)

if __name__ == "__main__":
    config_file = "config_pair_session_bitget.json"
    manager = StrategyManager(config_file)
    for strat_name, strat_config in manager.config.get('strategy', {}).items():
        if strat_config.get('active', False):
            family = strat_config.get('type', 'basketspread')
            manager.add_strategy(strat_name, family, strat_config)
    manager.process_all_current_state_logs()