from typing import Dict, List
from dataclasses import dataclass
import csv
import json
import os
from datetime import datetime

@dataclass
class PortfolioEntry:
    asset: str
    qty: float  # Number of tokens
    qty_pctg_aum_at_entry: float  # Percentage of AUM at entry
    entry_price: float
    entry_time: str  # Add entry timestamp to PortfolioEntry

class Strategy:
    def __init__(self, name: str, family: str, strategy_config: Dict):
        self.name = name
        self.family = family
        self.config = strategy_config
        print(f"Initializing strategy {name} with config: {self.config}")
        self.portfolio: Dict[str, Dict[str, float]] = {}  # asset -> {qty, qty_pctg_aum_at_entry, entry_price, entry_time}
        self.first_timestamp: str = None
        self.closed_positions: Dict[str, List[Dict]] = {}  # exit_time -> list of closed positions
    
    def get_position_counts(self) -> tuple[int, int]:
        """Return the number of long and short positions."""
        long_count = sum(1 for info in self.portfolio.values() if info['qty_pctg_aum_at_entry'] > 0)
        short_count = sum(1 for info in self.portfolio.values() if info['qty_pctg_aum_at_entry'] < 0)
        return long_count, short_count
    
    def get_pair_count(self) -> int:
        """Return the number of active pairs (for spread/copula)."""
        long_count, short_count = self.get_position_counts()
        return min(long_count, short_count)  # A pair requires both legs
    
    def get_portfolio_at_timestamp(self) -> List[PortfolioEntry]:
        """Return the current portfolio as a list of PortfolioEntry objects."""
        return [
            PortfolioEntry(
                asset=asset,
                qty=info['qty'],
                qty_pctg_aum_at_entry=info['qty_pctg_aum_at_entry'],
                entry_price=info['entry_price'],
                entry_time=info['entry_time']
            )
            for asset, info in self.portfolio.items()
        ]
    
    def calculate_lifetime(self, entry_time: str, exit_time: str) -> float:
        """Calculate the lifetime of a position in seconds."""
        entry_dt = datetime.strptime(entry_time, '%Y/%m/%d %H:%M:%S.%f')
        exit_dt = datetime.strptime(exit_time, '%Y/%m/%d %H:%M:%S.%f')
        return (exit_dt - entry_dt).total_seconds()
    
    def update_portfolio(self, signal: Dict):
        signal_type = signal['signal_type']
        direction = int(signal['direction'])
        timestamp = signal['timestamp']
        
        # Update first timestamp
        if self.first_timestamp is None or timestamp < self.first_timestamp:
            self.first_timestamp = timestamp
        
        # Handle exit signals
        if signal_type in ['exitsignal', 'exitforced_exit', 'LS_exitsignal', 'LS_exitforced_exit', 'LS_reshuffle signal']:
            if self.family in ['spread', 'copula']:
                if signal_type not in ['exitsignal', 'exitforced_exit']:
                    return
                asset1, asset2 = signal['pair'].split('__')
                entry_exit = signal.get('entry_exit')
                if entry_exit != 'xx':
                    print(f"Warning: {timestamp} - Strategy {self.name}: Invalid entry_exit '{entry_exit}' for exitsignal/exitforced_exit. Expected 'xx'. Ignoring.")
                    return
                if asset1 not in self.portfolio or asset2 not in self.portfolio:
                    print(f"Warning: {timestamp} - Strategy {self.name}: Attempting to close unopened pair {asset1}__{asset2}. Ignoring.")
                    return
                # Get exit prices from the signal
                exit_prices = list(map(float, signal['prices'].split(',')))
                if len(exit_prices) != 2:
                    print(f"Warning: {timestamp} - Strategy {self.name}: Invalid number of exit prices for pair {asset1}__{asset2}. Ignoring.")
                    return
                # Record closed positions
                for i, asset in enumerate([asset1, asset2]):
                    pos = self.portfolio[asset]
                    qty = pos['qty']
                    entry_price = pos['entry_price']
                    entry_time = pos['entry_time']
                    exit_price = exit_prices[i]
                    entry_value = qty * entry_price
                    exit_value = qty * exit_price
                    realized_pnl = exit_value - entry_value
                    lifetime = self.calculate_lifetime(entry_time, timestamp)
                    closed_position = {
                        'asset': asset,
                        'entry_time': entry_time,
                        'entry_price': entry_price,
                        'entry_value': entry_value,
                        'exit_time': timestamp,
                        'exit_value': exit_value,
                        'realized_pnl': realized_pnl,
                        'lifetime': lifetime
                    }
                    if timestamp not in self.closed_positions:
                        self.closed_positions[timestamp] = []
                    self.closed_positions[timestamp].append(closed_position)
                self.portfolio.pop(asset1, None)
                self.portfolio.pop(asset2, None)
            else:  # basketspread
                if signal_type not in ['LS_exitsignal', 'LS_exitforced_exit', 'LS_reshuffle signal']:
                    return
                if signal_type in ['LS_exitsignal', 'LS_exitforced_exit']:
                    asset = signal['asset']
                    if asset not in self.portfolio:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Attempting to close unopened position for asset {asset}. Ignoring.")
                        return
                    pos = self.portfolio[asset]
                    qty = pos['qty']
                    entry_price = pos['entry_price']
                    entry_time = pos['entry_time']
                    exit_price = float(signal['price'])
                    entry_value = qty * entry_price
                    exit_value = qty * exit_price
                    realized_pnl = exit_value - entry_value
                    lifetime = self.calculate_lifetime(entry_time, timestamp)
                    closed_position = {
                        'asset': asset,
                        'entry_time': entry_time,
                        'entry_price': entry_price,
                        'entry_value': entry_value,
                        'exit_time': timestamp,
                        'exit_value': exit_value,
                        'realized_pnl': realized_pnl,
                        'lifetime': lifetime
                    }
                    if timestamp not in self.closed_positions:
                        self.closed_positions[timestamp] = []
                    self.closed_positions[timestamp].append(closed_position)
                    self.portfolio.pop(asset, None)
                elif signal_type == 'LS_reshuffle signal':
                    asset1, asset2 = signal['pair'].split('__')
                    entry_exit = signal.get('entry_exit')
                    if entry_exit not in ['xn', 'nx']:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Invalid entry_exit '{entry_exit}' for LS_reshuffle signal. Expected 'xn' or 'nx'. Ignoring.")
                        return
                    if entry_exit == 'xn' and asset1 not in self.portfolio:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Attempting to close unopened position for asset {asset1}. Ignoring.")
                        return
                    if entry_exit == 'nx' and asset2 not in self.portfolio:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Attempting to close unopened position for asset {asset2}. Ignoring.")
                        return
                    exit_prices = list(map(float, signal['prices'].split(',')))
                    if entry_exit == 'xn':
                        pos = self.portfolio[asset1]
                        qty = pos['qty']
                        entry_price = pos['entry_price']
                        entry_time = pos['entry_time']
                        exit_price = exit_prices[0]  # Price for asset1
                        entry_value = qty * entry_price
                        exit_value = qty * exit_price
                        realized_pnl = exit_value - entry_value
                        lifetime = self.calculate_lifetime(entry_time, timestamp)
                        closed_position = {
                            'asset': asset1,
                            'entry_time': entry_time,
                            'entry_price': entry_price,
                            'entry_value': entry_value,
                            'exit_time': timestamp,
                            'exit_value': exit_value,
                            'realized_pnl': realized_pnl,
                            'lifetime': lifetime
                        }
                        if timestamp not in self.closed_positions:
                            self.closed_positions[timestamp] = []
                        self.closed_positions[timestamp].append(closed_position)
                        self.portfolio.pop(asset1, None)
                    elif entry_exit == 'nx':
                        pos = self.portfolio[asset2]
                        qty = pos['qty']
                        entry_price = pos['entry_price']
                        entry_time = pos['entry_time']
                        exit_price = exit_prices[1]  # Price for asset2
                        entry_value = qty * entry_price
                        exit_value = qty * exit_price
                        realized_pnl = exit_value - entry_value
                        lifetime = self.calculate_lifetime(entry_time, timestamp)
                        closed_position = {
                            'asset': asset2,
                            'entry_time': entry_time,
                            'entry_price': entry_price,
                            'entry_value': entry_value,
                            'exit_time': timestamp,
                            'exit_value': exit_value,
                            'realized_pnl': realized_pnl,
                            'lifetime': lifetime
                        }
                        if timestamp not in self.closed_positions:
                            self.closed_positions[timestamp] = []
                        self.closed_positions[timestamp].append(closed_position)
                        self.portfolio.pop(asset2, None)
        
        # Check position limits before adding new positions
        if self.family in ['spread', 'copula']:
            if signal_type != 'entry':
                return
            max_total_expo = float(self.config.get('max_total_expo', 16))
            max_pairs = max_total_expo
            current_pairs = self.get_pair_count()
            entry_exit = signal.get('entry_exit')
            if entry_exit == 'nn' and current_pairs >= max_pairs:
                print(f"Warning: {timestamp} - Strategy {self.name}: Number of pairs ({current_pairs}) exceeds maximum allowed ({max_pairs})")
                return
        
        elif self.family == 'basketspread':
            if signal_type not in ['LS_single_entry', 'LS_entry', 'LS_reshuffle signal']:
                return
            nb_long = int(self.config.get('nb_long', 18))
            nb_short = int(self.config.get('nb_short', 18))
            current_long, current_short = self.get_position_counts()
            if signal_type == 'LS_single_entry':
                qty_pctg_aum = float(signal['quantity']) * float(signal['price']) / (float(self.config.get('allocation', 2500)) * float(self.config.get('leverage', 1)))
                qty_pctg_aum = qty_pctg_aum if direction == 1 else -qty_pctg_aum
                if qty_pctg_aum > 0 and current_long >= nb_long:
                    print(f"Warning: {timestamp} - Strategy {self.name}: Number of long positions ({current_long}) exceeds maximum allowed ({nb_long})")
                    return
                if qty_pctg_aum < 0 and current_short >= nb_short:
                    print(f"Warning: {timestamp} - Strategy {self.name}: Number of short positions ({current_short}) exceeds maximum allowed ({nb_short})")
                    return
            elif signal_type in ['LS_entry', 'LS_reshuffle signal'] and signal.get('entry_exit') in ['nn', 'xn', 'nx']:
                price1, price2 = map(float, signal['prices'].split(','))
                quantity1, quantity2 = map(float, signal['quantities'].split(','))
                aum = float(self.config.get('allocation', 2500)) * float(self.config.get('leverage', 1))
                qty1_pctg_aum = (quantity1 * price1) / aum
                qty2_pctg_aum = (quantity2 * price2) / aum
                entry_exit = signal.get('entry_exit')
                if signal_type == 'LS_entry' and entry_exit != 'nn':
                    print(f"Warning: {timestamp} - Strategy {self.name}: Invalid entry_exit '{entry_exit}' for LS_entry. Expected 'nn'. Ignoring.")
                    return
                if signal_type == 'LS_reshuffle signal' and entry_exit not in ['xn', 'nx']:
                    return
                asset1_qty_pctg_aum = qty1_pctg_aum if direction == 1 else -qty1_pctg_aum
                asset2_qty_pctg_aum = -qty2_pctg_aum if direction == 1 else qty2_pctg_aum
                if entry_exit == 'nn':
                    if asset1_qty_pctg_aum > 0 and current_long >= nb_long:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of long positions ({current_long}) exceeds maximum allowed ({nb_long}) for asset1")
                        return
                    if asset1_qty_pctg_aum < 0 and current_short >= nb_short:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of short positions ({current_short}) exceeds maximum allowed ({nb_short}) for asset1")
                        return
                    if asset2_qty_pctg_aum > 0 and current_long >= nb_long:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of long positions ({current_long}) exceeds maximum allowed ({nb_long}) for asset2")
                        return
                    if asset2_qty_pctg_aum < 0 and current_short >= nb_short:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of short positions ({current_short}) exceeds maximum allowed ({nb_short}) for asset2")
                        return
                elif entry_exit == 'xn':
                    if asset2_qty_pctg_aum > 0 and current_long >= nb_long:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of long positions ({current_long}) exceeds maximum allowed ({nb_long}) for asset2")
                        return
                    if asset2_qty_pctg_aum < 0 and current_short >= nb_short:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of short positions ({current_short}) exceeds maximum allowed ({nb_short}) for asset2")
                        return
                elif entry_exit == 'nx':
                    if asset1_qty_pctg_aum > 0 and current_long >= nb_long:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of long positions ({current_long}) exceeds maximum allowed ({nb_long}) for asset1")
                        return
                    if asset1_qty_pctg_aum < 0 and current_short >= nb_short:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of short positions ({current_short}) exceeds maximum allowed ({nb_short}) for asset1")
                        return
        
        # Proceed with updating portfolio for non-exit signals
        if signal_type == 'LS_single_entry':
            if self.family != 'basketspread':
                return
            asset = signal['asset']
            quantity = float(signal['quantity'])
            price = float(signal['price'])
            allocation = float(self.config.get('allocation', 2500))
            leverage = float(self.config.get('leverage', 1))
            aum = allocation * leverage
            notional = quantity * price
            qty_pctg_aum = notional / aum if direction == 1 else -notional / aum
            qty = quantity if direction == 1 else -quantity  # Number of tokens
            self.portfolio[asset] = {
                'qty': qty,
                'qty_pctg_aum_at_entry': qty_pctg_aum,
                'entry_price': price,
                'entry_time': timestamp  # Store entry timestamp
            }
        elif signal_type in ['entry', 'LS_entry', 'LS_reshuffle signal']:
            asset1, asset2 = signal['pair'].split('__')
            price1, price2 = map(float, signal['prices'].split(','))
            entry_exit = signal.get('entry_exit')
            
            if self.family in ['spread', 'copula']:
                if signal_type != 'entry':
                    return
                if entry_exit != 'nn':
                    print(f"Warning: {timestamp} - Strategy {self.name}: Invalid entry_exit '{entry_exit}' for entry. Expected 'nn'. Ignoring.")
                    return
                max_total_expo = float(self.config.get('max_total_expo', 16))
                qty_pctg_aum_per_leg = 1.0 / (max_total_expo * 2)
                # For pair strategies, calculate qty (number of tokens) based on qty_pctg_aum and AUM
                allocation = float(self.config.get('allocation', 2500))
                leverage = float(self.config.get('leverage', 1))
                aum = allocation * leverage
                qty1 = (qty_pctg_aum_per_leg * aum) / price1  # Number of tokens for asset1
                qty2 = (qty_pctg_aum_per_leg * aum) / price2  # Number of tokens for asset2
                self.portfolio[asset1] = {
                    'qty': qty1 if direction == 1 else -qty1,
                    'qty_pctg_aum_at_entry': qty_pctg_aum_per_leg if direction == 1 else -qty_pctg_aum_per_leg,
                    'entry_price': price1,
                    'entry_time': timestamp  # Store entry timestamp
                }
                self.portfolio[asset2] = {
                    'qty': -qty2 if direction == 1 else qty2,
                    'qty_pctg_aum_at_entry': -qty_pctg_aum_per_leg if direction == 1 else qty_pctg_aum_per_leg,
                    'entry_price': price2,
                    'entry_time': timestamp  # Store entry timestamp
                }
            else:  # basketspread
                if signal_type not in ['LS_entry', 'LS_reshuffle signal']:
                    return
                allocation = float(self.config.get('allocation', 2500))
                leverage = float(self.config.get('leverage', 1))
                aum = allocation * leverage
                quantity1, quantity2 = map(float, signal['quantities'].split(','))
                notional1 = quantity1 * price1
                notional2 = quantity2 * price2
                qty1_pctg_aum = notional1 / aum
                qty2_pctg_aum = notional2 / aum
                
                if signal_type == 'LS_reshuffle signal':
                    if entry_exit == 'xn':
                        self.portfolio[asset2] = {
                            'qty': quantity2 if direction == 1 else -quantity2,
                            'qty_pctg_aum_at_entry': qty2_pctg_aum if direction == 1 else -qty2_pctg_aum,
                            'entry_price': price2,
                            'entry_time': timestamp  # Store entry timestamp
                        }
                    elif entry_exit == 'nx':
                        self.portfolio[asset1] = {
                            'qty': quantity1 if direction == 1 else -quantity1,
                            'qty_pctg_aum_at_entry': qty1_pctg_aum if direction == 1 else -qty1_pctg_aum,
                            'entry_price': price1,
                            'entry_time': timestamp  # Store entry timestamp
                        }
                elif signal_type == 'LS_entry':
                    if entry_exit == 'nn':
                        self.portfolio[asset1] = {
                            'qty': quantity1 if direction == 1 else -quantity1,
                            'qty_pctg_aum_at_entry': qty1_pctg_aum if direction == 1 else -qty1_pctg_aum,
                            'entry_price': price1,
                            'entry_time': timestamp  # Store entry timestamp
                        }
                        self.portfolio[asset2] = {
                            'qty': -quantity2 if direction == 1 else quantity2,
                            'qty_pctg_aum_at_entry': -qty2_pctg_aum if direction == 1 else qty2_pctg_aum,
                            'entry_price': price2,
                            'entry_time': timestamp  # Store entry timestamp
                        }
    
    def process_signals(self, signal_file: str, output_file: str) -> Dict[str, Dict]:
        # Reset portfolio state
        self.portfolio = {}
        self.first_timestamp = None
        self.closed_positions = {}
        
        # Compute portfolio
        portfolios: Dict[str, Dict] = {}
        try:
            with open(signal_file, 'r') as f:
                reader = csv.reader(f, delimiter=';')
                for row in reader:
                    if len(row) < 8 or row[1] != 'INFO' or row[3] != self.name:
                        continue
                    
                    signal = {}
                    try:
                        signal['timestamp'] = row[0]
                        signal['strategy'] = row[3]
                        signal['direction'] = row[4]
                        signal_type = row[9] if len(row) > 9 else row[8]
                        
                        if self.family in ['spread', 'copula']:
                            if signal_type not in ['entry', 'exitsignal', 'exitforced_exit']:
                                continue
                            if len(row) >= 10 and signal_type in ['entry', 'exitsignal', 'exitforced_exit']:
                                signal['pair'] = row[5]
                                signal['quantities'] = row[6]
                                signal['prices'] = row[7]
                                signal['entry_exit'] = row[8]
                                signal['signal_type'] = signal_type
                        elif self.family == 'basketspread':
                            if signal_type in ['LS_single_entry', 'LS_exitsignal', 'LS_exitforced_exit']:
                                if len(row) >= 9:
                                    signal['asset'] = row[5]
                                    signal['quantity'] = row[6]
                                    signal['price'] = row[7]
                                    signal['signal_type'] = signal_type
                            elif signal_type in ['LS_reshuffle signal', 'LS_entry']:
                                if len(row) >= 10:
                                    signal['pair'] = row[5]
                                    signal['quantities'] = row[6]
                                    signal['prices'] = row[7]
                                    signal['entry_exit'] = row[8]
                                    signal['signal_type'] = signal_type
                        
                    except IndexError as e:
                        print(f"Error parsing row {row}: {e}")
                        continue
                    
                    self.update_portfolio(signal)
                    current_portfolio = [
                        {
                            'asset': entry.asset,
                            'qty': entry.qty,  # Number of tokens
                            'qty_pctg_aum_at_entry': entry.qty_pctg_aum_at_entry,  # Percentage of AUM
                            'entry_price': entry.entry_price,
                            'usd_value_at_entry': entry.qty * entry.entry_price
                        }
                        for entry in self.get_portfolio_at_timestamp()
                    ]
                    # Calculate unbalance as the sum of qty_pctg_aum_at_entry
                    unbalance = sum(entry['qty_pctg_aum_at_entry'] for entry in current_portfolio)
                    portfolios[signal['timestamp']] = {
                        'strategy': self.name,
                        'portfolio': current_portfolio,
                        'portfolio_unbalance_ratio_of_aum': unbalance
                    }
                    # print(f"Processed signal at {signal['timestamp']} for {self.name}: {portfolios[signal['timestamp']]}")
        except FileNotFoundError:
            print(f"Signal file {signal_file} not found")
            return {}
        
        # Write portfolio file
        with open(output_file, 'w') as f:
            json.dump(portfolios, f, indent=4)
        
        # Write closed positions file
        closed_positions_file = output_file.replace('portfolio_', 'closed_positions_')
        with open(closed_positions_file, 'w') as f:
            json.dump(self.closed_positions, f, indent=4)
        
        return portfolios

class StrategyManager:
    def __init__(self, config_file: str = 'config_pair_session_bin.json'):
        self.strategies: Dict[str, Strategy] = {}
        try:
            with open(config_file, 'r') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            print(f"Error: Config file {config_file} not found. Using empty config.")
            self.config = {'strategy': {}, 'account_allocation': {}}
    
    def add_strategy(self, name: str, family: str, strategy_config: Dict):
        self.strategies[name] = Strategy(name, family, strategy_config)
    
    def process_all_strategies(self, signal_file: str):
        print("Processing signals for all strategies:")
        for strat_name in self.strategies:
            output_file = f"{self.config.get('output_directory', 'output_bin/')}portfolio_{strat_name}.json"
            print(f"Processing strategy: {strat_name}")
            self.strategies[strat_name].process_signals(signal_file, output_file)
    
    def build_account_portfolio(self, account: str):
        output_file = f"{self.config.get('output_directory', 'output_bin/')}portfolio_{account}.json"
        
        # Count active strategies by family
        family_counts = {'spread': 0, 'basketspread': 0}
        for strat_name, strat in self.strategies.items():
            if strat.config.get('account_trade') == account and strat.config.get('active', False):
                family = 'spread' if strat.family in ['spread', 'copula'] else 'basketspread'
                family_counts[family] += 1
                print(f"Counting strategy {strat_name} for account {account}: family={family}, family_counts={family_counts}")
        
        account_allocation = self.config.get('account_allocation', {}).get(account, {})
        spread_weight = account_allocation.get('spread', 1.0) / max(family_counts['spread'], 1)
        basketspread_weight = account_allocation.get('basketspread', 1.0) / max(family_counts['basketspread'], 1)
        print(f"Spread weight for account {account}: {spread_weight}")
        print(f"Basketspread weight for account {account}: {basketspread_weight}")
        
        # Collect all timestamps and strategy portfolios
        all_timestamps = set()
        strategy_portfolios = {}
        for strat_name, strat in self.strategies.items():
            if strat.config.get('account_trade') == account and strat.config.get('active', False):
                strategy_output_file = f"{self.config.get('output_directory', 'output_bin/')}portfolio_{strat_name}.json"
                if os.path.exists(strategy_output_file):
                    try:
                        with open(strategy_output_file, 'r') as f:
                            portfolios = json.load(f)
                            all_timestamps.update(portfolios.keys())
                            strategy_portfolios[strat_name] = portfolios
                    except json.JSONDecodeError:
                        print(f"Warning: Portfolio file {strategy_output_file} is corrupted.")
        
        # Build the aggregated portfolio by combining all strategy portfolios at each timestamp
        cumulative_portfolios = {}
        strategy_states = {strat_name: {} for strat_name in strategy_portfolios}
        
        for timestamp in sorted(all_timestamps):
            # Collect all positions at this timestamp from all strategies
            aggregated_positions_qty = {}  # Number of tokens
            aggregated_positions_qty_pctg_aum = {}  # Percentage of AUM
            
            for strat_name, portfolios in strategy_portfolios.items():
                # Get the current portfolio state for this strategy at this timestamp
                if timestamp in portfolios:
                    current_state = {
                        entry['asset']: {
                            'qty': entry['qty'],
                            'qty_pctg_aum_at_entry': entry['qty_pctg_aum_at_entry']
                        }
                        for entry in portfolios[timestamp]['portfolio']
                    }
                    strategy_states[strat_name] = current_state
                else:
                    # If no update at this timestamp, use the last known state
                    current_state = strategy_states[strat_name]
                
                # Apply appropriate weight only to qty_pctg_aum_at_entry
                weight = spread_weight if self.strategies[strat_name].family in ['spread', 'copula'] else basketspread_weight
                for asset, data in current_state.items():
                    qty = data['qty']
                    qty_pctg_aum = data['qty_pctg_aum_at_entry'] * weight  # Weight only the AUM percentage
                    
                    if asset in aggregated_positions_qty:
                        aggregated_positions_qty[asset] += qty
                        aggregated_positions_qty_pctg_aum[asset] += qty_pctg_aum
                    else:
                        aggregated_positions_qty[asset] = qty
                        aggregated_positions_qty_pctg_aum[asset] = qty_pctg_aum
            
            # Convert aggregated positions to portfolio entries, excluding near-zero quantities
            portfolio_entries = [
                {
                    'asset': asset,
                    'qty': qty,  # Unweighted number of tokens
                    'qty_pctg_aum_at_entry': qty_pctg_aum  # Weighted percentage of AUM
                }
                for asset, qty in aggregated_positions_qty.items()
                if abs(qty) > 1e-6 or abs(aggregated_positions_qty_pctg_aum[asset]) > 1e-6
            ]
            # Calculate unbalance as the sum of qty_pctg_aum_at_entry
            unbalance = sum(entry['qty_pctg_aum_at_entry'] for entry in portfolio_entries)
            cumulative_portfolios[timestamp] = {
                'account': account,
                'portfolio': portfolio_entries,
                'portfolio_unbalance_ratio_of_aum': unbalance
            }
        
        with open(output_file, 'w') as f:
            json.dump(cumulative_portfolios, f, indent=4)