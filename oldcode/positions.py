# positions.py
from typing import Dict, List
from config import Config
from oldcode.strategies import StrategyManager, Position

class PositionCalculator:
    def __init__(self, config: Config, strategy_manager: StrategyManager):
        self.config = config
        self.strategy_manager = strategy_manager
        self.total_aum = config.get_total_aum()
    
    def calculate_theoretical_positions(self, exchange: str, account: str, signal_file: str, timestamp: str) -> Dict[str, float]:
        total_positions: Dict[str, float] = {}
        
        # Get account allocation
        alloc_type, alloc_value = self.config.get_account_allocation(exchange, account)
        
        # Get strategy families and their allocations
        strategy_families = self.config.get_strategy_families()
        
        # Get strategies for this account
        account_strategies = self.config.get_strategies(exchange, account)
        
        for family, family_alloc in strategy_families.items():
            if family_alloc == 0 or not account_strategies[family]:
                continue
                
            # Calculate allocation per strategy
            strategies = account_strategies[family]
            strategy_weight = family_alloc / len(strategies) if strategies else 0
            
            for strategy in strategies:
                strat_name = strategy['name']
                strat_alloc = strategy.get('allocation', 0)
                if strat_alloc == 0:
                    continue
                
                # Get strategy positions at timestamp
                positions = self.strategy_manager.get_strategy_portfolio(strat_name, signal_file, timestamp)
                
                for pos in positions:
                    # Weight position by strategy allocation, family allocation, and account allocation
                    # Express as ratio of total AUM
                    if alloc_type == 'fixed':
                        weighted_quantity = (pos.quantity * strategy_weight * strat_alloc) / (alloc_value * 100)
                    else:  # variable
                        weighted_quantity = (pos.quantity * strategy_weight * strat_alloc * alloc_value) / 100
                    # Convert to AUM ratio
                    aum_ratio = weighted_quantity / self.total_aum if self.total_aum > 0 else 0
                    total_positions[pos.symbol] = total_positions.get(pos.symbol, 0) + aum_ratio
        
        return total_positions
    
    def aggregate_positions(self, signal_files: Dict[str, str]) -> Dict[str, Dict[str, float]]:
        # Aggregate positions across all timestamps
        aggregated_positions: Dict[str, Dict[str, float]] = {}
        
        # Collect all unique timestamps across all strategies
        all_timestamps = set()
        for strat_name, signal_file in signal_files.items():
            timestamps = self.strategy_manager.get_strategy_timestamps(strat_name, signal_file)
            all_timestamps.update(timestamps)
        all_timestamps = sorted(all_timestamps)
        
        for timestamp in all_timestamps:
            timestamp_positions: Dict[str, float] = {}
            for exchange in self.config.get_exchanges():
                for account in self.config.get_accounts(exchange):
                    signal_file = signal_files.get(f"{exchange}_{account}", signal_files.get("default", "default_signals.json"))
                    account_positions = self.calculate_theoretical_positions(exchange, account, signal_file, timestamp)
                    for symbol, quantity in account_positions.items():
                        timestamp_positions[symbol] = timestamp_positions.get(symbol, 0) + quantity
            aggregated_positions[timestamp] = timestamp_positions
        
        return aggregated_positions