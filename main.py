# main.py
from config import Config
from strategies import StrategyManager
from positions import PositionCalculator
from comparison import PositionComparator

def main():
    # Initialize components
    config = Config('config_pair_session_bin.json')
    strategy_manager = StrategyManager()
    
    # Register strategies and map signal files
    signal_files = {}
    for exchange in config.get_exchanges():
        for account in config.get_accounts(exchange):
            strategies = config.get_strategies(exchange, account)
            for family, strat_list in strategies.items():
                for strat in strat_list:
                    strategy_manager.add_strategy(strat['name'], family, strat)
                    signal_files[strat['name']] = f"signals_{strat['name']}.json"
    
    # Calculate aggregated theoretical positions
    calculator = PositionCalculator(config, strategy_manager)
    theoretical_positions = calculator.aggregate_positions(signal_files)
    
    # Save theoretical positions
    with open('aggregated_theoretical.json', 'w') as f:
        json.dump(theoretical_positions, f, indent=4)
    
    # Load actual positions
    actual_positions = PositionComparator.load_actual_positions('aggregated.pos')
    
    # Compare and save results
    comparator = PositionComparator()
    comparison = comparator.compare_positions(theoretical_positions, actual_positions)
    comparator.save_comparison(comparison, 'aggregated_comparison.json')
    
    # Optional: Generate per-account comparisons
    for exchange in config.get_exchanges():
        for account in config.get_accounts(exchange):
            signal_file = signal_files.get(f"{exchange}_{account}", signal_files.get("default", "default_signals.json"))
            timestamps = set()
            for strat_list in config.get_strategies(exchange, account).values():
                for strat in strat_list:
                    strat_timestamps = strategy_manager.get_strategy_timestamps(strat['name'], signal_files[strat['name']])
                    timestamps.update(strat_timestamps)
            timestamps = sorted(timestamps)
            
            account_theoretical = {}
            for timestamp in timestamps:
                account_theoretical[timestamp] = calculator.calculate_theoretical_positions(exchange, account, signal_file, timestamp)
            
            account_actual = comparator.load_actual_positions(f"{exchange}_{account}.pos")
            comparison = comparator.compare_positions(account_theoretical, account_actual)
            comparator.save_comparison(comparison, f"{exchange}_{account}_comparison.json")

if __name__ == "__main__":
    main()
