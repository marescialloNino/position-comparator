import json
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from strategies import StrategyManager

def main():
    config_file = 'config_test.json'
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Error: Config file {config_file} not found.")
        return
    
    output_dir = config.get('output_directory', 'output_bin/')
    os.makedirs(output_dir, exist_ok=True)
    
    strategy_manager = StrategyManager(config_file)
    
    # Add all active strategies
    print("Adding active strategies:")
    for strat_name, strat_config in config['strategy'].items():
        if strat_config.get('active', False):
            print(f"Adding strategy: {strat_name}")
            strategy_manager.add_strategy(strat_name, strat_config['type'], strat_config)
    
    # Process all strategy portfolios
    strategy_manager.process_all_strategies('session_bin_signal.log')
    
    # Build account portfolios
    accounts = set()
    for strat_name, strat_config in config['strategy'].items():
        if strat_config.get('active', False):
            account = strat_config.get('account_trade')
            if account:
                accounts.add(account)
    for account in accounts:
        print(f"Building portfolio for account: {account}")
        strategy_manager.build_account_portfolio(account)
    

if __name__ == '__main__':
    main()