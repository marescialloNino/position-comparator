import json
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from strategies import StrategyManager

def main():
    config_file = 'config_pair_session_bin.json'
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Error: Config file {config_file} not found.")
        return
    
    output_dir = config.get('output_directory', 'output_bin/')
    os.makedirs(output_dir, exist_ok=True)
    
    strategy_manager = StrategyManager(config_file)
    
    # Process all active strategies
    for strat_name, strat_config in config['strategy'].items():
        if strat_config.get('active', False):
            strategy_manager.add_strategy(strat_name, strat_config['type'])
            output_file = f"{output_dir}portfolio_{strat_name}.json"
            strategy_manager.process_strategy_signals(strat_name, 'signals.log', output_file)
    
    # Plot unbalance for "test1" account
    account = "test1"
    timestamps = set()
    unbalance_data = {}
    
    # Collect unbalance for each strategy under "test1"
    for strat_name, strat_config in config['strategy'].items():
        if strat_config.get('account_trade') == account and strat_config.get('active', False):
            portfolio_file = f"{output_dir}portfolio_{strat_name}.json"
            try:
                with open(portfolio_file, 'r') as f:
                    portfolio = json.load(f)
                unbalance_data[strat_name] = {}
                for ts, data in portfolio.items():
                    unbalance_data[strat_name][ts] = data.get('portfolio_unbalance', 0.0)
                    timestamps.add(ts)
            except FileNotFoundError:
                print(f"Portfolio file {portfolio_file} not found.")
    
    # Collect unbalance for the account
    account_portfolio_file = f"{output_dir}portfolio_{account}.json"
    try:
        with open(account_portfolio_file, 'r') as f:
            account_portfolio = json.load(f)
        unbalance_data[account] = {}
        for ts, data in account_portfolio.items():
            unbalance_data[account][ts] = data.get('portfolio_unbalance', 0.0)
            timestamps.add(ts)
    except FileNotFoundError:
        print(f"Account portfolio file {account_portfolio_file} not found.")
    
    # Plotting
    if timestamps:
        # Convert timestamps to datetime and sort
        try:
            timestamps_dt = sorted([
                datetime.strptime(ts, '%Y/%m/%d %H:%M:%S.%f') for ts in timestamps
            ])
            timestamp_strings = sorted(timestamps)
        except ValueError as e:
            print(f"Error: Invalid timestamp format in portfolio files. {e}")
            return
        
        # Plotting
        plt.figure(figsize=(10, 6))
        
        # Plot each strategy's unbalance with points and connecting lines
        for label, unbalances in unbalance_data.items():
            # Collect points only where data exists
            ts_points = []
            values = []
            for ts, ts_dt in zip(timestamp_strings, timestamps_dt):
                if ts in unbalances:
                    ts_points.append(ts_dt)
                    values.append(unbalances[ts])
            plt.plot(ts_points, values, marker='o', markersize=3, label=label)  # Small points with lines
        
        plt.xlabel('Timestamp')
        plt.ylabel('Portfolio Unbalance (Sum of Quantities)')
        plt.title(f'Portfolio Unbalance for {account} Account and Strategies')
        plt.legend()
        plt.grid(True)
        
        # Limit x-axis ticks to 5
        if len(timestamps_dt) > 5:
            tick_indices = [int(i * (len(timestamps_dt) - 1) / 4) for i in range(5)]
            plt.xticks([timestamps_dt[i] for i in tick_indices], rotation=45)
        else:
            plt.xticks(timestamps_dt, rotation=45)
        
        # Format x-axis with datetime
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d %H:%M:%S'))
        
        plt.tight_layout()
        
        # Save the plot
        plot_file = f"{output_dir}{account}_account_unbalance.png"
        plt.savefig(plot_file)
        plt.close()
        print(f"Plot saved to {plot_file}")
    else:
        print("No timestamps found for plotting.")

if __name__ == '__main__':
    main()