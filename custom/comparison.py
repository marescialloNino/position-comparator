import pandas as pd
from datetime import datetime, timedelta
import os

# Configuration
target_usd = 720000 / 32  # $22,500 (AUM $180,000, 4x leverage, 32 positions)
quantity_tolerance = 0.02  # 2% tolerance for quantity comparison (unused but kept)
alignment_tolerance = timedelta(minutes=5)  # 5-minute tolerance for timestamp alignment
duration_threshold = 1800  # 20 minutes in seconds for reporting discrepancies
quantity_match_tolerance = 0.07  # 7% tolerance for quantity matching
ignored_tickers = ['equity', 'imbalance']  # Tickers to ignore in pos

# Read current_state.log
log_data = []
tickers_set = set()
try:
    with open('nickel/basket3b/current_state.log', 'r') as f:
        header = f.readline().strip()  # Skip header (ts:positions)
        for line in f:
            ts_str, positions_str = line.strip().split(';')
            positions = eval('{' + positions_str + '}')
            log_data.append({'ts': ts_str, 'positions': positions})
            tickers_set.update(positions.keys())
except FileNotFoundError:
    print("Error: nickel/basket3b/current_state.log not found.")
    exit(1)

# Read current_state.pos
pos_data = {}
try:
    with open('nickel/basket3b/current_state.pos', 'r') as f:
        header = f.readline().strip()  # Skip header (ts:account)
        for line in f:
            ts_str, account_str = line.strip().split(';')
            account = eval('{' + account_str + '}')
            pos_data[ts_str] = account
except FileNotFoundError:
    print("Error: nickel/basket3b/current_state.pos not found.")
    exit(1)

# Read price_data.csv
try:
    price_df = pd.read_csv('price_data.csv')
    price_df['timestamp'] = pd.to_datetime(price_df['timestamp'], format='%Y/%m/%d %H:%M:%S.%f', errors='coerce')
except FileNotFoundError:
    print("Error: price_data.csv not found.")
    exit(1)
except Exception as e:
    print(f"Error reading price_data.csv: {e}")
    exit(1)

# Calculate theoretical quantities
active_positions = {}  # {ticker: {'entry_ts': ts, 'quantity': qty, 'direction': dir}}
theoretical_quantities = {}
previous_positions = {}

for entry in log_data:
    ts = entry['ts']
    current_positions = entry['positions']
    theoretical_quantities[ts] = {}

    try:
        ts_dt = datetime.strptime(ts, '%Y/%m/%d %H:%M:%S.%f')
        # Find closest price row within 3 minutes
        price_row = price_df.iloc[(price_df['timestamp'] - ts_dt).abs().argmin()]
        price_ts = price_row['timestamp']
        if abs((ts_dt - price_ts).total_seconds()) > 180:
            print(f"Warning: No price data within 3 minutes of {ts}")
            for ticker in current_positions:
                theoretical_quantities[ts][ticker] = None
            previous_positions = current_positions
            continue

        # Process each ticker
        for ticker, direction in current_positions.items():
            is_new = ticker not in previous_positions
            if is_new:
                price = price_row.get(ticker)
                if pd.isna(price) or price == 0:
                    print(f"{ticker} at {ts}: No valid price")
                    continue
                else:
                    quantity = (target_usd / price) * direction
                    theoretical_quantities[ts][ticker] = quantity
                    active_positions[ticker] = {
                        'entry_ts': ts,
                        'quantity': quantity,
                        'direction': direction
                    }
            else:
                if ticker in active_positions:
                    theoretical_quantities[ts][ticker] = active_positions[ticker]['quantity']
                else:
                    print(f"{ticker} at {ts}: Not in active_positions, treating as new")
                    price = price_row.get(ticker)
                    if pd.isna(price) or price == 0:
                        continue
                    else:
                        quantity = (target_usd / price) * direction
                        theoretical_quantities[ts][ticker] = quantity
                        active_positions[ticker] = {
                            'entry_ts': ts,
                            'quantity': quantity,
                            'direction': direction
                        }

        # Remove closed positions
        closed_tickers = [t for t in previous_positions if t not in current_positions]
        for ticker in closed_tickers:
            active_positions.pop(ticker)
            print(f"Closed position for {ticker} at {ts}")

        previous_positions = current_positions.copy()

    except Exception as e:
        print(f"Error processing timestamp {ts}: {e}")
        for ticker in current_positions:
            theoretical_quantities[ts][ticker] = None

# Prepare log_df and pos_df
log_df = pd.DataFrame([{'ts_dt': datetime.strptime(entry['ts'], '%Y/%m/%d %H:%M:%S.%f'), 'positions': entry['positions']} for entry in log_data])
pos_df = pd.DataFrame([{'ts_dt': datetime.strptime(ts, '%Y/%m/%d %H:%M:%S.%f'), 'positions': pos} for ts, pos in pos_data.items()])

# Function to find closest pos timestamp
def find_closest_pos(log_ts, pos_df, tolerance):
    pos_ts_diff = (pos_df['ts_dt'] - log_ts).abs()
    if pos_ts_diff.min() <= tolerance:
        return pos_df.iloc[pos_ts_diff.idxmin()]
    return None

# Track discrepancies
ongoing = {}  # {(ticker, type): (start_ts, last_seen_ts)} for ticker mismatches
ongoing_quantity = {}  # {(ticker, 'Quantity Mismatch'): (start_ts, last_seen_ts, theoretical_qty, actual_qty)} for quantity mismatches
discrepancies = []
quantity_discrepancies = []

for log_row in log_df.itertuples():
    log_ts = log_row.ts_dt
    log_ts_str = log_data[log_row.Index]['ts']
    log_tickers = set(log_row.positions.keys())

    # Find closest pos timestamp within alignment tolerance
    pos_row = find_closest_pos(log_ts, pos_df, alignment_tolerance)
    pos_tickers = set(pos_row.positions.keys()) - set(ignored_tickers) if pos_row is not None else set()

    # Identify ticker discrepancies
    current_missing = log_tickers - pos_tickers
    current_extra = pos_tickers - log_tickers

    # Update or start ticker discrepancies
    for ticker in current_missing:
        key = (ticker, 'Missing Ticker')
        if key not in ongoing:
            ongoing[key] = (log_ts, log_ts)
        else:
            ongoing[key] = (ongoing[key][0], log_ts)

    for ticker in current_extra:
        if ticker in ignored_tickers:
            continue
        key = (ticker, 'Extra Ticker')
        if key not in ongoing:
            ongoing[key] = (log_ts, log_ts)
        else:
            ongoing[key] = (ongoing[key][0], log_ts)

    # Resolve ticker discrepancies
    for key in list(ongoing.keys()):
        ticker, disc_type = key
        if (disc_type == 'Missing Ticker' and ticker not in current_missing) or \
           (disc_type == 'Extra Ticker' and ticker not in current_extra):
            start_ts, end_ts = ongoing.pop(key)
            duration = (end_ts - start_ts).total_seconds()
            if duration > duration_threshold:
                discrepancies.append({
                    'ticker': ticker,
                    'type': disc_type,
                    'start': start_ts.strftime('%Y/%m/%d %H:%M:%S.%f'),
                    'end': end_ts.strftime('%Y/%m/%d %H:%M:%S.%f'),
                    'duration_seconds': duration,
                    'details': f"{ticker} {'in log but missing in pos' if disc_type == 'Missing Ticker' else 'in pos but missing in log'} at {start_ts}"
                })

    # Check quantity mismatches for common tickers
    if pos_row is not None:
        theoretical_qty_dict = theoretical_quantities.get(log_ts_str, {})
        for ticker in log_tickers & pos_tickers:
            theoretical_qty = theoretical_qty_dict.get(ticker)
            actual_qty = pos_row.positions.get(ticker)
            if theoretical_qty is not None and actual_qty is not None:
                key = (ticker, 'Quantity Mismatch')
                # Check for quantity mismatch within 7% tolerance
                if abs(theoretical_qty) > 1e-6:  # Avoid division by zero
                    relative_diff = abs(actual_qty - theoretical_qty) / abs(theoretical_qty)
                else:
                    relative_diff = float('inf') if abs(actual_qty) > 1e-6 else 0
                if relative_diff > quantity_match_tolerance:
                    if key not in ongoing_quantity:
                        ongoing_quantity[key] = (log_ts, log_ts, theoretical_qty, actual_qty)
                    else:
                        ongoing_quantity[key] = (ongoing_quantity[key][0], log_ts, theoretical_qty, actual_qty)
                else:
                    if key in ongoing_quantity:
                        start_ts, end_ts, start_theoretical_qty, start_actual_qty = ongoing_quantity.pop(key)
                        duration = (end_ts - start_ts).total_seconds()
                        if duration > duration_threshold:
                            quantity_discrepancies.append({
                                'ticker': ticker,
                                'type': 'Quantity Mismatch',
                                'start': start_ts.strftime('%Y/%m/%d %H:%M:%S.%f'),
                                'end': end_ts.strftime('%Y/%m/%d %H:%M:%S.%f'),
                                'duration_seconds': duration,
                                'theoretical_quantity': start_theoretical_qty,
                                'actual_quantity': start_actual_qty,
                                'details': f"{ticker} quantity mismatch at {start_ts}: theoretical={start_theoretical_qty:.2f}, actual={start_actual_qty:.2f}"
                            })

# Handle ongoing discrepancies at the end
for key, (start_ts, last_seen_ts) in list(ongoing.items()):
    duration = (last_seen_ts - start_ts).total_seconds()
    if duration > duration_threshold:
        ticker, disc_type = key
        discrepancies.append({
            'ticker': ticker,
            'type': disc_type,
            'start': start_ts.strftime('%Y/%m/%d %H:%M:%S.%f'),
            'end': last_seen_ts.strftime('%Y/%m/%d %H:%M:%S.%f'),
            'duration_seconds': duration,
            'details': f"{ticker} {'in log but missing in pos' if disc_type == 'Missing Ticker' else 'in pos but missing in log'} at {start_ts}"
        })

for key, (start_ts, last_seen_ts, start_theoretical_qty, start_actual_qty) in list(ongoing_quantity.items()):
    duration = (last_seen_ts - start_ts).total_seconds()
    if duration > duration_threshold:
        ticker = key[0]
        quantity_discrepancies.append({
            'ticker': ticker,
            'type': 'Quantity Mismatch',
            'start': start_ts.strftime('%Y/%m/%d %H:%M:%S.%f'),
            'end': last_seen_ts.strftime('%Y/%m/%d %H:%M:%S.%f'),
            'duration_seconds': duration,
            'theoretical_quantity': start_theoretical_qty,
            'actual_quantity': start_actual_qty,
            'details': f"{ticker} quantity mismatch at {start_ts}: theoretical={start_theoretical_qty:.2f}, actual={start_actual_qty:.2f}"
        })

# Output discrepancies
if discrepancies or quantity_discrepancies:
    print("Discrepancies found:")
    if discrepancies:
        print("Ticker Discrepancies:")
        for d in discrepancies:
            print(f"{d['type']} for {d['ticker']}: from {d['start']} to {d['end']} (duration: {d['duration_seconds']} seconds) - {d['details']}")
    if quantity_discrepancies:
        print("Quantity Discrepancies:")
        for d in quantity_discrepancies:
            print(f"{d['type']} for {d['ticker']}: from {d['start']} to {d['end']} (duration: {d['duration_seconds']} seconds) - {d['details']}")
else:
    print("No discrepancies found.")

# Save ticker discrepancies to CSV
discrepancies_df = pd.DataFrame(discrepancies)
if not discrepancies_df.empty:
    discrepancies_df.to_csv('discrepancies.csv', index=False)
    print("Ticker discrepancies saved to discrepancies.csv")
else:
    print("No ticker discrepancies to save.")

# Save quantity discrepancies to CSV
quantity_discrepancies_df = pd.DataFrame(quantity_discrepancies)
if not quantity_discrepancies_df.empty:
    quantity_discrepancies_df.to_csv('quantity_discrepancies.csv', index=False)
    print("Quantity discrepancies saved to quantity_discrepancies.csv")
else:
    print("No quantity discrepancies to save.")

# Save theoretical quantities
theoretical_df = []
for ts in theoretical_quantities:
    for ticker, quantity in theoretical_quantities[ts].items():
        theoretical_df.append({
            'timestamp': ts,
            'ticker': ticker,
            'theoretical_quantity': quantity
        })
theoretical_df = pd.DataFrame(theoretical_df)
theoretical_df.to_csv('theoretical_quantities.csv', index=False)
print("Theoretical quantities saved to theoretical_quantities.csv")