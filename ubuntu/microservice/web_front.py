import os
import argparse
import yaml
import json
import requests
import pandas as pd
from pywebio import start_server
from pywebio.output import (
    put_markdown, put_buttons, put_text, clear, put_datatable, use_scope, put_button,
    put_html, put_column, put_row, put_tabs, put_table, put_link, put_scope
)
from pywebio.input import slider
from pywebio.session import info as session_info
from pywebio.session import set_env

GATEWAY = 'http://192.168.63.7:14440'

def get_any(upi, params):
    response = requests.get(upi, params=params)
    return response

def dict_to_df(data_dict, mode=True):
    try:
        if mode:
            df = pd.DataFrame.from_dict(data_dict, orient="index").stack().to_frame()
            df = pd.DataFrame(df[0].values.tolist(), index=df.index)
        else:
            dic_new = {(outerKey, innerKey, secondKey): values for outerKey, innerDict in data_dict.items()
                       for innerKey, secondDict in innerDict.items() for secondKey, values in secondDict.items()}
            df = pd.DataFrame(dic_new).T
            df.sort_index(axis=1, inplace=True)
    except Exception as e:
        print(f"Error in dict_to_df: {e}")
        df = pd.DataFrame()
    return df

def multistrategy_matching_to_df(details_dict):
    """
    Convert multistrategy position details JSON to a main DataFrame and a summary DataFrame for exposures.
    Excludes positions marked as dust (is_dust: True) from the main DataFrame.
    :param details_dict: Dictionary from /multistrategy_position_details endpoint.
    :return: Tuple of (main DataFrame, summary DataFrame) with main_df columns token, theo_amount, real_amount, ref_price, executing, matching, strategy_count, is_dust, is_mismatch, and summary_df with columns Net Exposure, Gross Exposure and rows Theo, Real.
    """
    try:
        # Create main DataFrame
        main_df = pd.DataFrame.from_dict(details_dict, orient='index')
        main_df.reset_index(inplace=True)
        main_df.rename(columns={'index': 'token'}, inplace=True)
        main_df = main_df[['token', 'theo_qty', 'real_qty', 'theo_amount', 'real_amount', 'ref_price', 'executing', 'matching', 'strategy_count', 'is_dust', 'is_mismatch','mismatch_count']]
        main_df['theo_amount'] = main_df['theo_amount'].fillna(0).astype(int)
        main_df['real_amount'] = main_df['real_amount'].fillna(0).astype(int)
        main_df['ref_price'] = main_df['ref_price'].fillna(0.0)
        main_df['strategy_count'] = main_df['strategy_count'].fillna(0).astype(int)
        main_df['is_dust'] = main_df['is_dust'].fillna(False)
        main_df['is_mismatch'] = main_df['is_mismatch'].fillna(False)
        
        # Create summary DataFrame (includes all positions, even dust)
        summary_df = pd.DataFrame({
            'Net Exposure': [int(main_df['theo_amount'].sum()), int(main_df['real_amount'].sum())],
            'Gross Exposure': [int(main_df['theo_amount'].abs().sum()), int(main_df['real_amount'].abs().sum())]
        }, index=['Theo', 'Real'])
        
        # Filter out dust positions from main DataFrame
        main_df = main_df[~main_df['is_dust']]
        main_df = main_df[['token','theo_amount', 'real_amount', 'executing', 'matching', 'strategy_count','mismatch_count']]
        
        return main_df, summary_df
    except Exception as e:
        print(f"Error converting multistrategy matching data: {e}")
        main_df = pd.DataFrame(columns=['token','theo_amount', 'real_amount', 'executing', 'matching', 'strategy_count','mismatch_count'])
        summary_df = pd.DataFrame({
            'Net Exposure': [0, 0],
            'Gross Exposure': [0, 0]
        }, index=['Theo', 'Real'])
        return main_df, summary_df

def status_req(account):
    uri = GATEWAY + '/status'
    response = get_any(uri, {})
    if response.ok:
        clear('status_rez')
        status_dict = {}
        try:
            status_dict = json.loads(response.content.decode())
        except Exception as e:
            print(f"JSON parse error for status: {e}")
        exchange, strat = account.split(':')
        if exchange in status_dict and strat in status_dict[exchange]:
            df = dict_to_df(status_dict[exchange][strat], True)
            with use_scope('status_rez'):
                put_html(df.to_html())
        else:
            with use_scope('status_rez'):
                put_text(f"No status data for {exchange}:{strat}")

def multiply_req(account):
    exchange, account = account.split(':')
    def display_factor(factor):
        with use_scope('liq_factor'):
            clear()
            put_text(f'Multiplication: {factor / 10}')
    with use_scope('liquidate_rez'):
        put_scope('liq_factor')
        display_factor(10)
        factor = slider('factor', min_value=0, max_value=20, value=10, onchange=display_factor)
    factor = float(factor / 10)
    params = {'exchange': exchange, 'account': account, 'factor': factor}
    uri = GATEWAY + '/multiply'
    response = get_any(uri, params)
    if response.ok:
        clear('liquidate_rez')
        with use_scope('liquidate_rez'):
            put_html(response.content.decode())
    else:
        with use_scope('liquidate_rez'):
            put_text(f"Error: Multiply endpoint returned status {response.status_code}: {response.text}")

def set_req(account):
    exchange, account = account.split(':')
    params = {'exchange': exchange, 'account': account}
    uri = GATEWAY + '/pose'
    response = get_any(uri, params)
    if response.ok:
        clear('adjust_req')
        with use_scope('adjust_req'):
            put_html(response.content.decode())
    else:
        with use_scope('adjust_req'):
            put_text(f"Error: Pose endpoint returned status {response.status_code}: {response.text}")

def matching_req(account):
    exchange, strat = account.split(':')
    params = {'exchange': exchange, 'strat': strat}
    uri = GATEWAY + '/matching'
    response = get_any(uri, params)
    if response.ok:
        clear('matching_rez')
        with use_scope('matching_rez'):
            put_html('<div class="metrics-container">')
            put_html(response.content.decode())
            put_html('</div>')
            figname = f'temp/{exchange}_{strat}_fig1.html'
            if os.path.exists(figname):
                with open(figname, 'r') as figfile:
                    figure = figfile.read()
                    put_html(figure)
            figname = f'temp/{exchange}_{strat}_fig2.html'
            if os.path.exists(figname):
                with open(figname, 'r') as figfile:
                    figure = figfile.read()
                    put_html(figure)
            figname = f'temp/{exchange}_{strat}_fig3.html'
            if os.path.exists(figname):
                with open(figname, 'r') as figfile:
                    figure = figfile.read()
                    put_html(figure)
    else:
        with use_scope('matching_rez'):
            put_text(f"Error: Matching endpoint returned status {response.status_code}: {response.text}")

def pnl_req():
    uri = GATEWAY + '/pnl'
    response = get_any(uri, params={})
    if response.ok:
        clear('pnl_rez')
        pnl_dict = {}
        try:
            pnl_dict = json.loads(response.content.decode())
        except Exception as e:
            print(f"JSON parse error for pnl: {e}")
        df = dict_to_df(pnl_dict, False)
        with use_scope('pnl_rez'):
            put_html(df.to_html(float_format='{:5.2f}'.format))
    else:
        with use_scope('pnl_rez'):
            put_text(f"Error: PnL endpoint returned status {response.status_code}: {response.text}")

def multistrategy_matching_req(account):
    exchange, account = account.split(':')
    session_name = exchange.replace('_fut', '')
    account_key = f"{account}"
    
    # Fetch multistrategy position details
    uri_details = GATEWAY + '/multistrategy_position_details'
    params_details = {'session': session_name, 'account_key': account_key}
    response_details = get_any(uri_details, params=params_details)
    print(f"Position details response status: {response_details.status_code}")
    
    clear('multistrategy_matching_rez')
    details_dict = {}
    if response_details.ok:
        try:
            details_dict = json.loads(response_details.content.decode())
        except Exception as e:
            print(f"JSON parse error for details: {e}")
            with use_scope('multistrategy_matching_rez'):
                put_text(f"Error: Failed to parse position details: {response_details.text}")
            return
    else:
        with use_scope('multistrategy_matching_rez'):
            put_text(f"Error: Position details endpoint returned status {response_details.status_code}: {response_details.text}")
        return
    
    main_df, summary_df = multistrategy_matching_to_df(details_dict)
    main_df = main_df.sort_values(by='theo_amount', ascending=False)
    
    # Log the number of dust positions filtered
    dust_count = len(details_dict) - len(main_df)
    print(f"Filtered {dust_count} dust positions for {session_name}:{account_key}")
    
    # Custom formatter for ref_price: 2 decimals if >= 1, 3 significant figures if < 1
    def format_ref_price(x):
        if pd.isna(x) or x == 0.0:
            return '0.00'
        elif x >= 1:
            return f'{x:.2f}'
        else:
            return f'{x:.3g}'
    
    with use_scope('multistrategy_matching_rez'):
        put_html('<div class="metrics-container">')
        # Display summary DataFrame
        put_html(summary_df.to_html(
            formatters={
                'Net Exposure': lambda x: f'{x:.0f}' if pd.notna(x) else 'N/A',
                'Gross Exposure': lambda x: f'{x:.0f}' if pd.notna(x) else 'N/A'
            },
            classes='card',
            index=False
        ))
        put_html('<br>')  # Add spacing between summary and main table
        # Display main table (dust positions already excluded)
        put_html(main_df.to_html(
            formatters={
                'theo_amount': lambda x: f'{x:.0f}' if pd.notna(x) else 'N/A',
                'real_amount': lambda x: f'{x:.0f}' if pd.notna(x) else 'N/A',
                'ref_price': format_ref_price,
                'strategy_count': lambda x: f'{x:d}',
                'is_dust': lambda x: str(x),
                'is_mismatch': lambda x: str(x)
            },
            classes='card',
            index=False
        ))
        put_html('</div>')

def main():
    global CONFIG
    session_config = CONFIG['session']
    config_files = {exchange: session_config[exchange]['config_file'] for exchange in session_config}

    put_html("""
    <style>
        .dashboard-container {
            width: 100%;
            max-width: 1200px;
        }
        .card {
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 15px;
            background: white;
        }
        @media (min-width: 768px) {
            .metrics-container {
                display: flex;
                justify-content: space-between;
            }
            .metric-card {
                width: 32%;
            }
        }
        @media (max-width: 767px) {
            .metric-card {
                width: 100%;
            }
            .chart-container {
                height: 300px !important;
            }
        }
    </style>
    """)
    put_markdown("# Au menu ce soir").style('text-align: center')

    put_column([
        put_tabs([
            {'title': 'PnL', 'content': put_scope('pnl')},
            {'title': 'matching', 'content': put_scope('matching')},
            {'title': 'status', 'content': put_scope('status')},
            {'title': 'multiply', 'content': put_scope('multiply')},
            {'title': 'adjust', 'content': put_scope('adjust')},
            {'title': 'Multistrategy Matching', 'content': put_scope('multistrategy_matching')}
        ])]
    )

    account_dict = {}
    multi_account_dict = {}

    for exchange, config_file in config_files.items():
        if exchange not in account_dict:
            account_dict[exchange] = set()
            multi_account_dict[exchange] = set()
        if os.path.exists(config_file):
            with open(config_file, 'r') as myfile:
                strat_params = json.load(myfile)
                for strat, strat_param in strat_params['strategy'].items():
                    if strat_param['active'] and strat_param['send_orders'] != 'dummy':
                        account_key = f"{strat_param['exchange_trade']}_{strat_param['account_trade']}"
                        account_dict[exchange].add(account_key)
                        multi_account_dict[exchange].add(account_key)
                with use_scope('multiply'):
                    exchange_label = exchange
                    if 'bin' in exchange and 'spot' not in exchange:
                        exchange_label = 'bin_fut'
                    elif 'bitget' in exchange:
                        exchange_label = 'bitget_fut'
                    put_buttons([exchange_label + ':' + str(account) for account in account_dict[exchange]], onclick=multiply_req)
                with use_scope('matching'):
                    put_buttons([exchange + ':' + str(strat) for strat, strat_param in strat_params['strategy'].items() if
                                 strat_param['active']], onclick=matching_req)
                with use_scope('status'):
                    put_buttons([exchange + ':' + str(strat) for strat, strat_param in strat_params['strategy'].items() if
                                 strat_param['active']], onclick=status_req)
                with use_scope('adjust'):
                    exchange_label = exchange
                    if 'bin' in exchange and 'spot' not in exchange:
                        exchange_label = 'bin_fut'
                    elif 'bitget' in exchange:
                        exchange_label = 'bitget_fut'
                    put_buttons([exchange_label + ':' + str(account) for account in account_dict[exchange]], onclick=set_req)
                with use_scope('multistrategy_matching'):
                    exchange_label = exchange
                    if 'bin' in exchange and 'spot' not in exchange:
                        exchange_label = 'bin_fut'
                    elif 'bitget' in exchange:
                        exchange_label = 'bitget_fut'
                    put_buttons([exchange_label + ':' + str(account) for account in multi_account_dict[exchange]],
                                onclick=multistrategy_matching_req)

    with use_scope('multiply'):
        put_scope('multiply_req')
    with use_scope('matching'):
        put_scope('matching_rez')
    with use_scope('pnl'):
        put_button('Get PnL', onclick=pnl_req)
        put_scope('pnl_rez')
    with use_scope('status'):
        put_scope('status_rez')
    with use_scope('adjust'):
        put_scope('adjust_req')
    with use_scope('multistrategy_matching'):
        put_scope('multistrategy_matching_rez')

if __name__ == '__main__':
    global CONFIG
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="input file", default='')
    args = parser.parse_args()
    config_file = args.config

    if config_file != '':
        with open(config_file, 'r') as myfile:
            CONFIG = yaml.load(myfile, Loader=yaml.FullLoader)
    else:
        CONFIG = {}
    start_server(main, debug=True, port=8881)
    set_env(title='Tartineur furtif', output_animation=False)