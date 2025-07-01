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

from pywebio.output import put_datatable, JSFunction, output_register_callback
from pywebio.utils import random_str

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
    except:
        df = pd.DataFrame()
    return df

def multistrategy_matching_to_df(data_dict, account_key, details_dict=None):
    """
    Convert multistrategy matching JSON and details JSON to a DataFrame with USD amounts.
    :param data_dict: Dictionary from /multistrategy_matching endpoint.
    :param account_key: The account key (e.g., 'bitget_2') to filter.
    :param details_dict: Dictionary from /multistrategy_position_details endpoint for prices and strategy counts.
    :return: DataFrame with columns token, theo_amount, real_amount, ref_price, executing, matching, strategy_count.
    """
    try:
        if account_key in data_dict:
            data = data_dict[account_key]
            df = pd.DataFrame.from_dict(data, orient='index')
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'token'}, inplace=True)
            df = df[['token', 'theo_qty', 'real_qty', 'executing', 'matching']]
            if details_dict:
                details_df = pd.DataFrame.from_dict(details_dict, orient='index')
                details_df.reset_index(inplace=True)
                details_df.rename(columns={'index': 'token'}, inplace=True)
                df = df.merge(details_df[['token', 'ref_price', 'strategy_count']], on='token', how='left')
                df['theo_amount'] = (df['theo_qty'] * df['ref_price']).round(0).astype(int)
                df['real_amount'] = (df['real_qty'] * df['ref_price']).round(0).astype(int)
                df['ref_price'] = df['ref_price'].fillna(0.0)
                df['strategy_count'] = df['strategy_count'].fillna(0).astype(int)
                df = df[['token', 'theo_amount', 'real_amount', 'ref_price', 'executing', 'matching', 'strategy_count']]
            else:
                df['theo_amount'] = 0
                df['real_amount'] = 0
                df['ref_price'] = 0.0
                df['strategy_count'] = 0
                df = df[['token', 'theo_amount', 'real_amount', 'ref_price', 'executing', 'matching', 'strategy_count']]
        else:
            df = pd.DataFrame(columns=['token', 'theo_amount', 'real_amount', 'ref_price', 'executing', 'matching', 'strategy_count'])
    except Exception as e:
        print(f"Error converting multistrategy matching data: {e}")
        df = pd.DataFrame(columns=['token', 'theo_amount', 'real_amount', 'ref_price', 'executing', 'matching', 'strategy_count'])
    return df

def status_req(account):
    uri = GATEWAY + '/status'
    response = get_any(uri, {})
    if response.ok:
        clear('status_rez')
        status_dict = {}
        try:
            status_dict = json.loads(response.content.decode())
        except:
            pass
        exchange, strat = account.split(':')
        if exchange in status_dict and strat in status_dict[exchange]:
            df = dict_to_df(status_dict[exchange][strat], True)
            with use_scope('status_rez'):
                put_html(df.to_html())

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

def set_req(account):
    exchange, account = account.split(':')
    params = {'exchange': exchange, 'account': account}
    uri = GATEWAY + '/pose'
    response = get_any(uri, params)

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

def pnl_req():
    uri = GATEWAY + '/pnl'
    response = get_any(uri, params={})
    if response.ok:
        clear('pnl_rez')
        pnl_dict = {}
        try:
            pnl_dict = json.loads(response.content.decode())
        except:
            pass
        df = dict_to_df(pnl_dict, False)
        with use_scope('pnl_rez'):
            put_html(df.to_html(float_format='{:5.2f}'.format))

def multistrategy_matching_req(account):
    exchange, account = account.split(':')
    session_name = exchange.replace('_fut', '')
    account_key = f"{session_name}_{account}"
    
    # Fetch multistrategy matching data
    uri_matching = GATEWAY + '/multistrategy_matching'
    response_matching = get_any(uri_matching, params={})
    print(f"Matching response status: {response_matching.status_code}, content: {response_matching.content.decode()}")
    
    # Fetch position details for prices and strategy counts
    uri_details = GATEWAY + '/multistrategy_position_details'
    params_details = {'session': session_name, 'account_key': account_key}
    response_details = get_any(uri_details, params=params_details)
    print(f"Position details response status: {response_details.status_code}, content: {response_details.content.decode()}")
    
    clear('multistrategy_matching_rez')
    matching_dict = {}
    try:
        matching_dict = json.loads(response_matching.content.decode())
        print(f"Matching dict: {matching_dict}")
    except Exception as e:
        print(f"JSON parse error for matching: {e}")
    
    details_dict = None
    if response_details.ok:
        try:
            details_dict = json.loads(response_details.content.decode())
            print(f"Position details dict: {details_dict}")
        except Exception as e:
            print(f"JSON parse error for details: {e}")
    
    df = multistrategy_matching_to_df(matching_dict, account_key, details_dict)
    print(f"DataFrame: {df}")
    
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
        if details_dict is None:
            put_text("Warning: Unable to fetch price and strategy count data. Showing quantities only.")
            put_html(df[['token', 'theo_qty', 'real_qty', 'executing', 'matching']].to_html(
                formatters={
                    'theo_qty': lambda x: f'{x:.2f}',
                    'real_qty': lambda x: f'{x:.2f}'
                }, classes='card'))
        else:
            put_html(df.to_html(formatters={
                'theo_amount': lambda x: f'{x:d}',
                'real_amount': lambda x: f'{x:d}',
                'ref_price': format_ref_price,
                'strategy_count': lambda x: f'{x:d}'
            }, classes='card'))
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
                        account_dict[exchange].add(strat_param['account_trade'])
                        multi_account_dict[exchange].add(strat_param['account_trade'])
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