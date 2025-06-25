import os
import argparse
import yaml
import json
import requests

import pandas as pd
from pywebio import start_server
from pywebio.output import (put_markdown, put_buttons, put_text, clear, put_datatable, use_scope, put_button,
                            put_html, put_column, put_row, put_tabs, put_table, put_link, put_scope)
from pywebio.input import slider
from pywebio.session import info as session_info
from pywebio.session import set_env


GATEWAY = 'http://localhost:14440'
#GATEWAY = 'http://54.249.107.88:14440'


def get_any(upi, params):
    response = requests.get(upi, params=params)
    return response


def main():
    global CONFIG
    session_config = CONFIG['session']
    config_files = {exchange: session_config[exchange]['config_file'] for exchange in session_config}

    put_html("""
    <style>
        /* Base styles */
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

        /* Desktop styles */
        @media (min-width: 768px) {
            .metrics-container {
                display: flex;
                justify-content: space-between;
            }

            .metric-card {
                width: 32%;
            }
        }

        /* Mobile styles */
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
            {'title': 'adjust', 'content': put_scope('adjust')}
        ])]
    )

    account_dict = {}

    for exchange, config_file in config_files.items():
        if exchange not in account_dict:
            account_dict[exchange] = set()
        if os.path.exists(config_file):
            with open(config_file, 'r') as myfile:
                strat_params = json.load(myfile)
                for strat, strat_param in strat_params['strategy'].items():
                    if strat_param['active'] and strat_param['send_orders'] == 'web':
                        account_dict[exchange].add(strat_param['account_trade'])
                with use_scope('multiply'):
                    exchange_label = exchange
                    if 'bin' in exchange and 'spot' not in exchange:
                        exchange_label = 'bin_fut'
                    elif 'bitget' in exchange:
                        exchange_label = 'bitget_fut'
                    put_buttons([exchange_label + ':' + str(account) for account in account_dict[exchange]], onclick=multiply_req)
                with use_scope('matching'):
                    put_buttons([exchange + ':' + strat for strat, strat_param in strat_params['strategy'].items() if
                                 strat_param['active']], onclick=matching_req)
                with use_scope('status'):
                    put_buttons([exchange + ':' + strat for strat, strat_param in strat_params['strategy'].items() if
                                 strat_param['active']], onclick=status_req)
                with use_scope('adjust'):
                    exchange_label = exchange
                    if 'bin' in exchange and 'spot' not in exchange:
                        exchange_label = 'bin_fut'
                    put_buttons([exchange_label + ':' + str(account) for account in account_dict[exchange]], onclick=set_req)
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


def dict_to_df(data_dict, mode=True):
    try:
        if mode:
            df = pd.DataFrame.from_dict(data_dict, orient="index").stack().to_frame()
            df = pd.DataFrame(df[0].values.tolist(), index=df.index)
        else:
            dic_new = {(outerKey, innerKey, secondKey): values for outerKey, innerDict in data_dict.items()
                       for innerKey, secondDict in
                       innerDict.items() for secondKey, values in secondDict.items()}
            df = pd.DataFrame(dic_new).T
            df.sort_index(axis=1, inplace=True)
    except:
        df = pd.DataFrame()
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
    # clear('adjust_req')
    # if not response.ok:
    #     pass
    # positions = json.loads(response.content.decode())
    # with use_scope('adjust_req'):
    #     for item in positions.get('current', []):
    #     put_buttons()
    #
    # uri = GATEWAY + '/multiply'
    # response = get_any(uri, params)
    # if response.ok:
    #     clear('liquidate_rez')
    #     with use_scope('liquidate_rez'):
    #         put_html(response.content.decode())

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
    start_server(main, debug=True, port=8880)
    set_env(title='Tartineur furtif', output_animation=False)

