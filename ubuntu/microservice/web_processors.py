import json
import sys
import asyncio
import aiofiles
import threading
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import base64
from io import BytesIO
from enum import Enum
import argparse
import traceback
import yaml
from io import StringIO

from datetime import datetime, timedelta
try:
    from datetime import UTC
except:
    from datetime import timezone
    UTC = timezone.utc
from fastapi import FastAPI, HTTPException
from starlette.responses import JSONResponse, HTMLResponse
import uvicorn
import dotenv

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from datafeed.utils_online import NpEncoder, parse_pair, utc_ize, today_utc
from datafeed.broker_handler import BrokerHandler
from web_broker import WebSpreaderBroker
from reporting.bot_reporting import TGMessenger

global app

SignalType = Enum('SignalType', [('STOP', 'stop'),
                                 ('REFRESH', 'refresh'),
                                 ('PNL', 'check_pnl'),
                                 ('POS', 'check_pos'),
                                 ('RUNNING', 'check_running'),
                                 ('FILE', 'update_file'),])

"""
UPI:
/status
/pnl
/pose?exchange=x&account=n
/matching?exchange=bybit&strat=pairs1
"""


def last_modif(hearbeat_file):
    if os.path.exists(hearbeat_file):
        last_date = utc_ize(os.path.getmtime(hearbeat_file))
        now = today_utc()
        age = (now - last_date).total_seconds()

        return age
    else:
        return None


def date_parser(date_str: str):
    if ':00+' in date_str:
        s2 = date_str.replace(' ', 'T')
    else:
        s1 = date_str.split('+')
        s2 = s1[0].split('.')[0].replace(' ', 'T') + '+' + s1[1]
    return datetime.strptime(s2, '%Y-%m-%dT%H:%M:%S%z')


async def read_pnl_file(pnl_file):
    try:
        async with aiofiles.open(pnl_file, 'r') as myfile:
            content = await myfile.read()
        df = pd.read_csv(StringIO(content), sep=',', index_col=[0], converters={0: date_parser})
    except:
        df = pd.DataFrame()

    return df


async def read_aum_file(aum_file, exchange):
    try:
        async with aiofiles.open(aum_file, 'r') as myfile:
            content = await myfile.read()
        df = pd.read_csv(StringIO(content), sep=';', header=None, converters={0:date_parser})

        if df.shape[1] == 2:
            df.columns = ['date', 'aum']
        elif df.shape[1] == 4:
            df.columns = ['date', 'free', 'total', 'asset']
            df['aum'] = df['total']
            if 'bitg' in exchange:
                df['aum'] += df['asset']
        else:
            df = pd.DataFrame()
    except:
        df = pd.DataFrame()

    return df


class Processor:
    '''
    Responsibility: interface between web query and strat status and commands
    trigger a refresh of pairs
    get status of each strat
    get pos
    '''

    def __init__(self, config):
        self.session_config = config['session']
        self.config_files = {exchange: self.session_config[exchange]['config_file'] for exchange in self.session_config}
        fmt = logging.Formatter('{asctime}:{levelname}:{name}:{message}', style='{')
        handler = TimedRotatingFileHandler(filename='output/web_processor.log',
                                           when="midnight", interval=1, backupCount=7)
        handler.setFormatter(fmt)
        logging.getLogger().setLevel(logging.INFO)
        logging.getLogger().addHandler(handler)

        self.echange_configs = {}
        for exchange, config_file in self.config_files.items():
            if os.path.exists(config_file):
                with open(config_file, 'r') as myfile:
                    params = json.load(myfile)
                    self.echange_configs[exchange] = params
            else:
                logging.warning(f'No config file for {exchange} with name {config_file}')
        self.aum = {}
        self.summaries = {}
        self.pnl = {}
        self.positions = {}
        self.matching = {}
        self.last_message_count = 0
        self.last_message = []

    async def update_config(self, exchange, config_file):
        if os.path.exists(config_file):
            logging.info(f'Updating {exchange} config {config_file}')
            async with aiofiles.open(config_file, 'r') as myfile:
                try:
                    content = await myfile.read()
                    params = json.loads(content)
                    self.echange_configs[exchange] = params
                except Exception as e:
                    logging.error(f'Unreadable config file {config_file}')
        else:
            logging.warning(f'No config file to update for {exchange} with name {config_file}')


    async def update_pnl(self, exchange, working_directory, strategy_name, strategy_param):
        logging.info('updating pnl for %s, %s', exchange, strategy_name)
        tz_string = datetime.now().astimezone().tzinfo
        now = pd.Timestamp(datetime.today()).tz_localize(tz_string).tz_convert('UTC')

        strategy_directory = os.path.join(working_directory, strategy_name)
        pnl_file = os.path.join(strategy_directory, 'pnl.csv')
        aum_file = os.path.join(strategy_directory, 'aum.csv')

        days = [2, 7, 30, 90, 180]
        if os.path.exists(pnl_file) and os.path.exists(aum_file):
            last_date = {day: now - timedelta(days=day) for day in days}

            try:
                pnl = await read_pnl_file(pnl_file)
                alloc_label = [col for col in pnl.columns if 'alloc' in col]
                if len(alloc_label) > 0:
                    label = alloc_label[0]
                else:
                    label = 'allocation'
                    pnl['allocation'] = 10
                pnl[label] = pnl[label].bfill()
                last = {day: round(pnl.loc[pnl.index > last_date[day], 'pnl_theo'].mean() * 1e4, 0) for day in days}

                def select_pnl(day):
                    val = pnl.loc[pnl.index > last_date[day], 'pnl_theo']
                    expo = pnl.loc[pnl.index > last_date[day], label]
                    return (val / expo).replace([np.inf, -np.inf], np.nan).sum(skipna=True).sum()
                last_cum = {day: round(select_pnl(day), 4) for day in days}
                for day, value in last.items():
                    if np.isnan(value) or np.isinf(value):
                        last[day] = 0
                for day, value in last_cum.items():
                    if np.isnan(value) or np.isinf(value):
                        last_cum[day] = 0

                pnl_dict = {'mean_theo': {f'{day:03d}d': last[day] for day in days},
                            'sum_theo': {f'{day:03d}d': last_cum[day] for day in days}}
            except:
                pnl_dict = {'mean_theo': {f'{day:03d}d': 0 for day in days},
                            'sum_theo': {f'{day:03d}d': 0 for day in days}}
                logging.error(f'Error in pnl file {pnl_file} for strat {strategy_name}')

            try:
                aum = await read_aum_file(aum_file, exchange)
                aum['deltat'] = (aum['date'] - aum['date'].shift()).apply(lambda x: x.total_seconds())
                aum.loc[0, 'deltat'] = np.nan
                aum['diff'] = aum['aum'].diff()
                aum['ref'] = np.nan
                for index in aum[aum['deltat'] == 0].index:
                    aum.loc[index, 'ref'] = aum.loc[index, 'diff']
                for index in aum[aum['deltat'].isna()].index:
                    aum.loc[index, 'ref'] = aum.loc[index, 'aum']
                aum['ref'] = aum['ref'].fillna(0).cumsum()
                aum['pnl'] = aum['diff'] - aum['ref'].diff()
                aum['perf'] = (aum['diff'] - aum['ref'].diff()) / aum['ref']
                aum.set_index('date', inplace=True)  # copie
                aum = aum.loc[~aum.index.duplicated(keep='last')]
                daily = aum['perf'].resample('1d').agg('sum').fillna(0)
                is_live = daily.loc[daily.index > last_date[days[0]]].std() > 0
                if is_live:
                    logging.info(f'{strategy_name} is live')
                    pnl_dict.update({'vol': {},
                                     'apr': {},
                                     'perfcum': {},
                                     'pnlcum': {},
                                     'drawdawn': {}
                                     })
            except:
                is_live = False
                aum = pd.DataFrame()
                daily = pd.Series()
                logging.error(f'Error in aum file {aum_file} for strat {strategy_name}')

            if is_live:
                for day in days:
                    try:
                        last_aum = aum.loc[aum.index > last_date[day]]
                        vol = daily.loc[daily.index > last_date[day]].std() * np.sqrt(365)
                        if np.isnan(vol):
                            vol = 0
                        perfcum = last_aum['perf'].cumsum()
                        pnlcum = last_aum['pnl'].cumsum()
                        xp_max = perfcum.expanding().max()
                        uw = perfcum - xp_max
                        drawdown = uw.expanding().min()
                        if len(perfcum) > 0:
                            days = (last_aum.index[-1] - last_aum.index[0]).days
                            if days > 0:
                                apr = perfcum.iloc[-1] / days * 365
                            else:
                                apr = 0
                            pc = perfcum.iloc[-1]
                            dd = drawdown.iloc[-1]
                            pnc = pnlcum.iloc[-1]
                        else:
                            apr = 0
                            pc = 0
                            dd = 0
                            pnc = 0
                        pnl_dict['pnlcum'].update({f'{day:03d}d': pnc})
                        pnl_dict['vol'].update({f'{day:03d}d': vol})
                        pnl_dict['apr'].update({f'{day:03d}d': apr})
                        pnl_dict['perfcum'].update({f'{day:03d}d': pc})
                        pnl_dict['drawdawn'].update({f'{day:03d}d': dd})
                    except:
                        pnl_dict['pnlcum'].update({f'{day:03d}d': 0})
                        pnl_dict['vol'].update({f'{day:03d}d': 0})
                        pnl_dict['apr'].update({f'{day:03d}d': 0})
                        pnl_dict['perfcum'].update({f'{day:03d}d': 0})
                        pnl_dict['drawdawn'].update({f'{day:03d}d': 0})
                        logging.error(f'Error in aum data for strat {strategy_name} for day {day}')

                    if day == 180:
                        logging.info(f'Generating graph for {strategy_name}')
                        fig, ax = plt.subplots()
                        ax.plot(perfcum.index, perfcum.values)
                        ax.set_xlabel('date')
                        ax.set_ylabel('Cum perf')
                        ax.set_title(f'{exchange}-{strategy_name}')
                        ax.grid()
                        for tick in ax.get_xticklabels():
                            tick.set_rotation(45)
                        ax.legend()
                        tmpfile = BytesIO()
                        fig.savefig(tmpfile, format='png')
                        encoded = base64.b64encode(tmpfile.getvalue()).decode('utf-8')
                        html = f'<html> <img src=\'data:image/png;base64,{encoded}\'></html>'
                        filename = f'temp/{exchange}_{strategy_name}_fig1.html'

                        with open(filename, 'w') as f:
                            f.write(html)

                        plt.close()

                        fig, ax = plt.subplots()
                        ax.plot(pnlcum.index, pnlcum.values)
                        ax.set_xlabel('date')
                        ax.set_ylabel('Cum pnl')
                        ax.set_title(f'{exchange}-{strategy_name}')
                        ax.grid()
                        for tick in ax.get_xticklabels():
                            tick.set_rotation(45)
                        ax.legend()
                        tmpfile = BytesIO()
                        fig.savefig(tmpfile, format='png')
                        encoded = base64.b64encode(tmpfile.getvalue()).decode('utf-8')
                        html = f'<html> <img src=\'data:image/png;base64,{encoded}\'></html>'
                        filename = f'temp/{exchange}_{strategy_name}_fig2.html'

                        with open(filename, 'w') as f:
                            f.write(html)
                        plt.close()

                    elif day == 30:
                        daily = last_aum['perf'].resample('1d').sum()
                        fig, ax = plt.subplots()
                        ax.bar(x=daily.index, height=daily.values, color=daily.apply(lambda x:'red' if x<0 else 'green'))
                        ax.set_xlabel('date')
                        ax.set_ylabel('Daily perf')
                        ax.set_title(f'{exchange}-{strategy_name}')
                        ax.grid()
                        for tick in ax.get_xticklabels():
                            tick.set_rotation(45)
                        tmpfile = BytesIO()
                        fig.savefig(tmpfile, format='png')
                        encoded = base64.b64encode(tmpfile.getvalue()).decode('utf-8')
                        html = f'<html> <img src=\'data:image/png;base64,{encoded}\'></html>'
                        filename = f'temp/{exchange}_{strategy_name}_fig3.html'

                        with open(filename, 'w') as f:
                            f.write(html)
                        plt.close()

            try:
                JSONResponse(pnl_dict)
                if exchange not in self.pnl:
                    self.pnl[exchange] = {strategy_name: pnl_dict}
                else:
                    self.pnl[exchange].update({strategy_name: pnl_dict})
                filename = f'temp/pnldict.json'

                with open(filename, 'w') as myfile:
                    j = json.dumps(self.pnl, indent=4, cls=NpEncoder)
                    print(j, file=myfile)

            except:
                logging.error(f'Error in pnl dict for strat {exchange}:{strategy_name}')
                self.pnl[exchange] = {strategy_name: {}}


    async def update_summary(self, exchange, working_directory, strategy_name, strategy_param):
        try:
            strategy_directory = os.path.join(working_directory, strategy_name)
            persistance_file = os.path.join(strategy_directory, strategy_param['persistance_file'])
            items = ['exchange_trade',
                     'account_trade',
                     'type',
                     'send_orders',
                     'max_total_expo',
                     'nb_short',
                     'nb_long',
                     'pos_matching',
                     'liquidate_unmatched',
                     'set_leverage',
                     'monitor_exec',
                     'use_aum',
                     'allocation',
                     'leverage',
                     'entry',
                     'exit',
                     'lookback',
                     'lookback_short',
                     'signal_vote_size',
                     'signal_vote_majority',
                     ]
            self.summaries[exchange][strategy_name] = {name: strategy_param.get(name, '') for name in items
                                                       if name in strategy_param}

            if os.path.exists(persistance_file):
                async with aiofiles.open(persistance_file, mode='r') as myfile:
                    content = await myfile.read()
                state = json.loads(content)
            else:
                state = {}
            summary_pairs = {}
            summary_coins = {}
            if 'current_pair_info' in state and len(state['current_pair_info']) > 0:
                for pair_name, pair_info in state['current_pair_info'].items():
                    s1, s2 = parse_pair(pair_name)
                    if 'position' not in pair_info:
                        continue
                    if 'in_execution' in pair_info and pair_info['in_execution'] and pair_info['position'] != 0:
                        continue
                    if 'in_execution' in pair_info:
                        if pair_info['in_execution']:
                            if 'target_qty' in pair_info and 'target_price' in pair_info:
                                summary_pairs[pair_name] = {
                                    'in_execution': pair_info['in_execution'],
                                    'target_qty': pair_info['target_qty'],
                                    'target_price': pair_info['target_price']
                                }
                        else:
                            if 'entry_data' in pair_info and 'quantity' in pair_info:
                                if pair_info["entry_data"][2] is None or np.isnan(pair_info["entry_data"][2]):
                                    entry_ts = datetime.fromtimestamp(0)
                                else:
                                    ts = pair_info["entry_data"][2] / 1e9
                                    entry_ts = datetime.fromtimestamp(int(ts), tz=UTC)
                                info1 = {
                                    'ref_price': pair_info['entry_data'][0],
                                    'quantity': pair_info['quantity'][0],
                                    'amount': pair_info['quantity'][0] * pair_info['entry_data'][0],
                                    'entry_ts': f'{entry_ts}'
                                }
                                info2 = {
                                    'ref_price': pair_info['entry_data'][1],
                                    'quantity': pair_info['quantity'][1],
                                    'amount': pair_info['quantity'][1] * pair_info['entry_data'][1],
                                    'entry_ts': f'{entry_ts}'
                                }
                                summary_pairs[pair_name] = {
                                    'in_execution': pair_info['in_execution'],
                                    s1: info1,
                                    s2: info2
                                }
                                summary_coins[s1] = info1
                                summary_coins[s2] = info2
            elif 'current_coin_info' in state:
                for coin, coin_info in state['current_coin_info'].items():
                    if 'position' not in coin_info:
                        continue
                    in_exec = coin_info.get('in_execution', False)
                    quantity = coin_info.get('quantity', 0)
                    position = coin_info.get('position', 0)
                    entry_data = coin_info.get('entry_data', [0, np.nan])
                    summary_coins[coin] = {
                        'in_execution': in_exec}
                    if quantity != 0:
                        if entry_data[1] is None or np.isnan(entry_data[1]):
                            entry_ts = datetime.fromtimestamp(0)
                        else:
                            ts = entry_data[1] / 1e9
                            entry_ts = datetime.fromtimestamp(int(ts), tz=UTC)
                        summary_coins[coin].update({
                            'position': quantity * position,
                            'amount': quantity * position * entry_data[0],
                            'entry_ts': f'{entry_ts}'
                        })
                    else:
                        target_qty = coin_info.get('target_qty', 0)
                        summary_coins[coin].update({'target_position': target_qty,
                                                    'position': 0.0,
                                                    'amount': 0.0,
                                                    'entry_ts': ''
                                                    })

            self.summaries[exchange][strategy_name]['theo'] = {
                'pairs': summary_pairs,
                'coins': summary_coins}
        except Exception as e:
            logging.warning(f'Exception {e.args[0]} during update_summary of account {exchange}.{strategy_name}')

    async def update_account(self, exchange, strategy_name, strategy_param):
        destination = strategy_param['send_orders']
        logging.info('updating account for %s, %s', exchange, strategy_name)
        if exchange not in self.matching:
            self.matching[exchange] = {}
        account = strategy_param['account_trade']

        trade_exchange = strategy_param['exchange_trade']
        try:
            if destination != 'dummy':
                all_positions = await self.get_account_position(trade_exchange, account)
                if 'pose' not in all_positions:
                    logging.info('empty account for %s, %s', exchange, strategy_name)
                    return
                positions = all_positions['pose']
                theo = {coin: pose for coin, pose in self.summaries[exchange][strategy_name]['theo']['coins'].items() if
                        pose != 0}
                current = pd.DataFrame({coin: value['amount'] for coin, value in positions.items()}, index=['current']).T
                theo_pos = pd.DataFrame({coin: value['amount'] for coin, value in theo.items()}, index=['theo']).T
                current_ts = pd.DataFrame({coin: value.get('entry_ts', np.nan) for coin, value in positions.items()},
                                          index=['current_entry_ts']).T
                theo_ts = pd.DataFrame({coin: value['entry_ts'] for coin, value in theo.items()}, index=['theo_ts']).T

                matching = pd.concat([current, theo_pos], axis=1).fillna(0)
                seuil_current = matching['current'].apply(np.abs).max() / 5
                seuil_theo = matching['theo'].apply(np.abs).max() / 5
                logging.info('Thresholds for  %s, %s: %f, %f', exchange, strategy_name, seuil_current, seuil_theo)
                significant = matching[
                    (matching['theo'].apply(np.abs) > seuil_theo) | (matching['current'].apply(np.abs) > seuil_theo)]
                matching = matching.loc[significant.index]
                matching.loc['Total'] = matching.sum(axis=0)
                nLong = current[(current.apply(np.abs) > seuil_current) & (current > 0)].count()['current']
                nShort = current[(current.apply(np.abs) > seuil_current) & (current < 0)].count()['current']
                matching.loc['nLong', 'current'] = nLong
                matching.loc['nShort', 'current'] = nShort
                nLong = theo_pos[(theo_pos.apply(np.abs) > seuil_theo) & (theo_pos > 0)].count()['theo']
                nShort = theo_pos[(theo_pos.apply(np.abs) > seuil_theo) & (theo_pos < 0)].count()['theo']
                matching.loc['nLong', 'theo'] = nLong
                matching.loc['nShort', 'theo'] = nShort

                matching['current_ts'] = current_ts
                matching['theo_ts'] = theo_ts
                if 'USDT total' in all_positions:
                    balance = all_positions['USDT total']
                else:
                    balance = np.nan
                matching.loc['USDT total'] = balance, np.nan, '', ''

                self.matching[exchange].update({strategy_name: matching})
        except Exception as e:
            logging.info(f'Exception {e.args[0]} for account {exchange}.{strategy_name}')


    async def check_running(self):
        message = []
        # checking heartbeat
        for exchange, param_dict in self.session_config.items():
            heartbeat_file = param_dict['session_file']
            age_seconds = last_modif(heartbeat_file)

            if age_seconds is not None:
                if age_seconds > 180:
                    if self.session_config[exchange].get('check_running'):
                        message += [
                        f'Heartbeat {heartbeat_file} of {exchange} unchanged for {int(age_seconds / 60)} minutes']
            else:
                if self.session_config[exchange].get('check_running'):
                    message += [f'No file {heartbeat_file} ']

        return message

    async def check_pnl(self):
        message = []

        for exchange, params in self.echange_configs.items():
            strategies = params['strategy']

            for strategy_name in strategies:
                strategy_param = strategies[strategy_name]
                if strategy_param['active'] and exchange in self.pnl and strategy_name in self.pnl[exchange]:
                    pnl_dict = self.pnl[exchange][strategy_name]
                    pnl_2d = pnl_dict.get('perfcum', {}).get('002d', 0.0)
                    if pnl_2d < -0.05 and self.session_config[exchange].get('check_pnl'):
                        message += [f'2day PnL < -5% for {strategy_name}@{exchange}']
        return message

    async def check_pos(self):
        message = []

        for exchange, accounts in self.matching.items():
            for strat, matching in accounts.items():
                logging.info(f'checking {exchange}:{strat}')
                try:
                    significant_factor = 0.2
                    total_factor = 1
                    theo_pose = matching['theo'].drop(['Total', 'USDT total', 'nLong', 'nShort'])
                    seuil_theo = theo_pose.apply(np.abs).median()
                    significant_theo = theo_pose[theo_pose.apply(np.abs) > (seuil_theo * significant_factor)].index
                    current_pose = matching['current'].drop(['Total', 'USDT total', 'nLong', 'nShort'])
                    current_pose = current_pose[current_pose.apply(np.abs) > 100]
                    seuil_current = current_pose.apply(np.abs).median()
                    significant_current = current_pose[current_pose.apply(np.abs) > (seuil_current * significant_factor)].index
                    significant_theo = set(significant_theo)
                    significant_current = set(significant_current)

                    # checking theo positions
                    n_long = theo_pose[(theo_pose.apply(np.abs) > (significant_factor * seuil_theo)) & (theo_pose > 0)].count()
                    n_short = theo_pose[(theo_pose.apply(np.abs) > (significant_factor * seuil_theo)) & (theo_pose < 0)].count()

                    if n_short != n_long and 'spot' not in exchange:
                        message += [f'{strat}@{exchange}: Theo pos imbalance']

                    if not self.session_config[exchange].get('check_realpose'):
                        continue

                    if np.abs(matching.loc['Total', 'current']) > (seuil_theo * total_factor):
                        message += [f'{strat}@{exchange}: Residual theo expo too large']

                    # checking positions mismatch
                    if significant_theo != significant_current:
                        d = significant_theo.difference(significant_current)
                        if len(d) > 0:
                            message += [f'Discrepancy {strat}@{exchange}: {d} have no pose in exchange account but should']
                        d = significant_current.difference(significant_theo)
                        if len(d) > 0:
                            message += [f'Discrepancy {strat}@{exchange}: {d} have pose in account but not in DB']

                    # test de l'expo
                    if np.abs(matching.loc['Total', 'current']) > (seuil_current * total_factor):
                        message += [f'{strat}@{exchange}: Residual expo too large']
                except Exception as e:
                    logging.error(f'Exception {e.args[0]} during check of {strat}@{exchange}')

        return message

    async def refresh(self):
        logging.info('refreshing')

        task_list = []
        for exchange, params in self.echange_configs.items():
            working_directory = params['working_directory']
            strategies = params['strategy']
            self.summaries[exchange] = {'exchange': params['exchange']}

            for strategy_name in strategies:
                strategy_param = strategies[strategy_name]
                if strategy_param['active']:
                    try:
                        await self.update_summary(exchange, working_directory, strategy_name, strategy_param)
                        await self.update_pnl(exchange, working_directory, strategy_name, strategy_param)
                    except Exception as e:
                        print(f'exception {e.args[0]} for {exchange}/{strategy_name}')
                        print(traceback.format_exc())
                    task_list.append(
                        asyncio.create_task(self.update_account(exchange, strategy_name, strategy_param)))
        await asyncio.gather(*task_list)

    def get_status(self):
        return self.summaries

    def get_pnl(self):
        return self.pnl

    def get_matching(self, exchange, strat):
        if exchange not in self.session_config:
            return f'Exchange not found: try {list(self.session_config.keys())} and strat name'
        if self.matching is None:
            return
        if exchange in self.matching and strat in self.matching[exchange]:
            return self.matching[exchange][strat]
        else:
            return None

    async def get_account_position(self, exchange, account):
        if 'ok' in exchange:
            exchange_name = 'okexfut'
        elif 'bin' in exchange and 'fut' in exchange:
            exchange_name = 'binancefut'
        elif 'bin' in exchange:
            exchange_name = 'binance'
        elif 'byb' in exchange:
            exchange_name = 'bybit'
        elif 'bitget' in exchange:
            exchange_name = 'bitget'
        else:
            return {}
        params = {
            'exchange_trade': exchange_name,
            'account_trade': account
        }
        end_point = BrokerHandler.build_end_point(exchange_name, account)
        bh = BrokerHandler(market_watch=exchange_name, end_point_trade=end_point, strategy_param=params, logger_name='default')
        try:
            positions = await end_point.get_positions_async()
        except Exception as e:
            logging.warning(f'exchange/account {exchange_name}/{account} sent exception {e.args}')
            positions = {}

        quotes = {}

        for coin in positions:
            symbol = bh.symbol_to_market_with_factor(coin)[0]
            ticker = await end_point._exchange_async.fetch_ticker(symbol)
            if 'last' in ticker:
                price = ticker['last']
            else:
                price = None
            quotes[symbol] = price

        cash = await end_point.get_cash_async(['USDT', 'BTC'])

        await end_point._exchange_async.close()
        await bh.close_exchange_async()

        response = {}
        for coin, info in positions.items():
            symbol = bh.symbol_to_market_with_factor(coin)[0]
            price = info[2]
            if price is None or price == np.nan:
                price = quotes.get(symbol)
            amount = info[1]
            if amount is None or amount == np.nan:
                amount = info[0] * price

            response[symbol] = {
                'entry_ts': f'{info[3]}',
                'quantity': info[0],
                'ref_price': price,
                'amount': amount
            }
        response_df = pd.DataFrame(response).T
        if len(response_df) > 0:
            response_df.sort_values(by='amount', inplace=True)
        response = {'pose': response_df.T.to_dict()}
        response.update({'USDT free': cash[0], 'USDT total': cash[1]})


        return response

    async def multiply(self, exchange, account, factor):
        positions = await self.get_account_position(exchange, account)
        if 'fut' in exchange and 'bin' in exchange:
            exchange_trade = 'binancefut'
            exclude = []
        elif 'bitget' in exchange:
            exchange_trade = 'bitget'
            exclude = []
        else:
            exchange_trade = 'binance'
            exclude = ['BNB', 'BNBUSDT']

        params = {
            'exchange_trade': exchange_trade,
            'account_trade': account
        }
        end_point = BrokerHandler.build_end_point(exchange_trade, account)
        bh = BrokerHandler(market_watch=exchange_trade, end_point_trade=end_point, strategy_param=params, logger_name='default')
        broker = WebSpreaderBroker(market=exchange, account=account, broker_handler=bh)
        response = ''
        if np.abs(factor - 1) < 0.05:
            return True

        for coin, info in positions['pose'].items():
            if coin in exclude:
                continue
            order_id = broker.get_id
            quantity = info['quantity']

            if quantity == 0:
                continue

            target = (factor - 1) * quantity
            nature = 'n' if factor > 1 else 'x'
            action = -1 if target < 0 else 1
            comment = 'liquidation' if factor < 0.1 else 'multiplication'
            target = np.abs(target)

            response = await broker.send_simple_order(order_id, coin=coin, action=action, price=None, target_quantity=target,
                                       comment=comment, nature=nature,
                                       translate_qty_incontracts=False, use_algo=True)
            await asyncio.sleep(5)

        return response

    def set_queue(self, queue):
        self._event_queue = queue


def runner(event, processor, pace):
    async def run_web_processor():
        uvicorn_error = logging.getLogger("uvicorn.error")
        uvicorn_error.disabled = True
        uvicorn_access = logging.getLogger("uvicorn.access")
        uvicorn_access.disabled = True
        global app
        app = FastAPI()

        # app.mount('/static', StaticFiles(directory='static', html=True), name='static')

        @app.get('/pose')
        async def read_position(exchange: str = 'okx', account: str = '1'):
            if exchange not in WebSpreaderBroker.ACCOUNT_DICT:
                raise HTTPException(status_code=404, detail=f'Exchange not found: '
                                                            f'try {list(WebSpreaderBroker.ACCOUNT_DICT.keys())}')
            if account not in WebSpreaderBroker.ACCOUNT_DICT[exchange]:
                raise HTTPException(status_code=404, detail=f'Account not found: '
                                                            f'try {list(WebSpreaderBroker.ACCOUNT_DICT[exchange].keys())}')
            report = await processor.get_account_position(exchange, account)
            return JSONResponse(report)

        @app.get('/multiply')
        async def multiply(exchange: str = '', account: str = '', factor=1.0):
            if exchange not in WebSpreaderBroker.ACCOUNT_DICT:
                raise HTTPException(status_code=404, detail=f'Exchange not found: '
                                                            f'try {list(WebSpreaderBroker.ACCOUNT_DICT.keys())}')
            if len(account) == 1:
                account = int(account)
            if account not in WebSpreaderBroker.ACCOUNT_DICT[exchange]:
                raise HTTPException(status_code=404, detail=f'Account not found: '
                                                            f'try {list(WebSpreaderBroker.ACCOUNT_DICT[exchange].keys())}')
            factor = float(factor)
            if factor < 0 or factor > 2:
                raise HTTPException(status_code=403, detail='Forbidden value: try between 0 and 2')
            report = await processor.multiply(exchange, account, factor)
            return JSONResponse(report)

        @app.get('/status')
        async def read_status():
            report = processor.get_status()
            return JSONResponse(report)

        @app.get('/pnl')
        async def read_pnl():
            report = processor.get_pnl()
            return JSONResponse(report)

        @app.get('/matching')
        async def read_matching(exchange: str = 'bin', strat: str = 'pairs1_melanion'):
            report = processor.get_matching(exchange, strat)
            if isinstance(report, pd.DataFrame):
                return HTMLResponse(report.to_html(formatters={'entry': lambda x: x.strftime('%d-%m-%Y %H:%M'),
                                                               'theo': lambda x: f'{x:.0f}',
                                                               'current': lambda x: f'{x:.0f}',
                                                               }))
            else:
                if report is not None:
                    return HTMLResponse(report)
                else:
                    return HTMLResponse('N/A')

        config = uvicorn.Config(app, port=14440, host='0.0.0.0', lifespan='on')
        server = uvicorn.Server(config)
        await server.serve()

    async def heartbeat(queue, pace, action):
        while True:
            await asyncio.sleep(pace)
            await queue.put(action)

    async def watch_file_modifications(queue):
        last_mod_times = {exchange: os.path.getmtime(file_path) for exchange, file_path in processor.config_files.items()}
        while True:
            await asyncio.sleep(10)
            for exchange, file_path in processor.config_files.items():
                current_mod_time = os.path.getmtime(file_path)
                if current_mod_time != last_mod_times[exchange]:
                    last_mod_times[exchange] = current_mod_time
                    await queue.put((SignalType.FILE, exchange, file_path))

    async def refresh():
        await processor.refresh()

    async def send_alert(message):
        for error in message:
            TGMessenger.send(error, 'CM')
        logging.info(f'Sent {len(message)} msg')

    async def check(checking_coro):
        messages = await checking_coro

        await send_alert(messages)

    async def main():
        event.loop = asyncio.get_event_loop()
        event.queue = asyncio.Queue()
        task_queue = asyncio.Queue()
        await refresh()
        event.set()
        web_runner = asyncio.create_task(run_web_processor())
        heart_runner = asyncio.create_task(heartbeat(task_queue, pace['REFRESH'],SignalType.REFRESH))
        pnl_runner = asyncio.create_task(heartbeat(task_queue, pace['PNL'], SignalType.PNL))
        pos_runner = asyncio.create_task(heartbeat(task_queue, pace['POS'], SignalType.POS))
        running_runner = asyncio.create_task(heartbeat(task_queue, pace['RUNNING'], SignalType.RUNNING))
        file_watcher = asyncio.create_task(watch_file_modifications(task_queue))

        while True:
            item = await task_queue.get()
            if item == SignalType.STOP:
                task_queue.task_done()
                break
            if item == SignalType.REFRESH:
                task = asyncio.create_task(refresh())
                task.add_done_callback(lambda _: task_queue.task_done())
            elif item == SignalType.PNL:
                task = asyncio.create_task(check(processor.check_pnl()))
                task.add_done_callback(lambda _: task_queue.task_done())
            elif item == SignalType.POS:
                task = asyncio.create_task(check(processor.check_pos()))
                task.add_done_callback(lambda _: task_queue.task_done())
            elif item == SignalType.RUNNING:
                task = asyncio.create_task(check(processor.check_running()))
                task.add_done_callback(lambda _: task_queue.task_done())
            elif isinstance(item, tuple) and item[0] == SignalType.FILE:
                await processor.update_config(item[1], item[2])
                task_queue.task_done()
        web_runner.cancel()
        heart_runner.cancel()
        pnl_runner.cancel()
        pos_runner.cancel()
        running_runner.cancel()
        file_watcher.cancel()
        await task_queue.join()

    asyncio.run(main())


if __name__ == '__main__':
    dotenv.load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="input file", default='')
    args = parser.parse_args()
    config_file = args.config
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    if config_file != '':
        with open(config_file, 'r') as myfile:
            config = yaml.load(myfile, Loader=yaml.FullLoader)
    else:
        config = {}

    pace = config.get('pace', {'REFRESH': 180, 'PNL': 1800, 'POS': 600, 'RUNNING': 300})
    started = threading.Event()
    processor = Processor(config)
    th = threading.Thread(target=runner, args=(started, processor, pace,))
    logging.info('Starting')
    th.start()
    started.wait()
    logging.info('Started')
    th.join()
    logging.info('Stopped')