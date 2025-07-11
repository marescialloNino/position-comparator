from fastapi import FastAPI, HTTPException
from pathlib import Path
from datetime import datetime, timedelta, timezone as UTC
import json
import pandas as pd
import time
import sys
import traceback
import matplotlib.pyplot as plt
import aiofiles
import asyncio
import os
import yaml
import base64
from io import BytesIO, StringIO
import logging
from logging.handlers import TimedRotatingFileHandler
import threading
import argparse
from dotenv import load_dotenv
import numpy as np
import aiohttp
from starlette.responses import JSONResponse, HTMLResponse
import uvicorn
from enum import Enum
from data_analyzer.aggregate_strategies import aggregate_theo_positions
from datafeed.utils_online import NpEncoder, parse_pair, utc_ize, today_utc
from datafeed.broker_handler import BrokerHandler
from web_broker import WebSpreaderBroker
from reporting.bot_reporting import TGMessenger
from data_analyzer.position_comparator import compare_positions
from paths import LOG_DIR

# Set up module-level logger
logger = logging.getLogger(__name__)
fmt = logging.Formatter('{asctime}:{levelname}:{name}:{message}', style='{')
handler = TimedRotatingFileHandler(filename=LOG_DIR  / 'web_processor.log',
                                   when="midnight", interval=1, backupCount=7)
handler.setFormatter(fmt)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

global app

SignalType = Enum('SignalType', [('STOP', 'stop'),
                                 ('REFRESH', 'refresh'),
                                 ('PNL', 'check_pnl'),
                                 ('POS', 'check_pos'),
                                 ('RUNNING', 'check_running'),
                                 ('FILE', 'update_file'),
                                 ('MULTI', 'update_account_multi'),
                                 ('MULTI_POS', 'check_multistrategy_position'),
                                 ('PRICE_UPDATE', 'update_prices')])

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
    def __init__(self, config):
        self.session_config = config['session']
        self.config_files = {session: self.session_config[session]['config_file'] for session in self.session_config}      
        self.config_position_matching_files = {session: self.session_config[session]['config_position_matching_file'] for session in self.session_config}
        self.session_configs = {}
        for session, config_file in self.config_files.items():
            if os.path.exists(config_file):
                with open(config_file, 'r') as myfile:
                    params = json.load(myfile)
                    self.session_configs[session] = params
            else:
                logger.warning(f'No config file for {session} with name {config_file}')

        self.session_matching_configs={}
        for session, config_matching_file in self.config_position_matching_files.items():
            if os.path.exists(config_matching_file):
                with open(config_matching_file, 'r') as myfile:
                    params = json.load(myfile)
                    self.session_matching_configs[session] = params
            else:
                logger.warning(f'No config file for {session} with name {config_file}')

        self.aum = {}
        self.strategy_state = {}
        self.summaries = {}
        self.pnl = {}
        self.account_positions_from_bot = {}
        self.account_positions_theo_from_bot = {}
        self.matching = {}
        self.multistrategy_matching = {}
        self.dashboard = {}
        self.last_message_count = 0
        self.last_message = []
        self.used_accounts = {}
        self.last_running_alert = {}  # Cache for last sent running alerts
        self.price_cache = {}  # {exchange: {token: {'price': float, 'timestamp': datetime}}}
        self.median_position_sizes = {}  # {exchange: float}
        self.last_price_update = {}  # {exchange: datetime}
        self.mismatch_counts = {}  # {session_key: {asset: int}} for tracking consecutive mismatches
        self._update_accounts_by_strat()

    def _update_accounts_by_strat(self):
        try:
            for session, session_params in self.session_configs.items():
                strategies = session_params['strategy']
                logger.info(f'Adding strategies for {session} session')
                for strategy_name, strategy_param in strategies.items():
                    logger.info(f'Adding strategy {strategy_name}')
                    strat_account = strategy_param['account_trade']
                    strat_exchange = strategy_param.get('exchange_trade', '')
                    active = strategy_param.get('active', False)
                    destination = strategy_param.get('send_orders', 'dummy')
                    if not active or destination == 'dummy':
                        continue
                    if session not in self.used_accounts:
                        self.used_accounts[session] = {}
                    self.used_accounts[session][strategy_name] = (strat_exchange, strat_account)
        except Exception as e:
            logger.error(f'Error updating accounts by strat: {e.args[0]}')
            logger.error(traceback.format_exc())
            self.used_accounts = {}
        logger.info(f'Built account dict {self.used_accounts}')

    async def update_account_multi(self):
        account_list = {}
        for session, accounts in self.used_accounts.items():
            logger.info(f'Updating account positions for {session}')
            if session not in account_list:
                account_list[session] = []
            for strategy_name, (trade_exchange, account) in accounts.items():
                if (trade_exchange, account) not in account_list[session]:
                    account_list[session].append((trade_exchange, account))
            if session not in self.account_positions_from_bot:
                self.account_positions_from_bot[session] = {}
            if session not in self.account_positions_theo_from_bot:
                self.account_positions_theo_from_bot[session] = {}
            working_directory = self.session_configs[session]['working_directory']
            for (trade_exchange, account) in account_list[session]:
                account_key = '_'.join((trade_exchange, account))
                logger.info(f'Getting account positions for {account_key}')
                trade_account_dir = os.path.join(working_directory, account_key)
                print(f'Checking positions for {account_key} in {trade_account_dir}')
                actual_pos_file = os.path.join(trade_account_dir, 'current_state.pos')
                if not os.path.exists(actual_pos_file):
                    logger.warning(f'Position file {actual_pos_file} does not exist')
                    self.account_positions_from_bot[session][account_key] = {'pose': {}}
                    continue
                async with aiofiles.open(actual_pos_file, 'r') as myfile:
                    lines = await myfile.readlines()
                    pos_str = lines[-1].strip() if lines else ''
                    try:
                        pos_data = {'pose': {}}
                        if pos_str:
                            parts = pos_str.split(';')
                            if len(parts) < 2:
                                raise ValueError("Invalid format: missing semicolon")
                            data_str = parts[1]
                            pairs = data_str.split(', ')
                            for pair in pairs:
                                key, value = pair.split(':')
                                key = key.strip("'")
                                if key in ['equity', 'imbalance']:
                                    continue
                                pos_data['pose'][key] = {'quantity': float(value)}
                        self.account_positions_from_bot[session][account_key] = pos_data
                        logger.info(f'Parsed exchange positions for {account_key} from bot')
                    except Exception as e:
                        logger.error(f'Error parsing exchange positions from {actual_pos_file}: {str(e)}')
                        self.account_positions_from_bot[session][account_key] = {'pose': {}}
                strategy_positions = []
                for strategy_name, (strat_exchange, strat_account) in accounts.items():
                    if strat_account == account and strat_exchange == trade_exchange:
                        strategy_dir = os.path.join(working_directory, strategy_name)
                        state_file = os.path.join(strategy_dir, 'current_state.json')
                        if not os.path.exists(state_file):
                            logger.warning(f'State file {state_file} does not exist')
                            strategy_positions.append({})
                            continue
                        async with aiofiles.open(state_file, 'r') as f:
                            try:
                                content = await f.read()
                                state = json.loads(content)
                                if not isinstance(state, dict):
                                    logger.error(f'Invalid JSON in {state_file}: expected dict, got {type(state)}')
                                    strategy_positions.append({})
                                else:
                                    strategy_positions.append(state)
                                    logger.info(f'Read valid current_state.json for {strategy_name}')
                            except Exception as e:
                                logger.error(f'Error reading {state_file}: {str(e)}')
                                strategy_positions.append({})
                try:
                    aggregated_theo = await aggregate_theo_positions(strategy_positions)
                    self.account_positions_theo_from_bot[session][account_key] = aggregated_theo
                    logger.info(f'Aggregated positions for {account_key}')
                except Exception as e:
                    logger.error(f'Error aggregating theoretical positions for {account_key}: {str(e)}')
                    self.account_positions_theo_from_bot[session][account_key] = {}

    async def update_pnl(self, exchange, working_directory, strategy_name, strategy_param):
        logger.info('updating pnl for %s, %s', exchange, strategy_name)
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
                logger.error(f'Error in pnl file {pnl_file} for strat {strategy_name}')
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
                aum.set_index('date', inplace=True)
                aum = aum.loc[~aum.index.duplicated(keep='last')]
                daily = aum['perf'].resample('1d').agg('sum').fillna(0)
                is_live = daily.loc[daily.index > last_date[days[0]]].std() > 0
                if is_live:
                    logger.info(f'{strategy_name} is live')
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
                logger.error(f'Error in aum file {aum_file} for strat {strategy_name}')
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
                        logger.error(f'Error in aum data for strat {strategy_name} for day {day}')
                    if day == 180:
                        logger.info(f'Generating graph for {strategy_name}')
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
                        ax.legend()
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
                logger.error(f'Error in pnl dict for strat {exchange}:{strategy_name}')
                self.pnl[exchange] = {strategy_name: {}}

    async def update_summary(self, exchange, working_directory, strategy_name, strategy_param):
        try:
            strategy_directory = os.path.join(working_directory, strategy_name)
            persistence_file = os.path.join(strategy_directory, strategy_param['persistence_file'])
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
                     'signal_vote_majority']
            self.summaries[exchange] = self.summaries.get(exchange, {'exchange': exchange})
            self.summaries[exchange][strategy_name] = {name: strategy_param.get(name, '') for name in items
                                                       if name in strategy_param}
            if os.path.exists(persistence_file):
                async with aiofiles.open(persistence_file, mode='r') as myfile:
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
            logger.warning(f'Exception {e.args[0]} during update_summary of account {exchange}.{strategy_name}')

    async def refresh(self):
        logger.info('refreshing')
        for exchange, params in self.session_configs.items():
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
                        logger.error(f'exception {e.args[0]} for {exchange}/{strategy_name}')
                        logger.error(traceback.format_exc())
        await self.build_dashboard()

    async def update_account(self, exchange, strategy_name, strategy_param):
        destination = strategy_param['send_orders']
        logger.info('updating account for %s, %s', exchange, strategy_name)
        if exchange not in self.matching:
            self.matching[exchange] = {}
        account = strategy_param['account_trade']
        trade_exchange = strategy_param['exchange_trade']
        try:
            if destination != 'dummy':
                all_positions = await self.get_account_position(trade_exchange, account)
                if 'pose' not in all_positions:
                    logger.info('empty account for %s, %s', exchange, strategy_name)
                    return
                positions = all_positions['pose']
                theo = {coin: pose for coin, pose in self.summaries[exchange][strategy_name]['theo']['coins'].items() if pose != 0}
                current = pd.DataFrame({coin: value['amount'] for coin, value in positions.items()}, index=['current']).T
                theo_pos = pd.DataFrame({coin: value['amount'] for coin, value in theo.items()}, index=['theo']).T
                current_ts = pd.DataFrame({coin: value.get('entry_ts', np.nan) for coin, value in positions.items()}, index=['current_entry_ts']).T
                theo_ts = pd.DataFrame({coin: value['entry_ts'] for coin, value in theo.items()}, index=['theo_ts']).T
                matching = pd.concat([current, theo_pos], axis=1).fillna(0)
                seuil_current = matching['current'].apply(np.abs).max() / 5
                seuil_theo = matching['theo'].apply(np.abs).max() / 5
                logger.info('Thresholds for %s, %s: %f, %f', exchange, strategy_name, seuil_current, seuil_theo)
                significant = matching[(matching['theo'].apply(np.abs) > seuil_theo) | (matching['current'].apply(np.abs) > seuil_theo)]
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
                    amount = all_positions['USDT total']
                else:
                    amount = np.nan
                matching.loc['USDT total'] = amount, np.nan, '', ''
                self.matching[exchange].update({strategy_name: matching})
        except Exception as e:
            logger.info(f'Exception {e.args[0]} for account {exchange}.{strategy_name}')

    async def check_running(self):
        message = []
        current_time = datetime.utcnow()
        for exchange, param_dict in self.session_config.items():
            heartbeat_file = param_dict['session_file']
            age_seconds = last_modif(heartbeat_file)
            alert_key = f"{exchange}:{heartbeat_file}"
            last_alert_time = self.last_running_alert.get(alert_key, datetime.min)
            if (current_time - last_alert_time).total_seconds() < 300:
                logger.info(f"Skipping duplicate running alert for {alert_key}")
                continue
            if age_seconds is not None:
                if age_seconds > 180:
                    if self.session_config[exchange].get('check_running'):
                        msg = f'Heartbeat {heartbeat_file} of {exchange} unchanged for {int(age_seconds / 60)} minutes'
                        message.append(msg)
                        self.last_running_alert[alert_key] = current_time
            else:
                if self.session_config[exchange].get('check_running'):
                    msg = f'No file {heartbeat_file} '
                    message.append(msg)
                    self.last_running_alert[alert_key] = current_time
        logger.info(f"check_running returning messages: {message}")
        return message

    async def check_pnl(self):
        message = []
        for exchange, params in self.session_configs.items():
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
        messages = []
        for exchange, accounts in self.matching.items():
            for strat, matching in accounts.items():
                logger.info(f'checking {exchange}:{strat}')
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
                    n_long = theo_pose[(theo_pose.apply(np.abs) > (significant_factor * seuil_theo)) & (theo_pose > 0)].count()
                    n_short = theo_pose[(theo_pose.apply(np.abs) > (significant_factor * seuil_theo)) & (theo_pose < 0)].count()
                    if n_short != n_long and 'spot' not in exchange:
                        messages += [f'{strat}@{exchange}: Theo pos imbalance']
                    if not self.session_config[exchange].get('check_realpose'):
                        continue
                    if np.abs(matching.loc['Total', 'current']) > (seuil_theo * total_factor):
                        messages += [f'{strat}@{exchange}: Residual theo expo too large']
                    if significant_theo != significant_current:
                        d = significant_theo.difference(significant_current)
                        if len(d) > 0:
                            messages += [f'Discrepancy {strat}@{exchange}: {d} have no pose in exchange account but should']
                        d = significant_current.difference(significant_theo)
                        if len(d) > 0:
                            messages += [f'Discrepancy {strat}@{exchange}: {d} have pose in account but not in DB']
                    if np.abs(matching.loc['Total', 'current']) > (seuil_current * total_factor):
                        messages += [f'{strat}@{exchange}: Residual expo too large']
                except Exception as e:
                    logger.error(f'Exception {e.args[0]} during check of {strat}@{exchange}')
        return messages

    async def fetch_token_prices(self, exchange_name, tokens):
        """Fetch current prices for a list of tokens from the exchange."""
        logger.info(f"Fetching token prices for {exchange_name}: {tokens}")
        
        params = {'exchange_trade': 'dummy', 'account_trade': 'dummy'}
        end_point = BrokerHandler.build_end_point(exchange_name, 'dummy')
        bh = BrokerHandler(market_watch=exchange_name, end_point_trade=end_point, strategy_param=params, logger_name='default')
        
        prices = {token: None for token in tokens}
        try:
            symbol_map = {token: token for token in tokens}
            async def fetch_ticker(symbol):
                try:
                    ticker = await end_point._exchange_async.fetch_ticker(symbol)
                    return symbol, ticker.get('last', None)
                except Exception as e:
                    logger.warning(f"Error fetching price for {symbol} on {exchange_name}: {str(e)}")
                    return symbol, None
            
            tasks = [fetch_ticker(symbol) for symbol in set(symbol_map.values())]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            symbol_prices = {symbol: price for symbol, price in results if not isinstance(price, Exception)}
            for token, symbol in symbol_map.items():
                price = symbol_prices.get(symbol)
                prices[token] = price
                if price is not None:
                    if exchange_name not in self.price_cache:
                        self.price_cache[exchange_name] = {}
                    self.price_cache[exchange_name][token] = {'price': price, 'timestamp': datetime.utcnow()}
                    
                else:
                    logger.warning(f"No 'last' price in ticker for {token} ({symbol}) on {exchange_name}")
            logger.info(f"Updated price cache for {exchange_name}")
            await end_point._exchange_async.close()
            await bh.close_exchange_async()
            return prices
        except Exception as e:
            logger.error(f"Error fetching prices for tokens on {exchange_name}: {str(e)}")
            logger.error(traceback.format_exc())
            await end_point._exchange_async.close()
            await bh.close_exchange_async()
            return prices

    async def update_prices_and_median(self):
        """Fetch token prices for all tokens in a session at once and compute median position sizes per session_key."""
        logger.info("Updating prices and median position sizes")
        try:
            # Initialize price cache and median position sizes
            self.price_cache = {}
            self.median_position_sizes = {}

            # Iterate over sessions
            for session in self.session_configs:
     
                # Collect all tokens for the session
                tokens = set()
                for session_key in self.account_positions_from_bot.get(session, {}):
                    for token in self.account_positions_from_bot[session].get(session_key, {}).get('pose', {}):
                        tokens.add(token)
                        exchange_trade = session_key.split('_')[0]  
                    if exchange_trade not in self.price_cache:
                        self.price_cache[exchange_trade] = {}
                    # Fetch prices for all tokens at once
                    try:
                        token_prices = await self.fetch_token_prices(exchange_trade, list(tokens))  # Pass list of tokens
                        for token, price in token_prices.items():
                            if price is not None:
                                self.price_cache[exchange_trade][token] = {'price': price, 'timestamp': datetime.utcnow()}
                                logger.debug(f"Fetched price for {exchange_trade}:{token} = {price}")
                            else:
                                logger.warning(f"Failed to fetch price for {exchange_trade}:{token}")
                    except Exception as e:
                        logger.error(f"Error fetching prices for {exchange_trade}: {str(e)}")
                        for token in tokens:
                            self.price_cache[exchange_trade][token] = {'price': None, 'timestamp': datetime.utcnow()}
                            logger.warning(f"Set price to None for {exchange_trade}:{token} due to fetch error")
                               
                    # Compute median position size per session_key
                    for session_key in self.account_positions_from_bot.get(session, {}):
                        amounts = []
                        for token, pos in self.account_positions_from_bot[session].get(session_key, {}).get('pose', {}).items():
                            qty = pos.get('quantity', 0)
                            price = self.price_cache[exchange_trade].get(token, {}).get('price')
                            if price is not None and qty != 0:
                                amount = abs(qty * price)
                                if amount > 100:  # Filter out small amounts
                                    amounts.append(amount)
                    
                        # Calculate median position size
                        if amounts:
                            median_size = np.median(amounts)
                            self.median_position_sizes[session_key] = median_size
                            logger.info(f"Median position size for {session_key}: {median_size:.2f} USD")
                        else:
                            self.median_position_sizes[session_key] = 0
                            logger.warning(f"No valid amounts for median calculation for {session_key}")
                    
                    self.last_price_update[exchange_trade] = datetime.utcnow()
        except Exception as e:
                logger.error(f"Error in update_prices_and_median: {str(e)}")
                logger.error(traceback.format_exc())

    async def get_multistrategy_position_details(self, session, account_key):
        """Fetch detailed position data with prices and strategy counts from self.multistrategy_matching using cached prices."""
        logger.info(f"Starting get_multistrategy_position_details for {session}:{account_key}")
        try:
            # Get precomputed matching data
            matching_data = self.multistrategy_matching.get(session, {}).get(account_key, {})
            if not matching_data:
                logger.warning(f"No matching data for {session}:{account_key}")
                return {}

            # Count strategies contributing to each token
            strategy_counts = {}
            working_directory = self.session_configs[session]['working_directory']
            for strategy_name, (strat_exchange, strat_account) in self.used_accounts[session].items():
                if f"{strat_exchange}_{strat_account}" == account_key:
                    strategy_dir = os.path.join(working_directory, strategy_name)
                    state_file = os.path.join(strategy_dir, 'current_state.json')
                    if os.path.exists(state_file):
                        async with aiofiles.open(state_file, 'r') as f:
                            try:
                                content = await f.read()
                                state = json.loads(content)
                                if 'current_coin_info' in state:
                                    for coin, coin_info in state['current_coin_info'].items():
                                        if coin_info.get('quantity', 0) != 0:
                                            strategy_counts[coin] = strategy_counts.get(coin, 0) + 1
                                elif 'current_pair_info' in state:
                                    for pair_name, pair_info in state['current_pair_info'].items():
                                        s1, s2 = parse_pair(pair_name)
                                        if pair_info.get('quantity', [0, 0])[0] != 0:
                                            strategy_counts[s1] = strategy_counts.get(s1, 0) + 1
                                        if pair_info.get('quantity', [0, 0])[1] != 0:
                                            strategy_counts[s2] = strategy_counts.get(s2, 0) + 1
                            except Exception as e:
                                logger.error(f"Error reading/parsing {state_file}: {str(e)}")
                                logger.error(traceback.format_exc())
                    else:
                        logger.warning(f"State file {state_file} does not exist")

            # Ensure price cache is fresh (not older than 10 minutes)
            exchange = account_key.split('_')[0]
            tokens = list(matching_data.keys())
            current_time = datetime.utcnow()
            prices = {}
            
            # Initialize price cache for exchange if not exists
            if exchange not in self.price_cache:
                self.price_cache[exchange] = {}
            if exchange not in self.last_price_update:
                self.last_price_update[exchange] = datetime.min.replace(tzinfo=UTC)

            # Refresh prices if cache is stale
            if (current_time - self.last_price_update[exchange]).total_seconds() > 600:
                logger.info(f"Price cache for {exchange} is stale, refreshing")
                await self.update_prices_and_median()

            # Use cached prices
            for token in tokens:
                if token in self.price_cache[exchange]:
                    prices[token] = self.price_cache[exchange][token]['price']
                    logger.debug(f"Using cached price for {exchange}:{token} = {prices[token]}")
                else:
                    prices[token] = None
                    logger.warning(f"No cached price available for {exchange}:{token}")

            # Build detailed result with amounts and dust/mismatch status
            result = {}
            median_size = self.median_position_sizes.get(account_key, 0)  # Use session_key instead of exchange
            for token in tokens:
                data = matching_data.get(token, {})
                theo_qty = data.get('theo_qty', 0.0)
                real_qty = data.get('real_qty', 0.0)
                price = prices.get(token)
                theo_amount = theo_qty * price if price is not None else None
                real_amount = real_qty * price if price is not None else None
                strategy_count = strategy_counts.get(token, 0)
                is_dust = data.get('is_dust', False)
                is_mismatch = data.get('is_mismatch', False)
                mismatch_count = data.get('mismatch_count', 0)

                result[token] = {
                    'theo_qty': theo_qty,
                    'real_qty': real_qty,
                    'theo_amount': theo_amount,
                    'real_amount': real_amount,
                    'ref_price': price,
                    'executing': data.get('executing', False),
                    'matching': data.get('matching', True),
                    'strategy_count': strategy_count,
                    'is_dust': is_dust,
                    'is_mismatch': is_mismatch,
                    'mismatch_count': mismatch_count
                }
            return result
        except Exception as e:
            logger.error(f"Exception in get_multistrategy_position_details for {session}:{account_key}: {str(e)}")
            logger.error(traceback.format_exc())
            return {}
        
    async def check_multistrategy_position(self):
        """Compare aggregated theoretical and actual positions for all accounts, update self.multistrategy_matching, return messages."""
        messages = []
        self.multistrategy_matching = {}
        for session in self.account_positions_theo_from_bot:
            matching_threshold = self.session_matching_configs[session]['tolerance_threshold']
            self.multistrategy_matching[session] = {}
            for session_key in self.account_positions_theo_from_bot[session]:
                try:
                    logger.info(f"Processing multistrategy position for {session}:{session_key}")
                    theo_positions = self.account_positions_theo_from_bot.get(session, {}).get(session_key, {}).get('pose', {})
                    real_positions = self.account_positions_from_bot.get(session, {}).get(session_key, {}).get('pose', {})
                    logger.debug(f"Theo positions for {session}:{session_key}: {theo_positions}")
                    logger.debug(f"Real positions for {session}:{session_key}: {real_positions}")
                    if not theo_positions or not real_positions:
                        logger.warning(f"Empty positions for {session}:{session_key} - theo: {theo_positions}, real: {real_positions}")
                        self.multistrategy_matching[session][session_key] = {}
                        continue
                    # Pass self as processor to compare_positions
                    result, position_messages = compare_positions(theo_positions, real_positions, session_key, matching_threshold, processor=self)
                    self.multistrategy_matching[session][session_key] = result
                    logger.debug(f"Comparison result for {session}:{session_key}: {result}")
                    # Filter messages based on mismatch_count >= 3
                    for message in position_messages:
                        # Extract asset and check mismatch_count from result
                        asset = message.split('Asset ')[1].split(' in ')[0]
                        mismatch_count = result.get(asset, {}).get('mismatch_count', 0)
                        if mismatch_count >= 3:
                            messages.append(message)
                except Exception as e:
                    logger.error(f'Exception {e} during multistrategy position comparison for {session}:{session_key}')
                    logger.error(traceback.format_exc())
                    self.multistrategy_matching[session][session_key] = {}
        
        # Send filtered messages via TGMessenger
        for message in messages:
            try:
                response = TGMessenger.send_message(message, 'CM', use_telegram=False)
                if response.get('ok'):
                    logger.info(f"Sent message to CM: {message}")
                else:
                    logger.error(f"Failed to send message to CM: {response}")
            except Exception as e:
                logger.error(f"Error sending message to CM: {e}")
        
        logger.info(f"Updated multistrategy_matching: {self.multistrategy_matching.keys()}")
        return messages

    async def update_current_state(self):
        logger.info(f'Updating strat states')
        for session, accounts in self.used_accounts.items():
            working_directory = self.session_configs[session]['working_directory']
            strategies = self.session_configs[session]['strategy']
            for strategy_name, exchange_account in accounts.items():
                logger.info(f'Updating current state for {strategy_name}')
                strategy_directory = os.path.join(working_directory, strategy_name)
                strategy_param = strategies[strategy_name]
                persistence_file = os.path.join(strategy_directory, strategy_param['persistence_file'])
                if os.path.exists(persistence_file):
                    async with aiofiles.open(persistence_file, mode='r') as myfile:
                        content = await myfile.read()
                    state = json.loads(content)
                else:
                    state = {}
                if session not in self.strategy_state:
                    self.strategy_state[session] = {}
                self.strategy_state[session][strategy_name] = state

    async def get_account_position(self, exchange_name, account):
        
        params = {
            'exchange_trade': 'dummy',
            'account_trade': account
        }
        end_point = BrokerHandler.build_end_point('dummy', account)
        bh = BrokerHandler(market_watch=exchange_name, end_point_trade=end_point, strategy_param=params, logger_name='default')
        try:
            positions = await end_point.get_positions_async()
        except Exception as e:
            logger.warning(f'exchange/account {exchange_name}/{account} sent exception {e.args}')
            positions = {}
        symbols = [bh.symbol_to_market_with_factor(coin)[0] for coin in positions]
        quotes = await self.fetch_token_prices(exchange_name, symbols)
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
                amount = info[0] * price if price is not None else None
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
            'exchange_trade': 'dummy',
            'account_trade': account
        }
        end_point = BrokerHandler.build_end_point('dummy', account)
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

    async def build_dashboard(self):
        logger.info('building dashboard')
        await self.update_account_multi()
        await self.update_current_state()
        self.dashboard = {session:
                              {
                                  "session_config": self.session_configs.get(session, {}),
                                  "session_strategy_states": self.strategy_state.get(session, {}),
                                  "theo_positions_from_bot": self.account_positions_theo_from_bot.get(session, {})
                              } for session in self.session_configs
        }

    async def update_config(self, exchange, config_file):
        if os.path.exists(config_file):
            logger.info(f'Updating {exchange} config {config_file}')
            async with aiofiles.open(config_file, 'r') as myfile:
                try:
                    content = await myfile.read()
                    params = json.loads(content)
                    self.session_configs[exchange] = params
                except Exception as e:
                    logger.error(f'Unreadable config file {config_file}')
        else:
            logger.warning(f'No config file to update for {exchange} with name {config_file}')

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
        @app.get('/dashboard')
        async def read_dashboard(session: str = 'bitget'):
            report = processor.get_dashboard(session=session)
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
        @app.get('/multistrategy_position_details')
        async def read_multistrategy_position_details(session: str = 'bitget', account_key: str = 'bitget_2'):
            report = await processor.get_multistrategy_position_details(session, account_key)
            if not report:
                raise HTTPException(status_code=404, detail=f"No data for {session}:{account_key}")
            return JSONResponse(report)
        @app.get("/multistrat_summary/{account_or_strategy}/{timeframe}")
        async def get_multistrat_summary(account_or_strategy: str, timeframe: str):
            """Endpoint to get PnL summary and plot data for an account or strategy."""
            try:
                # Validate timeframe
                timeframe_days = {"1d": 1, "3d": 3, "7d": 7, "30d": 30, "90d": 90}
                if timeframe not in timeframe_days:
                    raise HTTPException(status_code=400, detail="Invalid timeframe. Use: 1d, 3d, 7d, 30d, 90d")
                days = timeframe_days[timeframe]
                start_time = datetime.utcnow() - timedelta(days=days)
                end_time = datetime.utcnow()

                # Determine if input is account or strategy
                accounts = []
                strategies = []
                for session, session_params in processor.session_configs.items():
                    accounts.extend([str(params['account_trade']) for params in session_params['strategy'].values() if params.get('active', False)])
                    strategies.extend([strat_name for strat_name, params in session_params['strategy'].items() if params.get('active', False)])
                accounts = list(set(accounts))
                is_account = account_or_strategy in accounts
                entity = account_or_strategy
                if not (is_account or account_or_strategy in strategies):
                    raise HTTPException(status_code=404, detail=f"Entity {entity} not found")

                # Load PnL snapshots
                snapshot_dir = Path("C:/Users/Z640/dev/position-comparator/microservice/output_bitget/pnl_snapshots")
                theo_data = []
                real_data = []
                for file in snapshot_dir.glob(f"pnl_*_snapshot_{entity}_*.json"):
                    timestamp_str = file.stem.split('_')[-1]
                    try:
                        timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S").replace(tzinfo=UTC)
                        if start_time <= timestamp <= end_time:
                            async with aiofiles.open(file, 'r') as f:
                                data = await f.read()
                                data = json.loads(data)
                            if "theo" in file.stem:
                                theo_data.append((timestamp, data))
                            else:
                                real_data.append((timestamp, data))
                    except ValueError:
                        logger.warning(f"Invalid timestamp in filename: {file}")
                        continue

                # Aggregate PnL data
                theo_timestamps = []
                theo_realized_pnl = []
                theo_unrealized_pnl = []
                real_timestamps = []
                real_pnl = []

                for ts, data in sorted(theo_data, key=lambda x: x[0]):
                    key = f"account_{entity}" if is_account else entity
                    for ts_str, entry in data.get(key, {}).items():
                        try:
                            ts_dt = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S.%f").replace(tzinfo=UTC)
                            if start_time <= ts_dt <= end_time:
                                theo_timestamps.append(ts_dt)
                                theo_realized_pnl.append(entry.get("portfolio_realized_pnl", 0))
                                theo_unrealized_pnl.append(entry.get("portfolio_unrealized_pnl", 0))
                        except ValueError:
                            logger.warning(f"Invalid timestamp in theo data: {ts_str}")
                            continue

                for ts, data in sorted(real_data, key=lambda x: x[0]):
                    key = f"account_{entity}" if is_account else entity
                    for ts_str, entry in data.get(key, {}).items():
                        try:
                            ts_dt = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S.%f").replace(tzinfo=UTC)
                            if start_time <= ts_dt <= end_time:
                                real_timestamps.append(ts_dt)
                                real_pnl.append(entry.get("cumulative_usd_pnl", 0))
                        except ValueError:
                            logger.warning(f"Invalid timestamp in real data: {ts_str}")
                            continue

                # Compute average PnL by trade (placeholder, replace with your logic)
                avg_pnl = {"normal": 0.0, "account_weighted": 0.0} if is_account else {}

                # Prepare response
                response = {
                    "entity": entity,
                    "is_account": is_account,
                    "timeframe": timeframe,
                    "theo_pnl": {
                        "timestamps": [ts.strftime("%Y-%m-%d %H:%M:%S.%f") for ts in theo_timestamps],
                        "realized_pnl": theo_realized_pnl,
                        "unrealized_pnl": theo_unrealized_pnl
                    },
                    "real_pnl": {
                        "timestamps": [ts.strftime("%Y-%m-%d %H:%M:%S.%f") for ts in real_timestamps],
                        "cumulative_pnl": real_pnl
                    },
                    "avg_pnl": avg_pnl
                }
                return JSONResponse(response)
            except Exception as e:
                logger.error(f"Error in multistrat_summary for {account_or_strategy}: {str(e)}")
                logger.error(traceback.format_exc())
                raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
        ports = [14440, 14441, 14442]
        server = None
        for port in ports:
            try:
                logger.info(f"Attempting to start Uvicorn server on port {port}")
                config = uvicorn.Config(app, port=port, host='0.0.0.0', lifespan='on')
                server = uvicorn.Server(config)
                await server.serve()
                logger.info(f"Uvicorn server started successfully on port {port}")
                break
            except OSError as e:
                if e.errno == 10048:
                    logger.warning(f"Port {port} is already in use: {e}")
                    if port == ports[-1]:
                        logger.error("All ports tried, cannot start server")
                        return
                    continue
                else:
                    logger.error(f"Failed to start server on port {port}: {e}")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error starting server on port {port}: {e}")
                raise
        if server is None:
            logger.error("No available ports to start Uvicorn server")

    async def heartbeat(queue, pace, action):
        while True:
            await asyncio.sleep(pace)
            await queue.put(action)
            logger.info(f"Queued {action} task, queue size: {queue.qsize()}")

    async def watch_file_modifications(queue):
        last_mod_times = {exchange: os.path.getmtime(file_path) for exchange, file_path in processor.config_files.items()}
        while True:
            await asyncio.sleep(10)
            for exchange, file_path in processor.config_files.items():
                current_mod_time = os.path.getmtime(file_path)
                if current_mod_time != last_mod_times[exchange]:
                    last_mod_times[exchange] = current_mod_time
                    await queue.put((SignalType.FILE, exchange, file_path))
                    logger.info(f"Detected config file change for {exchange}: {file_path}")

    async def refresh():
        await processor.refresh()

    def send_alert(messages):
        for message in messages:
            try:
                response = TGMessenger.send_message(message, 'CM', use_telegram=False)
                if response.get('ok'):
                    logger.info(f"Sent message to CM: {message}")
                else:
                    logger.error(f"Failed to send message to CM: {response}")
            except Exception as e:
                logger.error(f"Error sending message to CM: {e}")
        logger.info(f'Sent {len(messages)} msg')

    async def check(checking_coro):
        messages = await checking_coro
        send_alert(messages)

    async def main():
        event.loop = asyncio.get_event_loop()
        event.queue = asyncio.Queue()
        task_queue = asyncio.Queue()
        
        # Ensure initial data population with correct order
        logger.info("Performing initial data population")
        await processor.update_account_multi()  # Step 1: Collect positions
        time.sleep(2)  # Ensure positions are collected before fetching prices
        await processor.update_prices_and_median()  # Step 2: Fetch prices and median sizes
        time.sleep(2)
        await processor.check_multistrategy_position()  # Step 3: Perform position comparison with prices
        
        event.set()
        
        web_runner = asyncio.create_task(run_web_processor())
        heart_runner = asyncio.create_task(heartbeat(task_queue, pace['REFRESH'], SignalType.REFRESH))
        pnl_runner = asyncio.create_task(heartbeat(task_queue, pace['PNL'], SignalType.PNL))
        pos_runner = asyncio.create_task(heartbeat(task_queue, pace['POS'], SignalType.POS))
        running_runner = asyncio.create_task(heartbeat(task_queue, pace['RUNNING'], SignalType.RUNNING))
        multi_runner = asyncio.create_task(heartbeat(task_queue, pace.get('MULTI', 30), SignalType.MULTI))
        multi_pos_runner = asyncio.create_task(heartbeat(task_queue, pace.get('MULTI_POS', 30), SignalType.MULTI_POS))
        price_update_runner = asyncio.create_task(heartbeat(task_queue, pace.get('PRICE_UPDATE', 600), SignalType.PRICE_UPDATE))
        file_watcher = asyncio.create_task(watch_file_modifications(task_queue))
        
        while True:
            logger.info(f"Processing task queue, size: {task_queue.qsize()}")
            item = await task_queue.get()
            logger.info(f"Processing task: {item}")
            try:
                if item == SignalType.STOP:
                    task_queue.task_done()
                    web_runner.cancel()
                    heart_runner.cancel()
                    pnl_runner.cancel()
                    pos_runner.cancel()
                    running_runner.cancel()
                    multi_runner.cancel()
                    multi_pos_runner.cancel()
                    price_update_runner.cancel()
                    file_watcher.cancel()
                    await task_queue.join()
                    break
                elif item == SignalType.REFRESH:
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
                elif item == SignalType.MULTI:
                    task = asyncio.create_task(processor.update_account_multi())
                    task.add_done_callback(lambda _: task_queue.task_done())
                elif item == SignalType.MULTI_POS:
                    task = asyncio.create_task(check(processor.check_multistrategy_position()))
                    task.add_done_callback(lambda _: task_queue.task_done())
                elif item == SignalType.PRICE_UPDATE:
                    task = asyncio.create_task(check(processor.update_prices_and_median()))
                    task.add_done_callback(lambda _: task_queue.task_done())
                elif isinstance(item, tuple) and item[0] == SignalType.FILE:
                    await processor.update_config(item[1], item[2])
                    task_queue.task_done()
            except Exception as e:
                logger.error(f"Error processing task {item}: {str(e)}")
                logger.error(traceback.format_exc())
                task_queue.task_done()

    asyncio.run(main())

if __name__ == '__main__':
    load_dotenv()
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
    pace = config.get('pace', {
        'REFRESH': 600,
        'PNL': 1800,
        'POS': 600,
        'RUNNING': 300,
        'MULTI': 30,
        'MULTI_POS': 30,
        'PRICE_UPDATE': 600
    })
    started = threading.Event()
    processor = Processor(config)
    th = threading.Thread(target=runner, args=(started, processor, pace,))
    logger.info('Starting')
    th.start()
    started.wait()
    logger.info('Started')
    th.join()
    logger.info('Stopped')