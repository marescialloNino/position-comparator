import os
import json
import aiofiles
import asyncio
import sys
import base64
import yaml
from io import StringIO, BytesIO
from datetime import datetime, timedelta, timezone as UTC
import threading
import logging
from logging.handlers import TimedRotatingFileHandler
import argparse
import traceback
from fastapi import FastAPI, HTTPException
from starlette.responses import JSONResponse, HTMLResponse
import uvicorn
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import aiohttp
from data_analyzer.aggregate_strategies import aggregate_theo_positions
from datafeed.utils_online import NpEncoder, parse_pair, utc_ize, today_utc
from datafeed.broker_handler import BrokerHandler
from web_broker import WebSpreaderBroker
from reporting.bot_reporting import TGMessenger
from data_analyzer.position_comparator import compare_positions
from enum import Enum
from datetime import datetime, timedelta

# Set up module-level logger
logger = logging.getLogger(__name__)
fmt = logging.Formatter('{asctime}:{levelname}:{name}:{message}', style='{')
handler = TimedRotatingFileHandler(filename='output/web_processor.log',
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
                                 ('MULTI_POS', 'check_multistrategy_position')])

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
        self.session_configs = {}
        for session, config_file in self.config_files.items():
            if os.path.exists(config_file):
                with open(config_file, 'r') as myfile:
                    params = json.load(myfile)
                    self.session_configs[session] = params
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
                logger.info(f'Getting account positions for {trade_exchange}_{account}')
                trade_account_dir = os.path.join(working_directory, account_key)
                actual_pos_file = os.path.join(trade_account_dir, 'current_state.pos')
                if os.path.exists(actual_pos_file):
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
                            logger.info(f'Parsed positions for {account_key}: {pos_data["pose"]}')
                        except Exception as e:
                            logger.error(f'Error parsing {actual_pos_file}: {e}')
                            self.account_positions_from_bot[session][account_key] = {'pose': {}}
                else:
                    logger.warning(f'No current_state.pos for {account_key}')
                    self.account_positions_from_bot[session][account_key] = {'pose': {}}
                strategy_positions = []
                for strategy_name, (strat_exchange, strat_account) in accounts.items():
                    if strat_account == account and strat_exchange == trade_exchange:
                        strategy_dir = os.path.join(working_directory, strategy_name)
                        state_file = os.path.join(strategy_dir, 'current_state.json')
                        if os.path.exists(state_file):
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
                                    logger.error(f'Error reading {state_file}: {e}')
                                    strategy_positions.append({})
                        else:
                            logger.warning(f'No current_state.json for {strategy_name} in {strategy_dir}')
                            strategy_positions.append({})
                logger.info(f'strategy_positions for {account_key}: type={type(strategy_positions)}')
                try:
                    aggregated_theo = await aggregate_theo_positions(strategy_positions)
                    self.account_positions_theo_from_bot[session][account_key] = aggregated_theo
                    logger.info(f'Aggregated positions for {account_key}: {aggregated_theo}')
                except Exception as e:
                    logger.error(f'Error aggregating theoretical positions for {account_key}: {e}')
                    self.account_positions_theo_from_bot[session][account_key] = {}
        return

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

    async def check_multistrategy_position(self):
        """Compare aggregated theoretical and actual positions for all accounts, return JSON for web app."""
        self.multistrategy_matching = {}
        messages = []
        for session in self.account_positions_theo_from_bot:
            for session_key in self.account_positions_theo_from_bot[session]:
                try:
                    theo_positions = self.account_positions_theo_from_bot.get(session, {}).get(session_key, {}).get('pose', {})
                    real_positions = self.account_positions_from_bot.get(session, {}).get(session_key, {}).get('pose', {})
                    result = compare_positions(theo_positions, real_positions, session_key)
                    self.multistrategy_matching[session_key] = result
                    logger.info(f"Multistrategy position comparison for {session}:{session_key}: {result}")
                    for token, data in result.items():
                        if not data['matching']:
                            messages.append(
                                f"Alert: Asset {token} in {session_key} has quantity mismatch. "
                                f"Theoretical qty: {data['theo_qty']}, Real qty: {data['real_qty']}"
                                f"{', executing: True' if data['executing'] else ''}."
                            )
                except Exception as e:
                    logger.error(f'Exception {e} during multistrategy position comparison for {session}:{session_key}')
                    self.multistrategy_matching[session_key] = {}
        return self.multistrategy_matching, messages

    async def fetch_token_prices(self, exchange, tokens):
        """Fetch current prices for a list of tokens from the exchange."""
        logger.info(f"Fetching prices for tokens {tokens} on exchange {exchange}")
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
            logger.warning(f"Unknown exchange {exchange}")
            return {token: None for token in tokens}
        
        params = {'exchange_trade': 'dummy', 'account_trade': 'dummy'}
        end_point = BrokerHandler.build_end_point(exchange_name, 'dummy')
        bh = BrokerHandler(market_watch=exchange_name, end_point_trade=end_point, strategy_param=params, logger_name='default')
        
        prices = {token: None for token in tokens}
        try:
            symbol_map = {token: token for token in tokens}
            logger.info(f"Symbol map: {symbol_map}")
            async def fetch_ticker(symbol):
                try:
                    ticker = await end_point._exchange_async.fetch_ticker(symbol)
                    logger.info(f"Raw ticker for {symbol}: {ticker}")
                    return symbol, ticker.get('last', None)
                except Exception as e:
                    logger.warning(f"Error fetching price for {symbol} on {exchange_name}: {str(e)}")
                    return symbol, None
            
            tasks = [fetch_ticker(symbol) for symbol in set(symbol_map.values())]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            logger.info(f"Fetch ticker results: {results}")
            
            symbol_prices = {symbol: price for symbol, price in results if not isinstance(price, Exception)}
            for token, symbol in symbol_map.items():
                price = symbol_prices.get(symbol)
                prices[token] = price
                if price is not None:
                    logger.info(f"Fetched price for {token} ({symbol}) on {exchange_name}: {price}")
                else:
                    logger.warning(f"No 'last' price in ticker for {token} ({symbol}) on {exchange_name}")
            
            await end_point._exchange_async.close()
            await bh.close_exchange_async()
            return prices
        except Exception as e:
            logger.error(f"Error fetching prices for tokens on {exchange_name}: {str(e)}")
            logger.error(traceback.format_exc())
            await end_point._exchange_async.close()
            await bh.close_exchange_async()
            return prices

    async def get_multistrategy_position_details(self, session, account_key):
        """Fetch detailed position data with prices and strategy counts for visualization."""
        logger.info(f"Starting get_multistrategy_position_details for {session}:{account_key}")
        try:
            # Get aggregated theoretical and real positions
            theo_positions = self.account_positions_theo_from_bot.get(session, {}).get(account_key, {}).get('pose', {})
            real_positions = self.account_positions_from_bot.get(session, {}).get(account_key, {}).get('pose', {})
            logger.info(f"Theo positions: {theo_positions}")
            logger.info(f"Real positions: {real_positions}")
            
            # Count strategies contributing to each token
            strategy_counts = {}
            working_directory = self.session_configs[session]['working_directory']
            logger.info(f"Working directory: {working_directory}")
            for strategy_name, (strat_exchange, strat_account) in self.used_accounts[session].items():
                if f"{strat_exchange}_{strat_account}" == account_key:
                    strategy_dir = os.path.join(working_directory, strategy_name)
                    state_file = os.path.join(strategy_dir, 'current_state.json')
                    logger.info(f"Checking state file: {state_file}")
                    if os.path.exists(state_file):
                        async with aiofiles.open(state_file, 'r') as f:
                            try:
                                content = await f.read()
                                state = json.loads(content)
                                logger.info(f"Read state for {strategy_name}: {state.keys()}")
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
            
            # Fetch current prices for all tokens
            exchange, _ = account_key.split('_')
            tokens = set(list(theo_positions.keys()) + list(real_positions.keys()))
            logger.info(f"Fetching prices for tokens: {tokens}")
            quotes = await self.fetch_token_prices(exchange, list(tokens))
            logger.info(f"Fetched quotes: {quotes}")

            # Build detailed result with amounts
            result = {}
            for token in tokens:
                theo_qty = theo_positions.get(token, {}).get('quantity', 0.0)
                real_qty = real_positions.get(token, {}).get('quantity', 0.0)
                price = quotes.get(token, None)
                theo_amount = theo_qty * price if price is not None else None
                real_amount = real_qty * price if price is not None else None
                matching = abs(theo_qty - real_qty) < 1e-6
                result[token] = {
                    'theo_amount': theo_amount,
                    'real_amount': real_amount,
                    'ref_price': price,
                    'executing': False,
                    'matching': matching,
                    'strategy_count': strategy_counts.get(token, 0)
                }
            logger.info(f"Detailed positions for {session}:{account_key}: {result}")
            return result
        except Exception as e:
            logger.error(f"Exception in get_multistrategy_position_details for {session}:{account_key}: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

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

    def get_status(self):
        return self.summaries

    def get_dashboard(self, session):
        return self.dashboard

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

    def get_multistrategy_matching(self):
        return self.multistrategy_matching

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
        @app.get('/multistrategy_matching')
        async def read_multistrategy_matching():
            async with aiohttp.ClientSession() as session:
                report, _ = await processor.check_multistrategy_position()
            return JSONResponse(report)
        @app.get('/multistrategy_position_details')
        async def read_multistrategy_position_details(session: str = 'bitget', account_key: str = 'bitget_2'):
            async with aiohttp.ClientSession() as http_session:
                report = await processor.get_multistrategy_position_details( session, account_key)
            return JSONResponse(report)
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
        
        await refresh()
        event.set()
        web_runner = asyncio.create_task(run_web_processor())
        heart_runner = asyncio.create_task(heartbeat(task_queue, pace['REFRESH'], SignalType.REFRESH))
        pnl_runner = asyncio.create_task(heartbeat(task_queue, pace['PNL'], SignalType.PNL))
        pos_runner = asyncio.create_task(heartbeat(task_queue, pace['POS'], SignalType.POS))
        running_runner = asyncio.create_task(heartbeat(task_queue, pace['RUNNING'], SignalType.RUNNING))
        multi_runner = asyncio.create_task(heartbeat(task_queue, pace.get('MULTI', 30), SignalType.MULTI))
        multi_pos_runner = asyncio.create_task(heartbeat(task_queue, pace.get('MULTI_POS', 30), SignalType.MULTI_POS))
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
    pace = config.get('pace', {'REFRESH': 180, 'PNL': 1800, 'POS': 600, 'RUNNING': 300, 'MULTI': 30, 'MULTI_POS': 30})
    started = threading.Event()
    processor = Processor(config)
    th = threading.Thread(target=runner, args=(started, processor, pace,))
    logger.info('Starting')
    th.start()
    started.wait()
    logger.info('Started')
    th.join()
    logger.info('Stopped')